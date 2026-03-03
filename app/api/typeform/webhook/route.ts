import { NextResponse } from "next/server";
import crypto from "crypto";
import { neon } from "@neondatabase/serverless";
import { findClient, createClient } from "../../../../lib/mindbody";

export const runtime = "nodejs";

const sql = neon(process.env.DATABASE_URL || "");

const FALLBACK_EMAIL_DOMAIN = "strideautomation.com";
const FALLBACK_PHONE_PREFIX = "555";

function timingSafeEqual(a: string, b: string) {
  const aBuf = Buffer.from(a);
  const bBuf = Buffer.from(b);
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

/**
 * Typeform-Signature: sha256=<base64(hmac_sha256(secret, rawBody))>
 */
async function verifyTypeform(req: Request, rawBody: string) {
  const secret = process.env.TYPEFORM_WEBHOOK_SECRET;
  if (!secret) throw new Error("Missing TYPEFORM_WEBHOOK_SECRET");

  const sigHeader =
    req.headers.get("typeform-signature") ||
    req.headers.get("Typeform-Signature");

  if (sigHeader) {
    const expected = crypto
      .createHmac("sha256", secret)
      .update(rawBody)
      .digest("base64");

    const provided = sigHeader.startsWith("sha256=")
      ? sigHeader.slice("sha256=".length)
      : sigHeader;

    return { ok: timingSafeEqual(provided, expected) };
  }

  // Optional fallback for Postman/manual calls
  const headerSecret = req.headers.get("typeform-secret");
  if (headerSecret && headerSecret === secret) return { ok: true };

  return { ok: false };
}

function normalize(s: string) {
  return s.toLowerCase().trim();
}

/**
 * Unique fallback phone per submission:
 * 555 + 7 digits derived from hash(token)
 */
function makeDummyPhone(token: string) {
  const hex = crypto.createHash("sha256").update(token).digest("hex");
  const digits = hex.replace(/\D/g, "").padEnd(30, "0");
  const last7 = digits.slice(-7);
  return `${FALLBACK_PHONE_PREFIX}${last7}`; // 10 digits
}

/**
 * Unique fallback email per submission:
 * pending+<short>@strideautomation.com
 */
function makeDummyEmail(token: string) {
  const short = crypto.createHash("sha256").update(token).digest("hex").slice(0, 10);
  return `pending+${short}@${FALLBACK_EMAIL_DOMAIN}`;
}

function extractLead(payload: any) {
  const formId = payload?.form_response?.form_id;
  const token = payload?.form_response?.token;

  const answers: any[] = payload?.form_response?.answers ?? [];
  const fields: any[] = payload?.form_response?.definition?.fields ?? [];

  const fieldById = new Map<string, any>();
  for (const f of fields) {
    if (f?.id) fieldById.set(f.id, f);
  }

  const getTitle = (answer: any) => {
    const fieldId = answer?.field?.id;
    const field = fieldId ? fieldById.get(fieldId) : null;
    return (field?.title ?? "").toString();
  };

  // Try refs first (if present)
  const getByRef = (ref: string) => {
    const a = answers.find((x) => x?.field?.ref === ref);
    if (!a) return null;
    if (a.type === "text") return a.text ?? null;
    if (a.type === "email") return a.email ?? null;
    if (a.type === "phone_number") return a.phone_number ?? null;
    return null;
  };

  let firstName = (getByRef("first_name") ?? "").toString().trim();
  let lastName = (getByRef("last_name") ?? "").toString().trim();
  let email = getByRef("email");
  let phone = getByRef("phone");

  // Fallback by field titles (Contact Info blocks)
  if (!firstName) {
    const a = answers.find((x) => normalize(getTitle(x)).includes("first name"));
    if (a?.type === "text") firstName = (a.text ?? "").toString().trim();
  }
  if (!lastName) {
    const a = answers.find((x) => normalize(getTitle(x)).includes("last name"));
    if (a?.type === "text") lastName = (a.text ?? "").toString().trim();
  }

  // Fallback by answer type
  if (!email) {
    const a = answers.find((x) => x?.type === "email");
    email = a?.email ?? null;
  }
  if (!phone) {
    const a = answers.find((x) => x?.type === "phone_number");
    phone = a?.phone_number ?? null;
  }

  return {
    formId,
    token,
    firstName,
    lastName,
    email,
    phone,
    answersCount: answers.length
  };
}

async function ensureTables() {
  if (!process.env.DATABASE_URL) {
    throw new Error("Missing DATABASE_URL. Connect Neon in Vercel to set it.");
  }

  await sql`
    create table if not exists tenants (
      id bigserial primary key,
      form_id text not null unique,
      location_name text,
      site_id integer not null,
      is_active boolean default true,
      created_at timestamptz default now()
    );
  `;

  await sql`
    create table if not exists processed_submissions (
      id bigserial primary key,
      typeform_token text not null unique,
      created_at timestamptz default now()
    );
  `;
}

export async function POST(req: Request) {
  const rawBody = await req.text();

  const verification = await verifyTypeform(req, rawBody);
  if (!verification.ok) {
    return NextResponse.json(
      { ok: false, error: "Unauthorized (Typeform verification failed)" },
      { status: 401 }
    );
  }

  let payload: any;
  try {
    payload = JSON.parse(rawBody);
  } catch {
    return NextResponse.json({ ok: false, error: "Invalid JSON" }, { status: 400 });
  }

  const lead = extractLead(payload);

  // Typeform test can come in empty
  if (!lead.formId || !lead.token || lead.answersCount === 0) {
    return NextResponse.json({ ok: true, status: "typeform_test_ok" });
  }

  if (!lead.firstName || !lead.lastName) {
    return NextResponse.json(
      {
        ok: false,
        error: "Missing firstName or lastName from Typeform payload",
        extracted: {
          firstName: lead.firstName,
          lastName: lead.lastName,
          email: lead.email,
          phone: lead.phone
        }
      },
      { status: 400 }
    );
  }

  await ensureTables();

  // Tenant lookup
  const tenantRes = await sql`
    select form_id, location_name, site_id, is_active
    from tenants
    where form_id = ${lead.formId}
    limit 1
  `;

  const tenant = (tenantRes as any)?.[0];
  if (!tenant || tenant.is_active === false) {
    return NextResponse.json({
      ok: true,
      status: "routed",
      routedTo: null,
      message: "No active tenant for this form_id"
    });
  }

  const siteId = Number(tenant.site_id);

  // Idempotency
  try {
    await sql`
      insert into processed_submissions (typeform_token)
      values (${lead.token})
    `;
  } catch {
    return NextResponse.json({
      ok: true,
      status: "deduped",
      routedTo: { locationName: tenant.location_name, siteId }
    });
  }

  // Unique fallbacks (avoid collisions)
  const normalizedEmail =
    lead.email && lead.email.trim().length > 0 ? lead.email.trim() : makeDummyEmail(lead.token);

  const normalizedPhoneRaw =
    lead.phone && lead.phone.trim().length > 0 ? lead.phone.trim() : makeDummyPhone(lead.token);

  const normalizedPhone = normalizedPhoneRaw.replace(/\D/g, "");

  try {
    const existing = await findClient(siteId, {
      firstName: lead.firstName,
      lastName: lead.lastName,
      email: normalizedEmail,
      phone: normalizedPhone
    });

    if (existing?.Id) {
      return NextResponse.json({
        ok: true,
        status: "exists",
        mbClientId: String(existing.Id),
        routedTo: { locationName: tenant.location_name, siteId },
        fallbacksUsed: {
          emailWasFallback: !lead.email,
          phoneWasFallback: !lead.phone
        }
      });
    }

    const created = await createClient(siteId, {
      firstName: lead.firstName,
      lastName: lead.lastName,
      email: normalizedEmail,
      phone: normalizedPhone
    });

    if (!created?.Id) {
      return NextResponse.json({ ok: false, error: "Mindbody create failed" }, { status: 500 });
    }

    return NextResponse.json({
      ok: true,
      status: "created",
      mbClientId: String(created.Id),
      routedTo: { locationName: tenant.location_name, siteId },
      fallbacksUsed: {
        emailWasFallback: !lead.email,
        phoneWasFallback: !lead.phone
      }
    });
  } catch (err: any) {
    const status = err?.response?.status ?? null;
    const data = err?.response?.data ?? null;
    const where = typeof err?.config?.url === "string" ? err.config.url : null;

    return NextResponse.json(
      {
        ok: false,
        error: err?.message ?? "Server error",
        mindbody: { status, where, data }
      },
      { status: 500 }
    );
  }
}
