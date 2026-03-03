import { NextResponse } from "next/server";
import crypto from "crypto";
import { neon } from "@neondatabase/serverless";
import { findClient, createClient } from "../../../../lib/mindbody";

export const runtime = "nodejs";

const sql = neon(process.env.DATABASE_URL || "");

const FALLBACK_PHONE = "5555555555";
const FALLBACK_EMAIL = "pending@strideautomation.com";

function timingSafeEqual(a: string, b: string) {
  const aBuf = Buffer.from(a);
  const bBuf = Buffer.from(b);
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

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

  const headerSecret = req.headers.get("typeform-secret");
  if (headerSecret && headerSecret === secret) return { ok: true };

  return { ok: false };
}

function normalize(s: string) {
  return s.toLowerCase().trim();
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

  let firstName = "";
  let lastName = "";
  let email: string | null = null;
  let phone: string | null = null;

  for (const answer of answers) {
    const title = normalize(getTitle(answer));

    if (answer.type === "email") {
      email = answer.email ?? null;
    }

    if (answer.type === "phone_number") {
      phone = answer.phone_number ?? null;
    }

    if (answer.type === "text") {
      if (!firstName && title.includes("first name")) {
        firstName = answer.text ?? "";
      }

      if (!lastName && title.includes("last name")) {
        lastName = answer.text ?? "";
      }
    }
  }

  return {
    formId,
    token,
    firstName: firstName.trim(),
    lastName: lastName.trim(),
    email,
    phone,
    answersCount: answers.length
  };
}

async function ensureTables() {
  if (!process.env.DATABASE_URL) {
    throw new Error("Missing DATABASE_URL.");
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

  if (!lead.formId || !lead.token || lead.answersCount === 0) {
    return NextResponse.json({
      ok: true,
      status: "typeform_test_ok"
    });
  }

  if (!lead.firstName || !lead.lastName) {
    return NextResponse.json({
      ok: false,
      error: "FirstName and LastName required in Typeform"
    });
  }

  await ensureTables();

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
      routedTo: null
    });
  }

  const siteId = Number(tenant.site_id);

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

  // 🔥 NORMALIZATION SECTION (your requested logic)

  const normalizedEmail =
    lead.email && lead.email.trim().length > 0
      ? lead.email.trim()
      : FALLBACK_EMAIL;

  const normalizedPhone =
    lead.phone && lead.phone.trim().length > 0
      ? lead.phone.replace(/\D/g, "")
      : FALLBACK_PHONE;

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
        routedTo: { locationName: tenant.location_name, siteId }
      });
    }

    const created = await createClient(siteId, {
      firstName: lead.firstName,
      lastName: lead.lastName,
      email: normalizedEmail,
      phone: normalizedPhone
    });

    if (!created?.Id) {
      return NextResponse.json(
        { ok: false, error: "Mindbody create failed" },
        { status: 500 }
      );
    }

    return NextResponse.json({
      ok: true,
      status: "created",
      mbClientId: String(created.Id),
      routedTo: { locationName: tenant.location_name, siteId }
    });
  } catch (err: any) {
    return NextResponse.json(
      {
        ok: false,
        error: err?.message ?? "Server error",
        mindbody: {
          status: err?.response?.status ?? null,
          where: err?.config?.url ?? null,
          data: err?.response?.data ?? null
        }
      },
      { status: 500 }
    );
  }
}
