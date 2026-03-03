import { NextResponse } from "next/server";
import crypto from "crypto";
import { sql } from "@neondatabase/serverless";
import { findClient, createClient } from "@/lib/mindbody";

export const runtime = "nodejs";

/** constant "pending" domain you wanted */
const PENDING_EMAIL_DOMAIN = "strideautomation.com";

/** Normalize phone to digits only; Mindbody is pretty tolerant if you send digits */
function normalizePhone(raw: string | null | undefined) {
  if (!raw) return "";
  return raw.replace(/\D/g, "");
}

function timingSafeEqual(a: string, b: string) {
  const aBuf = Buffer.from(a);
  const bBuf = Buffer.from(b);
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

/**
 * Typeform signs webhook payloads using HMAC-SHA256 with the secret you set in Typeform UI.
 * Header: Typeform-Signature: sha256=BASE64
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

    if (!timingSafeEqual(provided, expected)) {
      return { ok: false, mode: "signature" as const };
    }
    return { ok: true, mode: "signature" as const };
  }

  // Optional fallback: simple secret header (handy for Postman)
  const headerSecret = req.headers.get("typeform-secret");
  if (headerSecret && headerSecret === secret) {
    return { ok: true, mode: "header" as const };
  }

  return { ok: false, mode: "none" as const };
}

function sha1(input: string) {
  return crypto.createHash("sha1").update(input).digest("hex");
}

/**
 * Generate a unique fallback email per submission so we never collide.
 * ex: pending+ab12cd34@strideautomation.com
 */
function makeFallbackEmail(token: string) {
  const short = sha1(token).slice(0, 8);
  return `pending+${short}@${PENDING_EMAIL_DOMAIN}`;
}

/**
 * Generate a unique fallback phone per submission so we never collide.
 * We'll generate a US-looking 10-digit number in the safe 555 range:
 * 555 + 7 digits from token hash
 * ex: 5551234567
 */
function makeFallbackPhone(token: string) {
  const digits = sha1(token)
    .replace(/[a-f]/g, "") // strip hex letters
    .padEnd(12, "0"); // ensure enough digits
  const last7 = digits.slice(-7);
  return `555${last7}`; // 10 digits total
}

/**
 * Robust extraction:
 * - Works if you used `field.ref` (best)
 * - Also works for Contact Info blocks by:
 *   - looking at answer.type (email / phone_number / text)
 *   - mapping by field title in definition (First name/Last name/etc)
 */
function extractLead(payload: any) {
  const formId = payload?.form_response?.form_id;
  const token = payload?.form_response?.token;

  const answers: any[] = payload?.form_response?.answers ?? [];
  const fields: any[] = payload?.form_response?.definition?.fields ?? [];

  // build map of fieldId -> title (for fallbacks)
  const titleById = new Map<string, string>();
  for (const f of fields) {
    if (f?.id && typeof f?.title === "string") titleById.set(f.id, f.title);
  }

  const getByRef = (ref: string) => {
    const a = answers.find((x) => x?.field?.ref === ref);
    if (!a) return null;
    if (a.type === "text") return a.text ?? null;
    if (a.type === "email") return a.email ?? null;
    if (a.type === "phone_number") return a.phone_number ?? null;
    return null;
  };

  // 1) Try refs first (if you set them)
  let firstName = (getByRef("first_name") ?? "").toString().trim();
  let lastName = (getByRef("last_name") ?? "").toString().trim();
  let email = (getByRef("email") ?? "").toString().trim();
  let phone = (getByRef("phone") ?? "").toString().trim();

  // 2) Fallback by answer.type + title matching (Contact Info blocks)
  if (!email) {
    const a = answers.find((x) => x?.type === "email" && x?.email);
    email = (a?.email ?? "").toString().trim();
  }
  if (!phone) {
    const a = answers.find(
      (x) => x?.type === "phone_number" && x?.phone_number
    );
    phone = (a?.phone_number ?? "").toString().trim();
  }

  // For names, Contact Info usually sends first/last as text fields.
  // We'll use title matching to pick them safely.
  if (!firstName || !lastName) {
    const textAnswers = answers
      .filter((x) => x?.type === "text" && typeof x?.text === "string")
      .map((x) => {
        const fieldId = x?.field?.id as string | undefined;
        const title = fieldId ? titleById.get(fieldId) : "";
        return { title: (title ?? "").toLowerCase(), value: x.text.trim() };
      });

    if (!firstName) {
      const hit =
        textAnswers.find((t) => t.title.includes("first")) ??
        textAnswers.find((t) => t.title.includes("name") && !t.title.includes("last"));
      if (hit?.value) firstName = hit.value;
    }

    if (!lastName) {
      const hit =
        textAnswers.find((t) => t.title.includes("last")) ??
        textAnswers.find((t) => t.title.includes("surname"));
      if (hit?.value) lastName = hit.value;
    }
  }

  return {
    formId,
    token,
    firstName,
    lastName,
    email,
    phone
  };
}

export async function POST(req: Request) {
  const rawBody = await req.text();

  // Verify Typeform webhook
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

  if (!lead.formId || !lead.token) {
    return NextResponse.json(
      { ok: false, error: "Missing form_id or token" },
      { status: 400 }
    );
  }

  // Enforce names (Mindbody requires)
  if (!lead.firstName || !lead.lastName) {
    return NextResponse.json(
      {
        ok: false,
        error: "Missing firstName or lastName from Typeform payload",
        debug: {
          extracted: {
            firstName: lead.firstName,
            lastName: lead.lastName,
            email: lead.email,
            phone: lead.phone
          }
        }
      },
      { status: 400 }
    );
  }

  // Generate unique fallbacks ONLY if missing
  const finalEmail = lead.email && lead.email.length > 0 ? lead.email : makeFallbackEmail(lead.token);
  const finalPhoneRaw = lead.phone && lead.phone.length > 0 ? lead.phone : makeFallbackPhone(lead.token);
  const finalPhone = normalizePhone(finalPhoneRaw);

  // 1) Resolve tenant by form_id
  const tenantRes = await sql`
    select form_id, location_name, site_id, is_active
    from tenants
    where form_id = ${lead.formId}
    limit 1
  `;

  const tenant = tenantRes.rows?.[0] as any;
  if (!tenant || tenant.is_active === false) {
    return NextResponse.json({
      ok: true,
      status: "routed",
      routedTo: null,
      message: "No active tenant for this form_id"
    });
  }

  const siteId = Number(tenant.site_id);

  // 2) Idempotency: prevent double-processing same Typeform token
  await sql`
    create table if not exists processed_submissions (
      id bigserial primary key,
      typeform_token text not null unique,
      created_at timestamptz default now()
    )
  `;

  try {
    await sql`
      insert into processed_submissions (typeform_token)
      values (${lead.token})
    `;
  } catch {
    return NextResponse.json({
      ok: true,
      status: "routed",
      deduped: true,
      routedTo: { locationName: tenant.location_name, siteId }
    });
  }

  // 3) Find or create in Mindbody
  try {
    // Prefer searching by email/phone first (these are most unique)
    const existing = await findClient(siteId, {
      firstName: lead.firstName,
      lastName: lead.lastName,
      email: finalEmail,
      phone: finalPhone
    });

    if (existing?.Id) {
      return NextResponse.json({
        ok: true,
        status: "exists",
        mbClientId: String(existing.Id),
        routedTo: { locationName: tenant.location_name, siteId },
        usedFallbacks: {
          emailWasFallback: finalEmail !== lead.email,
          phoneWasFallback: normalizePhone(lead.phone) !== finalPhone
        }
      });
    }

    const created = await createClient(siteId, {
      firstName: lead.firstName,
      lastName: lead.lastName,
      email: finalEmail,
      phone: finalPhone
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
      routedTo: { locationName: tenant.location_name, siteId },
      usedFallbacks: {
        emailWasFallback: finalEmail !== lead.email,
        phoneWasFallback: normalizePhone(lead.phone) !== finalPhone
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
