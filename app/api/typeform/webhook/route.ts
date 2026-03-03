import { NextResponse } from "next/server";
import crypto from "crypto";
import { neon } from "@neondatabase/serverless";
import { findClient, createClient } from "../../../../lib/mindbody";

export const runtime = "nodejs";

const sql = neon(process.env.DATABASE_URL || "");

function timingSafeEqual(a: string, b: string) {
  const aBuf = Buffer.from(a);
  const bBuf = Buffer.from(b);
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

/**
 * Typeform Secret validation:
 * Typeform sends "Typeform-Signature: sha256=<base64>"
 * We compute base64(HMAC_SHA256(secret, rawBody)) and compare.
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

    return { ok: timingSafeEqual(provided, expected), mode: "signature" as const };
  }

  // Optional fallback (only if you manually send a header secret)
  const headerSecret = req.headers.get("typeform-secret");
  if (headerSecret && headerSecret === secret) {
    return { ok: true, mode: "header" as const };
  }

  return { ok: false, mode: "none" as const };
}

function extractLead(payload: any) {
  const formId = payload?.form_response?.form_id;
  const token = payload?.form_response?.token;
  const answers: any[] = payload?.form_response?.answers ?? [];

  const getByRef = (ref: string) => {
    const a = answers.find((x) => x?.field?.ref === ref);
    if (!a) return null;
    if (a.type === "text") return a.text ?? null;
    if (a.type === "email") return a.email ?? null;
    if (a.type === "phone_number") return a.phone_number ?? null;
    return null;
  };

  return {
    formId,
    token,
    firstName: (getByRef("first_name") ?? "").toString().trim(),
    lastName: (getByRef("last_name") ?? "").toString().trim(),
    email: getByRef("email"),
    phone: getByRef("phone")
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

  // Verify webhook
  const verification = await verifyTypeform(req, rawBody);
  if (!verification.ok) {
    return NextResponse.json(
      { ok: false, error: "Unauthorized (Typeform verification failed)" },
      { status: 401 }
    );
  }

  // Parse JSON
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

  // Idempotency: insert token, if duplicate then dedupe
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

  // Mindbody sync
  try {
    const existing = await findClient(siteId, {
      firstName: lead.firstName,
      lastName: lead.lastName,
      email: lead.email,
      phone: lead.phone
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
      email: lead.email,
      phone: lead.phone
    });

    if (!created?.Id) {
      return NextResponse.json({ ok: false, error: "Mindbody create failed" }, { status: 500 });
    }

    return NextResponse.json({
      ok: true,
      status: "created",
      mbClientId: String(created.Id),
      routedTo: { locationName: tenant.location_name, siteId }
    });
  } catch (err: any) {
    const status = err?.response?.status ?? null;
    const data = err?.response?.data ?? null;
    const where = typeof err?.config?.url === "string" ? err.config.url : null;

    return NextResponse.json(
      { ok: false, error: err?.message ?? "Server error", mindbody: { status, where, data } },
      { status: 500 }
    );
  }
}
