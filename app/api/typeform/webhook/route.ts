import { NextResponse } from "next/server";
import { getTenantByFormId, getIdempotency, insertIdempotency } from "../../../../lib/db";

export const runtime = "nodejs";

function verifyTypeformSecret(req: Request) {
  const incoming = req.headers.get("typeform-secret");
  const expected = process.env.TYPEFORM_WEBHOOK_SECRET;
  if (!expected) throw new Error("Missing TYPEFORM_WEBHOOK_SECRET");
  return incoming && incoming === expected;
}

export async function POST(req: Request) {
  try {
    if (!verifyTypeformSecret(req)) {
      return new NextResponse("Unauthorized", { status: 401 });
    }

    const payload = await req.json();

    const formId = payload?.form_response?.form_id;
    const responseId = payload?.form_response?.token; // idempotency key

    if (!formId || !responseId) {
      return NextResponse.json({ ok: false, error: "Missing form_id or token" }, { status: 400 });
    }

    // Idempotency check
    const existing = await getIdempotency(responseId);
    if (existing) {
      return NextResponse.json({
        ok: true,
        status: existing.status,
        mbClientId: existing.mb_client_id ?? null,
        deduped: true
      });
    }

    // Tenant routing
    const tenant = await getTenantByFormId(formId);
    if (!tenant) {
      await insertIdempotency({
        responseId,
        formId,
        status: "failed",
        error: "Unknown or inactive form_id"
      });
      return NextResponse.json({ ok: false, error: "Unknown or inactive form_id" }, { status: 400 });
    }

    // For now: just confirm routing works (Mindbody comes next step)
    await insertIdempotency({
      responseId,
      formId,
      status: "routed"
    });

    return NextResponse.json({
      ok: true,
      status: "routed",
      routedTo: {
        locationName: tenant.location_name ?? null,
        siteId: tenant.site_id
      }
    });
  } catch (err: any) {
    return NextResponse.json({ ok: false, error: err?.message ?? "Server error" }, { status: 500 });
  }
}
