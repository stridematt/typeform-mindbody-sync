import { NextResponse } from "next/server";
import {
  getTenantByFormId,
  getIdempotency,
  insertIdempotency
} from "../../../../lib/db";

export const runtime = "nodejs";

function verifyTypeformSecret(req: Request) {
  const incoming = req.headers.get("typeform-secret");
  const expected = process.env.TYPEFORM_WEBHOOK_SECRET;

  if (!expected) {
    throw new Error("TYPEFORM_WEBHOOK_SECRET not set");
  }

  return incoming === expected;
}

export async function POST(req: Request) {
  try {
    // Verify webhook secret
    if (!verifyTypeformSecret(req)) {
      return new NextResponse("Unauthorized", { status: 401 });
    }

    const payload = await req.json();

    const formId = payload?.form_response?.form_id;
    const responseId = payload?.form_response?.token;

    if (!formId || !responseId) {
      return NextResponse.json(
        { ok: false, error: "Missing form_id or response token" },
        { status: 400 }
      );
    }

    // Check idempotency (prevent duplicate processing)
    const existing = await getIdempotency(responseId);
    if (existing) {
      return NextResponse.json({
        ok: true,
        status: existing.status,
        mbClientId: existing.mb_client_id ?? null,
        deduped: true
      });
    }

    // Route to correct franchise location
    const tenant = await getTenantByFormId(formId);

    if (!tenant) {
      await insertIdempotency({
        responseId,
        formId,
        status: "failed",
        error: "Unknown or inactive form_id"
      });

      return NextResponse.json(
        { ok: false, error: "Unknown or inactive form_id" },
        { status: 400 }
      );
    }

    // For now we confirm routing works
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
    console.error("Webhook error:", err);

    return NextResponse.json(
      { ok: false, error: err?.message ?? "Server error" },
      { status: 500 }
    );
  }
}
