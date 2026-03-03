import { NextResponse } from "next/server";
import {
  getTenantByFormId,
  getIdempotency,
  insertIdempotency
} from "../../../../lib/db";
import { findClient, createClient, type Lead } from "../../../../lib/mindbody";

export const runtime = "nodejs";

function verifyTypeformSecret(req: Request) {
  const incoming = req.headers.get("typeform-secret");
  const expected = process.env.TYPEFORM_WEBHOOK_SECRET;

  if (!expected) throw new Error("TYPEFORM_WEBHOOK_SECRET not set");
  return incoming === expected;
}

function extractLead(payload: any): Lead {
  const answers: any[] = payload?.form_response?.answers ?? [];

  const byRef = (ref: string) => answers.find(a => a?.field?.ref === ref);

  const first =
    byRef("first_name")?.text ??
    byRef("firstname")?.text ??
    byRef("first")?.text ??
    "";

  const last =
    byRef("last_name")?.text ??
    byRef("lastname")?.text ??
    byRef("last")?.text ??
    "";

  const email =
    byRef("email")?.email ??
    answers.find(a => a?.type === "email")?.email ??
    null;

  const phone =
    byRef("phone")?.phone_number ??
    byRef("phone_number")?.phone_number ??
    answers.find(a => a?.type === "phone_number")?.phone_number ??
    null;

  return {
    firstName: String(first).trim(),
    lastName: String(last).trim(),
    email,
    phone
  };
}

export async function POST(req: Request) {
  try {
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

    const already = await getIdempotency(responseId);
    if (already) {
      return NextResponse.json({
        ok: true,
        status: already.status,
        mbClientId: already.mb_client_id ?? null,
        deduped: true
      });
    }

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

    const lead = extractLead(payload);

    if (!lead.firstName || !lead.lastName) {
      await insertIdempotency({
        responseId,
        formId,
        status: "failed",
        error: "Missing first and/or last name in Typeform payload"
      });
      return NextResponse.json(
        { ok: false, error: "Missing first and/or last name" },
        { status: 400 }
      );
    }

    const existing = await findClient(Number(tenant.site_id), lead);

    if (existing?.Id) {
      await insertIdempotency({
        responseId,
        formId,
        status: "exists",
        mbClientId: String(existing.Id)
      });

      return NextResponse.json({
        ok: true,
        status: "exists",
        mbClientId: String(existing.Id),
        routedTo: {
          locationName: tenant.location_name ?? null,
          siteId: tenant.site_id
        }
      });
    }

    const created = await createClient(Number(tenant.site_id), lead);

    if (!created?.Id) {
      await insertIdempotency({
        responseId,
        formId,
        status: "failed",
        error: "Mindbody create returned no client Id"
      });
      return NextResponse.json(
        { ok: false, error: "Mindbody create failed" },
        { status: 502 }
      );
    }

    await insertIdempotency({
      responseId,
      formId,
      status: "created",
      mbClientId: String(created.Id)
    });

    return NextResponse.json({
      ok: true,
      status: "created",
      mbClientId: String(created.Id),
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
