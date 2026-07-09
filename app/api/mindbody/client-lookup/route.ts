// TEMPORARY debug route — looks up a Mindbody client and shows their
// ProspectStage (Id + Description) + IsProspect, so you can read the real
// Sales Pipeline stage ID off a lead that's already in the pipeline.
// Deploy at: app/api/mindbody/client-lookup/route.ts
// Call examples:
//   GET /api/mindbody/client-lookup?siteId=5749750&email=someone@x.com&key=<SECRET>
//   GET /api/mindbody/client-lookup?siteId=5749750&firstName=Matt&lastName=Test&key=<SECRET>
// Delete this file once you've recorded the stage ID.
import { NextResponse } from "next/server";
import crypto from "crypto";
import { findClient } from "../../../../lib/mindbody";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function timingSafeEqual(a: string, b: string) {
  const aBuf = Buffer.from(a);
  const bBuf = Buffer.from(b);
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

export async function GET(req: Request) {
  const url = new URL(req.url);

  const secret = process.env.TYPEFORM_WEBHOOK_SECRET;
  const key = url.searchParams.get("key");
  if (!secret || !key || !timingSafeEqual(key, secret)) {
    return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 });
  }

  const siteId = Number(url.searchParams.get("siteId"));
  if (!Number.isInteger(siteId) || siteId <= 0) {
    return NextResponse.json({ ok: false, error: "Pass ?siteId=<number>" }, { status: 400 });
  }

  const email = url.searchParams.get("email") ?? undefined;
  const firstName = url.searchParams.get("firstName") ?? undefined;
  const lastName = url.searchParams.get("lastName") ?? undefined;
  const phone = url.searchParams.get("phone") ?? undefined;
  if (!email && !firstName && !lastName && !phone) {
    return NextResponse.json(
      { ok: false, error: "Pass at least one of ?email= ?firstName= ?lastName= ?phone=" },
      { status: 400 }
    );
  }

  try {
    const client: any = await findClient(siteId, { email, firstName, lastName, phone });
    if (!client) {
      return NextResponse.json({ ok: true, found: false });
    }
    return NextResponse.json({
      ok: true,
      found: true,
      clientId: client?.Id,
      isProspect: client?.IsProspect,
      prospectStage: client?.ProspectStage ?? null, // { Id, Description, Active }
      // full object in case ProspectStage lives under a different key
      client,
    });
  } catch (err: any) {
    return NextResponse.json({ ok: false, error: err?.message ?? "Server error" }, { status: 500 });
  }
}
