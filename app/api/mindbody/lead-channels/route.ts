// TEMPORARY debug route — lists Mindbody Lead Channels (Id + name) for a site.
// Deploy at: app/api/mindbody/lead-channels/route.ts
// Call:      GET /api/mindbody/lead-channels?siteId=5749750&key=<TYPEFORM_WEBHOOK_SECRET>
// Delete this file once you've recorded the channel IDs.
import { NextResponse } from "next/server";
import { listLeadChannels } from "../../../../lib/mindbody";
export const runtime = "nodejs";
export async function GET(req: Request) {
  const url = new URL(req.url);
  const key = url.searchParams.get("key");
  if (!key || key !== process.env.TYPEFORM_WEBHOOK_SECRET) {
    return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 });
  }
  const siteIdRaw = url.searchParams.get("siteId");
  const siteId = Number(siteIdRaw);
  if (!Number.isInteger(siteId) || siteId <= 0) {
    return NextResponse.json(
      { ok: false, error: "Pass ?siteId=<number>" },
      { status: 400 }
    );
  }
  try {
    const channels = await listLeadChannels(siteId);
    return NextResponse.json({
      ok: true,
      siteId,
      channels: (channels as any[]).map((c) => ({ id: c?.Id, name: c?.Name })),
      raw: channels,
    });
  } catch (err: any) {
    return NextResponse.json(
      { ok: false, error: err?.message ?? "Server error" },
      { status: 500 }
    );
  }
}
