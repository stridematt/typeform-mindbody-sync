// TEMPORARY debug route — lists Mindbody Sales Pipeline prospect stages
// (Id + Description) for a site.
// Deploy at: app/api/mindbody/prospect-stages/route.ts
// Call:      GET /api/mindbody/prospect-stages?siteId=5749750&key=<TYPEFORM_WEBHOOK_SECRET>
// Delete this file once you've recorded the stage IDs.
import { NextResponse } from "next/server";
import { listProspectStages } from "../../../../lib/mindbody";
export const runtime = "nodejs";
export async function GET(req: Request) {
  const url = new URL(req.url);
  const key = url.searchParams.get("key");
  if (!key || key !== process.env.TYPEFORM_WEBHOOK_SECRET) {
    return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 });
  }
  const siteId = Number(url.searchParams.get("siteId"));
  if (!Number.isInteger(siteId) || siteId <= 0) {
    return NextResponse.json(
      { ok: false, error: "Pass ?siteId=<number>" },
      { status: 400 }
    );
  }
  try {
    const stages = await listProspectStages(siteId);
    return NextResponse.json({
      ok: true,
      siteId,
      stages: (stages as any[]).map((s) => ({
        id: s?.Id,
        description: s?.Description,
        active: s?.Active,
      })),
      raw: stages,
    });
  } catch (err: any) {
    return NextResponse.json(
      { ok: false, error: err?.message ?? "Server error" },
      { status: 500 }
    );
  }
}
