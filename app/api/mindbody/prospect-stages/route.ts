// TEMPORARY debug route — lists Mindbody Sales Pipeline prospect stages
// (Id + Description + SalespipelineId) for one or more sites.
// Deploy at: app/api/mindbody/prospect-stages/route.ts
// Call:      GET /api/mindbody/prospect-stages?siteId=5749750&key=<TYPEFORM_WEBHOOK_SECRET>
//   - multiple sites: ?siteId=5749750,5749751
//   - include full objects: &raw=1
// Delete this file once you've recorded the stage IDs.
import { NextResponse } from "next/server";
import crypto from "crypto";
import { listProspectStages } from "../../../../lib/mindbody";

export const runtime = "nodejs";
export const dynamic = "force-dynamic"; // never cache debug output

function timingSafeEqual(a: string, b: string) {
  const aBuf = Buffer.from(a);
  const bBuf = Buffer.from(b);
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

function mapStages(stages: any[]) {
  return stages
    .map((s) => ({
      id: s?.Id,
      description: s?.Description,
      active: s?.Active,
      salesPipelineId: s?.SalespipelineId ?? s?.SalesPipelineId ?? null,
    }))
    .sort((a, b) => Number(a.id ?? 0) - Number(b.id ?? 0));
}

export async function GET(req: Request) {
  const url = new URL(req.url);

  const secret = process.env.TYPEFORM_WEBHOOK_SECRET;
  const key = url.searchParams.get("key");
  if (!secret || !key || !timingSafeEqual(key, secret)) {
    return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 });
  }

  const siteIds = (url.searchParams.get("siteId") ?? "")
    .split(",")
    .map((s) => Number(s.trim()))
    .filter((n) => Number.isInteger(n) && n > 0);
  if (siteIds.length === 0) {
    return NextResponse.json(
      { ok: false, error: "Pass ?siteId=<number> (comma-separate for multiple)" },
      { status: 400 }
    );
  }

  const includeRaw = url.searchParams.get("raw") === "1";

  const results = await Promise.all(
    siteIds.map(async (siteId) => {
      try {
        const stages = (await listProspectStages(siteId)) as any[];
        return {
          siteId,
          ok: true,
          stages: mapStages(stages),
          ...(includeRaw ? { raw: stages } : {}),
        };
      } catch (err: any) {
        return { siteId, ok: false, error: err?.message ?? "Server error" };
      }
    })
  );

  return NextResponse.json({ ok: results.every((r) => r.ok), results });
}
