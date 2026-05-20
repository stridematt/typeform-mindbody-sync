import { NextResponse } from "next/server";
import { neon } from "@neondatabase/serverless";
import { listLeadChannels } from "../../../../lib/mindbody";

export const runtime = "nodejs";

const sql = neon(process.env.DATABASE_URL || "");

/**
 * Diagnostic: list Lead Channels for the site mapped to the given sheetId.
 *
 * Usage:
 *   curl -X POST https://typeform-mindbody-sync.vercel.app/api/diagnostic/lead-channels \
 *     -H 'Content-Type: application/json' \
 *     -d '{"sheetId":"1PJncMQ_XJv1ZENe3i7oeJ9dgXgMq7RQxE93UAAMicgY"}'
 *
 * Returns: { ok: true, siteId: <n>, channels: [{ Id, Name, ... }] }
 *
 * Use the Id of the "Call Center" channel to populate DEFAULT_LEAD_CHANNEL_ID
 * in the HB Apps Script.
 */
export async function POST(req: Request) {
  try {
    const body = await req.json();
    const sheetId = body.sheetId;

    if (!sheetId) {
      return NextResponse.json(
        { ok: false, error: "Missing sheetId" },
        { status: 400 }
      );
    }

    const tenantRows = await sql`
      select sheet_id, location_name, site_id, is_active
      from sheet_tenants
      where sheet_id = ${sheetId}
      limit 1
    `;
    const tenant = tenantRows[0] as any;
    if (!tenant) {
      return NextResponse.json(
        { ok: false, error: "No tenant mapping for sheetId" },
        { status: 400 }
      );
    }

    const siteId = Number(tenant.site_id);
    const channels = await listLeadChannels(siteId);

    return NextResponse.json({
      ok: true,
      siteId,
      locationName: tenant.location_name,
      channels,
    });
  } catch (error: any) {
    return NextResponse.json(
      {
        ok: false,
        error: error?.message ?? "Server error",
        detail: error?.response?.data ?? null,
      },
      { status: 500 }
    );
  }
}
