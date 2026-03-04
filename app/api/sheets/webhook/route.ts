import { NextResponse } from "next/server";
import { neon } from "@neondatabase/serverless";
import { findClient, createClient } from "../../../../lib/mindbody";

export const runtime = "nodejs";

const sql = neon(process.env.DATABASE_URL || "");

export async function POST(req: Request) {
  try {
    const body = await req.json();

    const sheetId = body.sheetId;
    const sheetName = body.sheetName;
    const rowNumber = body.rowNumber;

    const firstName = body.lead.firstName;
    const lastName = body.lead.lastName;
    const email = body.lead.email;
    const phone = body.lead.phone;

    if (!firstName || !lastName) {
      return NextResponse.json({
        ok: false,
        error: "Missing name"
      });
    }

    // find tenant
    const tenantRows = await sql`
      select * from sheet_tenants
      where sheet_id = ${sheetId}
      limit 1
    `;

    const tenant = tenantRows[0];

    if (!tenant) {
      return NextResponse.json({
        ok: false,
        error: "No tenant mapping"
      });
    }

    const siteId = Number(tenant.site_id);

    // dedupe
    try {
      await sql`
        insert into processed_sheet_rows (sheet_id, sheet_name, row_number)
        values (${sheetId}, ${sheetName}, ${rowNumber})
      `;
    } catch {
      return NextResponse.json({
        ok: true,
        status: "deduped"
      });
    }

    const existing = await findClient(siteId, {
      firstName,
      lastName,
      email,
      phone
    });

    if (existing?.Id) {
      return NextResponse.json({
        ok: true,
        status: "exists",
        mbClientId: existing.Id
      });
    }

    const created = await createClient(siteId, {
      firstName,
      lastName,
      email,
      phone
    });

    return NextResponse.json({
      ok: true,
      status: "created",
      mbClientId: created?.Id || null
    });

  } catch (error:any) {

    return NextResponse.json({
      ok:false,
      error:error.message
    },{status:500})

  }
}
