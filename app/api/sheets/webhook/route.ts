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
    const firstName = body.lead?.firstName;
    const lastName = body.lead?.lastName;
    const email = body.lead?.email;
    const phone = body.lead?.phone;

    if (!sheetId || !sheetName || !rowNumber) {
      return NextResponse.json(
        { ok: false, error: "Missing sheetId, sheetName, or rowNumber" },
        { status: 400 }
      );
    }
    if (!firstName || !lastName) {
      return NextResponse.json({ ok: false, error: "Missing name" }, { status: 400 });
    }

    // find tenant
    const tenantRows = await sql`
      select sheet_id, location_name, site_id, is_active
      from sheet_tenants
      where sheet_id = ${sheetId}
      limit 1
    `;
    const tenant = tenantRows[0] as any;
    if (!tenant || tenant.is_active === false) {
      return NextResponse.json(
        { ok: false, error: "No active tenant mapping" },
        { status: 400 }
      );
    }
    const siteId = Number(tenant.site_id);

    // dedupe (sheet_id + sheet_name + row_number)
    // If the row was already processed, look the client up in Mindbody and
    // return their mbClientId so the Sheet can be backfilled. This covers
    // the case where a previous run succeeded server-side but the response
    // never made it back to the Sheet (or the Sheet write failed).
    let alreadyProcessed = false;
    try {
      await sql`
        insert into processed_sheet_rows (sheet_id, sheet_name, row_number)
        values (${sheetId}, ${sheetName}, ${Number(rowNumber)})
      `;
    } catch {
      alreadyProcessed = true;
    }

    if (alreadyProcessed) {
      const existing = await findClient(siteId, {
        firstName,
        lastName,
        email,
        phone
      });

      if (existing?.Id) {
        return NextResponse.json({
          ok: true,
          status: "deduped",
          mbClientId: String(existing.Id),
          routedTo: { locationName: tenant.location_name, siteId }
        });
      }

      // Row was marked processed but no client exists in Mindbody.
      // This is the failure case the backfill is trying to recover from:
      // the dedupe row got written but the Mindbody create never happened
      // (or was rolled back). Create the client now.
      const created = await createClient(
        siteId,
        { firstName, lastName, email, phone },
        { referralType: "Paid Lead" }
      );

      return NextResponse.json({
        ok: true,
        status: "deduped-recreated",
        mbClientId: created?.Id ? String(created.Id) : null,
        routedTo: { locationName: tenant.location_name, siteId }
      });
    }

    // find existing in Mindbody
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
        mbClientId: String(existing.Id),
        routedTo: { locationName: tenant.location_name, siteId }
      });
    }

    // create in Mindbody
    // IMPORTANT: referralType is ONLY applied here (Google Sheets flow)
    const created = await createClient(
      siteId,
      { firstName, lastName, email, phone },
      { referralType: "Paid Lead" }
    );

    return NextResponse.json({
      ok: true,
      status: "created",
      mbClientId: created?.Id ? String(created.Id) : null,
      routedTo: { locationName: tenant.location_name, siteId }
    });
  } catch (error: any) {
    return NextResponse.json(
      {
        ok: false,
        error: error?.message ?? "Server error"
      },
      { status: 500 }
    );
  }
}
