import { NextResponse } from "next/server";
import { neon } from "@neondatabase/serverless";
import { findClient, createClient, addContactLog } from "../../../../lib/mindbody";

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

    const referralTypeRaw = body.lead?.referralType;
    const referralType =
      typeof referralTypeRaw === "string" && referralTypeRaw.trim()
        ? referralTypeRaw.trim()
        : "Paid Lead";

    const salesRepRaw = body.lead?.salesRep;
    const salesRepNum =
      salesRepRaw !== undefined && salesRepRaw !== null && salesRepRaw !== ""
        ? Number(salesRepRaw)
        : undefined;
    const salesRep =
      salesRepNum !== undefined && Number.isFinite(salesRepNum)
        ? salesRepNum
        : undefined;

    const leadChannelIdRaw = body.lead?.leadChannelId;
    const leadChannelIdNum =
      leadChannelIdRaw !== undefined && leadChannelIdRaw !== null && leadChannelIdRaw !== ""
        ? Number(leadChannelIdRaw)
        : undefined;
    const leadChannelId =
      leadChannelIdNum !== undefined && Number.isFinite(leadChannelIdNum)
        ? leadChannelIdNum
        : undefined;

    // Optional: when true, the webhook will create a Contact Log
    // immediately after the client is created.
    const createFollowupTask = body.lead?.createFollowupTask === true;

    if (!sheetId || !sheetName || !rowNumber) {
      return NextResponse.json(
        { ok: false, error: "Missing sheetId, sheetName, or rowNumber" },
        { status: 400 }
      );
    }
    if (!firstName || !lastName) {
      return NextResponse.json({ ok: false, error: "Missing name" }, { status: 400 });
    }

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

    let alreadyProcessed = false;
    try {
      await sql`
        insert into processed_sheet_rows (sheet_id, sheet_name, row_number)
        values (${sheetId}, ${sheetName}, ${Number(rowNumber)})
      `;
    } catch {
      alreadyProcessed = true;
    }

    // Helper: best-effort follow-up task creation.
    // Logs failures but does NOT fail the overall request — losing the
    // stage placement is annoying but not as bad as losing the client.
    const maybeCreateFollowupTask = async (
      mbClientId: string | number | null | undefined
    ) => {
      if (!createFollowupTask) return null;
      if (!mbClientId) return null;
      try {
        const logResult = await addContactLog(siteId, {
          clientId: mbClientId,
          text: "Auto-created follow-up task from Paid Leads pipeline",
          assignedToStaffId: salesRep, // assign the task to the same rep on the client
          contactMethod: "Phone",
          contactName: `${firstName} ${lastName}`.trim(),
        });
        return { ok: true, response: logResult };
      } catch (err: any) {
        // Mindbody errors here often have useful detail; bubble it up
        const detail =
          err?.response?.data ??
          err?.message ??
          String(err);
        return { ok: false, error: detail };
      }
    };

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
      const created = await createClient(
        siteId,
        { firstName, lastName, email, phone },
        { referralType, salesRep, leadChannelId }
      );

      const mbClientId = created?.Id ? String(created.Id) : null;
      const followup = await maybeCreateFollowupTask(mbClientId);

      return NextResponse.json({
        ok: true,
        status: "deduped-recreated",
        mbClientId,
        followupTask: followup,
        routedTo: { locationName: tenant.location_name, siteId }
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
        mbClientId: String(existing.Id),
        routedTo: { locationName: tenant.location_name, siteId }
      });
    }

    const created = await createClient(
      siteId,
      { firstName, lastName, email, phone },
      { referralType, salesRep, leadChannelId }
    );

    const mbClientId = created?.Id ? String(created.Id) : null;
    const followup = await maybeCreateFollowupTask(mbClientId);

    return NextResponse.json({
      ok: true,
      status: "created",
      mbClientId,
      followupTask: followup,
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
