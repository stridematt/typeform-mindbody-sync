import { NextResponse } from "next/server";
import crypto from "crypto";
import { neon } from "@neondatabase/serverless";
import { findClient, createClient } from "../../../../lib/mindbody";

export const runtime = "nodejs";

const sql = neon(process.env.DATABASE_URL || "");

function requireEnv(name: string) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing ${name}`);
  return v;
}

function timingSafeEqual(a: string, b: string) {
  const aBuf = Buffer.from(a);
  const bBuf = Buffer.from(b);
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

function normalizeEmail(email: string | null | undefined) {
  const e = (email ?? "").trim();
  return e ? e : null;
}

function normalizePhone(phone: string | null | undefined) {
  const p = (phone ?? "").trim();
  if (!p) return null;
  return p.replace(/\D/g, "");
}

async function ensureTables() {
  await sql`
    create table if not exists sheet_tenants (
      id bigserial primary key,
      sheet_id text not null unique,
      location_name text,
      site_id integer not null,
      is_active boolean default true,
      created_at timestamptz default now()
    );
  `;

  await sql`
    create table if not exists processed_sheet_rows (
      id bigserial primary key,
      sheet_id text not null,
      sheet_name text not null,
      row_number integer not null,
      created_at timestamptz default now(),
      unique (sheet_id, sheet_name, row_number)
    );
  `;
}

type SheetWebhookPayload = {
  sheetId: string;
  sheetName: string;
  rowNumber: number;
  firstName: string;
  lastName: string;
  email?: string | null;
  phone?: string | null;
};

export async function POST(req: Request) {
  try {
    const secret = requireEnv("SHEETS_WEBHOOK_SECRET");
    const provided =
      req.headers.get("x-sheets-secret") ||
      req.headers.get("X-Sheets-Secret") ||
      "";

    if (!provided || !timingSafeEqual(provided, secret)) {
      return NextResponse.json(
        { ok: false, error: "Unauthorized (bad sheets secret)" },
        { status: 401 }
      );
    }

    const body = (await req.json()) as Partial<SheetWebhookPayload>;

    const sheetId = (body.sheetId ?? "").trim();
    const sheetName = (body.sheetName ?? "").trim();
    const rowNumber = Number(body.rowNumber);

    const firstName = (body.firstName ?? "").trim();
    const lastName = (body.lastName ?? "").trim();

    const email = normalizeEmail(body.email);
    const phone = normalizePhone(body.phone);

    if (!sheetId || !sheetName || !Number.isInteger(rowNumber) || rowNumber <= 1) {
      return NextResponse.json(
        { ok: false, error: "Missing or invalid sheetId/sheetName/rowNumber" },
        { status: 400 }
      );
    }

    if (!firstName || !lastName) {
      return NextResponse.json(
        { ok: false, error: "First Name and Last Name are required" },
        { status: 400 }
      );
    }

    await ensureTables();

    // 1) Resolve tenant (which Mindbody site to use) from sheet_id
    const tenantRows = await sql`
      select sheet_id, location_name, site_id, is_active
      from sheet_tenants
      where sheet_id = ${sheetId}
      limit 1
    `;
    const tenant = (tenantRows as any)?.[0];

    if (!tenant || tenant.is_active === false) {
      return NextResponse.json({
        ok: true,
        status: "routed",
        routedTo: null,
        message: "No active tenant for this sheet_id"
      });
    }

    const siteId = Number(tenant.site_id);

    // 2) Dedupe by (sheetId, sheetName, rowNumber)
    try {
      await sql`
        insert into processed_sheet_rows (sheet_id, sheet_name, row_number)
        values (${sheetId}, ${sheetName}, ${rowNumber})
      `;
    } catch {
      return NextResponse.json({
        ok: true,
        status: "deduped",
        routedTo: { locationName: tenant.location_name, siteId }
      });
    }

    // 3) Find or create in Mindbody
    const existing = await findClient(siteId, {
      firstName,
      lastName,
      email: email ?? undefined,
      phone: phone ?? undefined
    });

    if (existing?.Id) {
      return NextResponse.json({
        ok: true,
        status: "exists",
        mbClientId: String(existing.Id),
        routedTo: { locationName: tenant.location_name, siteId }
      });
    }

    const created = await createClient(siteId, {
      firstName,
      lastName,
      email: email ?? "",
      phone: phone ?? ""
    });

    if (!created?.Id) {
      return NextResponse.json({ ok: false, error: "Mindbody create failed" }, { status: 500 });
    }

    return NextResponse.json({
      ok: true,
      status: "created",
      mbClientId: String(created.Id),
      routedTo: { locationName: tenant.location_name, siteId }
    });
  } catch (err: any) {
    return NextResponse.json(
      {
        ok: false,
        error: err?.message ?? "Server error"
      },
      { status: 500 }
    );
  }
}
