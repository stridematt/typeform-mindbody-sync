// app/api/coach-log/route.ts
import { NextResponse } from "next/server";
import { findClient, createClient, addContactLog } from "../../../lib/mindbody";

export const runtime = "nodejs";

/* ============================================================
   STUDIO -> MINDBODY SITE ID
   Fill in each studio's real Mindbody SiteId via env vars
   (recommended) or by replacing the "X" fallback directly.
   Keys must match exactly what the website sends in `studio`.
============================================================ */
const STUDIO_SITE_IDS: Record<string, number> = {
  "Huntington Beach": Number(process.env.MINDBODY_SITE_ID_HB ?? "X"),
  "Pasadena":         Number(process.env.MINDBODY_SITE_ID_PASADENA ?? "X"),
  "Tustin":           Number(process.env.MINDBODY_SITE_ID_TUSTIN ?? "X"),
  "Southlands":       Number(process.env.MINDBODY_SITE_ID_SOUTHLANDS ?? "X"),
  "Southampton":      Number(process.env.MINDBODY_SITE_ID_SOUTHAMPTON ?? "X"),
};

/* ============================================================
   ABUSE GUARD
   This endpoint is called from the visitor's browser, so it
   carries no powerful secret. Protection = origin lock to the
   STRIDE domain. The Mindbody credentials live server-side in
   env vars (used inside lib/mindbody) and are never exposed.
============================================================ */
const ALLOWED_ORIGIN_SUFFIXES = ["stridefitness.com"]; // matches www. and bare domain

function originAllowed(origin: string): boolean {
  if (!origin) return false;
  try {
    const host = new URL(origin).hostname.toLowerCase();
    return ALLOWED_ORIGIN_SUFFIXES.some(
      (suf) => host === suf || host.endsWith("." + suf)
    );
  } catch {
    return false;
  }
}

function digitsOnly(s?: string | null) {
  return (s || "").replace(/\D/g, "");
}
function last10(s?: string | null) {
  const d = digitsOnly(s);
  return d.length > 10 ? d.slice(-10) : d;
}

function buildLogText(b: any): string {
  const parts = [
    "STRIDE website — coach intake",
    b?.goals_csv ? `Goals: ${String(b.goals_csv).trim()}` : null,
    b?.injuries ? `Anything to know (injuries/recovery):\n${String(b.injuries).trim()}` : null,
    b?.story ? `Their story (why now / what they're chasing):\n${String(b.story).trim()}` : null,
  ].filter(Boolean);
  return parts.join("\n\n").slice(0, 3800);
}

export async function POST(req: Request) {
  try {
    // --- origin lock ---
    const origin = req.headers.get("origin") || req.headers.get("referer") || "";
    if (!originAllowed(origin)) {
      return NextResponse.json({ ok: false, error: "Origin not allowed" }, { status: 403 });
    }

    const body = await req.json().catch(() => ({}));

    const studio = String(body?.studio || "").trim();
    const firstName = String(body?.first_name || "").trim();
    const phone = String(body?.phone || "").trim();

    const siteId = STUDIO_SITE_IDS[studio];
    if (!siteId || Number.isNaN(siteId)) {
      return NextResponse.json(
        { ok: false, error: `Unknown or unconfigured studio: ${studio}` },
        { status: 400 }
      );
    }
    if (!firstName) {
      return NextResponse.json({ ok: false, error: "Missing first_name" }, { status: 400 });
    }
    if (!last10(phone)) {
      return NextResponse.json({ ok: false, error: "Missing or invalid phone" }, { status: 400 });
    }

    // 1) Find or create the client (phone is the match key; first name only).
    //    NOTE: per lib/mindbody, we deliberately do NOT pass referralType/
    //    salesRep/leadChannelId here — this is the website flow, not the
    //    Google Sheets "Paid Leads" flow.
    let client = await findClient(siteId, { firstName, phone });
    let status = "found";

    if (!client?.Id) {
      client = await createClient(siteId, {
        firstName,
        lastName: "(STRIDE Lead)", // form collects first name only
        phone,
      });
      status = "created";
    }

    const clientId = client?.Id ? String(client.Id) : null;
    if (!clientId) {
      return NextResponse.json(
        { ok: false, error: "Could not find or create client" },
        { status: 502 }
      );
    }

    // 2) Write the contact log with the coach intake info.
    const mb = await addContactLog(siteId, {
      clientId,
      text: buildLogText(body),
      contactMethod: "Phone",
      contactName: firstName,
      // No assignedToStaffId here -> logged as a contact-log entry.
      // If you WANT it to become an open Sales-Pipeline follow-up task,
      // pass a staff Id (e.g. via env or per-studio map) — see note in chat.
    });

    return NextResponse.json({ ok: true, status, clientId, studio, siteId, mindbody: mb });
  } catch (err: any) {
    const mbStatus = err?.response?.status ?? null;
    const mbData = err?.response?.data ?? null;
    return NextResponse.json(
      { ok: false, error: err?.message ?? "Server error", mindbody: { status: mbStatus, data: mbData } },
      { status: 500 }
    );
  }
}
