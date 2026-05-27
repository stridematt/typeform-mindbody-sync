// app/api/coach-log/route.ts
import { NextResponse } from "next/server";
import { findClient, createClient, addContactLog } from "../../../lib/mindbody";

export const runtime = "nodejs";

/* ============================================================
   STUDIO -> MINDBODY SITE ID
   Reads each studio's SiteId from Vercel env vars.
   Keys must match exactly what the website sends in `studio`.
============================================================ */
const STUDIO_SITE_IDS: Record<string, number> = {
  "Huntington Beach": Number(process.env.MINDBODY_SITE_ID_HB),
  "Pasadena":         Number(process.env.MINDBODY_SITE_ID_PASADENA),
  "Tustin":           Number(process.env.MINDBODY_SITE_ID_TUSTIN),
  "Southlands":       Number(process.env.MINDBODY_SITE_ID_SOUTHLANDS),
  "Southampton":      Number(process.env.MINDBODY_SITE_ID_SOUTHAMPTON),
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

// CORS headers. Reflects the request Origin only if it's allowed, so the
// browser permits the cross-origin POST from the STRIDE site.
function corsHeaders(origin: string): Record<string, string> {
  const headers: Record<string, string> = {
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "86400",
    Vary: "Origin",
  };
  if (originAllowed(origin)) headers["Access-Control-Allow-Origin"] = origin;
  return headers;
}

// Preflight: the browser sends OPTIONS before the JSON POST.
export async function OPTIONS(req: Request) {
  const origin = req.headers.get("origin") || "";
  if (!originAllowed(origin)) {
    return new NextResponse(null, { status: 403 });
  }
  return new NextResponse(null, { status: 204, headers: corsHeaders(origin) });
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
  const origin = req.headers.get("origin") || req.headers.get("referer") || "";
  const cors = corsHeaders(origin);
  // Wrap NextResponse.json so every reply carries CORS headers.
  const json = (data: any, init?: { status?: number }) =>
    NextResponse.json(data, { status: init?.status ?? 200, headers: cors });

  try {
    // --- origin lock ---
    if (!originAllowed(origin)) {
      return json({ ok: false, error: "Origin not allowed" }, { status: 403 });
    }

    const body = await req.json().catch(() => ({}));

    const studio = String(body?.studio || "").trim();
    const firstName = String(body?.first_name || "").trim();
    const phone = String(body?.phone || "").trim();

    const siteId = STUDIO_SITE_IDS[studio];
    if (!siteId || Number.isNaN(siteId)) {
      return json({ ok: false, error: `Unknown or unconfigured studio: ${studio}` }, { status: 400 });
    }
    if (!firstName) {
      return json({ ok: false, error: "Missing first_name" }, { status: 400 });
    }
    if (!last10(phone)) {
      return json({ ok: false, error: "Missing or invalid phone" }, { status: 400 });
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
      return json({ ok: false, error: "Could not find or create client" }, { status: 502 });
    }

    // 2) Write the contact log with the coach intake info.
    const mb = await addContactLog(siteId, {
      clientId,
      text: buildLogText(body),
      contactMethod: "Phone",
      contactName: firstName,
      // No assignedToStaffId -> logged as a contact-log entry (not a follow-up task).
    });

    return json({ ok: true, status, clientId, studio, siteId, mindbody: mb });
  } catch (err: any) {
    const mbStatus = err?.response?.status ?? null;
    const mbData = err?.response?.data ?? null;
    return json(
      { ok: false, error: err?.message ?? "Server error", mindbody: { status: mbStatus, data: mbData } },
      { status: 500 }
    );
  }
}
