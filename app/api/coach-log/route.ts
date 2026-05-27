// app/api/coach-log/route.ts
import { NextResponse } from "next/server";
import axios from "axios";
import { createClient } from "../../../lib/mindbody";

export const runtime = "nodejs";

const MINDBODY_BASE_URL = "https://api.mindbodyonline.com/public/v6";

// Token cache per SiteId (mirrors lib/mindbody's internal pattern).
const _coachTokenCache = new Map<number, { token: string; issuedAt: number }>();
const _COACH_TOKEN_TTL_MS = 10 * 60 * 1000;

async function coachGetToken(siteId: number): Promise<string> {
  const apiKey = process.env.MINDBODY_API_KEY;
  const username = process.env.MINDBODY_USERNAME;
  const password = process.env.MINDBODY_PASSWORD;
  if (!apiKey || !username || !password) throw new Error("Missing Mindbody credentials");

  const cached = _coachTokenCache.get(siteId);
  if (cached && Date.now() - cached.issuedAt < _COACH_TOKEN_TTL_MS) return cached.token;

  const res = await axios.post(
    `${MINDBODY_BASE_URL}/usertoken/issue`,
    { Username: username, Password: password },
    { headers: { "Content-Type": "application/json", "Api-Key": apiKey, SiteId: String(siteId) }, timeout: 20000 }
  );
  const token = res.data?.AccessToken as string | undefined;
  if (!token) throw new Error("Mindbody token response missing AccessToken");
  _coachTokenCache.set(siteId, { token, issuedAt: Date.now() });
  return token;
}

/**
 * Add a plain CONTACT LOG entry (not a follow-up task).
 *
 * IMPORTANT: Mindbody's addcontactlog requires that FollowupByDate and
 * AssignedTo are either BOTH present or BOTH absent ("Must include
 * FollowupByDate with AssignedTo and vice versa"). lib/mindbody's
 * addContactLog always sets FollowupByDate (for the Sales Pipeline task
 * flow), so we can't use it for a log-only entry. This sends neither field.
 */
async function coachAddContactLog(
  siteId: number,
  input: { clientId: string; text: string; contactName?: string; contactMethod?: string }
) {
  const apiKey = process.env.MINDBODY_API_KEY as string;
  const token = await coachGetToken(siteId);

  const payload: Record<string, any> = {
    ClientId: String(input.clientId),
    Text: input.text,
    // ContactMethod is REQUIRED by Mindbody's addcontactlog. Always send it.
    ContactMethod: input.contactMethod || "Phone",
    // NO FollowupByDate and NO AssignedToStaffId -> logged as a plain
    // contact-log entry, satisfying Mindbody's pairing rule.
  };
  if (input.contactName) payload.ContactName = input.contactName;

  const res = await axios.post(`${MINDBODY_BASE_URL}/client/addcontactlog`, payload, {
    headers: {
      "Content-Type": "application/json",
      "Api-Key": apiKey,
      Authorization: `Bearer ${token}`,
      SiteId: String(siteId),
    },
    timeout: 25000,
  });
  return res.data ?? null;
}

/**
 * Strict phone match. lib/mindbody's findClient returns only the FIRST
 * SearchText result without verifying the phone, which can map a lead to
 * the WRONG existing client. This searches Mindbody directly and returns a
 * client ONLY if one of its phone fields matches the last-10 digits.
 * Scans all results, not just the first.
 */
async function findClientByPhoneStrict(siteId: number, phone: string) {
  const key10 = last10(phone);
  if (!key10) return null;

  const apiKey = process.env.MINDBODY_API_KEY as string;
  const token = await coachGetToken(siteId);

  const res = await axios.get(`${MINDBODY_BASE_URL}/client/clients`, {
    headers: {
      "Content-Type": "application/json",
      "Api-Key": apiKey,
      Authorization: `Bearer ${token}`,
      SiteId: String(siteId),
    },
    params: { SearchText: key10, Limit: 100, Offset: 0 },
    timeout: 25000,
  });

  const clients: any[] = res.data?.Clients ?? [];
  // Return the first client whose phone ACTUALLY matches (not just a fuzzy hit).
  return (
    clients.find((c) => {
      const phones = [c?.MobilePhone, c?.HomePhone, c?.WorkPhone].map((p) => last10(p));
      return phones.includes(key10);
    }) ?? null
  );
}

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

// Pull a usable origin from either the Origin header (normal CORS) or the
// Referer (some embeds/browsers send Referer but omit Origin on preflight).
function getRequestOrigin(req: Request): string {
  const origin = req.headers.get("origin");
  if (origin) return origin;
  const referer = req.headers.get("referer");
  if (referer) {
    try {
      const u = new URL(referer);
      return `${u.protocol}//${u.host}`; // strip path -> scheme + host
    } catch {
      /* ignore */
    }
  }
  return "";
}

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

// CORS headers. Echoes the request origin when it's an allowed STRIDE domain.
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

// Preflight: ALWAYS answer 204 with CORS headers. The browser needs the
// Access-Control-Allow-Origin header here or it blocks the follow-up POST.
// The real authorization check happens on the POST below.
export async function OPTIONS(req: Request) {
  const origin = getRequestOrigin(req);
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
  const tierLabel: Record<string, string> = {
    ignite: "Ignite",
    core: "Core",
    elite: "Elite",
  };

  // Format the day-by-day plan, if present.
  let planBlock = "";
  if (Array.isArray(b?.plan) && b.plan.length) {
    const lines = b.plan.map((p: any) => {
      const day = String(p?.day ?? "").trim();
      const focus = String(p?.focus ?? "").trim();
      const format = String(p?.format ?? "").trim();
      const bits = [day && `${day}:`, focus, format && `(${format})`].filter(Boolean);
      return `  - ${bits.join(" ")}`;
    });
    planBlock = `Suggested weekly plan:\n${lines.join("\n")}`;
  }

  const recTier = b?.recommended_tier ? (tierLabel[b.recommended_tier] || String(b.recommended_tier)) : "";

  const parts = [
    "STRIDE website — coach intake",
    b?.studio ? `Studio: ${String(b.studio).trim()}` : null,
    b?.goals_csv ? `Goals: ${String(b.goals_csv).trim()}` : null,
    b?.days_csv ? `Training days: ${String(b.days_csv).trim()}` : null,
    b?.days_per_week ? `Days per week: ${b.days_per_week}` : null,
    b?.formats_csv ? `Formats: ${String(b.formats_csv).trim()}` : null,
    b?.weekly_minutes ? `Weekly minutes: ${b.weekly_minutes}` : null,
    recTier ? `Recommended membership: ${recTier}` : null,
    planBlock || null,
    b?.injuries ? `Anything to know (injuries/recovery):\n${String(b.injuries).trim()}` : null,
    b?.story ? `Their story (why now / what they're chasing):\n${String(b.story).trim()}` : null,
  ].filter(Boolean);
  return parts.join("\n\n").slice(0, 3800);
}

export async function POST(req: Request) {
  const origin = getRequestOrigin(req);
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

    // 1) Find or create the client. We match STRICTLY on phone (last-10) so a
    //    fuzzy Mindbody search can never attach the log to the wrong client.
    //    If no phone-exact client exists, we create a new one.
    //    NOTE: per lib/mindbody, we deliberately do NOT pass referralType/
    //    salesRep/leadChannelId here — this is the website flow, not the
    //    Google Sheets "Paid Leads" flow.
    let client = await findClientByPhoneStrict(siteId, phone);
    let status = "found";

    if (!client?.Id) {
      // Mindbody's AddClient REQUIRES a non-empty Email. The form doesn't
      // collect one, so generate a unique placeholder keyed to the phone
      // (mirrors the Google Sheets flow's fallback-email approach). Using the
      // phone digits keeps it stable + unique so re-submits don't collide.
      const fallbackEmail = `lead+${last10(phone)}@stridefitness-leads.com`;
      client = await createClient(siteId, {
        firstName,
        lastName: "(STRIDE Lead)", // form collects first name only
        email: fallbackEmail,
        phone,
      });
      status = "created";
    }

    const clientId = client?.Id ? String(client.Id) : null;
    if (!clientId) {
      return json({ ok: false, error: "Could not find or create client" }, { status: 502 });
    }

    // 2) Write the contact log with the coach intake info.
    const mb = await coachAddContactLog(siteId, {
      clientId,
      text: buildLogText(body),
      contactMethod: "Phone",
      contactName: firstName,
      // log-only: no FollowupByDate / AssignedTo -> plain contact-log entry.
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
