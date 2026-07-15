// app/api/coach-log/route.ts
import { NextResponse } from "next/server";
import axios from "axios";
import { neon } from "@neondatabase/serverless";
import { createClient } from "../../../lib/mindbody";

export const runtime = "nodejs";

/*
 * AUDIT FIXES (2026-07-15) — browser-facing coach intake -> Mindbody.
 *
 *   [FIX 1] Search params corrected to Mindbody v6's documented request.-prefixed
 *           form (request.searchText / request.limit / request.offset) plus
 *           request.includeInactive, so the phone search actually filters (and
 *           finds inactive returning clients) instead of scanning an unfiltered
 *           page. Auth stays "Bearer <token>" — confirmed working on this account.
 *   [FIX 2] Phone validation now requires a real 10-digit number, so junk like
 *           "123" can no longer create a client.
 *   [FIX 3] Short-window idempotency guard keyed on site + last-10 phone, so a
 *           browser double-submit / retry can't create duplicate clients + logs.
 *           Best-effort (never blocks a real lead if the guard store is down),
 *           and time-boxed so a genuine later re-submit still logs.
 *   [FIX 4] Basic in-process rate limiting per IP + per phone as an abuse guard
 *           on top of the (spoofable) origin lock. For production-grade limiting
 *           use a shared store (Vercel KV / Upstash) — noted below.
 */

const MINDBODY_BASE_URL = "https://api.mindbodyonline.com/public/v6";

// Optional dedup store. If DATABASE_URL isn't set, the guard degrades to
// in-memory only (still catches same-instance double-clicks).
const sql = process.env.DATABASE_URL ? neon(process.env.DATABASE_URL) : null;

// Collapse repeat submissions for the same phone within this window (double
// clicks, Typeform-style retries, impatient refresh) while still allowing a
// genuine later re-submit to log again.
const DEDUP_WINDOW_MS = 3 * 60 * 1000;

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
 * AssignedTo are either BOTH present or BOTH absent. We send neither, so this is
 * logged as a plain contact-log entry.
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
    ContactMethod: input.contactMethod || "Phone", // required by Mindbody
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
 * Strict phone match. Returns a client ONLY if one of its phone fields matches
 * the last-10 digits. Scans ALL results (paginated), not just the first.
 * [FIX 1] request.-prefixed params + includeInactive.
 */
async function findClientByPhoneStrict(siteId: number, phone: string) {
  const key10 = last10(phone);
  if (key10.length !== 10) return null;
  const apiKey = process.env.MINDBODY_API_KEY as string;
  const token = await coachGetToken(siteId);

  const PAGE = 100;
  const MAX = 200;
  for (let offset = 0; offset < MAX; offset += PAGE) {
    const res = await axios.get(`${MINDBODY_BASE_URL}/client/clients`, {
      headers: {
        "Content-Type": "application/json",
        "Api-Key": apiKey,
        Authorization: `Bearer ${token}`,
        SiteId: String(siteId),
      },
      params: {
        "request.searchText": key10,
        "request.limit": PAGE,
        "request.offset": offset,
        "request.includeInactive": true,
      },
      timeout: 25000,
    });
    const clients: any[] = res.data?.Clients ?? [];
    const hit = clients.find((c) => {
      const phones = [c?.MobilePhone, c?.HomePhone, c?.WorkPhone].map((p) => last10(p));
      return phones.includes(key10);
    });
    if (hit) return hit;

    const total = Number(res.data?.PaginationResponse?.TotalResults ?? NaN);
    if (clients.length < PAGE) break;
    if (Number.isFinite(total) && offset + PAGE >= total) break;
  }
  return null;
}

/* ============================================================
   STUDIO -> MINDBODY SITE ID
============================================================ */
const STUDIO_SITE_IDS: Record<string, number> = {
  "Huntington Beach": Number(process.env.MINDBODY_SITE_ID_HB),
  "Pasadena": Number(process.env.MINDBODY_SITE_ID_PASADENA),
  "Tustin": Number(process.env.MINDBODY_SITE_ID_TUSTIN),
  "Southlands": Number(process.env.MINDBODY_SITE_ID_SOUTHLANDS),
  "Southampton": Number(process.env.MINDBODY_SITE_ID_SOUTHAMPTON),
};

/* ============================================================
   ABUSE GUARD — origin lock (spoofable; see rate limiter below)
============================================================ */
const ALLOWED_ORIGIN_SUFFIXES = ["stridefitness.com"];

function getRequestOrigin(req: Request): string {
  const origin = req.headers.get("origin");
  if (origin) return origin;
  const referer = req.headers.get("referer");
  if (referer) {
    try {
      const u = new URL(referer);
      return `${u.protocol}//${u.host}`;
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
    return ALLOWED_ORIGIN_SUFFIXES.some((suf) => host === suf || host.endsWith("." + suf));
  } catch {
    return false;
  }
}

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

export async function OPTIONS(req: Request) {
  const origin = getRequestOrigin(req);
  return new NextResponse(null, { status: 204, headers: corsHeaders(origin) });
}

/* ============================================================
   [FIX 4] Basic in-process rate limiter (best-effort).
   Per-instance only. For real protection back this with Vercel KV / Upstash.
============================================================ */
const RATE_MAX = 5; // requests
const RATE_WINDOW_MS = 60 * 1000; // per minute
const _rateBuckets = new Map<string, number[]>();

function rateLimited(key: string): boolean {
  const now = Date.now();
  const arr = (_rateBuckets.get(key) ?? []).filter((t) => now - t < RATE_WINDOW_MS);
  if (arr.length >= RATE_MAX) {
    _rateBuckets.set(key, arr);
    return true;
  }
  arr.push(now);
  _rateBuckets.set(key, arr);
  return false;
}

function clientIp(req: Request): string {
  const xff = req.headers.get("x-forwarded-for");
  if (xff) return xff.split(",")[0].trim();
  return req.headers.get("x-real-ip") || "unknown";
}

/* ============================================================
   [FIX 3] Short-window dedup guard (DB-backed, best-effort).
   Returns true if this (site, phone) was seen within DEDUP_WINDOW_MS.
   Also records this sighting. Never throws — a guard-store hiccup must not
   block a real lead.
============================================================ */
async function seenRecently(siteId: number, phone10: string): Promise<boolean> {
  if (!sql) return false;
  try {
    await sql`
      create table if not exists processed_coach_logs (
        id bigserial primary key,
        site_id integer not null,
        phone10 text not null,
        last_seen timestamptz not null default now(),
        unique (site_id, phone10)
      );
    `;
    // Insert or update, returning the PREVIOUS last_seen so we can tell whether
    // this is a rapid repeat. xmax = 0 means a fresh insert (no prior row).
    const rows = (await sql`
      insert into processed_coach_logs (site_id, phone10, last_seen)
      values (${siteId}, ${phone10}, now())
      on conflict (site_id, phone10)
      do update set last_seen = now()
      returning
        (xmax <> 0) as existed,
        (now() - processed_coach_logs.last_seen) as age
    `) as any[];
    const row = rows?.[0];
    if (!row || !row.existed) return false;
    // Postgres interval -> ms. neon returns interval as a string like "00:00:01.2".
    const ageMs = intervalToMs(row.age);
    return ageMs >= 0 && ageMs < DEDUP_WINDOW_MS;
  } catch (e) {
    console.log("coach-log dedup guard unavailable, proceeding:", e);
    return false;
  }
}

function intervalToMs(v: any): number {
  if (v == null) return -1;
  if (typeof v === "number") return v * 1000;
  const s = String(v);
  // Formats: "HH:MM:SS(.ms)" possibly prefixed with "D days ".
  let days = 0;
  let rest = s;
  const dm = s.match(/(\d+)\s+day/);
  if (dm) {
    days = Number(dm[1]);
    rest = s.replace(/.*day[s]?\s*/, "");
  }
  const m = rest.match(/(\d+):(\d+):(\d+(?:\.\d+)?)/);
  if (!m) return -1;
  const hours = Number(m[1]);
  const mins = Number(m[2]);
  const secs = Number(m[3]);
  return ((days * 24 + hours) * 3600 + mins * 60 + secs) * 1000;
}

/* ---------- Helpers ---------- */

function digitsOnly(s?: string | null) {
  return (s || "").replace(/\D/g, "");
}

// Last 10 digits (ignores +1 / country code). Returns "" if fewer than 10.
function last10(s?: string | null) {
  const d = digitsOnly(s);
  if (d.length < 10) return "";
  return d.slice(-10);
}

function esc(s?: string | null): string {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

/**
 * Build the contact-log body as HTML. Renders as formatted text only if the
 * Mindbody contact-log view interprets HTML; if raw tags appear, switch to a
 * plain-text builder. Visitor free text is always escaped.
 */
function buildLogText(b: any): string {
  const tierLabel: Record<string, string> = { ignite: "Ignite", core: "Core", elite: "Elite" };
  const section = (header: string, items: Array<string | null | undefined>): string => {
    const clean = items.map((i) => (i ?? "").toString().trim()).filter(Boolean);
    if (!clean.length) return "";
    const bullets = clean.map((i) => `<li>${esc(i)}</li>`).join("");
    return `<strong>${header}:</strong><ul>${bullets}</ul>`;
  };
  const goalsSection = section("Goals", [b?.story]);
  const injuriesSection = section("Injuries", [b?.injuries]);
  const recTier = b?.recommended_tier
    ? tierLabel[b.recommended_tier] || String(b.recommended_tier)
    : "";
  const membershipSection = section("Membership Recommendation", [recTier]);
  const priorities = String(b?.goals_csv ?? "")
    .split(",")
    .map((g) => g.trim())
    .filter(Boolean);
  const prioritiesSection = section("Priorities", priorities);
  const daysSection = section("Days Per Week", [b?.days_per_week ? String(b.days_per_week) : ""]);

  let planBullets = "";
  if (Array.isArray(b?.plan) && b.plan.length) {
    planBullets = b.plan
      .map((p: any) => {
        const day = esc(String(p?.day ?? "").trim());
        const focus = esc(String(p?.focus ?? "").trim());
        const format = esc(String(p?.format ?? "").trim());
        const rest = [focus, format && `(${format})`].filter(Boolean).join(" ");
        if (!day && !rest) return "";
        const dayPart = day ? `<strong>${day}:</strong>` : "";
        return `<li>${[dayPart, rest].filter(Boolean).join(" ")}</li>`;
      })
      .filter(Boolean)
      .join("");
  }
  const planSection = planBullets ? `<strong>Fitness Plan:</strong><ul>${planBullets}</ul>` : "";

  const html = [
    goalsSection,
    injuriesSection,
    membershipSection,
    prioritiesSection,
    daysSection,
    planSection,
  ]
    .filter(Boolean)
    .join("");
  return html.slice(0, 3800);
}

export async function POST(req: Request) {
  const origin = getRequestOrigin(req);
  const cors = corsHeaders(origin);
  const json = (data: any, init?: { status?: number }) =>
    NextResponse.json(data, { status: init?.status ?? 200, headers: cors });

  try {
    // --- origin lock (weak; spoofable — real guard is the rate limiter) ---
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
    const phone10 = last10(phone);
    if (phone10.length !== 10) {
      // [FIX 2] Require a real 10-digit US number.
      return json({ ok: false, error: "Missing or invalid phone" }, { status: 400 });
    }

    // [FIX 4] Rate limit per IP and per phone before doing any Mindbody work.
    if (rateLimited(`ip:${clientIp(req)}`) || rateLimited(`ph:${siteId}:${phone10}`)) {
      return json({ ok: false, error: "Too many requests" }, { status: 429 });
    }

    // [FIX 3] Collapse rapid duplicate submissions for the same phone.
    if (await seenRecently(siteId, phone10)) {
      return json({ ok: true, status: "duplicate_ignored", studio, siteId });
    }

    // 1) Find (strict last-10 phone) or create the client.
    let client = await findClientByPhoneStrict(siteId, phone);
    let status = "found";
    if (!client?.Id) {
      // Mindbody AddClient requires a non-empty Email. Generate a stable, unique
      // placeholder keyed to the phone so re-submits don't collide.
      const fallbackEmail = `lead+${phone10}@stridefitness-leads.com`;
      client = await createClient(siteId, {
        firstName,
        lastName: "(STRIDE Lead)",
        email: fallbackEmail,
        phone: phone10,
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
    });

    return json({ ok: true, status, clientId, studio, siteId, mindbody: mb });
  } catch (err: any) {
    const mbStatus = err?.response?.status ?? null;
    // Do not echo Mindbody's raw error body back to the browser.
    console.log("coach-log error:", { message: err?.message, mbStatus, data: err?.response?.data });
    return json({ ok: false, error: "Server error", mbStatus }, { status: 500 });
  }
}
