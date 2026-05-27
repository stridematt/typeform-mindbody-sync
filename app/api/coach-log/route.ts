import { NextResponse } from "next/server";
import axios from "axios";

export const runtime = "nodejs";

const MINDBODY_BASE_URL = "https://api.mindbodyonline.com/public/v6";

/* ============================================================
   STUDIO -> MINDBODY SITE ID MAP
   Fill in each studio's real Mindbody SiteId (replace the Xs).
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
   LIGHTWEIGHT ABUSE GUARD
   The browser calls this directly, so it can't carry a powerful
   secret. We (1) only accept requests from the STRIDE domain and
   (2) check a low-privilege submit token. The real Mindbody
   credentials live in server-side env vars and are never exposed.
============================================================ */
const ALLOWED_ORIGINS = [
  "https://X", // e.g. "https://www.stridefitness.com" — the domain the page is embedded on
];
// Optional extra check: a public, low-privilege token the page sends.
// Safe to expose because this endpoint can only write a contact log.
const PUBLIC_SUBMIT_TOKEN = process.env.COACH_LOG_PUBLIC_TOKEN ?? "X";

/* ============================================================
   Token cache per SiteId (mirrors your existing pattern)
============================================================ */
const tokenCache = new Map<number, { token: string; expiresAt: number }>();

function baseHeaders(siteId: number) {
  const apiKey = process.env.MINDBODY_API_KEY;
  if (!apiKey) throw new Error("Missing MINDBODY_API_KEY env var");
  return { "Api-Key": apiKey, SiteId: String(siteId), "Content-Type": "application/json" };
}

async function getUserToken(siteId: number) {
  const username = process.env.MINDBODY_USERNAME;
  const password = process.env.MINDBODY_PASSWORD;
  if (!username || !password) throw new Error("Missing MINDBODY_USERNAME or MINDBODY_PASSWORD");

  const now = Date.now();
  const cached = tokenCache.get(siteId);
  if (cached && cached.expiresAt > now) return cached.token;

  const resp = await axios.post(
    `${MINDBODY_BASE_URL}/usertoken/issue`,
    { Username: username, Password: password },
    { headers: baseHeaders(siteId), timeout: 15000 }
  );
  const token = resp.data?.AccessToken;
  if (!token) throw new Error("No AccessToken returned");
  tokenCache.set(siteId, { token, expiresAt: now + 45 * 60 * 1000 });
  return token;
}

async function authHeaders(siteId: number) {
  const token = await getUserToken(siteId);
  return { ...baseHeaders(siteId), Authorization: `Bearer ${token}` };
}

function digitsOnly(s?: string | null) {
  return (s || "").replace(/\D/g, "");
}
function last10(s?: string | null) {
  const d = digitsOnly(s);
  return d.length > 10 ? d.slice(-10) : d;
}

/* ============================================================
   Find client by phone (your GET /client/clients pattern)
============================================================ */
async function findClientByPhone(siteId: number, phone: string) {
  const key10 = last10(phone);
  if (!key10) return null;

  const resp = await axios.get(`${MINDBODY_BASE_URL}/client/clients`, {
    headers: await authHeaders(siteId),
    params: { SearchText: key10, Limit: 25, Offset: 0 },
    timeout: 15000,
  });

  const clients: any[] = resp.data?.Clients ?? [];
  // Confirm the phone actually matches (SearchText can be fuzzy)
  const hit = clients.find((c) => {
    const phones = [c?.MobilePhone, c?.HomePhone, c?.WorkPhone].map((p) => last10(p));
    return phones.includes(key10);
  });
  return hit ?? null;
}

/* ============================================================
   Create client (first name + phone; last name optional)
============================================================ */
async function createClient(siteId: number, params: { firstName: string; lastName: string; phone: string }) {
  const payload = {
    Client: {
      FirstName: params.firstName,
      LastName: params.lastName,
      MobilePhone: digitsOnly(params.phone),
      // Mark as a prospect/lead so it lands in the right place in Mindbody
      Action: "Add",
    },
  };
  const resp = await axios.post(`${MINDBODY_BASE_URL}/client/addclient`, payload, {
    headers: await authHeaders(siteId),
    timeout: 15000,
  });
  return resp.data?.Client ?? null;
}

/* ============================================================
   Add contact log
============================================================ */
async function addContactLog(siteId: number, params: { clientId: string; text: string; contactName: string }) {
  const typeId = process.env.MINDBODY_CONTACT_LOG_TYPE_ID
    ? Number(process.env.MINDBODY_CONTACT_LOG_TYPE_ID)
    : undefined;

  const payload: any = {
    ClientId: params.clientId,
    Text: params.text,
    ContactMethod: "Phone",
    ContactName: params.contactName,
    Test: false,
  };
  if (typeId !== undefined && Number.isFinite(typeId)) payload.TypeId = typeId;

  const resp = await axios.post(`${MINDBODY_BASE_URL}/client/addcontactlog`, payload, {
    headers: await authHeaders(siteId),
    timeout: 15000,
  });
  return resp.data ?? null;
}

/* ============================================================
   Build the contact-log text from the coach form fields
============================================================ */
function buildLogText(b: any): string {
  const parts = [
    "STRIDE website — coach intake",
    b.goals_csv ? `Goals: ${b.goals_csv}` : null,
    b.injuries ? `Anything to know (injuries/recovery):\n${String(b.injuries).trim()}` : null,
    b.story ? `Their story (why now / what they're chasing):\n${String(b.story).trim()}` : null,
  ].filter(Boolean);
  return parts.join("\n\n").slice(0, 3800);
}

/* ============================================================
   Handler
============================================================ */
export async function POST(req: Request) {
  try {
    // --- abuse guard: origin + public token ---
    const origin = req.headers.get("origin") || req.headers.get("referer") || "";
    const originOk = ALLOWED_ORIGINS.some((o) => o !== "https://X" && origin.startsWith(o));
    if (!originOk) {
      return NextResponse.json({ ok: false, error: "Origin not allowed" }, { status: 403 });
    }

    const body = await req.json().catch(() => ({}));

    if (PUBLIC_SUBMIT_TOKEN !== "X" && body?.token !== PUBLIC_SUBMIT_TOKEN) {
      return NextResponse.json({ ok: false, error: "Bad token" }, { status: 401 });
    }

    const studio = String(body?.studio || "").trim();
    const firstName = String(body?.first_name || "").trim();
    const phone = String(body?.phone || "").trim();

    const siteId = STUDIO_SITE_IDS[studio];
    if (!siteId || Number.isNaN(siteId)) {
      return NextResponse.json({ ok: false, error: `Unknown or unconfigured studio: ${studio}` }, { status: 400 });
    }
    if (!firstName) {
      return NextResponse.json({ ok: false, error: "Missing first_name" }, { status: 400 });
    }
    if (!last10(phone)) {
      return NextResponse.json({ ok: false, error: "Missing or invalid phone" }, { status: 400 });
    }

    // 1) Find or create the client (phone is the match key)
    let client = await findClientByPhone(siteId, phone);
    let status = "found";
    if (!client?.Id) {
      client = await createClient(siteId, {
        firstName,
        lastName: "(STRIDE Lead)", // form collects first name only; placeholder last name
        phone,
      });
      status = "created";
    }

    const clientId = client?.Id ? String(client.Id) : null;
    if (!clientId) {
      return NextResponse.json({ ok: false, error: "Could not find or create client" }, { status: 502 });
    }

    // 2) Write the contact log
    const mb = await addContactLog(siteId, {
      clientId,
      text: buildLogText(body),
      contactName: firstName,
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
