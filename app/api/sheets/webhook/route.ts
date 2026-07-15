import { NextResponse } from "next/server";
import crypto from "crypto";
import { neon } from "@neondatabase/serverless";
import { findClient, createClient } from "../../../../lib/mindbody";

export const runtime = "nodejs";

/*
 * CHANGE LOG (2026-06-16):
 *   handleSheetsCreate no longer trusts the loose findClient() result. New leads
 *   were attaching to unrelated existing clients because findClient() returns a
 *   loose searchText hit (name OR email OR phone) with no verification, unlike
 *   handleSheetsLookup which verifies phone and bails on ambiguity.
 *
 *   The Sheets create path now uses findClientForCreate(), which only treats a
 *   client as the same person when identity is verified:
 *     - exact match on a REAL email (single hit), OR
 *     - verified phone match (single hit) AND the name agrees.
 *   It never matches on name alone, and never on synthetic fallback email/phone
 *   (junk like "X", pending+...@strideautomation.com, 1556/555 dummy phones).
 *   A phone that matches a different-named person (shared household number) now
 *   creates a new client instead of attaching to that member.
 *
 * CHANGE LOG (2026-06-16, second pass):
 *   handleSheetsUpdateClient now protects an already-set Referred By. (unchanged)
 *
 * CHANGE LOG (2026-07-15): PHONE-MATCH / DUPLICATE-CREATION AUDIT
 *   Root cause of the duplicate-client reports was that findClientForCreate had
 *   become *too* strict / too narrow after the 2026-06-16 pass, so it failed to
 *   find real existing clients and then created a second copy. Fixes:
 *
 *   [FIX 1] findClientForCreate now searches the SAME set of phone candidates as
 *           handleSheetsLookup (tail-10, full-digits, and the raw formatted
 *           value), aggregating hits across candidates before filtering. Before,
 *           it searched only the 10-digit tail, so any client whose stored phone
 *           was indexed with a country code / formatting was missed.
 *
 *   [FIX 2] namesAgree now canonicalises common nicknames (Mike->Michael, etc.)
 *           and accepts a first-initial match, so a verified phone hit for the
 *           same person is no longer discarded (and duplicated) just because the
 *           first name is a nickname. Last-name equality is still required, which
 *           preserves the shared-household protection.
 *
 *   [FIX 3] mindbodyGetClientsBySearch now paginates (up to MB_SEARCH_MAX_RECORDS)
 *           instead of only seeing the first 20 rows. A phone searchText matches
 *           loosely on name/email/phone, so at a busy studio the true record
 *           could sit past row 20 and never reach the phone filter.
 *
 *   [FIX 4] handleSheetsCreate is now idempotent. It reserves a per-row lock in
 *           processed_sheet_rows_v1 (unique on site_id + sheet_id + row_number)
 *           BEFORE creating, and also dedupes on a real identity key
 *           (email/phone). This closes the retry / double-fire / Mindbody
 *           search-index-lag race that produced most duplicates even when the
 *           matching logic was correct.
 *
 *   NOTE: handleTypeform still calls the loose findClient(). Loose matching over-
 *   merges rather than duplicates, so it is not a source of the duplicate reports;
 *   migrating it to findClientForCreate() is tracked separately.
 */

const sql = neon(
  process.env.DATABASE_URL_V2 || process.env.DATABASE_URL || ""
);

const FALLBACK_EMAIL_DOMAIN = "strideautomation.com";
const FALLBACK_PHONE_PREFIX = "555";

// Default site for Sheets-originated flows when siteKey is missing.
// Kept for backward-compat with the original HB-only sheet.
const HB_SITE_ID = Number(process.env.MINDBODY_SITE_ID_HB || 0);

const MB_BASE = "https://api.mindbodyonline.com/public/v6";

// [FIX 3] Cap total records we page through per search. Mindbody's searchText is
// loose (matches name/email/phone), so the true phone/email match can be past
// the first page. 200 is plenty for a single first/last/phone/email lookup while
// bounding worst-case latency.
const MB_SEARCH_PAGE_SIZE = 100;
const MB_SEARCH_MAX_RECORDS = 200;

const PROTECTED_REFERRAL_TYPES = new Set([
  "met in person",
  "another client",
  "social media lead"
]);

/* ---------- Crypto / auth helpers ---------- */

function timingSafeEqual(a: string, b: string) {
  const aBuf = Buffer.from(a);
  const bBuf = Buffer.from(b);
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

async function verifyTypeform(req: Request, rawBody: string) {
  const secret = process.env.TYPEFORM_WEBHOOK_SECRET;
  if (!secret) throw new Error("Missing TYPEFORM_WEBHOOK_SECRET");
  const sigHeader =
    req.headers.get("typeform-signature") ||
    req.headers.get("Typeform-Signature");
  if (sigHeader) {
    const expected = crypto
      .createHmac("sha256", secret)
      .update(rawBody)
      .digest("base64");
    const provided = sigHeader.startsWith("sha256=")
      ? sigHeader.slice("sha256=".length)
      : sigHeader;
    return { ok: timingSafeEqual(provided, expected) };
  }
  const headerSecret = req.headers.get("typeform-secret");
  if (headerSecret && headerSecret === secret) return { ok: true };
  return { ok: false };
}

function verifySheetsSecret(req: Request) {
  const secret = process.env.SHEETS_WEBHOOK_SECRET;
  if (!secret) return { ok: false, reason: "Missing SHEETS_WEBHOOK_SECRET on server" };
  const provided =
    req.headers.get("x-sheets-secret") || req.headers.get("X-Sheets-Secret");
  if (!provided) return { ok: false, reason: "Missing x-sheets-secret header" };
  if (provided.length !== secret.length) return { ok: false, reason: "Bad secret" };
  if (!timingSafeEqual(provided, secret)) return { ok: false, reason: "Bad secret" };
  return { ok: true };
}

/* ---------- Multi-studio site resolution ---------- */

function resolveSiteId(siteKey: unknown): { ok: true; siteId: number } | { ok: false; reason: string } {
  const rawKey = String(siteKey || "").trim();
  if (!rawKey) {
    if (!HB_SITE_ID) {
      return { ok: false, reason: "Missing MINDBODY_SITE_ID_HB on server (no siteKey provided)" };
    }
    return { ok: true, siteId: HB_SITE_ID };
  }
  if (!/^[A-Za-z0-9_]+$/.test(rawKey)) {
    return { ok: false, reason: `Invalid siteKey: ${rawKey}` };
  }
  const envName = `MINDBODY_SITE_ID_${rawKey.toUpperCase()}`;
  const raw = process.env[envName];
  const parsed = Number(raw || 0);
  if (!parsed) {
    return { ok: false, reason: `Unknown siteKey "${rawKey}" (missing ${envName} on server)` };
  }
  return { ok: true, siteId: parsed };
}

function resolveReferralRelationshipId(
  siteKey: unknown
): { ok: true; relationshipId: number } | { ok: false; reason: string } {
  const rawKey = String(siteKey || "").trim();
  const readEnv = (envName: string): { ok: true; id: number } | { ok: false } => {
    const raw = process.env[envName];
    if (raw === undefined || raw === null || String(raw).trim() === "") return { ok: false };
    const parsed = Number(raw);
    if (!Number.isFinite(parsed) || parsed === 0) return { ok: false };
    return { ok: true, id: parsed };
  };
  if (!rawKey) {
    const hb = readEnv("MINDBODY_REFBY_RELATIONSHIP_ID_HB");
    if (!hb.ok) {
      return { ok: false, reason: "Missing/invalid MINDBODY_REFBY_RELATIONSHIP_ID_HB (no siteKey provided)" };
    }
    return { ok: true, relationshipId: hb.id };
  }
  if (!/^[A-Za-z0-9_]+$/.test(rawKey)) {
    return { ok: false, reason: `Invalid siteKey: ${rawKey}` };
  }
  const envName = `MINDBODY_REFBY_RELATIONSHIP_ID_${rawKey.toUpperCase()}`;
  const rel = readEnv(envName);
  if (!rel.ok) {
    return { ok: false, reason: `Missing/invalid ${envName} on server` };
  }
  return { ok: true, relationshipId: rel.id };
}

/* ---------- Generic helpers ---------- */

function normalize(s: string) {
  return s
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ")
    .replace(/&/g, "and");
}

function slugifyStudioName(s: string) {
  return normalize(s).replace(/[^a-z0-9]+/g, "");
}

function digitsOnly(s: string) {
  return (s || "").replace(/\D/g, "");
}

/*
 * Canonical 10-digit phone key, or "" if we cannot derive one. Single source of
 * truth for "are these the same phone" and for building identity keys.
 *
 * RULE (per product requirement): match on the LAST 10 digits. Taking the last
 * 10 digits inherently ignores any leading country code ("+1", "1", or other
 * prefixes) — so +15084467211, 1-508-446-7211, and (508) 446-7211 all reduce to
 * "5084467211" and resolve to the same profile.
 *
 * Examples: "+15084467211" -> "5084467211", "(508) 446-7211" -> "5084467211",
 *           "1 508 446 7211" -> "5084467211".
 *
 * CAVEAT: a value that carries a trailing extension ("508-446-7211 x4") would
 * put the extension in the last-10 window. Extensions are not expected in the
 * lead phone field; if they appear, strip them before this call.
 */
function canonicalPhone(s: string) {
  const d = digitsOnly(s);
  if (d.length < 10) return "";
  return d.slice(-10);
}

function phonesMatch(a: string, b: string) {
  const ca = canonicalPhone(a);
  const cb = canonicalPhone(b);
  if (!ca || !cb) return false;
  return ca === cb;
}

function makeDummyPhone(seed: string) {
  const hex = crypto.createHash("sha256").update(seed).digest("hex");
  const digits = hex.replace(/\D/g, "").padEnd(20, "0");
  const last7 = digits.slice(-7);
  return `${FALLBACK_PHONE_PREFIX}${last7}`;
}

function makeDummyEmail(seed: string) {
  const short = crypto.createHash("sha256").update(seed).digest("hex").slice(0, 10);
  return `pending+${short}@${FALLBACK_EMAIL_DOMAIN}`;
}

/* ---------- Identity-match guards ---------- */

function isFallbackEmail(email: string) {
  const e = email.trim().toLowerCase();
  return e.startsWith("pending+") || e.endsWith(`@${FALLBACK_EMAIL_DOMAIN}`);
}

/**
 * True for our synthetic fallback phones: Apps Script "1556" + 7 padded digits,
 * or webhook makeDummyPhone "555" + 7 digits. Real US numbers do not start 555,
 * and 556 is not an assigned area code, so this is safe on canonicalised digits.
 */
function isFallbackPhone(digits: string) {
  const d = digitsOnly(digits);
  return /^1556\d{7}$/.test(d) || /^555\d{7}$/.test(d);
}

/*
 * [FIX 2] Common nickname / formal-name equivalences. Bidirectional: we map both
 * sides to a canonical key before comparing. Keep this list conservative — only
 * unambiguous pairs, so we never merge genuinely different people.
 */
const NICKNAME_CANON: Record<string, string> = (() => {
  const groups: string[][] = [
    ["michael", "mike", "mikey"],
    ["robert", "rob", "bob", "bobby", "robbie"],
    ["william", "will", "bill", "billy", "willie"],
    ["richard", "rick", "rich", "dick", "ricky"],
    ["james", "jim", "jimmy", "jamie"],
    ["john", "johnny", "jack"],
    ["joseph", "joe", "joey"],
    ["charles", "charlie", "chuck"],
    ["thomas", "tom", "tommy"],
    ["christopher", "chris"],
    ["daniel", "dan", "danny"],
    ["matthew", "matt"],
    ["anthony", "tony"],
    ["david", "dave", "davey"],
    ["edward", "ed", "eddie", "ted"],
    ["steven", "stephen", "steve"],
    ["kenneth", "ken", "kenny"],
    ["nicholas", "nick", "nicky"],
    ["andrew", "andy", "drew"],
    ["benjamin", "ben", "benji"],
    ["samuel", "sam", "sammy"],
    ["alexander", "alex", "xander"],
    ["nathaniel", "nathan", "nate"],
    ["jonathan", "jon", "jonny"],
    ["timothy", "tim", "timmy"],
    ["gregory", "greg"],
    ["patrick", "pat"],
    ["elizabeth", "liz", "beth", "betsy", "eliza", "lisa"],
    ["katherine", "catherine", "kate", "katie", "kathy", "cathy", "kat"],
    ["margaret", "maggie", "meg", "peggy"],
    ["jennifer", "jen", "jenny"],
    ["jessica", "jess"],
    ["deborah", "deb", "debbie"],
    ["patricia", "pat", "patty", "trish"],
    ["susan", "sue", "susie"],
    ["barbara", "barb"],
    ["victoria", "vicky", "tori"],
    ["rebecca", "becca", "becky"],
    ["stephanie", "steph"],
    ["samantha", "sam"],
    ["alexandra", "alex", "lexi", "sandra"]
  ];
  const map: Record<string, string> = {};
  for (const g of groups) {
    const canon = g[0];
    for (const name of g) map[name] = canon;
  }
  return map;
})();

function canonFirstName(s: string) {
  const clean = normalize(s).replace(/[^a-z0-9]/g, "");
  return NICKNAME_CANON[clean] ?? clean;
}

/**
 * [FIX 2] Name agreement for phone-based matches. Still requires a real first and
 * last name on both sides and equal last names (preserves shared-household
 * protection). First names agree if they are equal, one is a clean prefix of the
 * other, they share a nickname canonical form, or they share a first initial when
 * one side is a single letter (e.g. "J" vs "John"). This stops us from creating a
 * duplicate for the same person just because they used a nickname.
 */
function namesAgree(
  leadFirst: string,
  leadLast: string,
  clientFirst: string,
  clientLast: string
) {
  const clean = (s: string) => normalize(s).replace(/[^a-z0-9]/g, "");
  const lf = clean(leadFirst);
  const ll = clean(leadLast);
  const cf = clean(clientFirst);
  const cl = clean(clientLast);
  if (!lf || !ll || !cf || !cl) return false;
  if (ll !== cl) return false;

  // Direct / prefix agreement.
  if (lf === cf || lf.startsWith(cf) || cf.startsWith(lf)) return true;

  // Nickname / formal-name equivalence.
  if (canonFirstName(leadFirst) === canonFirstName(clientFirst)) return true;

  // Single-initial agreement ("J Smith" vs "John Smith").
  if ((lf.length === 1 || cf.length === 1) && lf[0] === cf[0]) return true;

  return false;
}

function unauthorized(reason: string) {
  return NextResponse.json(
    { ok: false, error: `Unauthorized: ${reason}` },
    { status: 401 }
  );
}

function badRequest(error: string, extra?: Record<string, unknown>) {
  return NextResponse.json({ ok: false, error, ...(extra ?? {}) }, { status: 400 });
}

function serverError(error: string, extra?: Record<string, unknown>) {
  return NextResponse.json({ ok: false, error, ...(extra ?? {}) }, { status: 500 });
}

/* ---------- Typeform payload extraction ---------- */

function getAnswerValue(answer: any) {
  if (!answer) return null;
  switch (answer.type) {
    case "text": return answer.text ?? null;
    case "email": return answer.email ?? null;
    case "phone_number": return answer.phone_number ?? null;
    case "choice": return answer.choice?.label ?? null;
    case "choices": return answer.choices?.labels?.join(", ") ?? null;
    case "dropdown": return answer.dropdown?.label ?? null;
    case "boolean": return String(answer.boolean);
    case "number": return String(answer.number);
    case "url": return answer.url ?? null;
    case "date": return answer.date ?? null;
    default: return null;
  }
}

function extractLead(payload: any) {
  const formId = payload?.form_response?.form_id;
  const token = payload?.form_response?.token;
  const hidden = payload?.form_response?.hidden ?? {};
  const answers: any[] = payload?.form_response?.answers ?? [];
  const fields: any[] = payload?.form_response?.definition?.fields ?? [];

  const fieldById = new Map<string, any>();
  for (const f of fields) {
    if (f?.id) fieldById.set(f.id, f);
  }

  const getTitle = (answer: any) => {
    const fieldId = answer?.field?.id;
    const field = fieldId ? fieldById.get(fieldId) : null;
    return (field?.title ?? "").toString();
  };

  const getByRef = (ref: string) => {
    const a = answers.find((x) => x?.field?.ref === ref);
    return getAnswerValue(a);
  };

  const getByRefList = (refs: string[]) => {
    for (const ref of refs) {
      const value = getByRef(ref);
      if (value && value.toString().trim()) return value.toString().trim();
    }
    return null;
  };

  const findByTitleIncludes = (patterns: string[]) => {
    for (const answer of answers) {
      const title = normalize(getTitle(answer));
      if (patterns.some((p) => title.includes(normalize(p)))) {
        const value = getAnswerValue(answer);
        if (value && value.toString().trim()) return value.toString().trim();
      }
    }
    return null;
  };

  let firstName = (getByRefList(["first_name", "firstname", "first-name"]) ?? "").toString().trim();
  let lastName = (getByRefList(["last_name", "lastname", "last-name"]) ?? "").toString().trim();
  let email = getByRefList(["email", "email_address", "email-address"]);
  let phone = getByRefList(["phone", "phone_number", "phone-number", "mobile"]);

  let studioName =
    hidden.studio ||
    getByRefList([
      "studio",
      "studio_name",
      "studio-name",
      "location",
      "location_name",
      "location-name",
      "home_studio",
      "home-studio"
    ]);

  const attributionType =
    hidden.affiliate
      ? "affiliate"
      : hidden.coach
        ? "coach"
        : getByRefList(["affiliate", "affiliate_name", "affiliate-name"])
          ? "affiliate"
          : getByRefList(["coach", "coach_name", "coach-name"])
            ? "coach"
            : null;

  const attribution =
    hidden.affiliate ||
    hidden.coach ||
    getByRefList([
      "affiliate",
      "affiliate_name",
      "affiliate-name",
      "coach",
      "coach_name",
      "coach-name"
    ]) ||
    null;

  if (!firstName) firstName = findByTitleIncludes(["first name", "firstname"]) ?? "";
  if (!lastName) lastName = findByTitleIncludes(["last name", "lastname"]) ?? "";
  if (!email) {
    const a = answers.find((x) => x?.type === "email");
    email = a?.email ?? null;
  }
  if (!phone) {
    const a = answers.find((x) => x?.type === "phone_number");
    phone = a?.phone_number ?? null;
  }
  if (!studioName) {
    studioName = findByTitleIncludes([
      "studio",
      "location",
      "home studio",
      "which studio",
      "select studio",
      "choose studio"
    ]);
  }

  return {
    formId,
    token,
    firstName,
    lastName,
    email,
    phone,
    studioName,
    attribution,
    attributionType,
    answersCount: answers.length
  };
}

/* ---------- DB ---------- */

async function ensureTables() {
  if (!process.env.DATABASE_URL_V2 && !process.env.DATABASE_URL) {
    throw new Error("Missing DATABASE_URL_V2 or DATABASE_URL.");
  }
  await sql`
    create table if not exists studio_site_mappings (
      id bigserial primary key,
      studio_name text not null,
      studio_key text not null unique,
      site_id integer not null,
      is_active boolean default true,
      created_at timestamptz default now()
    );
  `;
  await sql`
    create table if not exists processed_submissions_v2 (
      id bigserial primary key,
      typeform_token text not null unique,
      form_id text,
      studio_name text,
      site_id integer,
      attribution text,
      attribution_type text,
      created_at timestamptz default now()
    );
  `;
  try {
    await sql`alter table processed_submissions_v2 add column if not exists attribution text`;
  } catch {}
  try {
    await sql`alter table processed_submissions_v2 add column if not exists attribution_type text`;
  } catch {}
}

/*
 * [FIX 4] Idempotency store for the Sheets create path. One row per sheet row
 * reserves the create, and one row per real identity key catches the same person
 * arriving on different sheet rows. Both are unique so concurrent requests race
 * on the DB, not on Mindbody's eventually-consistent search index.
 */
async function ensureSheetCreateTable() {
  if (!process.env.DATABASE_URL_V2 && !process.env.DATABASE_URL) {
    throw new Error("Missing DATABASE_URL_V2 or DATABASE_URL.");
  }
  await sql`
    create table if not exists processed_sheet_rows_v1 (
      id bigserial primary key,
      row_key text not null unique,
      identity_key text,
      site_id integer not null,
      sheet_id text,
      row_number integer,
      mb_client_id text,
      status text not null default 'in_progress',
      created_at timestamptz default now(),
      updated_at timestamptz default now()
    );
  `;
  // Index (NOT unique) to keep the identity lookup fast. row_key is the hard
  // exactly-once guarantee; identity is a best-effort reuse check (see below).
  // We deliberately avoid a unique constraint on identity: an INSERT that only
  // swallows conflicts on row_key would otherwise raise an *uncaught* unique
  // violation on a same-identity/different-row collision and 500 the request.
  try {
    await sql`
      create index if not exists processed_sheet_rows_v1_identity_idx
      on processed_sheet_rows_v1 (site_id, identity_key)
    `;
  } catch {}
}

async function getStudioMapping(studioName: string) {
  const studioKey = slugifyStudioName(studioName);
  const rows = await sql`
    select studio_name, studio_key, site_id, is_active
    from studio_site_mappings
    where studio_key = ${studioKey}
    limit 1
  `;
  return (rows as any)?.[0] ?? null;
}

/* ---------- Mindbody helpers ---------- */

async function getMindbodyStaffToken(siteId: number) {
  const apiKey = process.env.MINDBODY_API_KEY;
  const staffUsername = process.env.MINDBODY_STAFF_USERNAME;
  const staffPassword = process.env.MINDBODY_STAFF_PASSWORD;
  if (!apiKey || !staffUsername || !staffPassword) {
    throw new Error(
      "Missing MINDBODY_API_KEY / MINDBODY_STAFF_USERNAME / MINDBODY_STAFF_PASSWORD"
    );
  }
  const res = await fetch(`${MB_BASE}/usertoken/issue`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Api-Key": apiKey,
      SiteId: String(siteId)
    },
    body: JSON.stringify({ Username: staffUsername, Password: staffPassword })
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Mindbody usertoken/issue failed: ${res.status} ${text}`);
  }
  const data = JSON.parse(text);
  if (!data?.AccessToken) {
    throw new Error(`Mindbody usertoken/issue returned no AccessToken: ${text}`);
  }
  return String(data.AccessToken);
}

type MbResult = { status: number; ok: boolean; data: any; text: string };

async function mindbodyFetch(
  siteId: number,
  path: string,
  init: { method: "GET" | "POST"; body?: any; query?: Record<string, string> }
): Promise<MbResult> {
  const apiKey = process.env.MINDBODY_API_KEY;
  if (!apiKey) throw new Error("Missing MINDBODY_API_KEY");
  const token = await getMindbodyStaffToken(siteId);
  const url = new URL(`${MB_BASE}${path}`);
  if (init.query) {
    for (const [k, v] of Object.entries(init.query)) {
      url.searchParams.set(k, v);
    }
  }
  const res = await fetch(url.toString(), {
    method: init.method,
    headers: {
      "Content-Type": "application/json",
      "Api-Key": apiKey,
      SiteId: String(siteId),
      Authorization: token
    },
    body: init.body ? JSON.stringify(init.body) : undefined
  });
  const text = await res.text();
  let data: any = null;
  try {
    data = JSON.parse(text);
  } catch {}
  return { status: res.status, ok: res.ok, data, text };
}

/**
 * [FIX 3] Search clients by text, paging through results up to
 * MB_SEARCH_MAX_RECORDS. Returns a synthetic MbResult whose data.Clients holds
 * the accumulated rows. Because searchText matches loosely across name/email/
 * phone, the true match can be past the first page at a busy studio; only paging
 * guarantees the phone/email filter downstream can see it.
 */
async function mindbodyGetClientsBySearch(siteId: number, searchText: string): Promise<MbResult> {
  const allClients: any[] = [];
  let offset = 0;
  let last: MbResult | null = null;

  while (offset < MB_SEARCH_MAX_RECORDS) {
    const res = await mindbodyFetch(siteId, "/client/clients", {
      method: "GET",
      query: {
        "request.searchText": searchText,
        "request.limit": String(MB_SEARCH_PAGE_SIZE),
        "request.offset": String(offset),
        "request.includeInactive": "true"
      }
    });
    last = res;
    if (!res.ok) {
      // Surface the failure as-is; callers already handle non-OK.
      return res;
    }
    const batch: any[] = res.data?.Clients ?? [];
    allClients.push(...batch);

    const totalResults = Number(res.data?.PaginationResponse?.TotalResults ?? NaN);
    offset += MB_SEARCH_PAGE_SIZE;

    // Stop when the page was short, or we've reached the reported total.
    if (batch.length < MB_SEARCH_PAGE_SIZE) break;
    if (Number.isFinite(totalResults) && offset >= totalResults) break;
  }

  const base = last ?? { status: 200, ok: true, data: {}, text: "" };
  return {
    status: base.status,
    ok: base.ok,
    data: { ...(base.data ?? {}), Clients: allClients },
    text: base.text
  };
}

async function mindbodyUpdateClient(
  siteId: number,
  client: {
    mbClientId: string;
    firstName?: string;
    lastName?: string;
    referredBy?: string;
    salesRep?: string | number;
  }
) {
  const clientBody: any = { Id: client.mbClientId };
  if (client.firstName) clientBody.FirstName = client.firstName;
  if (client.lastName) clientBody.LastName = client.lastName;
  if (client.referredBy) clientBody.ReferredBy = client.referredBy;
  if (client.salesRep) {
    const repId = Number(client.salesRep);
    if (Number.isFinite(repId) && repId > 0) {
      clientBody.SalesReps = [{ Id: repId }];
    }
  }
  return mindbodyFetch(siteId, "/client/updateclient", {
    method: "POST",
    body: { Client: clientBody, CrossRegionalUpdate: false }
  });
}

async function mindbodyGetClientCompleteInfo(siteId: number, clientId: string) {
  return mindbodyFetch(siteId, "/client/clientcompleteinfo", {
    method: "GET",
    query: { ClientId: clientId }
  });
}

async function mindbodyAddReferralRelationship(
  siteId: number,
  args: {
    mbClientId: string;
    firstName: string;
    lastName: string;
    referrerClientId: string;
    relationshipId: number;
  }
) {
  const clientBody: any = {
    Id: args.mbClientId,
    FirstName: args.firstName,
    LastName: args.lastName,
    ClientRelationships: [
      {
        RelatedClientId: String(args.referrerClientId),
        Relationship: { Id: args.relationshipId },
        RelationshipName: "Referred By"
      }
    ]
  };
  return mindbodyFetch(siteId, "/client/updateclient", {
    method: "POST",
    body: { Client: clientBody, CrossRegionalUpdate: false }
  });
}

/*
 * [FIX 1] Build the ordered, de-duplicated list of phone search terms. Mirrors
 * (and extends) what handleSheetsLookup tries, so the create path can never find
 * fewer clients than the lookup path.
 */
function phoneSearchCandidates(rawPhone: string) {
  const digits = digitsOnly(rawPhone);
  const canon = canonicalPhone(rawPhone);
  return Array.from(
    new Set(
      [
        canon, // 10-digit canonical
        digits, // full digit string (may include country code)
        rawPhone.trim() // raw formatted value, in case MB indexed it that way
      ].filter((s) => s && s.length >= 7)
    )
  );
}

/**
 * Strict find-for-create. Only treats a Mindbody client as the same person when
 * identity is verified:
 *   1. Real-email exact match, single hit  -> reuse.
 *   2. Verified phone match, single hit, AND name agrees -> reuse.
 * A phone that matches a different-named person (shared household number) is NOT
 * reused. See FIX 1/2/3 in the change log for what changed on 2026-07-15.
 */
async function findClientForCreate(
  siteId: number,
  args: {
    firstName: string;
    lastName: string;
    email: string;
    phone: string; // digits only (already substituted/validated by caller)
    rawPhone: string; // original formatted value for search
    emailIsReal: boolean;
    phoneIsReal: boolean;
  }
): Promise<
  | { kind: "match"; id: string; via: "email" | "phone" }
  | { kind: "ambiguous"; via: "email" | "phone"; count: number }
  | { kind: "none" }
> {
  const emailLower = args.email.trim().toLowerCase();

  // 1. Real-email exact match.
  if (args.emailIsReal && emailLower) {
    const res = await mindbodyGetClientsBySearch(siteId, emailLower);
    if (res.ok) {
      const clients: any[] = res.data?.Clients ?? [];
      const exact = clients.filter(
        (c) => String(c?.Email || "").trim().toLowerCase() === emailLower
      );
      console.log("findClientForCreate email pass", {
        emailLower,
        returned: clients.length,
        exact: exact.length
      });
      if (exact.length === 1) return { kind: "match", id: String(exact[0].Id), via: "email" };
      if (exact.length > 1) return { kind: "ambiguous", via: "email", count: exact.length };
    }
  }

  // 2. Verified phone match, gated on name agreement.
  if (args.phoneIsReal && args.phone) {
    // [FIX 1] Aggregate hits across ALL phone search candidates before filtering,
    // and de-dupe clients by Id so multi-candidate overlap doesn't inflate counts.
    const byId = new Map<string, any>();
    const candidates = phoneSearchCandidates(args.rawPhone || args.phone);
    for (const searchText of candidates) {
      const res = await mindbodyGetClientsBySearch(siteId, searchText);
      if (!res.ok) continue;
      for (const c of (res.data?.Clients ?? [])) {
        if (c?.Id != null) byId.set(String(c.Id), c);
      }
    }
    const clients = Array.from(byId.values());

    const phoneMatches = clients.filter((c) => {
      const cand = [c?.MobilePhone, c?.HomePhone, c?.WorkPhone].filter(Boolean);
      return cand.some((p: string) => phonesMatch(p, args.phone));
    });
    const nameMatches = phoneMatches.filter((c) =>
      namesAgree(
        args.firstName,
        args.lastName,
        String(c?.FirstName || ""),
        String(c?.LastName || "")
      )
    );
    console.log("findClientForCreate phone pass", {
      phoneTail: canonicalPhone(args.phone),
      searchCandidates: candidates.length,
      returned: clients.length,
      phoneMatches: phoneMatches.length,
      nameMatches: nameMatches.length
    });

    // De-dupe the accepted matches by Id (defensive) before deciding.
    const nameMatchIds = Array.from(new Set(nameMatches.map((c) => String(c.Id))));
    if (nameMatchIds.length === 1) return { kind: "match", id: nameMatchIds[0], via: "phone" };
    if (nameMatchIds.length > 1) return { kind: "ambiguous", via: "phone", count: nameMatchIds.length };
    // phone matched but no name agreement -> treat as a new person.
  }

  return { kind: "none" };
}

/* ---------- Sheets -> Mindbody: lookup by phone ---------- */

async function handleSheetsLookup(req: Request, payload: any) {
  const auth = verifySheetsSecret(req);
  if (!auth.ok) return unauthorized(auth.reason!);

  const site = resolveSiteId(payload?.siteKey);
  if (!site.ok) return badRequest(site.reason);
  const siteId = site.siteId;

  const lead = payload?.lead ?? {};
  const rawPhone = String(lead?.phone || "");
  const phone = digitsOnly(rawPhone);
  if (!phone) return badRequest("Missing lead.phone");

  console.log("sheets lookup request", {
    siteKey: payload?.siteKey ?? null,
    siteId,
    rowNumber: payload?.rowNumber,
    backfill: !!payload?.backfill,
    phoneDigits: phone
  });

  try {
    // [FIX 1] Use the shared candidate builder so lookup and create stay in sync.
    const searchCandidates = phoneSearchCandidates(rawPhone);

    // Aggregate + de-dupe across candidates before verifying the phone, so a
    // single match found on candidate B isn't hidden by noise from candidate A.
    const byId = new Map<string, any>();
    let anyOk = false;
    let lastResult: MbResult | null = null;
    for (const searchText of searchCandidates) {
      const result = await mindbodyGetClientsBySearch(siteId, searchText);
      lastResult = result;
      if (!result.ok) {
        console.log("mindbody getClients non-OK", {
          status: result.status,
          bodySample: result.text?.slice(0, 300)
        });
        continue;
      }
      anyOk = true;
      const clients: any[] = result.data?.Clients ?? [];
      console.log("mindbody getClients returned", { searchText, count: clients.length });
      for (const c of clients) {
        if (c?.Id != null) byId.set(String(c.Id), c);
      }
    }

    const phoneMatches = Array.from(byId.values()).filter((c) => {
      const candidates = [c?.MobilePhone, c?.HomePhone, c?.WorkPhone].filter(Boolean);
      return candidates.some((p: string) => phonesMatch(p, phone));
    });

    if (phoneMatches.length === 1) {
      return NextResponse.json({
        ok: true,
        status: "found",
        mbClientId: String(phoneMatches[0].Id),
        siteId
      });
    }
    if (phoneMatches.length > 1) {
      return NextResponse.json({
        ok: true,
        status: "ambiguous",
        message: `Multiple Mindbody clients matched phone ${phone}`,
        count: phoneMatches.length
      });
    }

    if (!anyOk && lastResult && !lastResult.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: `Mindbody getClients failed: ${lastResult.status}`,
          mindbody: lastResult.data ?? lastResult.text
        },
        { status: 502 }
      );
    }
    return NextResponse.json({ ok: true, status: "not_found" });
  } catch (err: any) {
    console.log("sheets lookup error", { message: err?.message, stack: err?.stack });
    return serverError(err?.message ?? "Lookup failed");
  }
}

/* ---------- Sheets -> Mindbody: updateClient ---------- */

async function handleSheetsUpdateClient(req: Request, payload: any) {
  const auth = verifySheetsSecret(req);
  if (!auth.ok) return unauthorized(auth.reason!);

  const site = resolveSiteId(payload?.siteKey);
  if (!site.ok) return badRequest(site.reason);
  const siteId = site.siteId;

  const client = payload?.client ?? {};
  const mbClientId = String(client?.mbClientId || "").trim();
  if (!mbClientId) return badRequest("Missing client.mbClientId");

  const firstName = String(client?.firstName || "").trim();
  const lastName = String(client?.lastName || "").trim();
  const referredBy = String(client?.referredBy || "").trim();
  const salesRepRaw = client?.salesRep;

  if (!referredBy && !salesRepRaw) {
    return badRequest("Nothing to update (referredBy and salesRep both empty)");
  }

  let referredByToSend: string | undefined = referredBy || undefined;
  let keptReferredBy: string | null = null;
  if (referredByToSend) {
    const info = await mindbodyGetClientCompleteInfo(siteId, mbClientId);
    if (info.ok) {
      const c = info.data?.Client ?? info.data ?? {};
      const current = String(c?.ReferredBy ?? "").trim();
      if (current && PROTECTED_REFERRAL_TYPES.has(current.toLowerCase())) {
        referredByToSend = undefined;
        keptReferredBy = current;
      }
    } else {
      console.log("updateClient protection read failed; skipping referredBy", {
        mbClientId,
        status: info.status
      });
      referredByToSend = undefined;
      keptReferredBy = "unknown (read failed)";
    }
  }

  if (!referredByToSend && !salesRepRaw) {
    console.log("sheets updateClient skipped (protected ReferredBy)", {
      mbClientId,
      siteId,
      keptReferredBy
    });
    return NextResponse.json({
      ok: true,
      status: "skipped_protected",
      mbClientId,
      siteId,
      keptReferredBy
    });
  }

  console.log("sheets updateClient request", {
    siteKey: payload?.siteKey ?? null,
    siteId,
    rowNumber: payload?.rowNumber,
    mbClientId,
    hasFirstName: !!firstName,
    hasLastName: !!lastName,
    referredBy: referredByToSend || null,
    keptReferredBy,
    salesRep: salesRepRaw || null
  });

  try {
    const result = await mindbodyUpdateClient(siteId, {
      mbClientId,
      firstName,
      lastName,
      referredBy: referredByToSend,
      salesRep: salesRepRaw || undefined
    });
    console.log("mindbody updateClient result", {
      status: result.status,
      ok: result.ok,
      bodySample: result.text?.slice(0, 300)
    });
    if (!result.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: `Mindbody updateClient failed: ${result.status}`,
          mindbody: result.data ?? result.text
        },
        { status: 502 }
      );
    }
    return NextResponse.json({
      ok: true,
      status: "updated",
      mbClientId,
      siteId,
      keptReferredBy,
      mindbody: result.data
    });
  } catch (err: any) {
    console.log("sheets updateClient error", { message: err?.message, stack: err?.stack });
    return serverError(err?.message ?? "Update failed");
  }
}

/* ---------- Sheets -> Mindbody: link referral relationship ---------- */

async function handleSheetsLinkReferral(req: Request, payload: any) {
  const auth = verifySheetsSecret(req);
  if (!auth.ok) return unauthorized(auth.reason!);

  const site = resolveSiteId(payload?.siteKey);
  if (!site.ok) return badRequest(site.reason);
  const siteId = site.siteId;

  const rel = resolveReferralRelationshipId(payload?.siteKey);
  if (!rel.ok) return badRequest(rel.reason);
  const relationshipId = rel.relationshipId;

  const mbClientId = String(payload?.mbClientId || "").trim();
  const referrerEmail = String(payload?.referrerEmail || "").trim().toLowerCase();
  if (!mbClientId) return badRequest("Missing mbClientId");
  if (!referrerEmail) return badRequest("Missing referrerEmail");

  console.log("sheets linkReferral request", {
    siteKey: payload?.siteKey ?? null,
    siteId,
    relationshipId,
    mbClientId,
    referrerEmail
  });

  try {
    const search = await mindbodyGetClientsBySearch(siteId, referrerEmail);
    if (!search.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: `Mindbody getClients failed: ${search.status}`,
          mindbody: search.data ?? search.text
        },
        { status: 502 }
      );
    }
    const clients: any[] = search.data?.Clients ?? [];
    const emailMatches = clients.filter(
      (c) => String(c?.Email || "").trim().toLowerCase() === referrerEmail
    );
    if (emailMatches.length === 0) {
      return NextResponse.json({ ok: true, status: "referrer_not_found", referrerEmail });
    }
    if (emailMatches.length > 1) {
      return NextResponse.json({
        ok: true,
        status: "referrer_ambiguous",
        count: emailMatches.length,
        candidates: emailMatches.map((c) => String(c.Id))
      });
    }
    const referrerClientId = String(emailMatches[0].Id);
    if (referrerClientId === mbClientId) {
      return NextResponse.json({ ok: true, status: "self_referral", mbClientId });
    }

    const info = await mindbodyGetClientCompleteInfo(siteId, mbClientId);
    if (!info.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: `Mindbody clientcompleteinfo failed: ${info.status}`,
          mindbody: info.data ?? info.text
        },
        { status: 502 }
      );
    }
    const c = info.data?.Client ?? info.data ?? {};
    const firstName = String(c?.FirstName || "").trim();
    const lastName = String(c?.LastName || "").trim();
    if (!firstName || !lastName) {
      return serverError("Could not read FirstName/LastName for the new client; UpdateClient would fail");
    }
    const existingRels: any[] = c?.ClientRelationships ?? [];
    const alreadyLinked = existingRels.some(
      (r) =>
        String(r?.RelatedClientId) === referrerClientId &&
        Number(r?.Relationship?.Id) === Number(relationshipId)
    );
    if (alreadyLinked) {
      return NextResponse.json({ ok: true, status: "already_linked", mbClientId, referrerClientId });
    }

    const result = await mindbodyAddReferralRelationship(siteId, {
      mbClientId,
      firstName,
      lastName,
      referrerClientId,
      relationshipId
    });
    console.log("mindbody addReferralRelationship result", {
      status: result.status,
      ok: result.ok,
      bodySample: result.text?.slice(0, 300)
    });
    if (!result.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: `Mindbody updateClient failed: ${result.status}`,
          mindbody: result.data ?? result.text
        },
        { status: 502 }
      );
    }
    return NextResponse.json({
      ok: true,
      status: "linked",
      mbClientId,
      referrerClientId,
      siteId
    });
  } catch (err: any) {
    console.log("sheets linkReferral error", { message: err?.message, stack: err?.stack });
    return serverError(err?.message ?? "Link failed");
  }
}

/* ---------- Sheets -> Mindbody: get client relationships (discovery) ---------- */

async function handleSheetsGetRelationships(req: Request, payload: any) {
  const auth = verifySheetsSecret(req);
  if (!auth.ok) return unauthorized(auth.reason!);

  const site = resolveSiteId(payload?.siteKey);
  if (!site.ok) return badRequest(site.reason);
  const siteId = site.siteId;

  const mbClientId = String(payload?.mbClientId || "").trim();
  if (!mbClientId) return badRequest("Missing mbClientId");

  console.log("sheets getRelationships request", {
    siteKey: payload?.siteKey ?? null,
    siteId,
    mbClientId
  });

  try {
    const info = await mindbodyGetClientCompleteInfo(siteId, mbClientId);
    if (!info.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: `Mindbody clientcompleteinfo failed: ${info.status}`,
          mindbody: info.data ?? info.text
        },
        { status: 502 }
      );
    }
    const c = info.data?.Client ?? info.data ?? {};
    const rels: any[] = c?.ClientRelationships ?? [];
    const relationships = rels.map((r) => ({
      relationshipId: r?.Relationship?.Id ?? null,
      name1: r?.Relationship?.RelationshipName1 ?? null,
      name2: r?.Relationship?.RelationshipName2 ?? null,
      relatedClientId: r?.RelatedClientId ?? null,
      relationshipName: r?.RelationshipName ?? null
    }));
    return NextResponse.json({
      ok: true,
      status: "relationships",
      mbClientId,
      siteId,
      count: relationships.length,
      relationships
    });
  } catch (err: any) {
    console.log("sheets getRelationships error", { message: err?.message, stack: err?.stack });
    return serverError(err?.message ?? "Get relationships failed");
  }
}

/* ---------- Sheets -> Mindbody: find-or-create from Paid Leads row ---------- */

async function handleSheetsCreate(req: Request, payload: any) {
  const auth = verifySheetsSecret(req);
  if (!auth.ok) return unauthorized(auth.reason!);

  const site = resolveSiteId(payload?.siteKey);
  if (!site.ok) return badRequest(site.reason);
  const siteId = site.siteId;

  const lead = payload?.lead ?? {};
  const firstName = String(lead?.firstName || "").trim();
  const lastName = String(lead?.lastName || "").trim();
  if (!firstName || !lastName) {
    return badRequest("Missing lead.firstName or lead.lastName");
  }

  const rawEmail = String(lead?.email || "").trim();
  const rawPhone = String(lead?.phone || "").trim();
  const phoneDigitsRaw = digitsOnly(rawPhone);
  const phoneCanon = canonicalPhone(rawPhone);

  // Decide which identifiers are REAL and therefore safe to match on.
  const emailIsReal = rawEmail.includes("@") && !isFallbackEmail(rawEmail);
  const phoneIsReal = !!phoneCanon && !isFallbackPhone(phoneCanon);

  // For the actual create, substitute deterministic fallbacks for anything not
  // real, so we never write "X" into Mindbody as an email/phone.
  const seed = `${payload?.sheetId}:${payload?.rowNumber}:${firstName}:${lastName}`;
  const email = emailIsReal ? rawEmail : makeDummyEmail(seed);
  const phone = phoneIsReal ? phoneCanon : digitsOnly(makeDummyPhone(seed));

  // [FIX 4] Idempotency keys.
  //   rowKey   -> exactly-once per sheet row (kills retry / double-fire dupes).
  //   identityKey -> exactly-once per real person (kills same-person-two-rows dupes
  //                  and covers Mindbody's eventually-consistent search index).
  const sheetIdStr = String(payload?.sheetId ?? "");
  const rowNumberNum = Number(payload?.rowNumber);
  const rowKey = `${siteId}:${sheetIdStr}:${payload?.rowNumber ?? ""}`;
  const identityKey = emailIsReal
    ? `email:${email.toLowerCase()}`
    : phoneIsReal
      ? `phone:${phone}`
      : null;

  console.log("sheets create request", {
    siteKey: payload?.siteKey ?? null,
    siteId,
    rowNumber: payload?.rowNumber,
    backfill: !!payload?.backfill,
    firstName,
    lastName,
    email,
    phoneDigits: phone,
    emailIsReal,
    phoneIsReal,
    rowKey,
    identityKey,
    leadSource: lead?.leadSource ?? null,
    referralType: lead?.referralType ?? null,
    salesRep: lead?.salesRep ?? null
  });

  try {
    await ensureSheetCreateTable();

    // --- Reserve the row (exactly-once). If it already exists, short-circuit. ---
    const reserved = await sql`
      insert into processed_sheet_rows_v1 (row_key, identity_key, site_id, sheet_id, row_number, status)
      values (${rowKey}, ${identityKey}, ${siteId}, ${sheetIdStr}, ${Number.isFinite(rowNumberNum) ? rowNumberNum : null}, 'in_progress')
      on conflict (row_key) do nothing
      returning id
    `;
    if (!(reserved as any)?.length) {
      // Another request already owns this row. Return its outcome if known.
      const existingRows = await sql`
        select mb_client_id, status from processed_sheet_rows_v1 where row_key = ${rowKey} limit 1
      `;
      const existing = (existingRows as any)?.[0];
      if (existing?.mb_client_id) {
        return NextResponse.json({
          ok: true,
          status: "exists",
          matchedVia: "idempotency_row",
          mbClientId: String(existing.mb_client_id),
          siteId
        });
      }
      return NextResponse.json({
        ok: true,
        status: "in_progress",
        message: "This sheet row is already being processed",
        rowKey,
        siteId
      });
    }

    // --- Reserve the identity (exactly-once per real person across rows). ---
    // If another row already claimed this identity, reuse its client id instead
    // of creating a second Mindbody client.
    if (identityKey) {
      const claimedRows = await sql`
        select mb_client_id, status, row_key
        from processed_sheet_rows_v1
        where site_id = ${siteId} and identity_key = ${identityKey} and row_key <> ${rowKey}
        order by created_at asc
        limit 1
      `;
      const claimed = (claimedRows as any)?.[0];
      if (claimed?.mb_client_id) {
        await sql`
          update processed_sheet_rows_v1
          set mb_client_id = ${String(claimed.mb_client_id)}, status = 'exists', updated_at = now()
          where row_key = ${rowKey}
        `;
        return NextResponse.json({
          ok: true,
          status: "exists",
          matchedVia: "idempotency_identity",
          mbClientId: String(claimed.mb_client_id),
          siteId
        });
      }
      if (claimed && claimed.status === "in_progress") {
        // Another row with the same real identity is mid-flight and hasn't
        // written its client id yet. Don't create in parallel — release our own
        // reservation and let this row retry once the other finishes.
        await sql`delete from processed_sheet_rows_v1 where row_key = ${rowKey}`;
        return NextResponse.json({
          ok: true,
          status: "in_progress",
          message: "Another row with the same identity is being processed; retry shortly",
          identityKey,
          siteId
        });
      }
    }

    // --- Verified find against Mindbody. ---
    const found = await findClientForCreate(siteId, {
      firstName,
      lastName,
      email,
      phone,
      rawPhone,
      emailIsReal,
      phoneIsReal
    });

    if (found.kind === "ambiguous") {
      await sql`
        update processed_sheet_rows_v1
        set status = 'ambiguous', updated_at = now()
        where row_key = ${rowKey}
      `;
      return NextResponse.json({
        ok: true,
        status: "ambiguous",
        via: found.via,
        count: found.count,
        siteId,
        message: `Multiple Mindbody clients matched on ${found.via}; not auto-linking`
      });
    }

    if (found.kind === "match") {
      await sql`
        update processed_sheet_rows_v1
        set mb_client_id = ${found.id}, status = 'exists', updated_at = now()
        where row_key = ${rowKey}
      `;
      return NextResponse.json({
        ok: true,
        status: "exists",
        matchedVia: found.via,
        mbClientId: found.id,
        siteId,
        fallbacksUsed: {
          emailWasFallback: !emailIsReal,
          phoneWasFallback: !phoneIsReal
        }
      });
    }

    // --- Create. ---
    let created: any = null;
    try {
      created = await createClient(siteId, { firstName, lastName, email, phone });
    } catch (createErr) {
      // Roll back our reservation so a later retry can try again rather than
      // being stuck as a permanent 'in_progress' phantom.
      await sql`delete from processed_sheet_rows_v1 where row_key = ${rowKey}`;
      throw createErr;
    }
    if (!created?.Id) {
      await sql`delete from processed_sheet_rows_v1 where row_key = ${rowKey}`;
      return serverError("Mindbody create failed");
    }

    await sql`
      update processed_sheet_rows_v1
      set mb_client_id = ${String(created.Id)}, status = 'created', updated_at = now()
      where row_key = ${rowKey}
    `;

    return NextResponse.json({
      ok: true,
      status: "created",
      mbClientId: String(created.Id),
      siteId,
      fallbacksUsed: {
        emailWasFallback: !emailIsReal,
        phoneWasFallback: !phoneIsReal
      }
    });
  } catch (err: any) {
    const status = err?.response?.status ?? null;
    const data = err?.response?.data ?? null;
    const where = typeof err?.config?.url === "string" ? err.config.url : null;
    console.log("sheets create mindbody error", { status, where, data });
    return NextResponse.json(
      {
        ok: false,
        error: err?.message ?? "Server error",
        mindbody: { status, where, data }
      },
      { status: 500 }
    );
  }
}

/* ---------- Typeform handler ---------- */

async function handleTypeform(req: Request, rawBody: string) {
  const verification = await verifyTypeform(req, rawBody);
  console.log("verification result:", verification);
  if (!verification.ok) {
    return unauthorized("Typeform verification failed");
  }

  let payload: any;
  try {
    payload = JSON.parse(rawBody);
  } catch (parseErr) {
    console.log("payload parse failed:", parseErr);
    return badRequest("Invalid JSON");
  }

  const lead = extractLead(payload);
  console.log("lead extracted:", {
    formId: lead.formId,
    token: lead.token,
    firstName: lead.firstName,
    lastName: lead.lastName,
    email: lead.email,
    phone: lead.phone,
    studioName: lead.studioName,
    attribution: lead.attribution,
    attributionType: lead.attributionType,
    answersCount: lead.answersCount
  });

  if (!lead.formId || !lead.token || lead.answersCount === 0) {
    return NextResponse.json({ ok: true, status: "typeform_test_ok" });
  }

  const baseExtracted = {
    firstName: lead.firstName,
    lastName: lead.lastName,
    email: lead.email,
    phone: lead.phone,
    studioName: lead.studioName,
    attribution: lead.attribution,
    attributionType: lead.attributionType
  };

  if (!lead.firstName || !lead.lastName) {
    return badRequest("Missing firstName or lastName from Typeform payload", {
      extracted: baseExtracted
    });
  }
  if (!lead.studioName) {
    return badRequest("Missing studioName from Typeform payload", {
      extracted: baseExtracted
    });
  }
  if (!lead.attribution) {
    return badRequest("Missing attribution from Typeform payload", {
      extracted: baseExtracted
    });
  }

  await ensureTables();

  const mapping = await getStudioMapping(lead.studioName);
  console.log("studio mapping result:", mapping);
  if (!mapping || mapping.is_active === false) {
    return NextResponse.json({
      ok: true,
      status: "routed",
      routedTo: null,
      message: "No active site mapping for this studio",
      studioName: lead.studioName,
      studioKey: slugifyStudioName(lead.studioName),
      attribution: lead.attribution,
      attributionType: lead.attributionType
    });
  }

  const siteId = Number(mapping.site_id);

  try {
    await sql`
      insert into processed_submissions_v2 (
        typeform_token, form_id, studio_name, site_id, attribution, attribution_type
      )
      values (
        ${lead.token},
        ${lead.formId ?? null},
        ${lead.studioName},
        ${siteId},
        ${lead.attribution},
        ${lead.attributionType}
      )
    `;
    console.log("inserted processed submission", {
      token: lead.token,
      studioName: lead.studioName,
      siteId
    });
  } catch (insertErr) {
    console.log("processed submission insert failed, likely deduped:", insertErr);
    return NextResponse.json({
      ok: true,
      status: "deduped",
      routedTo: { studioName: mapping.studio_name, siteId },
      attribution: lead.attribution,
      attributionType: lead.attributionType
    });
  }

  const normalizedEmail =
    lead.email && lead.email.trim().length > 0 ? lead.email.trim() : makeDummyEmail(lead.token);
  const normalizedPhoneRaw =
    lead.phone && lead.phone.trim().length > 0 ? lead.phone.trim() : makeDummyPhone(lead.token);
  const normalizedPhone = digitsOnly(normalizedPhoneRaw);

  console.log("about to search/create in MB", {
    siteId,
    firstName: lead.firstName,
    lastName: lead.lastName,
    email: normalizedEmail,
    phone: normalizedPhone
  });

  try {
    // NOTE (2026-07-15): Typeform still uses the loose findClient(). Loose matching
    // over-merges rather than duplicates, so it is not implicated in the duplicate
    // reports; the token dedup above already blocks the retry case. Migrating this
    // to findClientForCreate() is tracked separately.
    const existing = await findClient(siteId, {
      firstName: lead.firstName,
      lastName: lead.lastName,
      email: normalizedEmail,
      phone: normalizedPhone
    });
    if (existing?.Id) {
      return NextResponse.json({
        ok: true,
        status: "exists",
        mbClientId: String(existing.Id),
        routedTo: { studioName: mapping.studio_name, siteId },
        attribution: lead.attribution,
        attributionType: lead.attributionType,
        fallbacksUsed: {
          emailWasFallback: !lead.email,
          phoneWasFallback: !lead.phone
        }
      });
    }

    const created = await createClient(siteId, {
      firstName: lead.firstName,
      lastName: lead.lastName,
      email: normalizedEmail,
      phone: normalizedPhone
    });
    if (!created?.Id) return serverError("Mindbody create failed");

    return NextResponse.json({
      ok: true,
      status: "created",
      mbClientId: String(created.Id),
      routedTo: { studioName: mapping.studio_name, siteId },
      attribution: lead.attribution,
      attributionType: lead.attributionType,
      fallbacksUsed: {
        emailWasFallback: !lead.email,
        phoneWasFallback: !lead.phone
      }
    });
  } catch (err: any) {
    const status = err?.response?.status ?? null;
    const data = err?.response?.data ?? null;
    const where = typeof err?.config?.url === "string" ? err.config.url : null;
    console.log("mindbody error:", { status, where, data });
    return NextResponse.json(
      {
        ok: false,
        error: err?.message ?? "Server error",
        mindbody: { status, where, data },
        routedTo: { studioName: mapping.studio_name, siteId },
        attribution: lead.attribution,
        attributionType: lead.attributionType
      },
      { status: 500 }
    );
  }
}

/* ---------- POST entrypoint ---------- */

export async function POST(req: Request) {
  try {
    console.log("🔥 webhook hit");
    console.log("env check:", {
      hasDatabaseUrlV2: !!process.env.DATABASE_URL_V2,
      hasDatabaseUrl: !!process.env.DATABASE_URL,
      hasTypeformSecret: !!process.env.TYPEFORM_WEBHOOK_SECRET,
      hasSheetsSecret: !!process.env.SHEETS_WEBHOOK_SECRET,
      hasHbSiteId: !!HB_SITE_ID,
      hasTustinSiteId: !!process.env.MINDBODY_SITE_ID_TUSTIN,
      hasSouthlandsSiteId: !!process.env.MINDBODY_SITE_ID_SOUTHLANDS,
      hasSouthamptonSiteId: !!process.env.MINDBODY_SITE_ID_SOUTHAMPTON,
      hasPasadenaSiteId: !!process.env.MINDBODY_SITE_ID_PASADENA,
      hasApiKey: !!process.env.MINDBODY_API_KEY,
      hasStaffCreds: !!(
        process.env.MINDBODY_STAFF_USERNAME && process.env.MINDBODY_STAFF_PASSWORD
      ),
      nodeEnv: process.env.NODE_ENV
    });

    const rawBody = await req.text();
    console.log("raw body length:", rawBody.length);

    let earlyPayload: any = null;
    try {
      earlyPayload = JSON.parse(rawBody);
    } catch {
      // Not JSON — let the Typeform path return the appropriate error below.
    }

    if (earlyPayload?.lookupOnly === true) {
      return await handleSheetsLookup(req, earlyPayload);
    }
    if (earlyPayload?.updateClient === true) {
      return await handleSheetsUpdateClient(req, earlyPayload);
    }
    if (earlyPayload?.linkReferral === true) {
      return await handleSheetsLinkReferral(req, earlyPayload);
    }
    if (earlyPayload?.getRelationships === true) {
      return await handleSheetsGetRelationships(req, earlyPayload);
    }
    if (earlyPayload?.sheetId && earlyPayload?.lead) {
      return await handleSheetsCreate(req, earlyPayload);
    }

    return await handleTypeform(req, rawBody);
  } catch (err: any) {
    console.log("top-level webhook error:", { message: err?.message, stack: err?.stack });
    return serverError(err?.message ?? "Unhandled server error");
  }
}
