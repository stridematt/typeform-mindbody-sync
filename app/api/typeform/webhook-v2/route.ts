import { NextResponse } from "next/server";
import crypto from "crypto";
import { neon } from "@neondatabase/serverless";
import { createClient, listLeadChannels, updateClient } from "../../../../lib/mindbody";

export const runtime = "nodejs";

/*
 * AUDIT FIXES (2026-07-15) — client de-duplication / phone matching
 *
 *   The old findClientByPhone() could not reliably find an existing client, so
 *   returning leads were created as duplicates. Root causes, all fixed here:
 *
 *   [FIX A] Auth header used "Bearer <token>". Mindbody Public API v6 expects the
 *           access token BARE (no "Bearer" prefix). The Bearer form 401s, the
 *           search returns nothing, and every returning lead is created again.
 *   [FIX B] Query params were "SearchText"/"Limit". Mindbody v6 GET
 *           /client/clients expects "request.searchText"/"request.limit"
 *           (+ request.offset, request.includeInactive). Unprefixed params are
 *           ignored, so the search came back unfiltered — no reliable match.
 *   [FIX C] Responses were parsed without checking res.ok, so auth/search
 *           failures were swallowed as "no match" and became duplicates. All
 *           Mindbody calls now check ok and throw on failure.
 *   [FIX D] Phone normalisation now uses the shared canonicalPhone() (LAST 10
 *           digits), so +1 / 1 / country codes and formatting are ignored and
 *           this webhook matches identically to the other route.
 *   [FIX E] Matching now checks a REAL email (exact) OR a verified last-10 phone
 *           gated on name agreement — never a synthetic fallback identifier, and
 *           never a shared-household number attaching to a different person.
 *   [FIX F] includeInactive=true + pagination, so inactive / past-page-1 clients
 *           are still found.
 *   [FIX G] Env vars unified: accepts MINDBODY_STAFF_USERNAME/PASSWORD (falls
 *           back to MINDBODY_USERNAME/PASSWORD) and DATABASE_URL_V2 || DATABASE_URL,
 *           so both webhooks read the same credentials and the same DB.
 */

const sql = neon(process.env.DATABASE_URL_V2 || process.env.DATABASE_URL || "");

const MB_BASE = "https://api.mindbodyonline.com/public/v6";
const FALLBACK_EMAIL_DOMAIN = "strideautomation.com";
const FALLBACK_PHONE_PREFIX = "555";

// [FIX F] Page through search results instead of only seeing the first page.
// Mindbody's searchText is loose (name/email/phone), so the true match can be
// past the first page at a busy studio.
const MB_SEARCH_PAGE_SIZE = 100;
const MB_SEARCH_MAX_RECORDS = 200;

// Maps our attribution type to the exact Mindbody Referral Type names so
// Analytics 2.0 groups these leads under a clean "Affiliate"/"Coach" Lead
// Source. Values MUST match the Referral Types configured in Mindbody
// (Manager Tools > Referral Types) or Mindbody falls back to "Other".
const REFERRAL_TYPE_BY_ATTRIBUTION: Record<string, string> = {
  affiliate: "Affiliate",
  coach: "Coach",
};

// Maps our attribution type to the Mindbody Lead Channel NAME (Sales Pipeline >
// Lead Channels). The channel must exist in Mindbody for the site or we leave
// the lead on the default "Public API" channel. Matched by name at runtime so
// it works across sites/pipelines without hardcoding numeric channel IDs.
const LEAD_CHANNEL_BY_ATTRIBUTION: Record<string, string> = {
  affiliate: "Grassroots",
  coach: "Grassroots",
};

// Prospect stage that new webhook leads are forced into after creation, so they
// enter the Sales Pipeline regardless of how the Lead Channel routes them.
// Setting the stage by ID is what actually sticks (Description alone was
// ignored). Id 1 = "New Lead" for site 5749750 (confirmed via client lookup).
// NOTE: if other studios sit on a different pipeline, their "New Lead" stage
// may have a different Id — revisit this when you add more sites.
const PIPELINE_STAGE_ID_FOR_NEW_LEADS = 1;
const PIPELINE_STAGE_FOR_NEW_LEADS = "New Lead";

// Cache resolved channel IDs per site+name to avoid repeat lookups.
const leadChannelIdCache = new Map<string, number | null>();

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

function digitsOnly(s: string | null | undefined) {
  return (s || "").replace(/\D/g, "");
}

/*
 * [FIX D] Canonical 10-digit phone key, or "" if we cannot derive one. Single
 * source of truth for "are these the same phone" — matches the other route.
 *
 * RULE: match on the LAST 10 digits, which inherently ignores any leading
 * country code ("+1", "1", etc). So +15084467211, 1-508-446-7211, and
 * (508) 446-7211 all reduce to "5084467211" and resolve to the same profile.
 *
 * CAVEAT: a value carrying a trailing extension ("508-446-7211 x4") would put
 * the extension in the last-10 window; extensions aren't expected in the lead
 * phone field. Strip them upstream if they ever appear.
 */
function canonicalPhone(s: string | null | undefined) {
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
 * or makeDummyPhone "555" + 7 digits. Real US numbers do not start 555.
 */
function isFallbackPhone(phone: string) {
  const d = digitsOnly(phone);
  return /^1556\d{7}$/.test(d) || /^555\d{7}$/.test(d);
}

/*
 * [FIX E] Conservative nickname / formal-name equivalences, used to gate phone
 * matches. Only unambiguous pairs, so we never merge genuinely different people.
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
    ["alexandra", "alex", "lexi", "sandra"],
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
 * [FIX E] Name agreement for phone-based matches. Requires a real first + last
 * name on both sides and equal last names (protects shared household numbers).
 * First names agree if equal, one is a clean prefix of the other, they share a
 * nickname canonical form, or one side is a single initial matching the other.
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
  if (lf === cf || lf.startsWith(cf) || cf.startsWith(lf)) return true;
  if (canonFirstName(leadFirst) === canonFirstName(clientFirst)) return true;
  if ((lf.length === 1 || cf.length === 1) && lf[0] === cf[0]) return true;
  return false;
}

/* ---------- Auth / verification ---------- */

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

/* ---------- Mindbody client search (fixed) ---------- */

/**
 * [FIX G] Issue a staff user token. Prefers MINDBODY_STAFF_* (used by the other
 * route) and falls back to MINDBODY_* so either deployment config works.
 */
async function getMindbodyToken(siteId: number) {
  const apiKey = process.env.MINDBODY_API_KEY;
  const username =
    process.env.MINDBODY_STAFF_USERNAME || process.env.MINDBODY_USERNAME;
  const password =
    process.env.MINDBODY_STAFF_PASSWORD || process.env.MINDBODY_PASSWORD;
  if (!apiKey || !username || !password) {
    throw new Error(
      "Missing Mindbody credentials (MINDBODY_API_KEY / STAFF_USERNAME / STAFF_PASSWORD)"
    );
  }
  const res = await fetch(`${MB_BASE}/usertoken/issue`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Api-Key": apiKey,
      SiteId: String(siteId),
    },
    body: JSON.stringify({ Username: username, Password: password }),
  });
  const text = await res.text();
  if (!res.ok) {
    // [FIX C] Fail loudly instead of returning an empty match.
    throw new Error(`Mindbody usertoken/issue failed: ${res.status} ${text}`);
  }
  const data = JSON.parse(text);
  if (!data?.AccessToken) {
    throw new Error(`Mindbody usertoken/issue returned no AccessToken: ${text}`);
  }
  return String(data.AccessToken);
}

/**
 * Search clients by text, paginated. [FIX A] bare token, [FIX B] request.-prefixed
 * params + includeInactive, [FIX C] throws on non-OK, [FIX F] pages results.
 * Returns the accumulated client rows.
 */
async function mindbodySearchClients(
  siteId: number,
  token: string,
  searchText: string
): Promise<any[]> {
  const apiKey = process.env.MINDBODY_API_KEY;
  if (!apiKey) throw new Error("Missing MINDBODY_API_KEY");

  const all: any[] = [];
  let offset = 0;

  while (offset < MB_SEARCH_MAX_RECORDS) {
    const url = new URL(`${MB_BASE}/client/clients`);
    url.searchParams.set("request.searchText", searchText); // [FIX B]
    url.searchParams.set("request.limit", String(MB_SEARCH_PAGE_SIZE));
    url.searchParams.set("request.offset", String(offset));
    url.searchParams.set("request.includeInactive", "true"); // [FIX F]

    const res = await fetch(url.toString(), {
      headers: {
        "Api-Key": apiKey,
        Authorization: `Bearer ${token}`, // Bearer confirmed against live coach-log flow
        SiteId: String(siteId),
      },
    });
    const text = await res.text();
    if (!res.ok) {
      // [FIX C] Surface the failure so we never treat it as "no match".
      throw new Error(`Mindbody client search failed: ${res.status} ${text}`);
    }
    let json: any = null;
    try {
      json = JSON.parse(text);
    } catch {
      throw new Error(`Mindbody client search returned non-JSON: ${text.slice(0, 200)}`);
    }
    const batch: any[] = json?.Clients ?? [];
    all.push(...batch);

    const total = Number(json?.PaginationResponse?.TotalResults ?? NaN);
    offset += MB_SEARCH_PAGE_SIZE;
    if (batch.length < MB_SEARCH_PAGE_SIZE) break;
    if (Number.isFinite(total) && offset >= total) break;
  }

  return all;
}

/**
 * [FIX E] Verified find-for-create. Only treats a Mindbody client as the same
 * person when identity is confirmed:
 *   1. Real-email exact match, single hit -> reuse.
 *   2. Verified last-10 phone match, single hit, AND name agrees -> reuse.
 * A phone that matches a different-named person (shared household number) is not
 * reused. Ambiguous (>1) is surfaced so a human can review instead of guessing.
 */
async function findExistingClient(
  siteId: number,
  args: {
    firstName: string;
    lastName: string;
    email: string;
    phone: string; // digits (already substituted/validated by caller)
    rawPhone: string;
    emailIsReal: boolean;
    phoneIsReal: boolean;
  }
): Promise<
  | { kind: "match"; client: any; via: "email" | "phone" }
  | { kind: "ambiguous"; via: "email" | "phone"; count: number }
  | { kind: "none" }
> {
  const token = await getMindbodyToken(siteId);
  const emailLower = args.email.trim().toLowerCase();

  // 1. Real-email exact match.
  if (args.emailIsReal && emailLower) {
    const clients = await mindbodySearchClients(siteId, token, emailLower);
    const exact = clients.filter(
      (c) => String(c?.Email || "").trim().toLowerCase() === emailLower
    );
    console.log("findExistingClient email pass", {
      emailLower,
      returned: clients.length,
      exact: exact.length,
    });
    if (exact.length === 1) return { kind: "match", client: exact[0], via: "email" };
    if (exact.length > 1) return { kind: "ambiguous", via: "email", count: exact.length };
  }

  // 2. Verified phone match, gated on name agreement.
  if (args.phoneIsReal && args.phone) {
    const canon = canonicalPhone(args.phone);
    // Search a few candidate forms and de-dupe by Id, so a client indexed with a
    // country code / formatting still surfaces.
    const candidates = Array.from(
      new Set(
        [canon, digitsOnly(args.rawPhone || args.phone), (args.rawPhone || "").trim()].filter(
          (s) => s && s.length >= 7
        )
      )
    );
    const byId = new Map<string, any>();
    for (const searchText of candidates) {
      const clients = await mindbodySearchClients(siteId, token, searchText);
      for (const c of clients) {
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
    console.log("findExistingClient phone pass", {
      phoneTail: canon,
      searchCandidates: candidates.length,
      returned: clients.length,
      phoneMatches: phoneMatches.length,
      nameMatches: nameMatches.length,
    });

    const ids = Array.from(new Set(nameMatches.map((c) => String(c.Id))));
    if (ids.length === 1) {
      return { kind: "match", client: byId.get(ids[0]), via: "phone" };
    }
    if (ids.length > 1) return { kind: "ambiguous", via: "phone", count: ids.length };
    // phone matched but no name agreement -> treat as a new person.
  }

  return { kind: "none" };
}

/* ---------- Lead channel resolution ---------- */

async function resolveLeadChannelId(siteId: number, channelName: string) {
  const cacheKey = `${siteId}:${channelName.toLowerCase()}`;
  if (leadChannelIdCache.has(cacheKey)) {
    return leadChannelIdCache.get(cacheKey) ?? null;
  }
  try {
    const channels = (await listLeadChannels(siteId)) as any[];
    const match = channels.find(
      (c) => String(c?.Name ?? "").toLowerCase() === channelName.toLowerCase()
    );
    const id =
      match?.Id !== undefined && match?.Id !== null ? Number(match.Id) : null;
    leadChannelIdCache.set(cacheKey, id);
    return id;
  } catch (err) {
    console.log("resolveLeadChannelId failed:", err);
    return null;
  }
}

/* ---------- Typeform payload extraction ---------- */

function getAnswerValue(answer: any) {
  if (!answer) return null;
  if (answer.type === "text") return answer.text ?? null;
  if (answer.type === "email") return answer.email ?? null;
  if (answer.type === "phone_number") return answer.phone_number ?? null;
  if (answer.type === "choice") return answer.choice?.label ?? null;
  if (answer.type === "choices") return answer.choices?.labels?.join(", ") ?? null;
  if (answer.type === "dropdown") return answer.dropdown?.label ?? null;
  if (answer.type === "boolean") return String(answer.boolean);
  if (answer.type === "number") return String(answer.number);
  if (answer.type === "url") return answer.url ?? null;
  if (answer.type === "date") return answer.date ?? null;
  return null;
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
      const matched = patterns.some((p) => title.includes(normalize(p)));
      if (matched) {
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
      "home-studio",
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
      "coach-name",
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
      "choose studio",
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
    answersCount: answers.length,
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

/* ---------- POST entrypoint ---------- */

export async function POST(req: Request) {
  try {
    console.log("🔥 webhook-v2 hit");
    console.log("env check:", {
      hasDatabaseUrlV2: !!process.env.DATABASE_URL_V2,
      hasDatabaseUrl: !!process.env.DATABASE_URL,
      hasTypeformSecret: !!process.env.TYPEFORM_WEBHOOK_SECRET,
      hasApiKey: !!process.env.MINDBODY_API_KEY,
      hasStaffCreds: !!(
        (process.env.MINDBODY_STAFF_USERNAME || process.env.MINDBODY_USERNAME) &&
        (process.env.MINDBODY_STAFF_PASSWORD || process.env.MINDBODY_PASSWORD)
      ),
      nodeEnv: process.env.NODE_ENV,
    });

    const rawBody = await req.text();
    console.log("raw body length:", rawBody.length);

    const verification = await verifyTypeform(req, rawBody);
    console.log("verification result:", verification);
    if (!verification.ok) {
      return NextResponse.json(
        { ok: false, error: "Unauthorized (Typeform verification failed)" },
        { status: 401 }
      );
    }

    let payload: any;
    try {
      payload = JSON.parse(rawBody);
      console.log("payload parsed successfully");
    } catch (parseErr) {
      console.log("payload parse failed:", parseErr);
      return NextResponse.json({ ok: false, error: "Invalid JSON" }, { status: 400 });
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
      answersCount: lead.answersCount,
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
      attributionType: lead.attributionType,
    };

    if (!lead.firstName || !lead.lastName) {
      return NextResponse.json(
        { ok: false, error: "Missing firstName or lastName from Typeform payload", extracted: baseExtracted },
        { status: 400 }
      );
    }
    if (!lead.studioName) {
      return NextResponse.json(
        { ok: false, error: "Missing studioName from Typeform payload", extracted: baseExtracted },
        { status: 400 }
      );
    }
    if (!lead.attribution) {
      return NextResponse.json(
        { ok: false, error: "Missing attribution from Typeform payload", extracted: baseExtracted },
        { status: 400 }
      );
    }

    console.log("before ensureTables");
    await ensureTables();
    console.log("after ensureTables");

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
        attributionType: lead.attributionType,
      });
    }

    const siteId = Number(mapping.site_id);

    // Idempotency: unique typeform_token blocks duplicate webhook deliveries
    // (Typeform retries) before we ever touch Mindbody.
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
        siteId,
      });
    } catch (insertErr) {
      console.log("processed submission insert failed, likely deduped:", insertErr);
      return NextResponse.json({
        ok: true,
        status: "deduped",
        routedTo: { studioName: mapping.studio_name, siteId },
        attribution: lead.attribution,
        attributionType: lead.attributionType,
      });
    }

    // Decide which identifiers are REAL (safe to match on). Substitute
    // deterministic fallbacks for the create so we never write junk into MB.
    const rawEmail = (lead.email || "").trim();
    const rawPhone = (lead.phone || "").trim();
    const emailIsReal = rawEmail.includes("@") && !isFallbackEmail(rawEmail);
    const phoneCanon = canonicalPhone(rawPhone);
    const phoneIsReal = !!phoneCanon && !isFallbackPhone(phoneCanon);

    const normalizedEmail = emailIsReal ? rawEmail : makeDummyEmail(lead.token);
    const normalizedPhone = phoneIsReal ? phoneCanon : digitsOnly(makeDummyPhone(lead.token));

    console.log("about to search/create in MB", {
      siteId,
      firstName: lead.firstName,
      lastName: lead.lastName,
      email: normalizedEmail,
      phone: normalizedPhone,
      emailIsReal,
      phoneIsReal,
      attribution: lead.attribution,
      attributionType: lead.attributionType,
    });

    try {
      const found = await findExistingClient(siteId, {
        firstName: lead.firstName,
        lastName: lead.lastName,
        email: normalizedEmail,
        phone: normalizedPhone,
        rawPhone,
        emailIsReal,
        phoneIsReal,
      });
      console.log("findExistingClient result:", found.kind);

      if (found.kind === "ambiguous") {
        // Don't guess — surface for human review rather than mis-attaching.
        return NextResponse.json({
          ok: true,
          status: "ambiguous",
          via: found.via,
          count: found.count,
          routedTo: { studioName: mapping.studio_name, siteId },
          attribution: lead.attribution,
          attributionType: lead.attributionType,
          message: `Multiple Mindbody clients matched on ${found.via}; not auto-linking`,
        });
      }

      if (found.kind === "match" && found.client?.Id) {
        return NextResponse.json({
          ok: true,
          status: "exists",
          matchedVia: found.via,
          mbClientId: String(found.client.Id),
          routedTo: { studioName: mapping.studio_name, siteId },
          attribution: lead.attribution,
          attributionType: lead.attributionType,
          fallbacksUsed: {
            emailWasFallback: !emailIsReal,
            phoneWasFallback: !phoneIsReal,
          },
        });
      }

      // Send the referral TYPE (e.g. "Affiliate"/"Coach") as ReferredBy so it
      // maps to a real Mindbody Referral Type and Analytics 2.0 reports a clean
      // Lead Source instead of "Public API". If the type is unknown, fall back
      // to the raw attribution name (lands under "Other"). The specific
      // referrer name is always retained in processed_submissions_v2.attribution.
      const referredBy =
        (lead.attributionType && REFERRAL_TYPE_BY_ATTRIBUTION[lead.attributionType]) ||
        lead.attribution ||
        undefined;

      // Resolve the Lead Channel to its Mindbody Id so Analytics 2.0 reports the
      // real Lead Channel instead of "Public API". Falls back to no channel.
      const channelName =
        (lead.attributionType && LEAD_CHANNEL_BY_ATTRIBUTION[lead.attributionType]) || null;
      const leadChannelId = channelName ? await resolveLeadChannelId(siteId, channelName) : null;
      console.log("resolved lead channel:", { channelName, leadChannelId });

      const created = await createClient(
        siteId,
        {
          firstName: lead.firstName,
          lastName: lead.lastName,
          email: normalizedEmail,
          phone: normalizedPhone,
        },
        {
          referredBy,
          leadChannelId: leadChannelId ?? undefined,
        }
      );
      console.log("created MB client result:", created?.Id ?? null);
      if (!created?.Id) {
        return NextResponse.json({ ok: false, error: "Mindbody create failed" }, { status: 500 });
      }

      // Force the new lead into the intake pipeline stage so it lands in the
      // Sales Pipeline regardless of Lead Channel routing. Non-fatal.
      let pipelineStage: string | null = null;
      try {
        await updateClient(siteId, created.Id, {
          prospectStageId: PIPELINE_STAGE_ID_FOR_NEW_LEADS,
          prospectStageDescription: PIPELINE_STAGE_FOR_NEW_LEADS,
        });
        pipelineStage = PIPELINE_STAGE_FOR_NEW_LEADS;
        console.log("moved lead to pipeline stage:", PIPELINE_STAGE_FOR_NEW_LEADS);
      } catch (stageErr) {
        console.log("failed to set pipeline stage:", stageErr);
      }

      return NextResponse.json({
        ok: true,
        status: "created",
        mbClientId: String(created.Id),
        routedTo: { studioName: mapping.studio_name, siteId },
        attribution: lead.attribution,
        attributionType: lead.attributionType,
        leadChannelId,
        pipelineStage,
        fallbacksUsed: {
          emailWasFallback: !emailIsReal,
          phoneWasFallback: !phoneIsReal,
        },
      });
    } catch (err: any) {
      const status = err?.response?.status ?? null;
      const data = err?.response?.data ?? null;
      const where = typeof err?.config?.url === "string" ? err.config.url : null;
      console.log("mindbody error:", { status, where, data, message: err?.message });
      return NextResponse.json(
        {
          ok: false,
          error: err?.message ?? "Server error",
          mindbody: { status, where, data },
          routedTo: { studioName: mapping.studio_name, siteId },
          attribution: lead.attribution,
          attributionType: lead.attributionType,
        },
        { status: 500 }
      );
    }
  } catch (err: any) {
    console.log("top-level webhook-v2 error:", { message: err?.message, stack: err?.stack });
    return NextResponse.json(
      { ok: false, error: err?.message ?? "Unhandled server error" },
      { status: 500 }
    );
  }
}
