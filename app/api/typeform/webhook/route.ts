import { NextResponse } from "next/server";
import crypto from "crypto";
import { neon } from "@neondatabase/serverless";
import { createClient } from "../../../../lib/mindbody";

export const runtime = "nodejs";

/*
 * AUDIT FIXES (2026-07-15) — this is the form_id -> tenants webhook variant.
 *
 *   [FIX 1] Phone is canonicalised to the LAST 10 digits before any lookup, so a
 *           "+1 508..." lead matches a stored "5084467211" (and vice-versa).
 *   [FIX 2] Verified matching replaces the loose findClient(): reuse an existing
 *           client only on a REAL email exact hit, or a verified last-10 phone hit
 *           whose name agrees. Synthetic fallbacks are never used as a match key,
 *           and a shared-household number does not attach to a different person.
 *           Ambiguous (>1) is surfaced for review instead of guessed.
 *   [FIX 3] Idempotency no longer loses leads. The token row is committed before
 *           the Mindbody call (blocking Typeform retries / concurrent deliveries),
 *           but is ROLLED BACK if the create fails, so a retry can succeed instead
 *           of being permanently "deduped" with no client ever created.
 *   [FIX 4] Mindbody search is done correctly here: bare token (no "Bearer"),
 *           request.-prefixed params, includeInactive, pagination, and .ok checks
 *           that throw on failure instead of silently returning "no match".
 *   [FIX 5] Richer field extraction (ref variants + title/type fallbacks) and a
 *           whitespace-collapsing normalize().
 *   [FIX 6] Timing-safe comparison for the plain-secret header fallback.
 *
 *   NOTE: this route keeps its own DATABASE_URL + tenants/processed_submissions
 *   tables (form_id routing model). If it runs alongside the studio/v2 webhook,
 *   confirm they should not share a dedup table — a submission that can reach both
 *   is deduped per-table and could be created twice across routes.
 */

const sql = neon(process.env.DATABASE_URL || "");

const MB_BASE = "https://api.mindbodyonline.com/public/v6";
const FALLBACK_EMAIL_DOMAIN = "strideautomation.com";
const FALLBACK_PHONE_PREFIX = "555";

// [FIX 4] Page through search results instead of only seeing the first page.
const MB_SEARCH_PAGE_SIZE = 100;
const MB_SEARCH_MAX_RECORDS = 200;

/* ---------- Generic helpers ---------- */

function normalize(s: string) {
  return s.toLowerCase().trim().replace(/\s+/g, " ").replace(/&/g, "and");
}

function digitsOnly(s: string | null | undefined) {
  return (s || "").replace(/\D/g, "");
}

/*
 * [FIX 1] Canonical 10-digit phone key, or "" if we cannot derive one. Match on
 * the LAST 10 digits, which ignores any leading country code ("+1", "1", etc).
 * Shared behaviour with the other webhook routes.
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
  return `${FALLBACK_PHONE_PREFIX}${last7}`; // 10 digits
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

function isFallbackPhone(phone: string) {
  const d = digitsOnly(phone);
  return /^1556\d{7}$/.test(d) || /^555\d{7}$/.test(d);
}

/* [FIX 2] Conservative nickname / formal-name equivalences for phone-match gating. */
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
 * [FIX 2] Name agreement for phone-based matches. Requires real first + last
 * names on both sides and equal last names (protects shared household numbers).
 * First names agree if equal, a clean prefix, a nickname equivalent, or a single
 * initial matching the other side.
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
  // [FIX 6] Timing-safe compare instead of ===.
  if (headerSecret && timingSafeEqual(headerSecret, secret)) return { ok: true };
  return { ok: false };
}

/* ---------- Mindbody client search (verified) ---------- */

/** Issue a staff user token. Accepts MINDBODY_STAFF_* or MINDBODY_* creds. */
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
  if (!res.ok) throw new Error(`Mindbody usertoken/issue failed: ${res.status} ${text}`);
  const data = JSON.parse(text);
  if (!data?.AccessToken) throw new Error(`Mindbody usertoken/issue returned no AccessToken: ${text}`);
  return String(data.AccessToken);
}

/** [FIX 4] Paginated client search: bare token, request.-prefixed params, includeInactive, throws on non-OK. */
async function mindbodySearchClients(siteId: number, token: string, searchText: string): Promise<any[]> {
  const apiKey = process.env.MINDBODY_API_KEY;
  if (!apiKey) throw new Error("Missing MINDBODY_API_KEY");
  const all: any[] = [];
  let offset = 0;
  while (offset < MB_SEARCH_MAX_RECORDS) {
    const url = new URL(`${MB_BASE}/client/clients`);
    url.searchParams.set("request.searchText", searchText);
    url.searchParams.set("request.limit", String(MB_SEARCH_PAGE_SIZE));
    url.searchParams.set("request.offset", String(offset));
    url.searchParams.set("request.includeInactive", "true");
    const res = await fetch(url.toString(), {
      headers: { "Api-Key": apiKey, Authorization: token, SiteId: String(siteId) },
    });
    const text = await res.text();
    if (!res.ok) throw new Error(`Mindbody client search failed: ${res.status} ${text}`);
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
 * [FIX 2] Verified find-for-create. Reuse an existing client only when identity
 * is confirmed: real-email exact single hit, or verified last-10 phone single hit
 * whose name agrees. Ambiguous (>1) is surfaced, not guessed.
 */
async function findExistingClient(
  siteId: number,
  args: {
    firstName: string;
    lastName: string;
    email: string;
    phone: string; // digits (substituted/validated by caller)
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

  if (args.emailIsReal && emailLower) {
    const clients = await mindbodySearchClients(siteId, token, emailLower);
    const exact = clients.filter(
      (c) => String(c?.Email || "").trim().toLowerCase() === emailLower
    );
    if (exact.length === 1) return { kind: "match", client: exact[0], via: "email" };
    if (exact.length > 1) return { kind: "ambiguous", via: "email", count: exact.length };
  }

  if (args.phoneIsReal && args.phone) {
    const canon = canonicalPhone(args.phone);
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
      namesAgree(args.firstName, args.lastName, String(c?.FirstName || ""), String(c?.LastName || ""))
    );
    const ids = Array.from(new Set(nameMatches.map((c) => String(c.Id))));
    if (ids.length === 1) return { kind: "match", client: byId.get(ids[0]), via: "phone" };
    if (ids.length > 1) return { kind: "ambiguous", via: "phone", count: ids.length };
  }

  return { kind: "none" };
}

/* ---------- Typeform payload extraction ---------- */

function getAnswerValue(answer: any) {
  if (!answer) return null;
  if (answer.type === "text") return answer.text ?? null;
  if (answer.type === "email") return answer.email ?? null;
  if (answer.type === "phone_number") return answer.phone_number ?? null;
  if (answer.type === "choice") return answer.choice?.label ?? null;
  if (answer.type === "dropdown") return answer.dropdown?.label ?? null;
  return null;
}

function extractLead(payload: any) {
  const formId = payload?.form_response?.form_id;
  const token = payload?.form_response?.token;
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
  // [FIX 5] Accept common ref spellings.
  const getByRefList = (refs: string[]) => {
    for (const ref of refs) {
      const v = getByRef(ref);
      if (v && v.toString().trim()) return v.toString().trim();
    }
    return null;
  };
  const findByTitleIncludes = (patterns: string[]) => {
    for (const a of answers) {
      const title = normalize(getTitle(a));
      if (patterns.some((p) => title.includes(normalize(p)))) {
        const v = getAnswerValue(a);
        if (v && v.toString().trim()) return v.toString().trim();
      }
    }
    return null;
  };

  let firstName = (getByRefList(["first_name", "firstname", "first-name"]) ?? "").toString().trim();
  let lastName = (getByRefList(["last_name", "lastname", "last-name"]) ?? "").toString().trim();
  let email = getByRefList(["email", "email_address", "email-address"]);
  let phone = getByRefList(["phone", "phone_number", "phone-number", "mobile"]);

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

  return { formId, token, firstName, lastName, email, phone, answersCount: answers.length };
}

/* ---------- DB ---------- */

async function ensureTables() {
  if (!process.env.DATABASE_URL) throw new Error("Missing DATABASE_URL.");
  await sql`
    create table if not exists tenants (
      id bigserial primary key,
      form_id text not null unique,
      location_name text,
      site_id integer not null,
      is_active boolean default true,
      created_at timestamptz default now()
    );
  `;
  await sql`
    create table if not exists processed_submissions (
      id bigserial primary key,
      typeform_token text not null unique,
      created_at timestamptz default now()
    );
  `;
}

/* ---------- POST entrypoint ---------- */

export async function POST(req: Request) {
  try {
    const rawBody = await req.text();
    const verification = await verifyTypeform(req, rawBody);
    if (!verification.ok) {
      return NextResponse.json(
        { ok: false, error: "Unauthorized (Typeform verification failed)" },
        { status: 401 }
      );
    }

    let payload: any;
    try {
      payload = JSON.parse(rawBody);
    } catch {
      return NextResponse.json({ ok: false, error: "Invalid JSON" }, { status: 400 });
    }

    const lead = extractLead(payload);
    if (!lead.formId || !lead.token || lead.answersCount === 0) {
      return NextResponse.json({ ok: true, status: "typeform_test_ok" });
    }
    if (!lead.firstName || !lead.lastName) {
      return NextResponse.json(
        {
          ok: false,
          error: "Missing firstName or lastName from Typeform payload",
          extracted: { firstName: lead.firstName, lastName: lead.lastName, email: lead.email, phone: lead.phone },
        },
        { status: 400 }
      );
    }

    await ensureTables();

    const tenantRows = await sql`
      select form_id, location_name, site_id, is_active
      from tenants
      where form_id = ${lead.formId}
      limit 1
    `;
    const tenant = (tenantRows as any)?.[0];
    if (!tenant || tenant.is_active === false) {
      return NextResponse.json({
        ok: true,
        status: "routed",
        routedTo: null,
        message: "No active tenant for this form_id",
      });
    }
    const siteId = Number(tenant.site_id);

    // Idempotency insert (blocks Typeform retries / concurrent deliveries).
    try {
      await sql`insert into processed_submissions (typeform_token) values (${lead.token})`;
    } catch {
      return NextResponse.json({
        ok: true,
        status: "deduped",
        routedTo: { locationName: tenant.location_name, siteId },
      });
    }

    // [FIX 3] From here on, any hard failure must release the token so a Typeform
    // retry can reprocess instead of being permanently deduped with no client.
    const releaseToken = async () => {
      try {
        await sql`delete from processed_submissions where typeform_token = ${lead.token}`;
      } catch (e) {
        console.log("failed to release idempotency token", { token: lead.token, e });
      }
    };

    try {
      // Decide which identifiers are REAL (safe to match on); substitute
      // deterministic fallbacks for the create so we never write junk to MB.
      const rawEmail = (lead.email || "").trim();
      const rawPhone = (lead.phone || "").trim();
      const emailIsReal = rawEmail.includes("@") && !isFallbackEmail(rawEmail);
      const phoneCanon = canonicalPhone(rawPhone);
      const phoneIsReal = !!phoneCanon && !isFallbackPhone(phoneCanon);

      const normalizedEmail = emailIsReal ? rawEmail : makeDummyEmail(lead.token);
      const normalizedPhone = phoneIsReal ? phoneCanon : digitsOnly(makeDummyPhone(lead.token));

      const found = await findExistingClient(siteId, {
        firstName: lead.firstName,
        lastName: lead.lastName,
        email: normalizedEmail,
        phone: normalizedPhone,
        rawPhone,
        emailIsReal,
        phoneIsReal,
      });

      if (found.kind === "ambiguous") {
        // Stable state — keep the token so we surface it once for human review.
        return NextResponse.json({
          ok: true,
          status: "ambiguous",
          via: found.via,
          count: found.count,
          routedTo: { locationName: tenant.location_name, siteId },
          message: `Multiple Mindbody clients matched on ${found.via}; not auto-linking`,
        });
      }

      if (found.kind === "match" && found.client?.Id) {
        return NextResponse.json({
          ok: true,
          status: "exists",
          matchedVia: found.via,
          mbClientId: String(found.client.Id),
          routedTo: { locationName: tenant.location_name, siteId },
          fallbacksUsed: { emailWasFallback: !emailIsReal, phoneWasFallback: !phoneIsReal },
        });
      }

      const created = await createClient(siteId, {
        firstName: lead.firstName,
        lastName: lead.lastName,
        email: normalizedEmail,
        phone: normalizedPhone,
      });
      if (!created?.Id) {
        await releaseToken(); // [FIX 3] allow retry
        return NextResponse.json({ ok: false, error: "Mindbody create failed" }, { status: 500 });
      }

      return NextResponse.json({
        ok: true,
        status: "created",
        mbClientId: String(created.Id),
        routedTo: { locationName: tenant.location_name, siteId },
        fallbacksUsed: { emailWasFallback: !emailIsReal, phoneWasFallback: !phoneIsReal },
      });
    } catch (err: any) {
      await releaseToken(); // [FIX 3] transient MB failure -> let Typeform retry succeed
      const status = err?.response?.status ?? null;
      const data = err?.response?.data ?? null;
      const where = typeof err?.config?.url === "string" ? err.config.url : null;
      return NextResponse.json(
        { ok: false, error: err?.message ?? "Server error", mindbody: { status, where, data } },
        { status: 500 }
      );
    }
  } catch (err: any) {
    console.log("top-level webhook error:", { message: err?.message, stack: err?.stack });
    return NextResponse.json(
      { ok: false, error: err?.message ?? "Unhandled server error" },
      { status: 500 }
    );
  }
}
