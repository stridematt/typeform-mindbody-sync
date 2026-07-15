// lib/mindbody.ts
import axios from "axios";

const BASE_URL = "https://api.mindbodyonline.com/public/v6";

/*
 * CHANGE LOG (2026-07-15) — dedup / phone-matching hardening
 *
 *   findClient() previously returned Clients[0] from a fuzzy SearchText query
 *   with NO verification, and fell back to matching on full NAME. That attached
 *   new leads to the wrong existing client (shared surnames, fuzzy hits) and
 *   missed real returning clients whose phone carried a country code.
 *
 *   findClient() is now identity-verified and DB-free (unchanged signature):
 *     1. Real email exact match (single hit) -> reuse.
 *     2. Verified last-10 phone match AND name agreement -> reuse.
 *     - Name is NEVER a standalone match key anymore.
 *     - Synthetic fallback emails/phones are never used as a match key.
 *     - Country codes / "+1" / formatting are ignored (last-10 comparison).
 *   Search now uses the documented request.-prefixed params + includeInactive
 *   and pages results. Auth is unchanged (Bearer).
 *
 *   BEHAVIOUR NOTE: because matching is now verified, findClient will return
 *   null (-> caller creates a new client) in cases where it previously attached
 *   to an unrelated client. That is the intended fix.
 *
 *   Also:
 *     - createClient now stores the phone in canonical last-10 form.
 *     - addContactLog no longer violates Mindbody's rule that FollowupByDate and
 *       AssignedTo must be both-present or both-absent, and defaults ContactMethod.
 */

function requireEnv(name: string) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing ${name}`);
  return v;
}

function assertValidSiteId(siteId: number) {
  if (!Number.isInteger(siteId) || siteId <= 0) {
    throw new Error(`Invalid siteId: ${siteId}`);
  }
  return siteId;
}

/* ---------- Phone / name / fallback helpers (added 2026-07-15) ---------- */

function digitsOnly(s?: string | null) {
  return (s || "").replace(/\D/g, "");
}

// Last 10 digits (ignores +1 / country code). "" if fewer than 10 digits.
function last10(s?: string | null) {
  const d = digitsOnly(s);
  if (d.length < 10) return "";
  return d.slice(-10);
}

function phonesMatch(a?: string | null, b?: string | null) {
  const ca = last10(a);
  const cb = last10(b);
  if (!ca || !cb) return false;
  return ca === cb;
}

function normalizeName(s: string) {
  return s.toLowerCase().trim().replace(/\s+/g, " ").replace(/&/g, "and");
}

// Synthetic identifiers generated elsewhere in the app must never be used as a
// match key, or unrelated leads collide on a shared placeholder. Covers the
// webhook (pending+...@strideautomation.com), the coach lead flow
// (lead+...@stridefitness-leads.com), and the 555/1556 dummy phones.
function isFallbackEmail(email: string) {
  const e = email.trim().toLowerCase();
  return (
    e.startsWith("pending+") ||
    e.startsWith("lead+") ||
    e.endsWith("@strideautomation.com") ||
    e.endsWith("@stridefitness-leads.com")
  );
}

function isFallbackPhone(phone: string) {
  const d = digitsOnly(phone);
  return /^1556\d{7}$/.test(d) || /^555\d{7}$/.test(d);
}

// Conservative nickname / formal-name equivalences for phone-match gating.
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
  for (const g of groups) for (const n of g) map[n] = g[0];
  return map;
})();

function canonFirstName(s: string) {
  const clean = normalizeName(s).replace(/[^a-z0-9]/g, "");
  return NICKNAME_CANON[clean] ?? clean;
}

// Requires real first + last names on both sides and equal last names (protects
// shared household numbers). First names agree if equal, a clean prefix, a
// nickname equivalent, or a single initial matching the other side.
function namesAgree(leadFirst: string, leadLast: string, clientFirst: string, clientLast: string) {
  const clean = (s: string) => normalizeName(s).replace(/[^a-z0-9]/g, "");
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

/* ---------- Auth ---------- */

const tokenCache = new Map<number, { token: string; issuedAt: number }>();
const TOKEN_TTL_MS = 10 * 60 * 1000;

async function getToken(siteId: number) {
  assertValidSiteId(siteId);
  const apiKey = requireEnv("MINDBODY_API_KEY");
  const username = requireEnv("MINDBODY_USERNAME");
  const password = requireEnv("MINDBODY_PASSWORD");
  const cached = tokenCache.get(siteId);
  if (cached && Date.now() - cached.issuedAt < TOKEN_TTL_MS) {
    return cached.token;
  }
  const res = await axios.post(
    `${BASE_URL}/usertoken/issue`,
    { Username: username, Password: password },
    {
      headers: {
        "Content-Type": "application/json",
        "Api-Key": apiKey,
        SiteId: String(siteId),
      },
      timeout: 20000,
    }
  );
  const accessToken = res.data?.AccessToken as string | undefined;
  if (!accessToken) {
    throw new Error("Mindbody token response missing AccessToken");
  }
  tokenCache.set(siteId, { token: accessToken, issuedAt: Date.now() });
  return accessToken;
}

async function mbClient(siteId: number) {
  assertValidSiteId(siteId);
  const apiKey = requireEnv("MINDBODY_API_KEY");
  const token = await getToken(siteId);
  return axios.create({
    baseURL: BASE_URL,
    timeout: 25000,
    headers: {
      "Content-Type": "application/json",
      "Api-Key": apiKey,
      Authorization: `Bearer ${token}`,
      SiteId: String(siteId),
    },
  });
}

/* ---------- Client search (paginated, request.-prefixed) ---------- */

const SEARCH_PAGE_SIZE = 100;
const SEARCH_MAX_RECORDS = 200;

/**
 * Search clients by text, paging through results. Mindbody's searchText is loose
 * (name/email/phone), so the true match can be past the first page at a busy
 * studio; only paging guarantees the caller's verifier can see it.
 */
async function searchClients(client: any, searchText: string): Promise<any[]> {
  const all: any[] = [];
  let offset = 0;
  while (offset < SEARCH_MAX_RECORDS) {
    const res = await client.get(`/client/clients`, {
      params: {
        "request.searchText": searchText,
        "request.limit": SEARCH_PAGE_SIZE,
        "request.offset": offset,
        "request.includeInactive": true,
      },
    });
    const batch: any[] = res.data?.Clients ?? [];
    all.push(...batch);
    const total = Number(res.data?.PaginationResponse?.TotalResults ?? NaN);
    offset += SEARCH_PAGE_SIZE;
    if (batch.length < SEARCH_PAGE_SIZE) break;
    if (Number.isFinite(total) && offset >= total) break;
  }
  return all;
}

/**
 * Find an existing Mindbody client for a lead, identity-verified.
 *
 * Match rules (in order):
 *   1. Real email exact match (single/first hit) -> reuse.
 *   2. Verified last-10 phone match AND name agreement -> reuse.
 * Returns the matched client object, or null (caller should create).
 *
 * Never matches on name alone, and never on a synthetic fallback email/phone.
 * Same signature as before, so existing callers need no changes.
 */
export async function findClient(
  siteId: number,
  input: { firstName?: string; lastName?: string; email?: string; phone?: string }
) {
  const client = await mbClient(siteId);

  const firstName = (input.firstName ?? "").trim();
  const lastName = (input.lastName ?? "").trim();
  const emailRaw = (input.email ?? "").trim();
  const phoneRaw = (input.phone ?? "").trim();

  const emailIsReal = emailRaw.includes("@") && !isFallbackEmail(emailRaw);
  const phoneCanon = last10(phoneRaw);
  const phoneIsReal = phoneCanon.length === 10 && !isFallbackPhone(phoneCanon);

  // 1. Real email exact match.
  if (emailIsReal) {
    const emailLower = emailRaw.toLowerCase();
    const rows = await searchClients(client, emailLower);
    const exact = rows.filter(
      (c) => String(c?.Email || "").trim().toLowerCase() === emailLower
    );
    if (exact.length >= 1) return exact[0];
  }

  // 2. Verified phone match, gated on name agreement.
  if (phoneIsReal) {
    const rows = await searchClients(client, phoneCanon);
    const phoneMatches = rows.filter((c) =>
      [c?.MobilePhone, c?.HomePhone, c?.WorkPhone]
        .filter(Boolean)
        .some((p: string) => phonesMatch(p, phoneCanon))
    );
    const nameMatches = phoneMatches.filter((c) =>
      namesAgree(firstName, lastName, String(c?.FirstName || ""), String(c?.LastName || ""))
    );
    if (nameMatches.length >= 1) return nameMatches[0];
    // phone matched but no name agreement -> treat as a new person.
  }

  return null;
}

/**
 * Create a prospect client in Mindbody.
 * Optional:
 *   - referredBy: sets the Mindbody "Referred By" field on the client. This is
 *     the value Analytics 2.0 reports as the Lead Source. If you do NOT send a
 *     value, Mindbody stamps API-created clients with the source "Public API".
 *     Pass the real source here (e.g. the affiliate/coach name from Typeform)
 *     so the lead is attributed correctly.
 *   - referralType: legacy alias for referredBy, kept for the Google Sheets
 *     flow (e.g. "Paid Lead"). If both are provided, referredBy wins.
 *   - salesRep: numeric staff Id assigned to Rep 1 on the client profile
 *   - leadChannelId: numeric Lead Management channel Id. Tags the lead with
 *     this channel. If the tenant has automated channel-to-stage mapping
 *     configured, this can also route the lead to a specific pipeline stage.
 *
 * NOTE: Mindbody's "Referred By" generally sticks best when the value matches a
 * Referral Type configured on the site (Manager Tools > Referral Types). If you
 * send a value that doesn't exist as a referral type, some sites may drop it.
 * Make sure your affiliate/coach names exist as referral types on each site.
 */
export async function createClient(
  siteId: number,
  input: { firstName: string; lastName: string; email?: string; phone?: string },
  options?: {
    referredBy?: string;
    referralType?: string;
    salesRep?: number;
    leadChannelId?: number;
  }
) {
  const client = await mbClient(siteId);
  // Store the phone in canonical last-10 form so records don't drift into mixed
  // formats (which makes future matching harder). Fall back to the raw value if
  // it isn't a full 10 digits, so we never silently drop data.
  const phoneToStore = input.phone ? last10(input.phone) || input.phone : "";
  const payload: Record<string, any> = {
    FirstName: input.firstName,
    LastName: input.lastName,
    Email: input.email ?? "",
    MobilePhone: phoneToStore,
    IsProspect: true,
  };
  // referredBy is the canonical option; referralType is kept as a legacy alias.
  const referredByValue = options?.referredBy ?? options?.referralType;
  if (referredByValue && referredByValue.trim()) {
    payload.ReferredBy = referredByValue.trim();
  }
  if (
    options?.salesRep !== undefined &&
    options?.salesRep !== null &&
    Number.isFinite(options.salesRep)
  ) {
    payload.SalesReps = [
      {
        Id: Number(options.salesRep),
        SalesRepNumber: 1,
      },
    ];
  }
  if (
    options?.leadChannelId !== undefined &&
    options?.leadChannelId !== null &&
    Number.isFinite(options.leadChannelId)
  ) {
    payload.LeadChannelId = Number(options.leadChannelId);
  }
  const res = await client.post(`/client/addclient`, payload);
  return res.data?.Client ?? null;
}

/**
 * Update an existing client.
 *
 * Optional:
 *   - salesRep: numeric staff Id for Rep 1
 *   - prospectStageDescription: name of a Sales Pipeline stage. Per Mindbody
 *     docs, on Ultimate-tier accounts the UpdateClient endpoint can move a
 *     client into a Sales Pipeline stage when both IsProspect=true and
 *     ProspectStage.Description are set. Docs only explicitly mention
 *     "New Lead" as a working value; custom stages like "Call Center" are
 *     untested and may or may not work depending on tenant configuration.
 */
export async function updateClient(
  siteId: number,
  mbClientId: string | number,
  updates: {
    salesRep?: number;
    prospectStageId?: number;
    prospectStageDescription?: string;
  }
) {
  const client = await mbClient(siteId);
  const clientPayload: Record<string, any> = {
    Id: String(mbClientId),
  };
  if (
    updates.salesRep !== undefined &&
    updates.salesRep !== null &&
    Number.isFinite(updates.salesRep)
  ) {
    clientPayload.SalesReps = [
      {
        Id: Number(updates.salesRep),
        SalesRepNumber: 1,
      },
    ];
  }
  const hasStageId =
    updates.prospectStageId !== undefined &&
    updates.prospectStageId !== null &&
    Number.isFinite(updates.prospectStageId);
  const hasStageDescription =
    typeof updates.prospectStageDescription === "string" &&
    updates.prospectStageDescription.trim().length > 0;
  if (hasStageId || hasStageDescription) {
    // Per docs: setting IsProspect=true alongside ProspectStage is what triggers
    // the Sales Pipeline opportunity create/move on Ultimate-tier accounts.
    // Sending the stage Id is the most reliable; Description is included as a
    // fallback/label. Get valid Ids from listProspectStages().
    clientPayload.IsProspect = true;
    clientPayload.ProspectStage = {};
    if (hasStageId) {
      clientPayload.ProspectStage.Id = Number(updates.prospectStageId);
    }
    if (hasStageDescription) {
      clientPayload.ProspectStage.Description =
        updates.prospectStageDescription!.trim();
    }
  }
  const res = await client.post(`/client/updateclient`, {
    Client: clientPayload,
    CrossRegionalUpdate: false,
  });
  return res.data?.Client ?? null;
}

/**
 * Add a Contact Log. When assignedToStaffId is provided this creates an OPEN
 * FOLLOW-UP TASK (= "Sales Followup Task" in the Sales Pipeline UI), which is
 * what fires the pipeline trigger:
 *   "If the sales followup task is created immediately after lead creation,
 *    then move it to <destination> stage."
 * When assignedToStaffId is omitted it writes a plain contact-log entry.
 *
 * Mindbody rule (POST /client/addcontactlog): FollowupByDate and AssignedTo must
 * be BOTH present or BOTH absent. We honour that:
 *   - task mode (staff id given)  -> send AssignedToStaffId + FollowupByDate + IsComplete=false
 *   - log mode  (no staff id)     -> send NEITHER
 * ContactMethod is required by Mindbody, so it defaults to "Phone".
 */
export async function addContactLog(
  siteId: number,
  input: {
    clientId: string | number;
    text?: string;
    assignedToStaffId?: number;
    followupByDate?: Date; // defaults to "tomorrow" when in task mode
    contactMethod?: string; // free-text; e.g. "Phone"
    contactName?: string; // who to contact (typically the client's name)
  }
) {
  const client = await mbClient(siteId);

  const payload: Record<string, any> = {
    ClientId: String(input.clientId),
    Text: input.text ?? "Auto-created follow-up task from Paid Leads pipeline",
    // Required by Mindbody's addcontactlog.
    ContactMethod: input.contactMethod || "Phone",
  };

  const hasStaff =
    input.assignedToStaffId !== undefined &&
    input.assignedToStaffId !== null &&
    Number.isFinite(input.assignedToStaffId);

  if (hasStaff) {
    // Task mode: AssignedTo + FollowupByDate must travel together.
    const followupByDate =
      input.followupByDate ?? new Date(Date.now() + 24 * 60 * 60 * 1000);
    payload.AssignedToStaffId = Number(input.assignedToStaffId);
    payload.FollowupByDate = followupByDate.toISOString();
    payload.IsComplete = false;
  }
  // Log mode (no staff id): send neither FollowupByDate nor AssignedTo.

  if (input.contactName) {
    payload.ContactName = input.contactName;
  }

  const res = await client.post(`/client/addcontactlog`, payload);
  return res.data ?? null;
}

/**
 * List Lead Channels configured for the site.
 *
 * Hits GET /site/sites?includeLeadChannels=true per Mindbody docs.
 * Use the returned Id to populate LeadChannelId in AddClient calls.
 */
export async function listLeadChannels(siteId: number) {
  const client = await mbClient(siteId);
  const res = await client.get(`/site/sites`, {
    params: {
      siteIds: siteId,
      includeLeadChannels: true,
    },
  });
  // Mindbody returns { Sites: [{ ..., LeadChannels: [...] }] }
  const sites = res.data?.Sites ?? [];
  return sites[0]?.LeadChannels ?? [];
}

/**
 * List the Sales Pipeline prospect stages for a site.
 *
 * Hits GET /client/prospectstages. Each stage has { Id, Description, Active }.
 * Use the Id with updateClient({ prospectStageId }) to move a lead into a stage.
 */
export async function listProspectStages(siteId: number) {
  const client = await mbClient(siteId);
  const res = await client.get(`/client/prospectstages`);
  // Mindbody returns { ProspectStages: [...] }
  return res.data?.ProspectStages ?? [];
}
