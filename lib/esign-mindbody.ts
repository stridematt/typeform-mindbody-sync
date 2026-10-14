// lib/esign-mindbody.ts
// Mindbody calls used only by the SMS e-sign route (app/api/esign/route.ts).
// Kept separate from lib/mindbody.ts on purpose so the e-sign flow cannot change
// the behavior of any existing integration. Uses the same env vars and auth as
// lib/mindbody.ts (MINDBODY_API_KEY / USERNAME / PASSWORD, Bearer staff token).
import axios from "axios";

const BASE_URL = "https://api.mindbodyonline.com/public/v6";

function requireEnv(name: string) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing ${name}`);
  return v;
}

function last10(s?: string | null) {
  const d = (s || "").replace(/\D/g, "");
  return d.length < 10 ? "" : d.slice(-10);
}

function phonesMatch(a?: string | null, b?: string | null) {
  const ca = last10(a);
  const cb = last10(b);
  return !!ca && !!cb && ca === cb;
}

// Same synthetic identifiers lib/mindbody.ts refuses to match on.
function isFallbackEmail(email: string) {
  const e = email.trim().toLowerCase();
  return e.startsWith("pending+") || e.startsWith("lead+") ||
    e.endsWith("@strideautomation.com") || e.endsWith("@stridefitness-leads.com");
}

function cleanName(s: string) {
  return (s || "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

// Tie-break only (shared household phone): same last name and the first names
// are equal or one is a prefix of the other ("Matt" / "Matthew").
function namesAgree(aFirst: string, aLast: string, bFirst: string, bLast: string) {
  const af = cleanName(aFirst), al = cleanName(aLast), bf = cleanName(bFirst), bl = cleanName(bLast);
  if (!af || !al || !bf || !bl || al !== bl) return false;
  return af === bf || af.startsWith(bf) || bf.startsWith(af);
}

/* ---------- Auth (own small cache; same credentials as lib/mindbody.ts) ---------- */

const tokenCache = new Map<number, { token: string; issuedAt: number }>();
const TOKEN_TTL_MS = 10 * 60 * 1000;

async function getToken(siteId: number) {
  const cached = tokenCache.get(siteId);
  if (cached && Date.now() - cached.issuedAt < TOKEN_TTL_MS) return cached.token;
  const res = await axios.post(
    `${BASE_URL}/usertoken/issue`,
    { Username: requireEnv("MINDBODY_USERNAME"), Password: requireEnv("MINDBODY_PASSWORD") },
    {
      headers: { "Content-Type": "application/json", "Api-Key": requireEnv("MINDBODY_API_KEY"), SiteId: String(siteId) },
      timeout: 20000,
    }
  );
  const token = res.data?.AccessToken as string | undefined;
  if (!token) throw new Error("Mindbody token response missing AccessToken");
  tokenCache.set(siteId, { token, issuedAt: Date.now() });
  return token;
}

async function mbClient(siteId: number) {
  if (!Number.isInteger(siteId) || siteId <= 0) throw new Error(`Invalid siteId: ${siteId}`);
  const token = await getToken(siteId);
  return axios.create({
    baseURL: BASE_URL,
    timeout: 25000,
    headers: {
      "Content-Type": "application/json",
      "Api-Key": requireEnv("MINDBODY_API_KEY"),
      Authorization: `Bearer ${token}`,
      SiteId: String(siteId),
    },
  });
}

async function searchClients(client: any, searchText: string): Promise<any[]> {
  const all: any[] = [];
  for (let offset = 0; offset < 200; offset += 100) {
    const res = await client.get(`/client/clients`, {
      params: {
        "request.searchText": searchText,
        "request.limit": 100,
        "request.offset": offset,
        "request.includeInactive": true,
      },
    });
    const batch: any[] = res.data?.Clients ?? [];
    all.push(...batch);
    if (batch.length < 100) break;
  }
  return all;
}

/**
 * Resolve the addressable Client ID for the member who just signed by SMS reply.
 *
 * The replying phone is the proof of identity, so every path requires the
 * client's phone (mobile, home or work, last-10) to match it:
 *   1. The Mindbody ID stored on the GHL contact, read via clientcompleteinfo
 *      (accepts either Id form) and re-read off the returned record, so a
 *      pre-reassignment internal number still resolves to the addressable Id.
 *   2. Otherwise a phone search. One phone match wins. Several (shared household
 *      number) are narrowed by real email, then by name. Still several ->
 *      ambiguous, and nothing is written.
 */
export async function resolveClientForEsign(
  siteId: number,
  input: { mbClientId?: string; phone: string; email?: string; firstName?: string; lastName?: string }
): Promise<{ clientId: string; via: "id" | "phone" }> {
  const client = await mbClient(siteId);
  const phoneCanon = last10(input.phone);
  if (!phoneCanon) {
    throw Object.assign(new Error("No 10-digit phone to match the Mindbody client"), { code: "no_phone" });
  }
  const phoneMatchesClient = (c: any) =>
    [c?.MobilePhone, c?.HomePhone, c?.WorkPhone].filter(Boolean).some((p: string) => phonesMatch(p, phoneCanon));

  const storedId = String(input.mbClientId ?? "").trim();
  if (storedId) {
    try {
      const res = await client.get(`/client/clientcompleteinfo`, { params: { ClientId: storedId } });
      const c = res.data?.Client ?? res.data;
      if (c?.Id != null && phoneMatchesClient(c)) return { clientId: String(c.Id).trim(), via: "id" };
    } catch {
      // fall through to phone search
    }
  }

  let matches = (await searchClients(client, phoneCanon)).filter(phoneMatchesClient);

  if (matches.length > 1) {
    const emailLower = (input.email ?? "").trim().toLowerCase();
    if (emailLower.includes("@") && !isFallbackEmail(emailLower)) {
      const byEmail = matches.filter((c) => String(c?.Email || "").trim().toLowerCase() === emailLower);
      if (byEmail.length >= 1) matches = byEmail;
    }
  }
  if (matches.length > 1 && input.firstName && input.lastName) {
    const byName = matches.filter((c) =>
      namesAgree(input.firstName!, input.lastName!, String(c?.FirstName || ""), String(c?.LastName || ""))
    );
    if (byName.length >= 1) matches = byName;
  }

  if (matches.length === 1 && matches[0]?.Id != null) return { clientId: String(matches[0].Id), via: "phone" };
  if (matches.length === 0) {
    throw Object.assign(new Error("No Mindbody client matches this phone"), { code: "client_not_found" });
  }
  throw Object.assign(new Error(`${matches.length} Mindbody clients share this phone`), { code: "client_ambiguous" });
}

/** Upload a file to the client's Documents tab (POST /client/uploadclientdocument). */
export async function uploadClientDocument(
  siteId: number,
  input: { clientId: string | number; fileName: string; mediaType: string; base64: string }
) {
  const client = await mbClient(siteId);
  const res = await client.post(`/client/uploadclientdocument`, {
    ClientId: String(input.clientId),
    File: { FileName: input.fileName, MediaType: input.mediaType, Buffer: input.base64 },
  });
  return res.data ?? null;
}

/**
 * Plain contact log entry (no AssignedTo/FollowupByDate, so it is a log, not a
 * Sales Followup Task, and fires no pipeline triggers).
 */
export async function addEsignContactLog(
  siteId: number,
  input: { clientId: string | number; text: string; contactMethod: string; contactName?: string }
) {
  const client = await mbClient(siteId);
  const payload: Record<string, any> = {
    ClientId: String(input.clientId),
    Text: input.text,
    ContactMethod: input.contactMethod,
  };
  if (input.contactName) payload.ContactName = input.contactName;
  const res = await client.post(`/client/addcontactlog`, payload);
  return res.data ?? null;
}
