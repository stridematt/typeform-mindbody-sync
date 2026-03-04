import axios from "axios";

const BASE_URL = "https://api.mindbodyonline.com/public/v6";

type TokenCacheEntry = { token: string; expiresAt: number };
const tokenCache = new Map<number, TokenCacheEntry>();

function requireEnv(name: string) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing ${name}`);
  return v;
}

function parseSiteId(value: string, envName: string) {
  const cleaned = value.trim();
  if (!/^\d+$/.test(cleaned)) {
    throw new Error(`${envName} must be digits only. Got: "${value}"`);
  }
  const n = Number(cleaned);
  if (!Number.isInteger(n) || n <= 0) {
    throw new Error(`${envName} must be a positive integer. Got: "${value}"`);
  }
  return n;
}

function nowMs() {
  return Date.now();
}

/**
 * Issue a Mindbody user token.
 * For your setup, Mindbody expects a valid SiteId header on /usertoken/issue.
 * We ALWAYS use MINDBODY_TOKEN_SITE_ID here (a known-good site you have access to).
 */
async function getUserToken() {
  const apiKey = requireEnv("MINDBODY_API_KEY");
  const username = requireEnv("MINDBODY_USERNAME");
  const password = requireEnv("MINDBODY_PASSWORD");

  const tokenSiteId = parseSiteId(requireEnv("MINDBODY_TOKEN_SITE_ID"), "MINDBODY_TOKEN_SITE_ID");

  const cacheKey = tokenSiteId;
  const cached = tokenCache.get(cacheKey);
  if (cached && cached.expiresAt > nowMs() + 30_000) return cached.token;

  const res = await axios.post(
    `${BASE_URL}/usertoken/issue`,
    { Username: username, Password: password },
    {
      headers: {
        "Content-Type": "application/json",
        "Api-Key": apiKey,
        SiteId: String(tokenSiteId)
      },
      timeout: 20000
    }
  );

  const token = res.data?.AccessToken as string | undefined;
  const expiresIn = Number(res.data?.ExpiresIn ?? 900);

  if (!token) throw new Error("Mindbody token response missing AccessToken");

  tokenCache.set(cacheKey, { token, expiresAt: nowMs() + expiresIn * 1000 });
  return token;
}

function requireSiteId(siteId: number) {
  if (!Number.isInteger(siteId) || siteId <= 0) {
    throw new Error(`Invalid siteId: ${siteId}`);
  }
  return siteId;
}

async function mbo(siteId: number) {
  const apiKey = requireEnv("MINDBODY_API_KEY");
  const token = await getUserToken();

  requireSiteId(siteId);

  return axios.create({
    baseURL: BASE_URL,
    timeout: 25000,
    headers: {
      "Content-Type": "application/json",
      "Api-Key": apiKey,
      Authorization: `Bearer ${token}`,
      SiteId: String(siteId)
    }
  });
}

export async function findClient(
  siteId: number,
  input: { firstName?: string; lastName?: string; email?: string | null; phone?: string | null }
) {
  const client = await mbo(siteId);

  const candidates: string[] = [];
  const email = (input.email ?? "").trim();
  const phone = (input.phone ?? "").trim();
  if (email) candidates.push(email);
  if (phone) candidates.push(phone);
  const fullName = `${input.firstName ?? ""} ${input.lastName ?? ""}`.trim();
  if (fullName) candidates.push(fullName);

  for (const q of candidates) {
    const res = await client.get(`/client/clients`, {
      params: { SearchText: q },
      timeout: 20000
    });

    const clients: any[] = res.data?.Clients ?? [];
    if (!clients.length) continue;

    const emailNorm = email.toLowerCase();
    const phoneDigits = phone.replace(/\D/g, "");

    const exact = clients.find((c) => {
      const cEmail = (c?.Email ?? "").toLowerCase().trim();
      const cPhone = (c?.MobilePhone ?? "").replace(/\D/g, "");
      return (emailNorm && cEmail === emailNorm) || (phoneDigits && cPhone === phoneDigits);
    });

    return exact ?? clients[0] ?? null;
  }

  return null;
}

export async function createClient(
  siteId: number,
  input: { firstName: string; lastName: string; email: string; phone: string }
) {
  const client = await mbo(siteId);

  const payload = {
    FirstName: input.firstName,
    LastName: input.lastName,
    Email: input.email,
    MobilePhone: input.phone,
    IsProspect: true
  };

  const res = await client.post(`/client/addclient`, payload, { timeout: 20000 });
  return res.data?.Client ?? res.data ?? null;
}

/**
 * Add a Contact Log entry so Mindbody shows lead source cleanly.
 */
export async function addLeadSourceContactLog(siteId: number, clientId: string, leadSource: string) {
  const client = await mbo(siteId);

  const payload: any = {
    ClientId: clientId,
    Text: `Lead Source: ${leadSource}`,
    ContactMethod: "Other",
    ContactName: leadSource
  };

  // Optional: if you want it assigned to a staff user in Mindbody
  const staffIdRaw = process.env.MINDBODY_ASSIGNED_TO_STAFF_ID;
  if (staffIdRaw && /^\d+$/.test(staffIdRaw.trim())) {
    payload.AssignedToStaffId = Number(staffIdRaw.trim());
  }

  const res = await client.post(`/client/addcontactlog`, payload, { timeout: 20000 });
  return res.data ?? null;
}
