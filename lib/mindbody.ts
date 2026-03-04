// lib/mindbody.ts
import axios from "axios";

const BASE_URL = "https://api.mindbodyonline.com/public/v6";

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

/**
 * Issue a Mindbody user token for the SAME SiteId you will call next.
 * This avoids "token site id does not match requested site" and removes the need
 * for MINDBODY_TOKEN_SITE_ID entirely.
 */
const tokenCache = new Map<number, { token: string; issuedAt: number }>();
const TOKEN_TTL_MS = 10 * 60 * 1000; // 10 minutes (safe default)

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

export async function findClient(
  siteId: number,
  input: { firstName?: string; lastName?: string; email?: string; phone?: string }
) {
  const client = await mbClient(siteId);

  const candidates: string[] = [];
  if (input.email) candidates.push(input.email);
  if (input.phone) candidates.push(input.phone);

  const fullName = `${input.firstName ?? ""} ${input.lastName ?? ""}`.trim();
  if (fullName) candidates.push(fullName);

  for (const q of candidates) {
    const res = await client.get(`/client/clients`, { params: { SearchText: q } });
    const found = res.data?.Clients?.[0];
    if (found?.Id) return found;
  }

  return null;
}

export async function createClient(
  siteId: number,
  input: { firstName: string; lastName: string; email?: string; phone?: string }
) {
  const client = await mbClient(siteId);

  const payload = {
    FirstName: input.firstName,
    LastName: input.lastName,
    Email: input.email ?? "",
    MobilePhone: input.phone ?? "",
    IsProspect: true,
  };

  const res = await client.post(`/client/addclient`, payload);
  return res.data?.Client ?? null;
}
