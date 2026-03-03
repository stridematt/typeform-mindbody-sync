import axios from "axios";

const MINDBODY_BASE_URL = "https://api.mindbodyonline.com/public/v6";

// Cache tokens per siteId (required by Mindbody)
const tokenCache = new Map<number, { token: string; expiresAt: number }>();

function baseHeaders(siteId: number) {
  const apiKey = process.env.MINDBODY_API_KEY;
  if (!apiKey) throw new Error("Missing MINDBODY_API_KEY env var");

  return {
    "Api-Key": apiKey,
    "SiteId": String(siteId),
    "Content-Type": "application/json"
  };
}

async function getUserToken(siteId: number) {
  const username = process.env.MINDBODY_USERNAME;
  const password = process.env.MINDBODY_PASSWORD;

  if (!username || !password) {
    throw new Error("Missing MINDBODY_USERNAME or MINDBODY_PASSWORD");
  }

  const now = Date.now();
  const cached = tokenCache.get(siteId);
  if (cached && cached.expiresAt > now) return cached.token;

  const resp = await axios.post(
    `${MINDBODY_BASE_URL}/usertoken/issue`,
    { Username: username, Password: password },
    { headers: baseHeaders(siteId), timeout: 15000 }
  );

  const token = resp.data?.AccessToken;
  if (!token) throw new Error("Mindbody usertoken/issue returned no AccessToken");

  // Cache ~45 minutes
  tokenCache.set(siteId, { token, expiresAt: now + 45 * 60 * 1000 });
  return token;
}

async function authHeaders(siteId: number) {
  const token = await getUserToken(siteId);
  return {
    ...baseHeaders(siteId),
    Authorization: `Bearer ${token}`
  };
}

function normalizeEmail(email?: string | null) {
  if (!email) return null;
  return email.trim().toLowerCase();
}

function normalizePhone(phone?: string | null) {
  if (!phone) return null;
  return phone.replace(/[^\d]/g, "");
}

export type Lead = {
  firstName: string;
  lastName: string;
  email?: string | null;
  phone?: string | null;
};

export async function findClient(siteId: number, lead: Lead) {
  const searchText = normalizePhone(lead.phone) || normalizeEmail(lead.email);
  if (!searchText) return null;

  const resp = await axios.get(`${MINDBODY_BASE_URL}/client/clients`, {
    headers: await authHeaders(siteId),
    params: { SearchText: searchText, Limit: 1, Offset: 0 },
    timeout: 15000
  });

  return resp.data?.Clients?.[0] ?? null;
}

export async function createClient(siteId: number, lead: Lead) {
  const resp = await axios.post(
    `${MINDBODY_BASE_URL}/client/addclient`,
    {
      FirstName: lead.firstName.trim(),
      LastName: lead.lastName.trim(),
      Email: normalizeEmail(lead.email),
      MobilePhone: normalizePhone(lead.phone)
    },
    {
      headers: await authHeaders(siteId),
      timeout: 15000
    }
  );

  return resp.data ?? null;
}
