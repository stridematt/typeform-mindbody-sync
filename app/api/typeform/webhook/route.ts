import axios from "axios";

const MINDBODY_BASE_URL = "https://api.mindbodyonline.com/public/v6";

// simple in-memory token cache (works great on warm serverless instances)
let cachedToken: { token: string; expiresAt: number; siteId: number } | null = null;

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
    throw new Error("Missing MINDBODY_USERNAME or MINDBODY_PASSWORD env vars");
  }

  // reuse cached token for the same site if still valid
  const now = Date.now();
  if (cachedToken && cachedToken.siteId === siteId && cachedToken.expiresAt > now) {
    return cachedToken.token;
  }

  const url = `${MINDBODY_BASE_URL}/usertoken/issue`;

  const resp = await axios.post(
    url,
    { Username: username, Password: password },
    { headers: baseHeaders(siteId), timeout: 15000 }
  );

  const token = resp.data?.AccessToken;
  if (!token) throw new Error("Mindbody usertoken/issue returned no AccessToken");

  // Mindbody tokens typically last a while; cache for 45 minutes to be safe
  cachedToken = { token, siteId, expiresAt: now + 45 * 60 * 1000 };
  return token;
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

async function authHeaders(siteId: number) {
  const token = await getUserToken(siteId);
  return {
    ...baseHeaders(siteId),
    Authorization: `Bearer ${token}`
  };
}

export async function findClient(siteId: number, lead: Lead) {
  const phone = normalizePhone(lead.phone);
  const email = normalizeEmail(lead.email);
  const searchText = phone || email;
  if (!searchText) return null;

  const url = `${MINDBODY_BASE_URL}/client/clients`;

  const resp = await axios.get(url, {
    headers: await authHeaders(siteId),
    params: { SearchText: searchText, Limit: 1, Offset: 0 },
    timeout: 15000
  });

  const clients = resp.data?.Clients ?? [];
  return clients[0] ?? null;
}

export async function createClient(siteId: number, lead: Lead) {
  const url = `${MINDBODY_BASE_URL}/client/addorupdateclients`;

  const payload = {
    Test: false,
    Clients: [
      {
        FirstName: lead.firstName.trim(),
        LastName: lead.lastName.trim(),
        Email: normalizeEmail(lead.email),
        MobilePhone: normalizePhone(lead.phone)
      }
    ]
  };

  const resp = await axios.post(url, payload, {
    headers: await authHeaders(siteId),
    timeout: 15000
  });

  const clients = resp.data?.Clients ?? [];
  return clients[0] ?? null;
}
