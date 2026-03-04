import axios from "axios";

const MBO_BASE_URL = "https://api.mindbodyonline.com/public/v6";

type TokenCacheEntry = { token: string; expiresAt: number };
const tokenCache = new Map<number, TokenCacheEntry>();

function requireEnv(name: string) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing ${name}`);
  return v;
}

function nowMs() {
  return Date.now();
}

async function getUserToken(siteId: number) {
  const cached = tokenCache.get(siteId);
  if (cached && cached.expiresAt > nowMs() + 30_000) return cached.token;

  const apiKey = requireEnv("MINDBODY_API_KEY");
  const username = requireEnv("MINDBODY_USERNAME");
  const password = requireEnv("MINDBODY_PASSWORD");

  const url = `${MBO_BASE_URL}/usertoken/issue`;

  const res = await axios.post(
    url,
    { Username: username, Password: password, SiteId: siteId },
    {
      headers: {
        "Api-Key": apiKey,
        "Content-Type": "application/json"
      },
      timeout: 20_000
    }
  );

  const token = res.data?.AccessToken || res.data?.access_token;
  const expiresIn = Number(res.data?.ExpiresIn ?? res.data?.expires_in ?? 900);

  if (!token) throw new Error("Mindbody token response missing AccessToken");

  tokenCache.set(siteId, { token, expiresAt: nowMs() + expiresIn * 1000 });
  return token;
}

function authHeaders(apiKey: string, token?: string) {
  const h: Record<string, string> = {
    "Api-Key": apiKey,
    "Content-Type": "application/json"
  };
  if (token) h.Authorization = `Bearer ${token}`;
  return h;
}

export async function findClient(
  siteId: number,
  input: { firstName?: string; lastName?: string; email?: string | null; phone?: string | null }
) {
  const apiKey = requireEnv("MINDBODY_API_KEY");
  const token = await getUserToken(siteId);

  const searchText =
    (input.email && input.email.trim()) ||
    (input.phone && input.phone.trim()) ||
    `${input.firstName ?? ""} ${input.lastName ?? ""}`.trim();

  const url = `${MBO_BASE_URL}/client/clients`;

  const res = await axios.get(url, {
    headers: authHeaders(apiKey, token),
    params: {
      SearchText: searchText,
      IsProspect: true
    },
    timeout: 20_000
  });

  const clients: any[] = res.data?.Clients ?? [];
  if (!clients.length) return null;

  const emailNorm = (input.email ?? "").toLowerCase().trim();
  const phoneDigits = (input.phone ?? "").replace(/\D/g, "");

  const exact = clients.find((c) => {
    const cEmail = (c?.Email ?? "").toLowerCase().trim();
    const cPhone = (c?.MobilePhone ?? "").replace(/\D/g, "");
    return (emailNorm && cEmail === emailNorm) || (phoneDigits && cPhone === phoneDigits);
  });

  return exact ?? clients[0] ?? null;
}

export async function createClient(
  siteId: number,
  input: { firstName: string; lastName: string; email: string; phone: string }
) {
  const apiKey = requireEnv("MINDBODY_API_KEY");
  const token = await getUserToken(siteId);

  const url = `${MBO_BASE_URL}/client/addclient`;

  const payload: any = {
    FirstName: input.firstName,
    LastName: input.lastName,
    Email: input.email,
    MobilePhone: input.phone,
    IsProspect: true
  };

  const res = await axios.post(url, payload, {
    headers: authHeaders(apiKey, token),
    timeout: 20_000
  });

  return res.data?.Client ?? res.data ?? null;
}

export async function addLeadSourceContactLog(
  siteId: number,
  clientId: string,
  leadSourceLabel: string
) {
  const apiKey = requireEnv("MINDBODY_API_KEY");
  const token = await getUserToken(siteId);

  const url = `${MBO_BASE_URL}/client/addcontactlog`;

  const assignedToStaffIdRaw = process.env.MINDBODY_ASSIGNED_TO_STAFF_ID;
  const assignedToStaffId = assignedToStaffIdRaw ? Number(assignedToStaffIdRaw) : undefined;

  const payload: any = {
    ClientId: clientId,
    Text: `Lead Source: ${leadSourceLabel}`,
    ContactName: leadSourceLabel,
    ContactMethod: "Other"
  };

  if (Number.isFinite(assignedToStaffId)) {
    payload.AssignedToStaffId = assignedToStaffId;
  }

  const res = await axios.post(url, payload, {
    headers: authHeaders(apiKey, token),
    timeout: 20_000
  });

  return res.data ?? null;
}
