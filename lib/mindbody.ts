import axios from "axios";

const BASE_URL = "https://api.mindbodyonline.com/public/v6";

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

/**
 * Issue a Mindbody user token.
 * Some accounts require a SiteId header here, and it must be a valid numeric site.
 * We use MINDBODY_TOKEN_SITE_ID for this purpose.
 */
async function getToken() {
  const apiKey = requireEnv("MINDBODY_API_KEY");
  const username = requireEnv("MINDBODY_USERNAME");
  const password = requireEnv("MINDBODY_PASSWORD");

  const tokenSiteIdRaw = requireEnv("MINDBODY_TOKEN_SITE_ID");
  const tokenSiteId = parseSiteId(tokenSiteIdRaw, "MINDBODY_TOKEN_SITE_ID");

  const res = await axios.post(
    `${BASE_URL}/usertoken/issue`,
    { Username: username, Password: password },
    {
      headers: {
        "Content-Type": "application/json",
        "Api-Key": apiKey,
        // Important: use a known-good site id here (digits only)
        SiteId: String(tokenSiteId)
      },
      timeout: 20000
    }
  );

  const accessToken = res.data?.AccessToken as string | undefined;
  if (!accessToken) {
    throw new Error("Mindbody token response missing AccessToken");
  }
  return accessToken;
}

async function mbClient(siteId: number) {
  const apiKey = requireEnv("MINDBODY_API_KEY");
  const token = await getToken();

  if (!Number.isInteger(siteId) || siteId <= 0) {
    throw new Error(`Invalid siteId passed to mbClient: ${siteId}`);
  }

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
  input: { firstName: string; lastName: string; email: string; phone: string }
) {
  const client = await mbClient(siteId);

  const candidates: string[] = [];
  if (input.email) candidates.push(input.email);
  if (input.phone) candidates.push(input.phone);
  const fullName = `${input.firstName} ${input.lastName}`.trim();
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
  input: { firstName: string; lastName: string; email: string; phone: string }
) {
  const client = await mbClient(siteId);

  const payload = {
    FirstName: input.firstName,
    LastName: input.lastName,
    Email: input.email,
    MobilePhone: input.phone,
    IsProspect: true
  };

  const res = await client.post(`/client/addclient`, payload);
  return res.data?.Client ?? null;
}
