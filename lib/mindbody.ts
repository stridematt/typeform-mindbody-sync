import axios from "axios";

const BASE_URL = "https://api.mindbodyonline.com/public/v6";

function requireEnv(name: string) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing ${name}`);
  return v;
}

async function getToken() {
  const apiKey = requireEnv("MINDBODY_API_KEY");
  const username = requireEnv("MINDBODY_USERNAME");
  const password = requireEnv("MINDBODY_PASSWORD");

  const res = await axios.post(
    `${BASE_URL}/usertoken/issue`,
    { Username: username, Password: password },
    {
      headers: {
        "Content-Type": "application/json",
        "Api-Key": apiKey
      },
      timeout: 15000
    }
  );

  return res.data?.AccessToken as string;
}

async function mbRequest(siteId: number) {
  const apiKey = requireEnv("MINDBODY_API_KEY");
  const token = await getToken();

  return axios.create({
    baseURL: BASE_URL,
    timeout: 20000,
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
  const client = await mbRequest(siteId);

  // Best match approach: email first, then phone, then name.
  const candidates: string[] = [];
  if (input.email) candidates.push(input.email);
  if (input.phone) candidates.push(input.phone);
  candidates.push(`${input.firstName} ${input.lastName}`.trim());

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
  const client = await mbRequest(siteId);

  const payload = {
    FirstName: input.firstName,
    LastName: input.lastName,
    Email: input.email,
    MobilePhone: input.phone,
    // Setting this helps keep it a lead/prospect
    IsProspect: true
  };

  const res = await client.post(`/client/addclient`, payload);
  return res.data?.Client ?? null;
}
