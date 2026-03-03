import axios from "axios";

const MINDBODY_BASE_URL = "https://api.mindbodyonline.com/public/v6";

function mbHeaders(siteId: number) {
  const apiKey = process.env.MINDBODY_API_KEY;
  if (!apiKey) throw new Error("Missing MINDBODY_API_KEY env var");

  return {
    "Api-Key": apiKey,
    "SiteId": String(siteId),
    "Content-Type": "application/json"
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
  const phone = normalizePhone(lead.phone);
  const email = normalizeEmail(lead.email);

  const searchText = phone || email;
  if (!searchText) return null;

  const url = `${MINDBODY_BASE_URL}/client/clients`;

  const resp = await axios.get(url, {
    headers: mbHeaders(siteId),
    params: {
      SearchText: searchText,
      Limit: 1,
      Offset: 0
    },
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
    headers: mbHeaders(siteId),
    timeout: 15000
  });

  const clients = resp.data?.Clients ?? [];
  return clients[0] ?? null;
}
