import { NextResponse } from "next/server";
import axios from "axios";

const MINDBODY_BASE_URL = "https://api.mindbodyonline.com/public/v6";

export const runtime = "nodejs";

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
  if (!token) throw new Error("No AccessToken returned");

  tokenCache.set(siteId, { token, expiresAt: now + 45 * 60 * 1000 });
  return token;
}

async function authHeaders(siteId: number) {
  const token = await getUserToken(siteId);
  return { ...baseHeaders(siteId), Authorization: `Bearer ${token}` };
}

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const siteId = Number(searchParams.get("siteId"));
    const clientId = searchParams.get("clientId");

    if (!siteId || !clientId) {
      return NextResponse.json(
        { ok: false, error: "Missing siteId or clientId" },
        { status: 400 }
      );
    }

    const resp = await axios.get(`${MINDBODY_BASE_URL}/client/client`, {
      headers: await authHeaders(siteId),
      params: { ClientId: clientId },
      timeout: 15000
    });

    return NextResponse.json({ ok: true, client: resp.data });
  } catch (err: any) {
    const status = err?.response?.status ?? null;
    const data = err?.response?.data ?? null;
    const where = typeof err?.config?.url === "string" ? err.config.url : null;

    return NextResponse.json(
      { ok: false, error: err?.message ?? "Server error", mindbody: { status, where, data } },
      { status: 500 }
    );
  }
}
