import { NextResponse } from "next/server";
import axios from "axios";

export const runtime = "nodejs";

const MINDBODY_BASE_URL = "https://api.mindbodyonline.com/public/v6";

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const siteId = Number(searchParams.get("siteId"));

    if (!siteId) {
      return NextResponse.json(
        { ok: false, error: "Missing siteId" },
        { status: 400 }
      );
    }

    const apiKey = process.env.MINDBODY_API_KEY;
    const username = process.env.MINDBODY_USERNAME;
    const password = process.env.MINDBODY_PASSWORD;

    if (!apiKey || !username || !password) {
      return NextResponse.json(
        { ok: false, error: "Missing Mindbody env vars" },
        { status: 500 }
      );
    }

    const resp = await axios.post(
      `${MINDBODY_BASE_URL}/usertoken/issue`,
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

    return NextResponse.json({
      ok: true,
      siteId,
      hasAccessToken: !!resp.data?.AccessToken,
      data: resp.data,
    });
  } catch (err: any) {
    return NextResponse.json(
      {
        ok: false,
        error: err?.message ?? "Server error",
        status: err?.response?.status ?? null,
        data: err?.response?.data ?? null,
        where: err?.config?.url ?? null,
      },
      { status: 500 }
    );
  }
}
