import type { NextApiRequest, NextApiResponse } from "next";
import crypto from "crypto";

function safeCompare(a: string, b: string) {
  const aBuf = Buffer.from(a);
  const bBuf = Buffer.from(b);
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

function digitsOnly(v?: string | null) {
  return (v || "").replace(/\D/g, "");
}

function last10(v?: string | null) {
  const d = digitsOnly(v);
  if (!d) return "";
  return d.length > 10 ? d.slice(-10) : d;
}

function getPhone(body: any) {
  const candidates = [
    body?.contact?.mobile_number,
    body?.contact?.mobile,
    body?.contact?.phone,
    body?.contact?.phone_number,
    body?.conversation?.contact?.mobile_number,
    body?.conversation?.contact?.phone,
  ];

  for (const c of candidates) {
    const p = last10(c);
    if (p) return p;
  }

  return "";
}

function getName(body: any) {
  const name =
    body?.contact?.name ||
    [
      body?.contact?.first_name,
      body?.contact?.last_name
    ].filter(Boolean).join(" ");

  return name || "Avochato Contact";
}

function buildSummary(body: any) {
  const summary =
    body?.summary?.text ||
    body?.summary ||
    body?.data?.summary ||
    "";

  const latest =
    body?.latest_message?.text ||
    body?.latest_message?.body ||
    body?.message?.text ||
    body?.message?.body ||
    "";

  const convo =
    body?.conversation?.id ||
    body?.conversation_id ||
    "";

  const tags = body?.contact?.tags || body?.tags || [];

  const parts = [
    "AvoAI Summary Generated",
    convo ? `Conversation ID: ${convo}` : null,
    tags.length ? `Tags: ${tags.join(", ")}` : null,
    summary ? `Summary:\n${summary}` : null,
    latest ? `Latest Message:\n${latest}` : null,
  ].filter(Boolean);

  return parts.join("\n\n").slice(0, 4000);
}

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  try {

    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const expected = process.env.AVOCHATO_WEBHOOK_SECRET;

    if (!expected) {
      console.error("Missing AVOCHATO_WEBHOOK_SECRET env var");
      return res.status(500).json({ error: "Server configuration error" });
    }

    const headerSecret = req.headers["x-avochato-secret"];
    const querySecret = req.query.secret;

    const provided =
      (Array.isArray(headerSecret) ? headerSecret[0] : headerSecret) ||
      (Array.isArray(querySecret) ? querySecret[0] : querySecret);

    if (!provided || !safeCompare(provided, expected)) {
      console.error("Unauthorized webhook attempt");
      return res.status(401).json({ error: "Unauthorized" });
    }

    const body = req.body || {};

    console.log("Webhook received");
    console.log("Payload:", JSON.stringify(body));

    const phone = getPhone(body);

    if (!phone) {
      console.error("No phone number found in payload");
      return res.status(400).json({ error: "Phone number missing" });
    }

    const name = getName(body);
    const summary = buildSummary(body);

    console.log("Parsed phone:", phone);
    console.log("Contact name:", name);

    const siteId = process.env.MINDBODY_SITE_ID_HB;

    if (!siteId) {
      throw new Error("Missing MINDBODY_SITE_ID_HB");
    }

    /*
      STEP 1
      Lookup client in Mindbody by phone
      (Implementation depends on your existing SOAP / API helper)
    */

    const clientId = null; // placeholder until lookup implemented

    if (!clientId) {
      console.log("Client not found for phone:", phone);

      return res.status(202).json({
        ok: true,
        message: "Webhook received but client not found",
        phone
      });
    }

    /*
      STEP 2
      Create contact log in Mindbody
    */

    const result = {
      clientId,
      summary
    };

    console.log("Contact log created", result);

    return res.status(200).json({
      ok: true,
      clientId
    });

  } catch (err: any) {
    console.error(err);
    return res.status(500).json({
      error: err?.message || "Server error"
    });
  }
}
