import type { NextApiRequest, NextApiResponse } from "next";
import crypto from "crypto";

/* =========================
   Small utilities
========================= */

function mustEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing ${name}`);
  return v;
}

function firstVal(v: any): string | undefined {
  if (v === undefined || v === null) return undefined;
  return Array.isArray(v) ? String(v[0]) : String(v);
}

function timingSafeEqual(a: string, b: string) {
  const aa = Buffer.from(a || "", "utf8");
  const bb = Buffer.from(b || "", "utf8");
  if (aa.length !== bb.length) return false;
  return crypto.timingSafeEqual(aa, bb);
}

function digitsOnly(input?: string | null): string {
  if (!input) return "";
  return String(input).replace(/\D/g, "");
}

function last10Digits(input?: string | null): string {
  const d = digitsOnly(input);
  if (!d) return "";
  return d.length > 10 ? d.slice(-10) : d;
}

function escapeXml(s: string) {
  return s
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

/* =========================
   Avochato payload parsing
========================= */

function getAvochatoMobile(body: any): { raw?: string; key10?: string } {
  const candidates = [
    body?.contact?.mobile_number, // your Postman payload
    body?.contact?.mobileNumber,
    body?.contact?.mobile,
    body?.contact?.phone_number,
    body?.contact?.phoneNumber,
    body?.contact?.phone,
    body?.conversation?.contact?.mobile_number,
    body?.conversation?.contact?.phone,
  ];

  for (const c of candidates) {
    const raw = typeof c === "string" ? c : c?.toString?.();
    const key10 = last10Digits(raw);
    if (key10) return { raw, key10 };
  }

  return {};
}

function getContactName(body: any): string {
  const name =
    body?.contact?.name ||
    [body?.contact?.first_name, body?.contact?.last_name].filter(Boolean).join(" ") ||
    [body?.contact?.firstName, body?.contact?.lastName].filter(Boolean).join(" ") ||
    "Avochato Contact";

  return String(name);
}

function getTags(body: any): string[] {
  const tags = body?.contact?.tags ?? body?.tags;
  if (Array.isArray(tags)) return tags.map((t) => String(t));
  return [];
}

function buildContactLogText(body: any): string {
  const tags = getTags(body);
  const convoId = body?.conversation?.id || body?.conversation_id || body?.conversationId || "";

  const summary =
    body?.summary?.text ||
    body?.summary ||
    body?.data?.summary ||
    body?.av_ai_summary ||
    "";

  const latest =
    body?.latest_message?.text ||
    body?.latest_message?.body ||
    body?.latestMessage?.text ||
    body?.message?.text ||
    body?.message?.body ||
    "";

  const parts = [
    "AvoAI Summary (Avochato)",
    convoId ? `Conversation ID: ${convoId}` : null,
    tags.length ? `Tags: ${tags.join(", ")}` : null,
    summary ? `Summary:\n${String(summary).trim()}` : null,
    latest ? `Latest Message:\n${String(latest).trim()}` : null,
  ].filter(Boolean);

  return parts.join("\n\n").slice(0, 3800);
}

/* =========================
   Mindbody SOAP: GetClients
========================= */

async function mindbodySoapGetClients(params: {
  siteId: number;
  searchText: string;
}): Promise<string> {
  const soapUrl = "https://api.mindbodyonline.com/0_5_1/ClientService.asmx";
  const soapAction = "http://clients.mindbodyonline.com/api/0_5_1/GetClients";

  const sourceName = mustEnv("MINDBODY_SOURCE_NAME");
  const sourcePass = mustEnv("MINDBODY_SOURCE_PASSWORD");

  // Important: Fields ensures phones come back in response
  const xml = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetClients xmlns="http://clients.mindbodyonline.com/api/0_5_1">
      <Request>
        <SourceCredentials>
          <SourceName>${escapeXml(sourceName)}</SourceName>
          <Password>${escapeXml(sourcePass)}</Password>
          <SiteIDs>
            <int>${params.siteId}</int>
          </SiteIDs>
        </SourceCredentials>

        <SearchText>${escapeXml(params.searchText)}</SearchText>
        <PageSize>50</PageSize>
        <CurrentPageIndex>0</CurrentPageIndex>

        <Fields>
          <string>Clients.ID</string>
          <string>Clients.FirstName</string>
          <string>Clients.LastName</string>
          <string>Clients.Email</string>
          <string>Clients.MobilePhone</string>
          <string>Clients.HomePhone</string>
          <string>Clients.WorkPhone</string>
        </Fields>
      </Request>
    </GetClients>
  </soap:Body>
</soap:Envelope>`;

  const r = await fetch(soapUrl, {
    method: "POST",
    headers: {
      "Content-Type": "text/xml; charset=utf-8",
      SOAPAction: soapAction,
    },
    body: xml,
  });

  const text = await r.text();
  if (!r.ok) throw new Error(`Mindbody SOAP GetClients HTTP ${r.status}: ${text.slice(0, 400)}`);
  return text;
}

function extractClientsFromSoap(xml: string): Array<{
  id?: string;
  mobilePhone?: string;
  homePhone?: string;
  workPhone?: string;
  firstName?: string;
  lastName?: string;
  email?: string;
}> {
  const clients: any[] = [];
  const blocks = xml.match(/<Client\b[\s\S]*?<\/Client>/g) || [];

  for (const b of blocks) {
    const get = (tag: string) => {
      const m = b.match(new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`));
      return m?.[1]?.trim();
    };

    clients.push({
      id: get("ID") || get("Id"),
      mobilePhone: get("MobilePhone"),
      homePhone: get("HomePhone"),
      workPhone: get("WorkPhone"),
      firstName: get("FirstName"),
      lastName: get("LastName"),
      email: get("Email"),
    });
  }

  return clients;
}

async function mindbodyFindClientIdByPhone(params: {
  siteId: number;
  phoneKey10: string;
  fullDigits: string;
}): Promise<string | null> {
  const variants = Array.from(
    new Set(
      [
        params.phoneKey10, // best for US
        params.fullDigits, // sometimes works
        params.fullDigits.length === 11 && params.fullDigits.startsWith("1") ? params.fullDigits.slice(1) : "",
      ].filter(Boolean)
    )
  );

  for (const searchText of variants) {
    const xml = await mindbodySoapGetClients({ siteId: params.siteId, searchText });
    const clients = extractClientsFromSoap(xml);

    console.log("SOAP searchText:", searchText, "clientsReturned:", clients.length);
    if (clients.length) {
      const c0 = clients[0];
      console.log("SOAP first client sample:", {
        id: c0.id,
        firstName: c0.firstName,
        lastName: c0.lastName,
        mobilePhone: c0.mobilePhone,
        homePhone: c0.homePhone,
        workPhone: c0.workPhone,
        email: c0.email,
      });
    }

    const hit = clients.find((c) => {
      const phones = [c.mobilePhone, c.homePhone, c.workPhone].map((p) => last10Digits(p));
      return phones.includes(params.phoneKey10);
    });

    if (hit?.id) return String(hit.id);
  }

  return null;
}

/* =========================
   Mindbody Public API v6: Create Contact Log
========================= */

async function mindbodyIssueUserToken(siteId: number): Promise<string> {
  const apiKey = mustEnv("MINDBODY_API_KEY");
  const username = mustEnv("MINDBODY_STAFF_USERNAME");
  const password = mustEnv("MINDBODY_STAFF_PASSWORD");

  const r = await fetch("https://api.mindbodyonline.com/public/v6/usertoken/issue", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Api-Key": apiKey,
      SiteId: String(siteId),
    },
    body: JSON.stringify({ Username: username, Password: password }),
  });

  const json = await r.json().catch(() => ({}));
  if (!r.ok) {
    throw new Error(`Mindbody usertoken/issue failed ${r.status}: ${JSON.stringify(json).slice(0, 400)}`);
  }

  const token = json?.AccessToken || json?.access_token || json?.Token;
  if (!token) throw new Error(`Mindbody token missing: ${JSON.stringify(json).slice(0, 300)}`);

  return String(token);
}

async function mindbodyAddContactLog(params: {
  siteId: number;
  userToken: string;
  clientId: string;
  contactName: string;
  text: string;
}): Promise<any> {
  const apiKey = mustEnv("MINDBODY_API_KEY");
  const typeId = Number(mustEnv("MINDBODY_CONTACT_LOG_TYPE_ID")); // "Avochato Summary"

  const payload = {
    client_id: params.clientId,
    text: params.text,
    contact_method: "Phone",
    contact_name: params.contactName,
    types: [{ id: typeId }],
    test: false,
  };

  const r = await fetch("https://api.mindbodyonline.com/public/v6/client/addcontactlog", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Api-Key": apiKey,
      SiteId: String(params.siteId),
      Authorization: `Bearer ${params.userToken}`,
    },
    body: JSON.stringify(payload),
  });

  const json = await r.json().catch(() => ({}));
  if (!r.ok) {
    throw new Error(`Mindbody addcontactlog failed ${r.status}: ${JSON.stringify(json).slice(0, 500)}`);
  }

  return json;
}

/* =========================
   Handler
========================= */

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  try {
    if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

    // Auth: header OR query
    const expected = mustEnv("AVOCHATO_WEBHOOK_SECRET");
    const headerSecret = firstVal(req.headers["x-avochato-secret"] as any);
    const querySecret = firstVal(req.query.secret as any);
    const provided = headerSecret || querySecret;

    if (!provided || !timingSafeEqual(String(provided), expected)) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const body = req.body || {};
    const siteId = Number(mustEnv("MINDBODY_SITE_ID_HB"));

    const mobile = getAvochatoMobile(body);
    if (!mobile.key10) return res.status(400).json({ error: "Phone number missing in payload" });

    const fullDigits = digitsOnly(mobile.raw || "");
    const contactName = getContactName(body);
    const logText = buildContactLogText(body);

    console.log("Incoming phone raw:", mobile.raw, "phoneKey10:", mobile.key10, "fullDigits:", fullDigits);
    console.log("Using siteId:", siteId);

    // 1) Find Mindbody client by phone (SOAP)
    const clientId = await mindbodyFindClientIdByPhone({
      siteId,
      phoneKey10: mobile.key10,
      fullDigits,
    });

    if (!clientId) {
      return res.status(202).json({
        ok: true,
        message: "Webhook received but client not found",
        phone: mobile.key10,
      });
    }

    // 2) Create contact log (Public API v6)
    const token = await mindbodyIssueUserToken(siteId);
    const mbResp = await mindbodyAddContactLog({
      siteId,
      userToken: token,
      clientId,
      contactName,
      text: logText,
    });

    return res.status(200).json({
      ok: true,
      clientId,
      phone: mobile.key10,
      mindbody: mbResp,
    });
  } catch (err: any) {
    console.error(err);
    return res.status(500).json({ error: err?.message || "Server error" });
  }
}
