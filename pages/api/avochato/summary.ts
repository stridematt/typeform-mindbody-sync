import type { NextApiRequest, NextApiResponse } from "next";

function getHeader(req: NextApiRequest, name: string) {
  const v = req.headers[name.toLowerCase()];
  return Array.isArray(v) ? v[0] : v;
}

function mustEnv(name: string) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env ${name}`);
  return v;
}

// Keep only digits. This is the key to matching across systems reliably.
function digitsOnly(input?: string | null): string {
  if (!input) return "";
  return String(input).replace(/\D/g, "");
}

// Normalize to last 10 digits for US matching (Mindbody often stores with/without +1)
function last10(d: string): string {
  if (!d) return "";
  return d.length > 10 ? d.slice(-10) : d;
}

function buildNote(opts: {
  summaryText?: string;
  latestMessageText?: string;
  contactName?: string;
  mobile?: string;
  email?: string;
  tags?: string[];
  conversationId?: string;
  createdAt?: string;
}) {
  const lines: string[] = [];
  lines.push("AvoAI Summary (Avochato)");
  lines.push("");
  if (opts.summaryText) {
    lines.push(`Summary: ${opts.summaryText}`);
    lines.push("");
  }
  if (opts.latestMessageText) {
    lines.push(`Latest message: "${opts.latestMessageText}"`);
    lines.push("");
  }
  const contactBits = [
    opts.contactName ? `Name: ${opts.contactName}` : null,
    opts.mobile ? `Mobile: ${opts.mobile}` : null,
    opts.email ? `Email: ${opts.email}` : null,
  ].filter(Boolean);
  if (contactBits.length) lines.push(contactBits.join(" | "));
  if (opts.conversationId) lines.push(`Conversation ID: ${opts.conversationId}`);
  if (opts.tags?.length) lines.push(`Tags: ${opts.tags.join(", ")}`);
  if (opts.createdAt) lines.push(`Captured at: ${opts.createdAt}`);
  return lines.join("\n");
}

/**
 * MINDBODY SOAP call: GetClients
 * Docs show the SOAP operation and request shape. :contentReference[oaicite:0]{index=0}
 *
 * Implementation notes:
 * - We use SearchText with multiple variants of the phone.
 * - Then we do exact matching on returned client phone fields after normalization.
 */
async function mindbodyGetClientsBySearchText(params: {
  siteId: number;
  searchText: string;
}) {
  const soapUrl = "https://api.mindbodyonline.com/0_5_1/ClientService.asmx";
  const soapAction = "http://clients.mindbodyonline.com/api/0_5_1/GetClients";

  const sourceName = mustEnv("MINDBODY_SOURCE_NAME");
  const sourcePass = mustEnv("MINDBODY_SOURCE_PASSWORD");

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
  if (!r.ok) throw new Error(`Mindbody GetClients HTTP ${r.status}: ${text.slice(0, 300)}`);

  return text;
}

// Minimal XML escaping
function escapeXml(s: string) {
  return s
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

/**
 * Very lightweight XML extraction for Client IDs and phone fields.
 * If you already have an XML parser in your repo, swap this out.
 */
function extractClientsFromGetClientsResponse(xml: string): Array<{
  id?: string;
  mobilePhone?: string;
  homePhone?: string;
  workPhone?: string;
  email?: string;
  firstName?: string;
  lastName?: string;
}> {
  // This is intentionally simple and resilient.
  // We look for <Client> ... </Client> blocks and grab key tags.
  const clients: any[] = [];
  const clientBlocks = xml.match(/<Client\b[\s\S]*?<\/Client>/g) || [];
  for (const block of clientBlocks) {
    const get = (tag: string) => {
      const m = block.match(new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`));
      return m?.[1]?.trim();
    };
    clients.push({
      id: get("ID") || get("Id"),
      mobilePhone: get("MobilePhone"),
      homePhone: get("HomePhone"),
      workPhone: get("WorkPhone"),
      email: get("Email"),
      firstName: get("FirstName"),
      lastName: get("LastName"),
    });
  }
  return clients;
}

/**
 * TODO: Implement note creation in your preferred Mindbody method.
 * Some orgs store notes via SOAP or via a supported internal endpoint.
 * If you already have a “create client note/log” call in your existing webhook, reuse it here.
 */
async function mindbodyCreateClientNote(params: {
  siteId: number;
  clientId: string;
  note: string;
}) {
  // Placeholder: wire into your existing Mindbody write call.
  // If you paste the file where you already create clients / write data,
  // I’ll plug the correct "create note/log" call into this function.
  console.log("TODO mindbodyCreateClientNote", params);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  try {
    if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

    // Security: shared secret header from Avochato webhook settings
    const expected = mustEnv("AVOCHATO_WEBHOOK_SECRET");
    const provided = getHeader(req, "x-avochato-secret");
    if (!provided || provided !== expected) return res.status(401).json({ error: "Unauthorized" });

    const hbSiteId = Number(mustEnv("MINDBODY_HB_SITE_ID"));

    // Payload fields can vary. We’ll focus on what you asked: mobile number for matching.
    const payload: any = req.body;

    const tags: string[] | undefined = payload?.contact?.tags || payload?.tags;
    const contactName =
      payload?.contact?.name ||
      [payload?.contact?.first_name, payload?.contact?.last_name].filter(Boolean).join(" ") ||
      undefined;

    // IMPORTANT: use mobile number from Avochato
    const mobileRaw =
      payload?.contact?.mobile ||
      payload?.contact?.mobile_number ||
      payload?.contact?.phone_number ||
      payload?.contact?.phone ||
      undefined;

    const email =
      payload?.contact?.email ||
      (Array.isArray(payload?.contact?.emails) ? payload.contact.emails[0] : undefined);

    const summaryText =
      payload?.summary?.text ||
      payload?.summary ||
      payload?.av_ai_summary ||
      payload?.data?.summary ||
      undefined;

    const latestMessageText =
      payload?.latest_message?.text ||
      payload?.latestMessage?.text ||
      payload?.message?.text ||
      undefined;

    const conversationId =
      payload?.conversation?.id ||
      payload?.conversation_id ||
      payload?.conversation?.conversation_id ||
      undefined;

    const createdAt = payload?.created_at || new Date().toISOString();

    const mobileDigits = digitsOnly(mobileRaw);
    const mobileLast10 = last10(mobileDigits);

    if (!mobileLast10) {
      return res.status(400).json({ error: "Missing contact mobile number in payload" });
    }

    // Search Mindbody multiple ways because SearchText behavior can vary.
    // 1) last 10 digits (best for US)
    // 2) full digits
    const searchVariants = Array.from(
      new Set([mobileLast10, mobileDigits].filter(Boolean))
    );

    let matchedClientId: string | null = null;
    let debugCandidates: any[] = [];

    for (const searchText of searchVariants) {
      const xml = await mindbodyGetClientsBySearchText({ siteId: hbSiteId, searchText });
      const clients = extractClientsFromGetClientsResponse(xml);
      debugCandidates = debugCandidates.concat(clients);

      // Exact match by normalized phone across all returned fields
      const hit = clients.find((c) => {
        const candidatePhones = [c.mobilePhone, c.homePhone, c.workPhone].map((p) => last10(digitsOnly(p)));
        return candidatePhones.includes(mobileLast10);
      });

      if (hit?.id) {
        matchedClientId = hit.id;
        break;
      }
    }

    if (!matchedClientId) {
      // No match. Do not create a new client from a summary event.
      return res.status(202).json({
        ok: true,
        skipped: true,
        reason: "No matching Mindbody client by phone",
        hbSiteId,
        mobileLast10,
        candidatesChecked: debugCandidates.length,
      });
    }

    const note = buildNote({
      summaryText,
      latestMessageText,
      contactName,
      mobile: mobileRaw,
      email,
      tags,
      conversationId,
      createdAt,
    });

    await mindbodyCreateClientNote({ siteId: hbSiteId, clientId: matchedClientId, note });

    return res.status(200).json({ ok: true, hbSiteId, clientId: matchedClientId });
  } catch (err: any) {
    console.error(err);
    return res.status(500).json({ error: err?.message || "Server error" });
  }
}
