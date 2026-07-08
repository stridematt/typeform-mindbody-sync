// app/api/cancellation-log/route.ts
//
// Typeform "cancellation survey" webhook -> writes a CLEAN contact log onto
// the member's existing Mindbody client record.
//
// Identity comes from a HIDDEN field on the Typeform link: the exact Mindbody
// ClientId (plus `siteid`, `first_name`, `last_name`). No phone/email match is
// needed. If the client id is missing or the client can't be found, we SKIP
// and report 200 (the member already exists; a miss means bad link data, not a
// new lead — and returning 200 stops Typeform from retrying).
//
// Contact-log call mirrors the proven coach-log flow: send ContactMethod
// (required by Mindbody) and NEITHER FollowupByDate NOR AssignedTo, which makes
// it a plain log entry instead of a Sales-Pipeline follow-up task.
import { NextResponse } from "next/server";
import crypto from "crypto";

export const runtime = "nodejs";

const MINDBODY_BASE_URL = "https://api.mindbodyonline.com/public/v6";

/* ============================================================
   ALLOWED STUDIO SITE IDS
   Built from the per-studio Vercel env vars. The Typeform link
   sends the numeric `siteid` directly, so we don't MAP by studio
   name here — we just use this set to validate that the incoming
   siteid is one of our real studios. A siteid not in this set is
   skipped (see handler). If NONE of these env vars are set, the
   allowlist check is disabled so the route still works.
============================================================ */
/**
 * Numeric ID of the "Notes" contact-log TYPE (the checkbox in Mindbody).
 * The API can't look types up by name, so the ID is configured via env.
 * Type IDs can differ per site, so a per-site override is checked first:
 *   MINDBODY_NOTES_TYPE_ID_<siteId>   e.g. MINDBODY_NOTES_TYPE_ID_5749887
 * falling back to a shared MINDBODY_NOTES_TYPE_ID. Returns null if unset,
 * in which case the log is still written but no type box is ticked.
 */
function notesTypeId(siteId: number): number | null {
  const raw =
    process.env[`MINDBODY_NOTES_TYPE_ID_${siteId}`] ?? process.env.MINDBODY_NOTES_TYPE_ID;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function allowedSiteIds(): Set<number> {
  return new Set(
    [
      process.env.MINDBODY_SITE_ID_HB,
      process.env.MINDBODY_SITE_ID_PASADENA,
      process.env.MINDBODY_SITE_ID_TUSTIN,
      process.env.MINDBODY_SITE_ID_SOUTHLANDS,
      process.env.MINDBODY_SITE_ID_SOUTHAMPTON,
    ]
      .map((v) => Number(v))
      .filter((n) => Number.isFinite(n) && n > 0)
  );
}

/* ============================================================
   Token cache per SiteId
============================================================ */
const _tokenCache = new Map<number, { token: string; issuedAt: number }>();
const _TOKEN_TTL_MS = 10 * 60 * 1000;

async function getToken(siteId: number): Promise<string> {
  const apiKey = process.env.MINDBODY_API_KEY;
  const username = process.env.MINDBODY_USERNAME;
  const password = process.env.MINDBODY_PASSWORD;
  if (!apiKey || !username || !password) throw new Error("Missing Mindbody credentials");

  const cached = _tokenCache.get(siteId);
  if (cached && Date.now() - cached.issuedAt < _TOKEN_TTL_MS) return cached.token;

  const res = await fetch(`${MINDBODY_BASE_URL}/usertoken/issue`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "Api-Key": apiKey, SiteId: String(siteId) },
    body: JSON.stringify({ Username: username, Password: password }),
  });
  const json: any = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`usertoken/issue ${res.status}: ${JSON.stringify(json).slice(0, 400)}`);
  const token = json?.AccessToken;
  if (!token) throw new Error("Mindbody token response missing AccessToken");

  _tokenCache.set(siteId, { token, issuedAt: Date.now() });
  return token;
}

/* ============================================================
   Typeform signature verification
============================================================ */
function timingSafeEqual(a: string, b: string) {
  const aBuf = Buffer.from(a || "", "utf8");
  const bBuf = Buffer.from(b || "", "utf8");
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

function verifyTypeform(req: Request, rawBody: string): boolean {
  const secret = process.env.TYPEFORM_CANCELLATION_SECRET || process.env.TYPEFORM_WEBHOOK_SECRET;
  if (!secret) throw new Error("Missing TYPEFORM_CANCELLATION_SECRET");

  const sigHeader =
    req.headers.get("typeform-signature") || req.headers.get("Typeform-Signature");
  if (sigHeader) {
    const expected = crypto.createHmac("sha256", secret).update(rawBody).digest("base64");
    const provided = sigHeader.startsWith("sha256=") ? sigHeader.slice("sha256=".length) : sigHeader;
    return timingSafeEqual(provided, expected);
  }
  // Fallback: some setups send a plain shared-secret header instead of an HMAC.
  const headerSecret = req.headers.get("typeform-secret");
  return !!headerSecret && headerSecret === secret;
}

/* ============================================================
   Payload extraction
============================================================ */
function normalize(s: string) {
  return s.toLowerCase().trim().replace(/\s+/g, " ");
}

function getAnswerValue(answer: any): string | null {
  if (!answer) return null;
  switch (answer.type) {
    case "text":
    case "long_text":
      return answer.text ?? null;
    case "choice":
      // A multiple-choice "Other" answer carries free text in choice.other.
      return answer.choice?.label ?? answer.choice?.other ?? null;
    case "choices":
      return answer.choices?.labels?.join(", ") ?? answer.choices?.other ?? null;
    case "dropdown":
      return answer.dropdown?.label ?? null;
    case "email":
      return answer.email ?? null;
    case "phone_number":
      return answer.phone_number ?? null;
    case "number":
      return String(answer.number);
    case "boolean":
      return String(answer.boolean);
    default:
      return null;
  }
}

/** Pull the first hidden value present under any of the given keys. */
function hiddenVal(hidden: Record<string, any>, keys: string[]): string | null {
  for (const k of keys) {
    const v = hidden?.[k];
    if (v !== undefined && v !== null && String(v).trim()) return String(v).trim();
  }
  return null;
}

function extractLead(payload: any) {
  const fr = payload?.form_response ?? {};
  const formId = fr.form_id;
  const token = fr.token;
  const hidden: Record<string, any> = fr.hidden ?? {};
  const answers: any[] = fr.answers ?? [];
  const fields: any[] = fr.definition?.fields ?? [];

  const titleById = new Map<string, string>();
  for (const f of fields) if (f?.id) titleById.set(f.id, String(f.title ?? ""));
  const titleOf = (a: any) => titleById.get(a?.field?.id ?? "") ?? "";

  // Find an answer whose field title contains ANY of the patterns.
  const byTitle = (patterns: string[]): string | null => {
    for (const a of answers) {
      const t = normalize(titleOf(a));
      if (patterns.some((p) => t.includes(normalize(p)))) {
        const v = getAnswerValue(a);
        if (v && v.toString().trim()) return v.toString().trim();
      }
    }
    return null;
  };

  // Identity — hidden fields on the personalized link.
  const clientId = hiddenVal(hidden, [
    "mindbodyClientId",
    "mindbody_client_id",
    "clientId",
    "client_id",
    "clientid",
    "mbClientId",
    "mb_client_id",
  ]);
  const siteId = hiddenVal(hidden, ["siteid", "siteId", "site_id", "SiteId"]);
  const firstName = hiddenVal(hidden, ["first_name", "firstName", "firstname"]) ?? "";
  const lastName = hiddenVal(hidden, ["last_name", "lastName", "lastname"]) ?? "";

  // Survey answers.
  const reason =
    byTitle(["main reason", "reason you're cancelling", "reason you are cancelling", "why are you cancelling"]) ??
    null;
  const improvement =
    byTitle(["done better", "one thing we could", "could have done", "how could we improve", "improve"]) ??
    null;

  return {
    formId,
    token,
    clientId,
    siteId: siteId ? Number(siteId) : null,
    firstName,
    lastName,
    reason,
    improvement,
    answersCount: answers.length,
  };
}

/* ============================================================
   Clean contact-log body (HTML — Mindbody renders it)
   Bold header, then each question as its own block: bold label
   on one line, the answer beneath, with spacing between blocks.
   Visitor-supplied answers are escaped so they can't break markup.
============================================================ */
function esc(s?: string | null): string {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function buildLogText(lead: ReturnType<typeof extractLead>): string {
  const block = (label: string, value: string | null) =>
    `<p><strong>${label}</strong><br>${esc(value || "(not provided)")}</p>`;

  const html =
    `<p><strong>Cancellation Survey Response</strong></p>` +
    block("Reason for cancelling:", lead.reason) +
    block("What we could have done better:", lead.improvement);

  return html.slice(0, 3800);
}

/* ============================================================
   Mindbody: verify client exists + add contact log
============================================================ */
async function clientExists(siteId: number, clientId: string): Promise<boolean> {
  const apiKey = process.env.MINDBODY_API_KEY as string;
  const token = await getToken(siteId);
  const url = `${MINDBODY_BASE_URL}/client/clients?ClientIds=${encodeURIComponent(clientId)}&Limit=1`;
  const res = await fetch(url, {
    headers: { "Api-Key": apiKey, Authorization: `Bearer ${token}`, SiteId: String(siteId) },
  });
  if (!res.ok) return false;
  const json: any = await res.json().catch(() => ({}));
  const clients: any[] = json?.Clients ?? [];
  return clients.some((c) => String(c?.Id) === String(clientId));
}

async function addContactLog(
  siteId: number,
  input: {
    clientId: string;
    text: string;
    contactName?: string;
    contactMethod?: string;
    typeIds?: number[];
  }
) {
  const apiKey = process.env.MINDBODY_API_KEY as string;
  const token = await getToken(siteId);

  const payload: Record<string, any> = {
    ClientId: String(input.clientId),
    Text: input.text,
    // ContactMethod is REQUIRED by addcontactlog. Always send it.
    ContactMethod: input.contactMethod || "Note",
    // NO FollowupByDate and NO AssignedToStaffId -> plain contact-log entry
    // (Mindbody requires those two to be both present or both absent).
  };
  if (input.contactName) payload.ContactName = input.contactName;
  // Ticks the matching type checkbox(es) in Mindbody (e.g. "Notes").
  // The API only accepts numeric type IDs, so these come from env (see
  // notesTypeId). Omitted entirely when no ID is configured.
  if (input.typeIds && input.typeIds.length) {
    payload.Types = input.typeIds.map((id) => ({ Id: id }));
  }

  const res = await fetch(`${MINDBODY_BASE_URL}/client/addcontactlog`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Api-Key": apiKey,
      Authorization: `Bearer ${token}`,
      SiteId: String(siteId),
    },
    body: JSON.stringify(payload),
  });
  const json: any = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`addcontactlog ${res.status}: ${JSON.stringify(json).slice(0, 800)}`);
  return json ?? null;
}

/* ============================================================
   Handler
============================================================ */
export async function POST(req: Request) {
  try {
    const rawBody = await req.text();

    if (!verifyTypeform(req, rawBody)) {
      return NextResponse.json(
        { ok: false, error: "Unauthorized (Typeform verification failed)" },
        { status: 401 }
      );
    }

    let payload: any;
    try {
      payload = JSON.parse(rawBody);
    } catch {
      return NextResponse.json({ ok: false, error: "Invalid JSON" }, { status: 400 });
    }

    const lead = extractLead(payload);

    // Typeform "test" ping / empty payload -> acknowledge so setup succeeds.
    if (!lead.formId || !lead.token || lead.answersCount === 0) {
      return NextResponse.json({ ok: true, status: "typeform_test_ok" });
    }

    // Skip + report (200) when we can't identify the member.
    if (!lead.clientId) {
      return NextResponse.json({
        ok: true,
        status: "skipped",
        reason: "missing_client_id",
        token: lead.token,
      });
    }
    if (!lead.siteId || Number.isNaN(lead.siteId)) {
      return NextResponse.json({
        ok: true,
        status: "skipped",
        reason: "missing_or_invalid_siteid",
        token: lead.token,
      });
    }

    // Validate against the configured studio allowlist (if configured).
    const allow = allowedSiteIds();
    if (allow.size > 0 && !allow.has(lead.siteId)) {
      return NextResponse.json({
        ok: true,
        status: "skipped",
        reason: "unknown_siteid",
        siteId: lead.siteId,
        token: lead.token,
      });
    }

    // Confirm the client exists before logging. Miss -> skip + report (200).
    const exists = await clientExists(lead.siteId, lead.clientId);
    if (!exists) {
      return NextResponse.json({
        ok: true,
        status: "skipped",
        reason: "client_not_found",
        clientId: lead.clientId,
        siteId: lead.siteId,
        token: lead.token,
      });
    }

    const typeId = notesTypeId(lead.siteId);
    const mb = await addContactLog(lead.siteId, {
      clientId: lead.clientId,
      text: buildLogText(lead),
      contactName: [lead.firstName, lead.lastName].filter(Boolean).join(" ") || undefined,
      contactMethod: "Note",
      typeIds: typeId ? [typeId] : undefined,
    });

    return NextResponse.json({
      ok: true,
      status: "logged",
      clientId: lead.clientId,
      siteId: lead.siteId,
      mindbody: mb,
    });
  } catch (err: any) {
    return NextResponse.json(
      { ok: false, error: err?.message ?? "Server error" },
      { status: 500 }
    );
  }
}
