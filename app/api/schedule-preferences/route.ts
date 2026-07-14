// app/api/schedule-preferences/route.ts
//
// Typeform "class schedule preferences" webhook -> writes a CLEAN contact log
// onto the member's existing Mindbody client record.
//
// IDENTITY: this form does NOT carry a Mindbody ClientId. We match the member
// by EMAIL first, then fall back to PHONE. We attach the log only when the
// lookup returns EXACTLY ONE client. Zero or multiple matches -> SKIP and
// report 200 (returning 200 stops Typeform from retrying).
//
// DAY QUESTIONS: each weekday is a MULTI-SELECT time picker (member checks one
// or more time slots, or "I do not plan to work out on <day>"). Typeform's
// webhook sends these as `choices` answers. We identify the 7 day questions by
// the presence of an "I do not plan to work out" option and assign Mon..Sun by
// their ORDER in the form (the "do not plan" option text has a Sat/Sun typo, so
// we deliberately do NOT trust it for labeling).
//
// Contact-log call mirrors the proven coach-log flow: send ContactMethod
// (required by Mindbody) and NEITHER FollowupByDate NOR AssignedTo, which makes
// it a plain log entry instead of a Sales-Pipeline follow-up task.

import { NextResponse } from "next/server";
import crypto from "crypto";

export const runtime = "nodejs";

const MINDBODY_BASE_URL = "https://api.mindbodyonline.com/public/v6";

const DAY_LABELS = [
  "Monday",
  "Tuesday",
  "Wednesday",
  "Thursday",
  "Friday",
  "Saturday",
  "Sunday",
] as const;

/* ============================================================
   Notes contact-log TYPE id (per-site override -> shared fallback)
============================================================ */
function notesTypeId(siteId: number): number | null {
  const raw =
    process.env[`MINDBODY_NOTES_TYPE_ID_${siteId}`] ?? process.env.MINDBODY_NOTES_TYPE_ID;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : null;
}

/* ============================================================
   ALLOWED STUDIO SITE IDS
   NOTE: add the new opening studio's site id env (e.g.
   MINDBODY_SITE_ID_<STUDIO>=5753322) or the route will skip it
   with reason "unknown_siteid".
============================================================ */
function allowedSiteIds(): Set<number> {
  return new Set(
    [
      process.env.MINDBODY_SITE_ID_HB,
      process.env.MINDBODY_SITE_ID_PASADENA,
      process.env.MINDBODY_SITE_ID_TUSTIN,
      process.env.MINDBODY_SITE_ID_SOUTHLANDS,
      process.env.MINDBODY_SITE_ID_SOUTHAMPTON,
      process.env.MINDBODY_SITE_ID_OPENING, // <-- new studio (5753322)
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
  const secret = process.env.TYPEFORM_SCHEDULE_SECRET || process.env.TYPEFORM_WEBHOOK_SECRET;
  if (!secret) throw new Error("Missing TYPEFORM_SCHEDULE_SECRET");

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
      return answer.choice?.label ?? answer.choice?.other ?? null;
    case "choices":
      // Multi-select: join every checked time slot.
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

  // field.id -> title, and field.id -> answer.
  const titleById = new Map<string, string>();
  for (const f of fields) if (f?.id) titleById.set(f.id, String(f.title ?? ""));

  const answerByFieldId = new Map<string, any>();
  for (const a of answers) {
    const id = a?.field?.id;
    if (id) answerByFieldId.set(id, a);
  }

  // ---- Day preference questions ----
  // A day question is a multiple-choice field whose option list contains an
  // "I do not plan to work out on ..." choice. We collect them IN FORM ORDER
  // and assign Monday..Sunday by position (the option text has a Sat/Sun typo,
  // so we don't trust it to label the day).
  const dayFieldIds: string[] = [];
  for (const f of fields) {
    const choices: any[] = f?.choices ?? [];
    const hasDoNotPlan = choices.some((c) =>
      normalize(String(c?.label ?? "")).includes("do not plan to work out")
    );
    if (hasDoNotPlan && f?.id) dayFieldIds.push(f.id);
  }

  const days: Record<(typeof DAY_LABELS)[number], string | null> = {
    Monday: null,
    Tuesday: null,
    Wednesday: null,
    Thursday: null,
    Friday: null,
    Saturday: null,
    Sunday: null,
  };
  dayFieldIds.slice(0, 7).forEach((fid, i) => {
    const label = DAY_LABELS[i];
    const v = getAnswerValue(answerByFieldId.get(fid));
    days[label] = v && v.trim() ? v.trim() : null;
  });

  // ---- Goal / injuries question (title contains "goal") ----
  const byTitle = (patterns: string[]): string | null => {
    for (const a of answers) {
      const t = normalize(titleById.get(a?.field?.id ?? "") ?? "");
      if (patterns.some((p) => t.includes(normalize(p)))) {
        const v = getAnswerValue(a);
        if (v && v.toString().trim()) return v.toString().trim();
      }
    }
    return null;
  };
  const injuriesGoals = byTitle(["goal", "injur"]);

  // ---- Answer-typed identity fallbacks ----
  const answerByType = (type: string): string | null => {
    for (const a of answers) {
      if (a?.type === type) {
        const v = getAnswerValue(a);
        if (v && v.toString().trim()) return v.toString().trim();
      }
    }
    return null;
  };

  // ---- Identity (hidden fields first, then typed answers) ----
  const email =
    hiddenVal(hidden, ["email", "Email", "email_address"]) ?? answerByType("email");
  const phone =
    hiddenVal(hidden, ["phone_number", "phone", "Phone", "phoneNumber"]) ??
    answerByType("phone_number");
  const siteId = hiddenVal(hidden, ["siteid", "siteId", "site_id", "SiteId"]);
  const firstName = hiddenVal(hidden, ["first_name", "firstName", "firstname"]) ?? "";
  const lastName = hiddenVal(hidden, ["last_name", "lastName", "lastname"]) ?? "";

  return {
    formId,
    token,
    email,
    phone,
    siteId: siteId ? Number(siteId) : null,
    firstName,
    lastName,
    days,
    injuriesGoals,
    answersCount: answers.length,
  };
}

/* ============================================================
   Clean contact-log body (HTML — Mindbody renders it)
   Bold header, then each day + Injuries/Goals as its own block:
   bold label on one line, the answer beneath, spacing between.
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
    `<p><strong>Class Schedule Preferences</strong></p>` +
    block("Monday Class Preference:", lead.days.Monday) +
    block("Tuesday Class Preference:", lead.days.Tuesday) +
    block("Wednesday Class Preference:", lead.days.Wednesday) +
    block("Thursday Class Preference:", lead.days.Thursday) +
    block("Friday Class Preference:", lead.days.Friday) +
    block("Saturday Class Preference:", lead.days.Saturday) +
    block("Sunday Class Preference:", lead.days.Sunday) +
    block("Injuries/Goals:", lead.injuriesGoals);

  return html.slice(0, 3800);
}

/* ============================================================
   Mindbody: find client by email -> phone, then add contact log
============================================================ */
function digits(s?: string | null): string {
  return String(s ?? "").replace(/\D/g, "");
}
function phoneMatch(a?: string | null, b?: string | null): boolean {
  const da = digits(a);
  const db = digits(b);
  if (da.length < 10 || db.length < 10) return false;
  return da.slice(-10) === db.slice(-10);
}

async function searchClients(siteId: number, query: string): Promise<any[]> {
  const apiKey = process.env.MINDBODY_API_KEY as string;
  const token = await getToken(siteId);
  const url = `${MINDBODY_BASE_URL}/client/clients?SearchText=${encodeURIComponent(query)}&Limit=100`;
  const res = await fetch(url, {
    headers: { "Api-Key": apiKey, Authorization: `Bearer ${token}`, SiteId: String(siteId) },
  });
  if (!res.ok) return [];
  const json: any = await res.json().catch(() => ({}));
  return Array.isArray(json?.Clients) ? json.Clients : [];
}

/**
 * Resolve to a single Mindbody client.
 *   -> { clientId } on an unambiguous single match
 *   -> { skip: "reason" } otherwise (missing info / not found / ambiguous)
 */
async function resolveClient(
  siteId: number,
  email: string | null,
  phone: string | null
): Promise<{ clientId?: string; skip?: string; method?: "email" | "phone" }> {
  // 1) Email — exact, case-insensitive.
  if (email) {
    const byEmail = (await searchClients(siteId, email)).filter(
      (c) => String(c?.Email ?? "").toLowerCase() === email.toLowerCase()
    );
    const ids = Array.from(new Set(byEmail.map((c) => String(c?.Id)).filter(Boolean)));
    if (ids.length === 1) return { clientId: ids[0], method: "email" };
    if (ids.length > 1) return { skip: "ambiguous_email" };
  }

  // 2) Phone — last-10-digits match across mobile/home/work.
  if (phone) {
    const byPhone = (await searchClients(siteId, digits(phone))).filter(
      (c) =>
        phoneMatch(c?.MobilePhone, phone) ||
        phoneMatch(c?.HomePhone, phone) ||
        phoneMatch(c?.WorkPhone, phone)
    );
    const ids = Array.from(new Set(byPhone.map((c) => String(c?.Id)).filter(Boolean)));
    if (ids.length === 1) return { clientId: ids[0], method: "phone" };
    if (ids.length > 1) return { skip: "ambiguous_phone" };
  }

  if (!email && !phone) return { skip: "missing_contact_info" };
  return { skip: "client_not_found" };
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

    // Resolve the member by email -> phone. Anything but a single match skips.
    const match = await resolveClient(lead.siteId, lead.email, lead.phone);
    if (!match.clientId) {
      return NextResponse.json({
        ok: true,
        status: "skipped",
        reason: match.skip ?? "client_not_found",
        email: lead.email,
        phone: lead.phone,
        siteId: lead.siteId,
        token: lead.token,
      });
    }

    const typeId = notesTypeId(lead.siteId);
    const mb = await addContactLog(lead.siteId, {
      clientId: match.clientId,
      text: buildLogText(lead),
      contactName: [lead.firstName, lead.lastName].filter(Boolean).join(" ") || undefined,
      contactMethod: "Note",
      typeIds: typeId ? [typeId] : undefined,
    });

    return NextResponse.json({
      ok: true,
      status: "logged",
      clientId: match.clientId,
      matchedBy: match.method,
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
