// lib/esign.ts
// One-time codes, SMS reply evaluation, and the acceptance certificate PDF for
// the STRIDE Fitness phone-sale e-sign flow (see app/api/esign/route.ts).
import crypto from "crypto";
import { PDFDocument, StandardFonts, rgb, type PDFFont } from "pdf-lib";

export const DEFAULT_TTL_MINUTES = 15;
export const MAX_ATTEMPTS = 3;

/* ---------- Codes ---------- */

export function generateCode() {
  // 6 digits, never leading 0, so it survives number coercion in GHL fields.
  return String(crypto.randomInt(100000, 1000000));
}

export function generateRequestId(now = new Date()) {
  const stamp = now.toISOString().slice(0, 10).replace(/-/g, "");
  return `ES-${stamp}-${crypto.randomBytes(4).toString("hex").toUpperCase()}`;
}

/**
 * Accepts "AGREE", "I agree", "Agreed!", "AGREE 482913", "agree482913".
 * Never counts "DISAGREE", "I don't agree", "do not agree", "not agree".
 */
export function parseReply(text: unknown) {
  const upper = String(text ?? "").toUpperCase().replace(/[\u2018\u2019]/g, "'");
  const negated = /(^|[^A-Z])(NOT|DON'?T|DO NOT|NEVER|CAN'?T|CANNOT)\s+(YET\s+)?AGREE/.test(upper);
  const hasAgree = !negated && /(^|[^A-Z])AGREED?(?![A-Z])/.test(upper);
  const compact = upper.replace(/(\d)[\s-]+(?=\d)/g, "$1");
  const codes = compact.match(/\d{6}/g) ?? [];
  return { hasAgree, codes };
}

function safeEqual(a: string, b: string) {
  const ba = Buffer.from(a);
  const bb = Buffer.from(b);
  return ba.length === bb.length && crypto.timingSafeEqual(ba, bb);
}

export type EsignResult = "signed" | "wrong_code" | "need_agree" | "need_code" | "unrecognized" | "expired" | "locked";

/**
 * agree_only: the reply "AGREE" from the contact's phone, inside the window, signs.
 * code:       the reply must carry the one-time code (and AGREE unless requireAgree=false).
 */
export type ReplyMode = "agree_only" | "code";

export function replyModeFromEnv(): ReplyMode {
  return String(process.env.ESIGN_REPLY_MODE || "agree_only").trim().toLowerCase() === "code" ? "code" : "agree_only";
}

export function evaluateReply(input: {
  replyText: unknown;
  mode?: ReplyMode;
  expectedCode?: string;
  expiresAt: string;
  attempts?: unknown;
  requireAgree?: boolean;
  now?: Date;
}): { result: EsignResult; attempts: number } {
  const now = input.now ?? new Date();
  const prior = Number.parseInt(String(input.attempts ?? "0"), 10) || 0;
  if (prior >= MAX_ATTEMPTS) return { result: "locked", attempts: prior };

  const exp = Date.parse(input.expiresAt);
  if (!Number.isFinite(exp) || now.getTime() > exp) return { result: "expired", attempts: prior };

  const { hasAgree, codes } = parseReply(input.replyText);

  if ((input.mode ?? "agree_only") === "agree_only") {
    // No code to get wrong, so attempts never climb; anything but AGREE goes to staff.
    return { result: hasAgree ? "signed" : "unrecognized", attempts: prior };
  }

  // Ordinary chatter ("what is this?") does not burn an attempt.
  if (codes.length === 0) return { result: hasAgree ? "need_code" : "unrecognized", attempts: prior };

  if (!input.expectedCode || !codes.some((c) => safeEqual(c, input.expectedCode!))) {
    const next = prior + 1;
    return { result: next >= MAX_ATTEMPTS ? "locked" : "wrong_code", attempts: next };
  }
  if (input.requireAgree !== false && !hasAgree) return { result: "need_agree", attempts: prior };
  return { result: "signed", attempts: prior };
}

/* ---------- Certificate PDF ---------- */

export type CertificateInput = {
  requestId: string;
  studioName: string;
  siteId: number;
  mbClientId: string;
  ghlContactId: string;
  firstName: string;
  lastName: string;
  email: string;
  phone: string;
  planName: string;
  price: string;
  term: string;
  startDate: string;
  renewal: string;
  termsUrl: string;
  termsVersion: string;
  codeSentAt: string;
  replyText: string;
  replyAt: string;
  messageId: string;
  staffName: string;
  replyMode: ReplyMode;
  requireAgree: boolean;
};

const INK = rgb(0.11, 0.12, 0.14);
const MUTED = rgb(0.42, 0.44, 0.48);
const RULE = rgb(0.85, 0.86, 0.88);
const ACCENT = rgb(0.0, 0.66, 0.85);

// Standard PDF fonts only cover WinAnsi; fold everything else to plain ASCII.
export function clean(value: unknown) {
  return String(value ?? "")
    .replace(/[\u2018\u2019]/g, "'")
    .replace(/[\u201C\u201D]/g, '"')
    .replace(/[\u2013\u2014]/g, "-")
    .replace(/\u2026/g, "...")
    .normalize("NFKD")
    .replace(/[^\x20-\x7E]/g, "")
    .trim()
    // GHL sends the literal text "null" for an empty merge field.
    .replace(/^(null|undefined)$/i, "");
}

export const DEFAULT_TERM = "Month to month, 30 day cancellation notice required";

/** Calendar date (YYYY-MM-DD) of an instant in the studio's time zone. */
function localYmd(iso: string, timeZone: string) {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  const parts = new Intl.DateTimeFormat("en-CA", { timeZone, year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(d);
  const get = (t: string) => Number(parts.find((x) => x.type === t)?.value);
  return { y: get("year"), m: get("month"), d: get("day") };
}

function fmtYmd(y: number, m: number, d: number) {
  return new Date(Date.UTC(y, m - 1, d)).toLocaleDateString("en-US", { timeZone: "UTC", year: "numeric", month: "short", day: "numeric" });
}

/**
 * Start date = the day the record is signed (studio time). Renewal = the same day
 * next month, clamped to the last day of a shorter month (Jan 31 -> Feb 28).
 */
export function membershipDates(signedAtIso: string, timeZone: string) {
  const s = localYmd(signedAtIso, timeZone);
  if (!s) return { startDate: "", renewalDate: "" };
  const ny = s.m === 12 ? s.y + 1 : s.y;
  const nm = s.m === 12 ? 1 : s.m + 1;
  const lastDay = new Date(Date.UTC(ny, nm, 0)).getUTCDate();
  return { startDate: fmtYmd(s.y, s.m, s.d), renewalDate: fmtYmd(ny, nm, Math.min(s.d, lastDay)) };
}

export function formatPhone(phone: string) {
  const d = String(phone || "").replace(/\D/g, "");
  if (d.length < 10) return clean(phone);
  const t = d.slice(-10);
  return `(${t.slice(0, 3)}) ${t.slice(3, 6)}-${t.slice(6)}`;
}

function fmtTime(iso: string, timeZone: string) {
  const d = new Date(iso);
  if (!iso || Number.isNaN(d.getTime())) return "n/a";
  const local = d.toLocaleString("en-US", {
    timeZone, year: "numeric", month: "short", day: "numeric",
    hour: "numeric", minute: "2-digit", second: "2-digit", timeZoneName: "short",
  });
  return `${local}  (${d.toISOString()})`;
}

/** Canonical record; its SHA-256 is printed on the PDF and returned to GHL. */
export function buildEvidence(p: CertificateInput, timeZone = "America/Los_Angeles") {
  const dates = membershipDates(p.replyAt, timeZone);
  return {
    requestId: clean(p.requestId),
    studio: clean(p.studioName),
    mindbodySiteId: String(p.siteId),
    mindbodyClientId: clean(p.mbClientId),
    ghlContactId: clean(p.ghlContactId),
    memberName: clean(`${p.firstName} ${p.lastName}`),
    memberEmail: clean(p.email),
    memberPhone: formatPhone(p.phone),
    // Fixed for every phone sale; GHL's term field is no longer used.
    term: clean(process.env.ESIGN_TERM) || DEFAULT_TERM,
    startDate: dates.startDate,
    renewalDate: dates.renewalDate,
    termsUrl: clean(p.termsUrl),
    termsVersion: clean(p.termsVersion),
    codeSentAt: clean(p.codeSentAt),
    replyText: clean(p.replyText),
    replyReceivedAt: clean(p.replyAt),
    replyMessageId: clean(p.messageId),
    staffName: clean(p.staffName),
    replyMode: p.replyMode,
  };
}

function wrap(text: string, font: PDFFont, size: number, maxWidth: number) {
  const words = clean(text).split(/\s+/).filter(Boolean);
  const lines: string[] = [];
  let line = "";
  for (const w of words) {
    const test = line ? `${line} ${w}` : w;
    if (font.widthOfTextAtSize(test, size) <= maxWidth) { line = test; continue; }
    if (line) lines.push(line);
    let rest = w; // hard-break long tokens such as URLs
    while (font.widthOfTextAtSize(rest, size) > maxWidth) {
      let i = rest.length;
      while (i > 1 && font.widthOfTextAtSize(rest.slice(0, i), size) > maxWidth) i--;
      lines.push(rest.slice(0, i));
      rest = rest.slice(i);
    }
    line = rest;
  }
  if (line) lines.push(line);
  return lines.length ? lines : [""];
}

export async function buildCertificatePdf(p: CertificateInput, timeZone = "America/Los_Angeles") {
  const evidence = buildEvidence(p, timeZone);
  const sha256 = crypto.createHash("sha256").update(JSON.stringify(evidence)).digest("hex");

  const pdf = await PDFDocument.create();
  pdf.setTitle(`Membership Terms Acceptance ${evidence.requestId}`);
  pdf.setSubject("SMS electronic signature record");
  pdf.setAuthor("STRIDE Fitness");
  pdf.setCreator("STRIDE Fitness");
  pdf.setProducer("STRIDE Fitness");

  const page = pdf.addPage([612, 792]);
  const reg = await pdf.embedFont(StandardFonts.Helvetica);
  const bold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const M = 54;
  const W = 612 - M * 2;
  let y = 792 - M;

  page.drawRectangle({ x: 0, y: 792 - 8, width: 612, height: 8, color: ACCENT });
  page.drawText("STRIDE FITNESS", { x: M, y: y - 4, size: 11, font: bold, color: ACCENT });
  const rec = `Record ${evidence.requestId}`;
  page.drawText(rec, { x: M + W - reg.widthOfTextAtSize(rec, 9), y: y - 4, size: 9, font: reg, color: MUTED });
  y -= 30;
  page.drawText("Membership Terms Acceptance", { x: M, y, size: 20, font: bold, color: INK });
  y -= 18;
  page.drawText(p.replyMode === "agree_only" ? "Electronic signature by SMS reply" : "Electronic signature by SMS reply with one-time code", { x: M, y, size: 10.5, font: reg, color: MUTED });
  y -= 22;

  const how =
    p.replyMode === "agree_only"
      ? `by replying AGREE from the mobile number ${evidence.memberPhone} to the text that sent them the terms link. `
      : `by replying from the mobile number ${evidence.memberPhone} with the one-time code sent to that number` +
        (p.requireAgree ? " and the word AGREE. " : ". ");
  const statement =
    `${evidence.memberName || "The member"} accepted the STRIDE Fitness membership terms identified below ` +
    how +
    "The reply was received within the validity window and is recorded below as the member's electronic signature.";
  for (const l of wrap(statement, reg, 10.5, W)) { page.drawText(l, { x: M, y, size: 10.5, font: reg, color: INK }); y -= 15; }
  y -= 8;

  const section = (title: string) => {
    y -= 6;
    page.drawText(title.toUpperCase(), { x: M, y, size: 8.5, font: bold, color: ACCENT });
    y -= 6;
    page.drawLine({ start: { x: M, y }, end: { x: M + W, y }, thickness: 0.75, color: RULE });
    y -= 14;
  };
  const LABEL_W = 150;
  const row = (label: string, value: string) => {
    page.drawText(label, { x: M, y, size: 9, font: reg, color: MUTED });
    for (const l of wrap(value || "n/a", reg, 10, W - LABEL_W)) {
      page.drawText(l, { x: M + LABEL_W, y, size: 10, font: reg, color: INK });
      y -= 14;
    }
    y -= 2;
  };

  section("Member");
  row("Name", evidence.memberName);
  row("Mobile number", evidence.memberPhone);
  row("Email", evidence.memberEmail);
  row("Mindbody client ID", evidence.mindbodyClientId);
  row("Studio", evidence.studio);

  section("Membership");
  row("Term", evidence.term);
  row("Start date", evidence.startDate);
  row("Renewal date", evidence.renewalDate);
  row("Terms link", evidence.termsUrl);
  row("Terms version", evidence.termsVersion);

  section("Signature event");
  row(p.replyMode === "agree_only" ? "Terms text sent" : "Code sent", fmtTime(p.codeSentAt, timeZone));
  row("Reply received", fmtTime(p.replyAt, timeZone));
  row("Reply text", `"${evidence.replyText}"`);
  // GHL's Customer Replied trigger exposes no message id, so the row only
  // appears when a caller supplies one.
  if (evidence.replyMessageId) row("Message ID", evidence.replyMessageId);
  row("Sent by staff", evidence.staffName);
  row("GHL contact ID", evidence.ghlContactId);

  const f = M - 6;
  page.drawLine({ start: { x: M, y: f + 34 }, end: { x: M + W, y: f + 34 }, thickness: 0.75, color: RULE });
  page.drawText("Record fingerprint (SHA-256 of the signature record)", { x: M, y: f + 20, size: 8, font: reg, color: MUTED });
  page.drawText(sha256, { x: M, y: f + 8, size: 8, font: reg, color: INK });
  page.drawText(`Generated ${new Date().toISOString()}`, { x: M, y: f - 4, size: 7.5, font: reg, color: MUTED });

  const bytes = await pdf.save();
  return { base64: Buffer.from(bytes).toString("base64"), sha256 };
}
