import { NextResponse } from "next/server";
import crypto from "crypto";
import { resolveClientForEsign, uploadClientDocument, addEsignContactLog } from "../../../lib/esign-mindbody";
import {
  DEFAULT_TTL_MINUTES,
  buildCertificatePdf,
  evaluateReply,
  generateCode,
  generateRequestId,
  replyModeFromEnv,
  type EsignResult,
} from "../../../lib/esign";

export const runtime = "nodejs";
export const maxDuration = 30;

/*
 * CHANGE LOG (2026-10-14): SMS e-sign for phone membership sales.
 *
 *   Called by two GHL Webhook actions (flat JSON, header x-esign-secret):
 *     { action: "start" }   -> issues a request id + expiry (and, in code mode,
 *                              a 6-digit code) for GHL to text the member with
 *                              the terms link.
 *     { action: "verify" }  -> checks the member's reply ("AGREE", or in code
 *                              mode "AGREE 482913").
 *                              When it checks out: builds the Membership Terms
 *                              Acceptance PDF, uploads it to the client's
 *                              Mindbody Documents, adds a contact log entry.
 *
 *   Every handled request returns 200 with flat fields for GHL response mapping.
 *   Once the reply checks out the signature is final: a Mindbody write failure
 *   returns esignStatus "signed_writeback_failed" (member is not asked again)
 *   with writebackError for a manual retry.
 *
 *   "test": true on verify resolves the client and builds the PDF but writes
 *   nothing to Mindbody, returning certificateBase64 instead.
 *
 *   Env: ESIGN_WEBHOOK_SECRET (required), MINDBODY_SITE_ID_{KEY} (existing),
 *   ESIGN_REPLY_MODE ("agree_only" | "code", default agree_only),
 *   ESIGN_CODE_TTL_MINUTES (15, the reply window in both modes),
 *   ESIGN_REQUIRE_AGREE ("true", code mode only),
 *   ESIGN_TIMEZONE ("America/Los_Angeles"), ESIGN_CONTACT_METHOD ("Text Message").
 */

const STATUS_BY_RESULT: Record<EsignResult, string> = {
  signed: "signed",
  wrong_code: "pending",
  need_agree: "pending",
  need_code: "pending",
  unrecognized: "pending",
  expired: "expired",
  locked: "locked",
};

function timingSafeEqual(a: string, b: string) {
  const aBuf = Buffer.from(a);
  const bBuf = Buffer.from(b);
  return aBuf.length === bBuf.length && crypto.timingSafeEqual(aBuf, bBuf);
}

function verifySecret(req: Request) {
  const secret = process.env.ESIGN_WEBHOOK_SECRET;
  if (!secret) return { ok: false, reason: "Missing ESIGN_WEBHOOK_SECRET on server" };
  const provided = req.headers.get("x-esign-secret") || "";
  if (!provided || !timingSafeEqual(provided, secret)) return { ok: false, reason: "Bad secret" };
  return { ok: true };
}

// Same MINDBODY_SITE_ID_{KEY} convention as the sheets webhook, but no HB
// fallback: a signature must never land on the wrong studio's client.
function resolveSiteId(siteKey: unknown): { ok: true; siteId: number } | { ok: false; reason: string } {
  const rawKey = String(siteKey || "").trim();
  if (!rawKey) return { ok: false, reason: "siteKey is required" };
  if (!/^[A-Za-z0-9_]+$/.test(rawKey)) return { ok: false, reason: `Invalid siteKey: ${rawKey}` };
  const envName = `MINDBODY_SITE_ID_${rawKey.toUpperCase()}`;
  const parsed = Number(process.env[envName] || 0);
  if (!parsed) return { ok: false, reason: `Unknown siteKey "${rawKey}" (missing ${envName} on server)` };
  return { ok: true, siteId: parsed };
}

// GHL sends the literal text "null" for an empty merge field.
function str(v: unknown) {
  const t = v == null ? "" : String(v).trim();
  return /^(null|undefined)$/i.test(t) ? "" : t;
}

function errMessage(e: any) {
  return e?.response?.data?.Error?.Message || e?.response?.data?.Message || e?.message || String(e);
}

function last4(phone: string) {
  return phone.replace(/\D/g, "").slice(-4);
}

function handleStart(p: Record<string, unknown>, now = new Date()) {
  if (!str(p.phone)) return { status: 400, body: { ok: false, error: "phone is required" } };
  const ttl = Number(process.env.ESIGN_CODE_TTL_MINUTES) || DEFAULT_TTL_MINUTES;
  const mode = replyModeFromEnv();
  return {
    status: 200,
    body: {
      ok: true,
      esignStatus: "pending",
      esignReplyMode: mode,
      esignCode: mode === "code" ? generateCode() : "",
      esignExpiresAt: new Date(now.getTime() + ttl * 60_000).toISOString(),
      esignCodeSentAt: now.toISOString(),
      esignRequestId: generateRequestId(now),
      esignAttempts: 0,
      esignTtlMinutes: ttl,
    },
  };
}

async function handleVerify(p: Record<string, unknown>, now = new Date()) {
  if (str(p.esignStatus).toLowerCase() === "signed") {
    return { status: 200, body: { ok: true, result: "already_signed", esignStatus: "signed" } };
  }
  const mode = replyModeFromEnv();
  // A missing expiry (contact never got a terms text, or fields were cleared) is
  // not a bad request: evaluateReply treats it as "expired", so GHL gets a 200 it
  // can branch on. This also lets GHL's webhook test run against any contact.
  const required = mode === "code" ? ["siteKey", "phone", "esignCode"] : ["siteKey", "phone"];
  for (const f of required) {
    if (!str(p[f])) return { status: 400, body: { ok: false, error: `${f} is required` } };
  }

  const requireAgree = str(process.env.ESIGN_REQUIRE_AGREE || "true").toLowerCase() !== "false";
  const outcome = evaluateReply({
    replyText: p.replyText,
    mode,
    expectedCode: str(p.esignCode),
    expiresAt: str(p.esignExpiresAt),
    attempts: p.esignAttempts,
    requireAgree,
    now,
  });

  const base: Record<string, unknown> = {
    ok: true,
    result: outcome.result,
    esignStatus: STATUS_BY_RESULT[outcome.result],
    esignAttempts: outcome.attempts,
  };
  if (outcome.result !== "signed") return { status: 200, body: base };

  const replyAtRaw = str(p.replyAt);
  const signedAt = (replyAtRaw && !Number.isNaN(Date.parse(replyAtRaw)) ? new Date(replyAtRaw) : now).toISOString();
  const body: Record<string, unknown> = { ...base, esignSignedAt: signedAt };
  const failed = (reason: string, code?: string) => ({
    status: 200,
    body: { ...body, esignStatus: "signed_writeback_failed", writebackError: reason, writebackErrorCode: code ?? null },
  });

  const site = resolveSiteId(p.siteKey);
  if (!site.ok) return failed(site.reason, "bad_site");

  try {
    const { clientId, via } = await resolveClientForEsign(site.siteId, {
      mbClientId: str(p.mbClientId),
      phone: str(p.phone),
      email: str(p.email),
      firstName: str(p.firstName),
      lastName: str(p.lastName),
    });

    const requestId = str(p.esignRequestId);
    const cert = await buildCertificatePdf(
      {
        requestId,
        studioName: str(p.studioName),
        siteId: site.siteId,
        mbClientId: clientId,
        ghlContactId: str(p.ghlContactId),
        firstName: str(p.firstName),
        lastName: str(p.lastName),
        email: str(p.email),
        phone: str(p.phone),
        planName: str(p.planName),
        price: str(p.price),
        term: str(p.term),
        startDate: str(p.startDate),
        renewal: str(p.renewal),
        termsUrl: str(p.termsUrl),
        termsVersion: str(p.termsVersion),
        codeSentAt: str(p.esignCodeSentAt),
        replyText: str(p.replyText),
        replyAt: signedAt,
        messageId: str(p.messageId),
        staffName: str(p.staffName),
        replyMode: mode,
        requireAgree,
      },
      process.env.ESIGN_TIMEZONE || "America/Los_Angeles"
    );

    const fileName = `Membership Terms Acceptance ${signedAt.slice(0, 10)} ${requestId}`.trim();
    body.mbClientId = clientId;
    body.idCorrected = via === "phone" && str(p.mbClientId) !== clientId;
    body.documentFileName = `${fileName}.pdf`;
    body.esignRecordHash = cert.sha256;

    if (p.test === true || str(p.test).toLowerCase() === "true") {
      body.testMode = true;
      body.certificateBase64 = cert.base64;
      return { status: 200, body };
    }

    const upload = await uploadClientDocument(site.siteId, { clientId, fileName, mediaType: "pdf", base64: cert.base64 });
    body.documentUploaded = true;
    body.documentMediaType = upload.mediaType;

    // Plain log entry (no staff id = not a follow-up task). A failure here does
    // not undo the upload, so it is reported but not treated as a failed write-back.
    try {
      const terms = [str(p.termsVersion), str(p.termsUrl)].filter(Boolean).join(" ");
      await addEsignContactLog(site.siteId, {
        clientId,
        text:
          "Membership terms e-signed via SMS (month to month, 30 day cancellation notice)." +
          (terms ? ` Terms ${terms}.` : "") +
          ` Reply from phone ending ${last4(str(p.phone))}. Record ${requestId}, certificate in Documents.`,
        contactMethod: process.env.ESIGN_CONTACT_METHOD || "Text Message",
        contactName: str(p.staffName) || "STRIDE Fitness",
      });
      body.contactLogAdded = true;
    } catch (e) {
      body.contactLogAdded = false;
      body.contactLogError = errMessage(e);
    }

    return { status: 200, body };
  } catch (e: any) {
    return failed(errMessage(e), e?.code);
  }
}

/**
 * GHL pastes {{message.body}} into the JSON body without escaping, so a reply
 * containing a double quote or a line break breaks the JSON. replyText is the
 * last field in the verify body, so when a normal parse fails we take everything
 * between `"replyText": "` and the final `"` before the closing brace as the raw
 * reply, re-encode it, and parse again.
 */
function parseGhlBody(raw: string): Record<string, unknown> | null {
  try {
    return JSON.parse(raw) ?? {};
  } catch {
    const key = /"replyText"\s*:\s*"/.exec(raw);
    const end = raw.lastIndexOf("}");
    if (!key || end < 0) return null;
    const start = key.index + key[0].length;
    const close = raw.lastIndexOf('"', end);
    if (close < start) return null;
    const repaired = raw.slice(0, start - 1) + JSON.stringify(raw.slice(start, close)) + raw.slice(close + 1);
    try {
      return JSON.parse(repaired) ?? {};
    } catch {
      return null;
    }
  }
}

export async function POST(req: Request) {
  const auth = verifySecret(req);
  if (!auth.ok) return NextResponse.json({ ok: false, error: auth.reason }, { status: 401 });

  const payload = parseGhlBody(await req.text());
  if (!payload) return NextResponse.json({ ok: false, error: "Body must be JSON" }, { status: 400 });

  const action = str(payload.action).toLowerCase();
  let out: { status: number; body: Record<string, unknown> };
  if (action === "start" || action === "resend") out = handleStart(payload);
  else if (action === "verify") out = await handleVerify(payload);
  else out = { status: 400, body: { ok: false, error: "action must be start, resend, or verify" } };

  console.log("esign", {
    action,
    siteKey: payload.siteKey ?? null,
    ghlContactId: payload.ghlContactId ?? null,
    result: out.body.result ?? null,
    esignStatus: out.body.esignStatus ?? null,
    writebackError: out.body.writebackError ?? null,
  });
  return NextResponse.json(out.body, { status: out.status });
}
