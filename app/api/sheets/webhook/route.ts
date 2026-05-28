import { NextResponse } from "next/server";
import crypto from "crypto";
import { neon } from "@neondatabase/serverless";
import { findClient, createClient } from "../../../../lib/mindbody";

export const runtime = "nodejs";

const sql = neon(
  process.env.DATABASE_URL_V2 || process.env.DATABASE_URL || ""
);

const FALLBACK_EMAIL_DOMAIN = "strideautomation.com";
const FALLBACK_PHONE_PREFIX = "555";

// Hardcoded HB site ID for Sheets-originated flows (lookup + updateClient).
const HB_SITE_ID = Number(process.env.MINDBODY_SITE_ID_HB || 0);

const MB_BASE = "https://api.mindbodyonline.com/public/v6";

/* ---------- Crypto / auth helpers ---------- */

function timingSafeEqual(a: string, b: string) {
  const aBuf = Buffer.from(a);
  const bBuf = Buffer.from(b);
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

async function verifyTypeform(req: Request, rawBody: string) {
  const secret = process.env.TYPEFORM_WEBHOOK_SECRET;
  if (!secret) throw new Error("Missing TYPEFORM_WEBHOOK_SECRET");

  const sigHeader =
    req.headers.get("typeform-signature") ||
    req.headers.get("Typeform-Signature");

  if (sigHeader) {
    const expected = crypto
      .createHmac("sha256", secret)
      .update(rawBody)
      .digest("base64");

    const provided = sigHeader.startsWith("sha256=")
      ? sigHeader.slice("sha256=".length)
      : sigHeader;

    return { ok: timingSafeEqual(provided, expected) };
  }

  const headerSecret = req.headers.get("typeform-secret");
  if (headerSecret && headerSecret === secret) return { ok: true };

  return { ok: false };
}

function verifySheetsSecret(req: Request) {
  const secret = process.env.SHEETS_WEBHOOK_SECRET;
  if (!secret) return { ok: false, reason: "Missing SHEETS_WEBHOOK_SECRET on server" };

  const provided =
    req.headers.get("x-sheets-secret") || req.headers.get("X-Sheets-Secret");
  if (!provided) return { ok: false, reason: "Missing x-sheets-secret header" };

  if (provided.length !== secret.length) return { ok: false, reason: "Bad secret" };
  if (!timingSafeEqual(provided, secret)) return { ok: false, reason: "Bad secret" };

  return { ok: true };
}

/* ---------- Generic helpers ---------- */

function normalize(s: string) {
  return s
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ")
    .replace(/&/g, "and");
}

function slugifyStudioName(s: string) {
  return normalize(s).replace(/[^a-z0-9]+/g, "");
}

function digitsOnly(s: string) {
  return (s || "").replace(/\D/g, "");
}

function phonesMatch(a: string, b: string) {
  const da = digitsOnly(a);
  const db = digitsOnly(b);
  if (!da || !db) return false;
  // Compare last 10 digits to ignore +1 / country-code differences.
  const tailA = da.slice(-10);
  const tailB = db.slice(-10);
  return tailA.length === 10 && tailA === tailB;
}

function makeDummyPhone(seed: string) {
  const hex = crypto.createHash("sha256").update(seed).digest("hex");
  const digits = hex.replace(/\D/g, "").padEnd(20, "0");
  const last7 = digits.slice(-7);
  return `${FALLBACK_PHONE_PREFIX}${last7}`;
}

function makeDummyEmail(seed: string) {
  const short = crypto.createHash("sha256").update(seed).digest("hex").slice(0, 10);
  return `pending+${short}@${FALLBACK_EMAIL_DOMAIN}`;
}

function unauthorized(reason: string) {
  return NextResponse.json(
    { ok: false, error: `Unauthorized: ${reason}` },
    { status: 401 }
  );
}

function badRequest(error: string, extra?: Record<string, unknown>) {
  return NextResponse.json({ ok: false, error, ...(extra ?? {}) }, { status: 400 });
}

function serverError(error: string, extra?: Record<string, unknown>) {
  return NextResponse.json({ ok: false, error, ...(extra ?? {}) }, { status: 500 });
}

/* ---------- Typeform payload extraction ---------- */

function getAnswerValue(answer: any) {
  if (!answer) return null;

  switch (answer.type) {
    case "text": return answer.text ?? null;
    case "email": return answer.email ?? null;
    case "phone_number": return answer.phone_number ?? null;
    case "choice": return answer.choice?.label ?? null;
    case "choices": return answer.choices?.labels?.join(", ") ?? null;
    case "dropdown": return answer.dropdown?.label ?? null;
    case "boolean": return String(answer.boolean);
    case "number": return String(answer.number);
    case "url": return answer.url ?? null;
    case "date": return answer.date ?? null;
    default: return null;
  }
}

function extractLead(payload: any) {
  const formId = payload?.form_response?.form_id;
  const token = payload?.form_response?.token;
  const hidden = payload?.form_response?.hidden ?? {};

  const answers: any[] = payload?.form_response?.answers ?? [];
  const fields: any[] = payload?.form_response?.definition?.fields ?? [];

  const fieldById = new Map<string, any>();
  for (const f of fields) {
    if (f?.id) fieldById.set(f.id, f);
  }

  const getTitle = (answer: any) => {
    const fieldId = answer?.field?.id;
    const field = fieldId ? fieldById.get(fieldId) : null;
    return (field?.title ?? "").toString();
  };

  const getByRef = (ref: string) => {
    const a = answers.find((x) => x?.field?.ref === ref);
    return getAnswerValue(a);
  };

  const getByRefList = (refs: string[]) => {
    for (const ref of refs) {
      const value = getByRef(ref);
      if (value && value.toString().trim()) return value.toString().trim();
    }
    return null;
  };

  const findByTitleIncludes = (patterns: string[]) => {
    for (const answer of answers) {
      const title = normalize(getTitle(answer));
      if (patterns.some((p) => title.includes(normalize(p)))) {
        const value = getAnswerValue(answer);
        if (value && value.toString().trim()) return value.toString().trim();
      }
    }
    return null;
  };

  let firstName = (getByRefList(["first_name", "firstname", "first-name"]) ?? "").toString().trim();
  let lastName = (getByRefList(["last_name", "lastname", "last-name"]) ?? "").toString().trim();
  let email = getByRefList(["email", "email_address", "email-address"]);
  let phone = getByRefList(["phone", "phone_number", "phone-number", "mobile"]);

  let studioName =
    hidden.studio ||
    getByRefList([
      "studio",
      "studio_name",
      "studio-name",
      "location",
      "location_name",
      "location-name",
      "home_studio",
      "home-studio"
    ]);

  const attributionType =
    hidden.affiliate
      ? "affiliate"
      : hidden.coach
        ? "coach"
        : getByRefList(["affiliate", "affiliate_name", "affiliate-name"])
          ? "affiliate"
          : getByRefList(["coach", "coach_name", "coach-name"])
            ? "coach"
            : null;

  const attribution =
    hidden.affiliate ||
    hidden.coach ||
    getByRefList([
      "affiliate",
      "affiliate_name",
      "affiliate-name",
      "coach",
      "coach_name",
      "coach-name"
    ]) ||
    null;

  if (!firstName) firstName = findByTitleIncludes(["first name", "firstname"]) ?? "";
  if (!lastName) lastName = findByTitleIncludes(["last name", "lastname"]) ?? "";

  if (!email) {
    const a = answers.find((x) => x?.type === "email");
    email = a?.email ?? null;
  }

  if (!phone) {
    const a = answers.find((x) => x?.type === "phone_number");
    phone = a?.phone_number ?? null;
  }

  if (!studioName) {
    studioName = findByTitleIncludes([
      "studio",
      "location",
      "home studio",
      "which studio",
      "select studio",
      "choose studio"
    ]);
  }

  return {
    formId,
    token,
    firstName,
    lastName,
    email,
    phone,
    studioName,
    attribution,
    attributionType,
    answersCount: answers.length
  };
}

/* ---------- DB ---------- */

async function ensureTables() {
  if (!process.env.DATABASE_URL_V2 && !process.env.DATABASE_URL) {
    throw new Error("Missing DATABASE_URL_V2 or DATABASE_URL.");
  }

  await sql`
    create table if not exists studio_site_mappings (
      id bigserial primary key,
      studio_name text not null,
      studio_key text not null unique,
      site_id integer not null,
      is_active boolean default true,
      created_at timestamptz default now()
    );
  `;

  await sql`
    create table if not exists processed_submissions_v2 (
      id bigserial primary key,
      typeform_token text not null unique,
      form_id text,
      studio_name text,
      site_id integer,
      attribution text,
      attribution_type text,
      created_at timestamptz default now()
    );
  `;

  try {
    await sql`alter table processed_submissions_v2 add column if not exists attribution text`;
  } catch {}
  try {
    await sql`alter table processed_submissions_v2 add column if not exists attribution_type text`;
  } catch {}
}

async function getStudioMapping(studioName: string) {
  const studioKey = slugifyStudioName(studioName);

  const rows = await sql`
    select studio_name, studio_key, site_id, is_active
    from studio_site_mappings
    where studio_key = ${studioKey}
    limit 1
  `;

  return (rows as any)?.[0] ?? null;
}

/* ---------- Mindbody helpers ---------- */

async function getMindbodyStaffToken(siteId: number) {
  const apiKey = process.env.MINDBODY_API_KEY;
  const staffUsername = process.env.MINDBODY_STAFF_USERNAME;
  const staffPassword = process.env.MINDBODY_STAFF_PASSWORD;

  if (!apiKey || !staffUsername || !staffPassword) {
    throw new Error(
      "Missing MINDBODY_API_KEY / MINDBODY_STAFF_USERNAME / MINDBODY_STAFF_PASSWORD"
    );
  }

  const res = await fetch(`${MB_BASE}/usertoken/issue`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Api-Key": apiKey,
      SiteId: String(siteId)
    },
    body: JSON.stringify({ Username: staffUsername, Password: staffPassword })
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Mindbody usertoken/issue failed: ${res.status} ${text}`);
  }

  const data = JSON.parse(text);
  if (!data?.AccessToken) {
    throw new Error(`Mindbody usertoken/issue returned no AccessToken: ${text}`);
  }
  return String(data.AccessToken);
}

type MbResult = { status: number; ok: boolean; data: any; text: string };

async function mindbodyFetch(
  siteId: number,
  path: string,
  init: { method: "GET" | "POST"; body?: any; query?: Record<string, string> }
): Promise<MbResult> {
  const apiKey = process.env.MINDBODY_API_KEY;
  if (!apiKey) throw new Error("Missing MINDBODY_API_KEY");

  const token = await getMindbodyStaffToken(siteId);

  const url = new URL(`${MB_BASE}${path}`);
  if (init.query) {
    for (const [k, v] of Object.entries(init.query)) {
      url.searchParams.set(k, v);
    }
  }

  const res = await fetch(url.toString(), {
    method: init.method,
    headers: {
      "Content-Type": "application/json",
      "Api-Key": apiKey,
      SiteId: String(siteId),
      Authorization: token
    },
    body: init.body ? JSON.stringify(init.body) : undefined
  });

  const text = await res.text();
  let data: any = null;
  try {
    data = JSON.parse(text);
  } catch {}

  return { status: res.status, ok: res.ok, data, text };
}

async function mindbodyGetClientsBySearch(siteId: number, searchText: string) {
  return mindbodyFetch(siteId, "/client/clients", {
    method: "GET",
    query: {
      "request.searchText": searchText,
      "request.limit": "20",
      "request.offset": "0",
      "request.includeInactive": "true"
    }
  });
}

async function mindbodyUpdateClient(
  siteId: number,
  client: {
    mbClientId: string;
    firstName?: string;
    lastName?: string;
    referredBy?: string;
    salesRep?: string | number;
  }
) {
  const clientBody: any = { Id: client.mbClientId };

  // FirstName / LastName are required by Mindbody's UpdateClient request body
  // even when they aren't changing. We pass them through from the sheet.
  if (client.firstName) clientBody.FirstName = client.firstName;
  if (client.lastName) clientBody.LastName = client.lastName;
  if (client.referredBy) clientBody.ReferredBy = client.referredBy;

  if (client.salesRep) {
    const repId = Number(client.salesRep);
    if (Number.isFinite(repId) && repId > 0) {
      // SalesReps[0] is "Rep 1" on the Mindbody client profile.
      clientBody.SalesReps = [{ Id: repId }];
    }
  }

  return mindbodyFetch(siteId, "/client/updateclient", {
    method: "POST",
    body: { Client: clientBody, CrossRegionalUpdate: false }
  });
}

/* ---------- Sheets -> Mindbody: lookup by phone ---------- */

async function handleSheetsLookup(req: Request, payload: any) {
  const auth = verifySheetsSecret(req);
  if (!auth.ok) return unauthorized(auth.reason!);

  if (!HB_SITE_ID) {
    return serverError("Missing MINDBODY_SITE_ID_HB on server");
  }

  const lead = payload?.lead ?? {};
  const phone = digitsOnly(String(lead?.phone || ""));
  if (!phone) return badRequest("Missing lead.phone");

  console.log("sheets lookup request", {
    rowNumber: payload?.rowNumber,
    backfill: !!payload?.backfill,
    phoneDigits: phone,
    siteId: HB_SITE_ID
  });

  try {
    // Try last 10 digits first, then the full string. Dedupe with a Set.
    const searchCandidates = Array.from(
      new Set([phone.slice(-10), phone].filter(Boolean))
    );

    let matched: any = null;
    let lastResult: MbResult | null = null;

    for (const searchText of searchCandidates) {
      const result = await mindbodyGetClientsBySearch(HB_SITE_ID, searchText);
      lastResult = result;

      if (!result.ok) {
        console.log("mindbody getClients non-OK", {
          status: result.status,
          bodySample: result.text?.slice(0, 300)
        });
        continue;
      }

      const clients: any[] = result.data?.Clients ?? [];
      console.log("mindbody getClients returned", {
        searchText,
        count: clients.length
      });

      // SearchText can match name/email too, so verify the phone explicitly.
      const phoneMatches = clients.filter((c) => {
        const candidates = [c?.MobilePhone, c?.HomePhone, c?.WorkPhone].filter(Boolean);
        return candidates.some((p: string) => phonesMatch(p, phone));
      });

      if (phoneMatches.length === 1) {
        matched = phoneMatches[0];
        break;
      }

      if (phoneMatches.length > 1) {
        return NextResponse.json({
          ok: true,
          status: "ambiguous",
          message: `Multiple Mindbody clients matched phone ${phone}`,
          count: phoneMatches.length
        });
      }
    }

    if (matched?.Id) {
      return NextResponse.json({
        ok: true,
        status: "found",
        mbClientId: String(matched.Id),
        siteId: HB_SITE_ID
      });
    }

    if (lastResult && !lastResult.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: `Mindbody getClients failed: ${lastResult.status}`,
          mindbody: lastResult.data ?? lastResult.text
        },
        { status: 502 }
      );
    }

    return NextResponse.json({ ok: true, status: "not_found" });
  } catch (err: any) {
    console.log("sheets lookup error", { message: err?.message, stack: err?.stack });
    return serverError(err?.message ?? "Lookup failed");
  }
}

/* ---------- Sheets -> Mindbody: updateClient ---------- */

async function handleSheetsUpdateClient(req: Request, payload: any) {
  const auth = verifySheetsSecret(req);
  if (!auth.ok) return unauthorized(auth.reason!);

  if (!HB_SITE_ID) {
    return serverError("Missing MINDBODY_SITE_ID_HB on server");
  }

  const client = payload?.client ?? {};
  const mbClientId = String(client?.mbClientId || "").trim();
  if (!mbClientId) return badRequest("Missing client.mbClientId");

  const firstName = String(client?.firstName || "").trim();
  const lastName = String(client?.lastName || "").trim();
  const referredBy = String(client?.referredBy || "").trim();
  const salesRepRaw = client?.salesRep;

  if (!referredBy && !salesRepRaw) {
    return badRequest("Nothing to update (referredBy and salesRep both empty)");
  }

  console.log("sheets updateClient request", {
    rowNumber: payload?.rowNumber,
    mbClientId,
    hasFirstName: !!firstName,
    hasLastName: !!lastName,
    referredBy: referredBy || null,
    salesRep: salesRepRaw || null
  });

  try {
    const result = await mindbodyUpdateClient(HB_SITE_ID, {
      mbClientId,
      firstName,
      lastName,
      referredBy: referredBy || undefined,
      salesRep: salesRepRaw || undefined
    });

    console.log("mindbody updateClient result", {
      status: result.status,
      ok: result.ok,
      bodySample: result.text?.slice(0, 300)
    });

    if (!result.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: `Mindbody updateClient failed: ${result.status}`,
          mindbody: result.data ?? result.text
        },
        { status: 502 }
      );
    }

    return NextResponse.json({
      ok: true,
      status: "updated",
      mbClientId,
      siteId: HB_SITE_ID,
      mindbody: result.data
    });
  } catch (err: any) {
    console.log("sheets updateClient error", { message: err?.message, stack: err?.stack });
    return serverError(err?.message ?? "Update failed");
  }
}

/* ---------- Sheets -> Mindbody: find-or-create from Paid Leads row ---------- */

async function handleSheetsCreate(req: Request, payload: any) {
  const auth = verifySheetsSecret(req);
  if (!auth.ok) return unauthorized(auth.reason!);

  if (!HB_SITE_ID) {
    return serverError("Missing MINDBODY_SITE_ID_HB on server");
  }

  const lead = payload?.lead ?? {};
  const firstName = String(lead?.firstName || "").trim();
  const lastName = String(lead?.lastName || "").trim();

  if (!firstName || !lastName) {
    return badRequest("Missing lead.firstName or lead.lastName");
  }

  const rawEmail = String(lead?.email || "").trim();
  const rawPhone = String(lead?.phone || "").trim();

  // The Apps Script fills in fallback email/phone per row, but we re-derive
  // deterministic fallbacks here if either is somehow blank.
  const seed = `${payload?.sheetId}:${payload?.rowNumber}:${firstName}:${lastName}`;
  const email = rawEmail || makeDummyEmail(seed);
  const phone = digitsOnly(rawPhone) || digitsOnly(makeDummyPhone(seed));

  console.log("sheets create request", {
    rowNumber: payload?.rowNumber,
    backfill: !!payload?.backfill,
    firstName,
    lastName,
    email,
    phoneDigits: phone,
    leadSource: lead?.leadSource ?? null,
    referralType: lead?.referralType ?? null,
    salesRep: lead?.salesRep ?? null,
    siteId: HB_SITE_ID
  });

  try {
    const existing = await findClient(HB_SITE_ID, {
      firstName,
      lastName,
      email,
      phone
    });

    if (existing?.Id) {
      return NextResponse.json({
        ok: true,
        status: "exists",
        mbClientId: String(existing.Id),
        siteId: HB_SITE_ID,
        fallbacksUsed: {
          emailWasFallback: !rawEmail,
          phoneWasFallback: !rawPhone
        }
      });
    }

    const created = await createClient(HB_SITE_ID, {
      firstName,
      lastName,
      email,
      phone
    });

    if (!created?.Id) return serverError("Mindbody create failed");

    return NextResponse.json({
      ok: true,
      status: "created",
      mbClientId: String(created.Id),
      siteId: HB_SITE_ID,
      fallbacksUsed: {
        emailWasFallback: !rawEmail,
        phoneWasFallback: !rawPhone
      }
    });
  } catch (err: any) {
    const status = err?.response?.status ?? null;
    const data = err?.response?.data ?? null;
    const where = typeof err?.config?.url === "string" ? err.config.url : null;

    console.log("sheets create mindbody error", { status, where, data });

    return NextResponse.json(
      {
        ok: false,
        error: err?.message ?? "Server error",
        mindbody: { status, where, data }
      },
      { status: 500 }
    );
  }
}

/* ---------- Typeform handler ---------- */

async function handleTypeform(req: Request, rawBody: string) {
  const verification = await verifyTypeform(req, rawBody);
  console.log("verification result:", verification);

  if (!verification.ok) {
    return unauthorized("Typeform verification failed");
  }

  let payload: any;
  try {
    payload = JSON.parse(rawBody);
  } catch (parseErr) {
    console.log("payload parse failed:", parseErr);
    return badRequest("Invalid JSON");
  }

  const lead = extractLead(payload);

  console.log("lead extracted:", {
    formId: lead.formId,
    token: lead.token,
    firstName: lead.firstName,
    lastName: lead.lastName,
    email: lead.email,
    phone: lead.phone,
    studioName: lead.studioName,
    attribution: lead.attribution,
    attributionType: lead.attributionType,
    answersCount: lead.answersCount
  });

  if (!lead.formId || !lead.token || lead.answersCount === 0) {
    return NextResponse.json({ ok: true, status: "typeform_test_ok" });
  }

  const baseExtracted = {
    firstName: lead.firstName,
    lastName: lead.lastName,
    email: lead.email,
    phone: lead.phone,
    studioName: lead.studioName,
    attribution: lead.attribution,
    attributionType: lead.attributionType
  };

  if (!lead.firstName || !lead.lastName) {
    return badRequest("Missing firstName or lastName from Typeform payload", {
      extracted: baseExtracted
    });
  }
  if (!lead.studioName) {
    return badRequest("Missing studioName from Typeform payload", {
      extracted: baseExtracted
    });
  }
  if (!lead.attribution) {
    return badRequest("Missing attribution from Typeform payload", {
      extracted: baseExtracted
    });
  }

  await ensureTables();

  const mapping = await getStudioMapping(lead.studioName);
  console.log("studio mapping result:", mapping);

  if (!mapping || mapping.is_active === false) {
    return NextResponse.json({
      ok: true,
      status: "routed",
      routedTo: null,
      message: "No active site mapping for this studio",
      studioName: lead.studioName,
      studioKey: slugifyStudioName(lead.studioName),
      attribution: lead.attribution,
      attributionType: lead.attributionType
    });
  }

  const siteId = Number(mapping.site_id);

  try {
    await sql`
      insert into processed_submissions_v2 (
        typeform_token, form_id, studio_name, site_id, attribution, attribution_type
      )
      values (
        ${lead.token},
        ${lead.formId ?? null},
        ${lead.studioName},
        ${siteId},
        ${lead.attribution},
        ${lead.attributionType}
      )
    `;
    console.log("inserted processed submission", {
      token: lead.token,
      studioName: lead.studioName,
      siteId
    });
  } catch (insertErr) {
    console.log("processed submission insert failed, likely deduped:", insertErr);
    return NextResponse.json({
      ok: true,
      status: "deduped",
      routedTo: { studioName: mapping.studio_name, siteId },
      attribution: lead.attribution,
      attributionType: lead.attributionType
    });
  }

  const normalizedEmail =
    lead.email && lead.email.trim().length > 0 ? lead.email.trim() : makeDummyEmail(lead.token);
  const normalizedPhoneRaw =
    lead.phone && lead.phone.trim().length > 0 ? lead.phone.trim() : makeDummyPhone(lead.token);
  const normalizedPhone = digitsOnly(normalizedPhoneRaw);

  console.log("about to search/create in MB", {
    siteId,
    firstName: lead.firstName,
    lastName: lead.lastName,
    email: normalizedEmail,
    phone: normalizedPhone
  });

  try {
    const existing = await findClient(siteId, {
      firstName: lead.firstName,
      lastName: lead.lastName,
      email: normalizedEmail,
      phone: normalizedPhone
    });

    if (existing?.Id) {
      return NextResponse.json({
        ok: true,
        status: "exists",
        mbClientId: String(existing.Id),
        routedTo: { studioName: mapping.studio_name, siteId },
        attribution: lead.attribution,
        attributionType: lead.attributionType,
        fallbacksUsed: {
          emailWasFallback: !lead.email,
          phoneWasFallback: !lead.phone
        }
      });
    }

    const created = await createClient(siteId, {
      firstName: lead.firstName,
      lastName: lead.lastName,
      email: normalizedEmail,
      phone: normalizedPhone
    });

    if (!created?.Id) return serverError("Mindbody create failed");

    return NextResponse.json({
      ok: true,
      status: "created",
      mbClientId: String(created.Id),
      routedTo: { studioName: mapping.studio_name, siteId },
      attribution: lead.attribution,
      attributionType: lead.attributionType,
      fallbacksUsed: {
        emailWasFallback: !lead.email,
        phoneWasFallback: !lead.phone
      }
    });
  } catch (err: any) {
    const status = err?.response?.status ?? null;
    const data = err?.response?.data ?? null;
    const where = typeof err?.config?.url === "string" ? err.config.url : null;

    console.log("mindbody error:", { status, where, data });

    return NextResponse.json(
      {
        ok: false,
        error: err?.message ?? "Server error",
        mindbody: { status, where, data },
        routedTo: { studioName: mapping.studio_name, siteId },
        attribution: lead.attribution,
        attributionType: lead.attributionType
      },
      { status: 500 }
    );
  }
}

/* ---------- POST entrypoint ---------- */

export async function POST(req: Request) {
  try {
    console.log("🔥 webhook hit");
    console.log("env check:", {
      hasDatabaseUrlV2: !!process.env.DATABASE_URL_V2,
      hasDatabaseUrl: !!process.env.DATABASE_URL,
      hasTypeformSecret: !!process.env.TYPEFORM_WEBHOOK_SECRET,
      hasSheetsSecret: !!process.env.SHEETS_WEBHOOK_SECRET,
      hasHbSiteId: !!HB_SITE_ID,
      hasApiKey: !!process.env.MINDBODY_API_KEY,
      hasStaffCreds: !!(
        process.env.MINDBODY_STAFF_USERNAME && process.env.MINDBODY_STAFF_PASSWORD
      ),
      nodeEnv: process.env.NODE_ENV
    });

    const rawBody = await req.text();
    console.log("raw body length:", rawBody.length);

    // Peek at the body to route Sheets requests before Typeform verification.
    let earlyPayload: any = null;
    try {
      earlyPayload = JSON.parse(rawBody);
    } catch {
      // Not JSON — let the Typeform path return the appropriate error below.
    }

    if (earlyPayload?.lookupOnly === true) {
      return await handleSheetsLookup(req, earlyPayload);
    }

    if (earlyPayload?.updateClient === true) {
      return await handleSheetsUpdateClient(req, earlyPayload);
    }

    // Sheets lead-capture posts from the Apps Script:
    //   { sheetId, sheetName, rowNumber, lead, backfill? }
    // Neither lookupOnly nor updateClient — these create (or find) a
    // Mindbody client from a row in the Paid Leads sheet.
    if (earlyPayload?.sheetId && earlyPayload?.lead) {
      return await handleSheetsCreate(req, earlyPayload);
    }

    return await handleTypeform(req, rawBody);
  } catch (err: any) {
    console.log("top-level webhook error:", { message: err?.message, stack: err?.stack });
    return serverError(err?.message ?? "Unhandled server error");
  }
}
