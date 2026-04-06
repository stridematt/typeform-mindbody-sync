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

function getAnswerValue(answer: any) {
  if (!answer) return null;

  if (answer.type === "text") return answer.text ?? null;
  if (answer.type === "email") return answer.email ?? null;
  if (answer.type === "phone_number") return answer.phone_number ?? null;
  if (answer.type === "choice") return answer.choice?.label ?? null;
  if (answer.type === "choices") return answer.choices?.labels?.join(", ") ?? null;
  if (answer.type === "dropdown") return answer.dropdown?.label ?? null;
  if (answer.type === "boolean") return String(answer.boolean);
  if (answer.type === "number") return String(answer.number);
  if (answer.type === "url") return answer.url ?? null;
  if (answer.type === "date") return answer.date ?? null;

  return null;
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
      const matched = patterns.some((p) => title.includes(normalize(p)));
      if (matched) {
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

  const coach =
    hidden.coach ||
    getByRefList([
      "coach",
      "coach_name",
      "coach-name"
    ]) ||
    null;

  if (!firstName) {
    firstName = findByTitleIncludes(["first name", "firstname"]) ?? "";
  }

  if (!lastName) {
    lastName = findByTitleIncludes(["last name", "lastname"]) ?? "";
  }

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
    coach,
    answersCount: answers.length
  };
}

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
      created_at timestamptz default now()
    );
  `;
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

export async function POST(req: Request) {
  console.log("🔥 webhook-v2 hit");

  const rawBody = await req.text();
  console.log("raw body length:", rawBody.length);

  const verification = await verifyTypeform(req, rawBody);
  console.log("verification result:", verification);

  if (!verification.ok) {
    return NextResponse.json(
      { ok: false, error: "Unauthorized (Typeform verification failed)" },
      { status: 401 }
    );
  }

  let payload: any;
  try {
    payload = JSON.parse(rawBody);
    console.log("payload parsed successfully");
  } catch {
    return NextResponse.json({ ok: false, error: "Invalid JSON" }, { status: 400 });
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
    coach: lead.coach,
    answersCount: lead.answersCount
  });

  if (!lead.formId || !lead.token || lead.answersCount === 0) {
    return NextResponse.json({ ok: true, status: "typeform_test_ok" });
  }

  if (!lead.firstName || !lead.lastName) {
    return NextResponse.json(
      {
        ok: false,
        error: "Missing firstName or lastName from Typeform payload",
        extracted: {
          firstName: lead.firstName,
          lastName: lead.lastName,
          email: lead.email,
          phone: lead.phone,
          studioName: lead.studioName,
          coach: lead.coach
        }
      },
      { status: 400 }
    );
  }

  if (!lead.studioName) {
    return NextResponse.json(
      {
        ok: false,
        error: "Missing studioName from Typeform payload",
        extracted: {
          firstName: lead.firstName,
          lastName: lead.lastName,
          email: lead.email,
          phone: lead.phone,
          coach: lead.coach
        }
      },
      { status: 400 }
    );
  }

  if (!lead.coach) {
    return NextResponse.json(
      {
        ok: false,
        error: "Missing coach from Typeform payload",
        extracted: {
          firstName: lead.firstName,
          lastName: lead.lastName,
          email: lead.email,
          phone: lead.phone,
          studioName: lead.studioName,
          coach: lead.coach
        }
      },
      { status: 400 }
    );
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
      coach: lead.coach
    });
  }

  const siteId = Number(mapping.site_id);

  try {
    await sql`
      insert into processed_submissions_v2 (typeform_token, form_id, studio_name, site_id)
      values (${lead.token}, ${lead.formId ?? null}, ${lead.studioName}, ${siteId})
    `;

    console.log("inserted processed submission", {
      token: lead.token,
      studioName: lead.studioName,
      siteId
    });
  } catch {
    return NextResponse.json({
      ok: true,
      status: "deduped",
      routedTo: { studioName: mapping.studio_name, siteId },
      coach: lead.coach
    });
  }

  const normalizedEmail =
    lead.email && lead.email.trim().length > 0 ? lead.email.trim() : makeDummyEmail(lead.token);

  const normalizedPhoneRaw =
    lead.phone && lead.phone.trim().length > 0 ? lead.phone.trim() : makeDummyPhone(lead.token);

  const normalizedPhone = normalizedPhoneRaw.replace(/\D/g, "");

  console.log("about to search/create in MB", {
    siteId,
    firstName: lead.firstName,
    lastName: lead.lastName,
    email: normalizedEmail,
    phone: normalizedPhone,
    coach: lead.coach
  });

  try {
    const existing = await findClient(siteId, {
      firstName: lead.firstName,
      lastName: lead.lastName,
      email: normalizedEmail,
      phone: normalizedPhone
    });

    console.log("existing MB client result:", existing);

    if (existing?.Id) {
      return NextResponse.json({
        ok: true,
        status: "exists",
        mbClientId: String(existing.Id),
        routedTo: { studioName: mapping.studio_name, siteId },
        coach: lead.coach,
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

    console.log("created MB client result:", created);

    if (!created?.Id) {
      return NextResponse.json({ ok: false, error: "Mindbody create failed" }, { status: 500 });
    }

    return NextResponse.json({
      ok: true,
      status: "created",
      mbClientId: String(created.Id),
      routedTo: { studioName: mapping.studio_name, siteId },
      coach: lead.coach,
      fallbacksUsed: {
        emailWasFallback: !lead.email,
        phoneWasFallback: !lead.phone
      }
    });
  } catch (err: any) {
    const status = err?.response?.status ?? null;
    const data = err?.response?.data ?? null;
    const where = typeof err?.config?.url === "string" ? err.config.url : null;

    console.log("mindbody error:", {
      status,
      where,
      data
    });

    return NextResponse.json(
      {
        ok: false,
        error: err?.message ?? "Server error",
        mindbody: { status, where, data },
        routedTo: { studioName: mapping.studio_name, siteId },
        coach: lead.coach
      },
      { status: 500 }
    );
  }
}
