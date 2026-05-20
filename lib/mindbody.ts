// lib/mindbody.ts
import axios from "axios";

const BASE_URL = "https://api.mindbodyonline.com/public/v6";

function requireEnv(name: string) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing ${name}`);
  return v;
}

function assertValidSiteId(siteId: number) {
  if (!Number.isInteger(siteId) || siteId <= 0) {
    throw new Error(`Invalid siteId: ${siteId}`);
  }
  return siteId;
}

const tokenCache = new Map<number, { token: string; issuedAt: number }>();
const TOKEN_TTL_MS = 10 * 60 * 1000;

async function getToken(siteId: number) {
  assertValidSiteId(siteId);
  const apiKey = requireEnv("MINDBODY_API_KEY");
  const username = requireEnv("MINDBODY_USERNAME");
  const password = requireEnv("MINDBODY_PASSWORD");

  const cached = tokenCache.get(siteId);
  if (cached && Date.now() - cached.issuedAt < TOKEN_TTL_MS) {
    return cached.token;
  }

  const res = await axios.post(
    `${BASE_URL}/usertoken/issue`,
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

  const accessToken = res.data?.AccessToken as string | undefined;
  if (!accessToken) {
    throw new Error("Mindbody token response missing AccessToken");
  }

  tokenCache.set(siteId, { token: accessToken, issuedAt: Date.now() });
  return accessToken;
}

async function mbClient(siteId: number) {
  assertValidSiteId(siteId);
  const apiKey = requireEnv("MINDBODY_API_KEY");
  const token = await getToken(siteId);

  return axios.create({
    baseURL: BASE_URL,
    timeout: 25000,
    headers: {
      "Content-Type": "application/json",
      "Api-Key": apiKey,
      Authorization: `Bearer ${token}`,
      SiteId: String(siteId),
    },
  });
}

export async function findClient(
  siteId: number,
  input: { firstName?: string; lastName?: string; email?: string; phone?: string }
) {
  const client = await mbClient(siteId);

  const candidates: string[] = [];
  if (input.email) candidates.push(input.email);
  if (input.phone) candidates.push(input.phone);
  const fullName = `${input.firstName ?? ""} ${input.lastName ?? ""}`.trim();
  if (fullName) candidates.push(fullName);

  for (const q of candidates) {
    const res = await client.get(`/client/clients`, { params: { SearchText: q } });
    const found = res.data?.Clients?.[0];
    if (found?.Id) return found;
  }
  return null;
}

/**
 * Create a prospect client in Mindbody.
 * Optional:
 *   - referralType: sets the Mindbody Referral Type dropdown (e.g. "Paid Lead")
 *   - salesRep: numeric staff Id assigned to Rep 1 on the client profile
 *   - leadChannelId: numeric Lead Management channel Id. Tags the lead with
 *     this channel. If the tenant has automated channel-to-stage mapping
 *     configured, this can also route the lead to a specific pipeline stage.
 *
 * IMPORTANT: Only pass referralType from your Google Sheets flow.
 * Do NOT pass it from your Typeform flow.
 */
export async function createClient(
  siteId: number,
  input: { firstName: string; lastName: string; email?: string; phone?: string },
  options?: { referralType?: string; salesRep?: number; leadChannelId?: number }
) {
  const client = await mbClient(siteId);

  const payload: Record<string, any> = {
    FirstName: input.firstName,
    LastName: input.lastName,
    Email: input.email ?? "",
    MobilePhone: input.phone ?? "",
    IsProspect: true,
  };

  if (options?.referralType) {
    payload.ReferredBy = options.referralType;
  }

  if (
    options?.salesRep !== undefined &&
    options?.salesRep !== null &&
    Number.isFinite(options.salesRep)
  ) {
    payload.SalesReps = [
      {
        Id: Number(options.salesRep),
        SalesRepNumber: 1,
      },
    ];
  }

  if (
    options?.leadChannelId !== undefined &&
    options?.leadChannelId !== null &&
    Number.isFinite(options.leadChannelId)
  ) {
    payload.LeadChannelId = Number(options.leadChannelId);
  }

  const res = await client.post(`/client/addclient`, payload);
  return res.data?.Client ?? null;
}

/**
 * Update an existing client.
 *
 * Optional:
 *   - salesRep: numeric staff Id for Rep 1
 *   - prospectStageDescription: name of a Sales Pipeline stage. Per Mindbody
 *     docs, on Ultimate-tier accounts the UpdateClient endpoint can move a
 *     client into a Sales Pipeline stage when both IsProspect=true and
 *     ProspectStage.Description are set. Docs only explicitly mention
 *     "New Lead" as a working value; custom stages like "Call Center" are
 *     untested and may or may not work depending on tenant configuration.
 */
export async function updateClient(
  siteId: number,
  mbClientId: string | number,
  updates: { salesRep?: number; prospectStageDescription?: string }
) {
  const client = await mbClient(siteId);

  const clientPayload: Record<string, any> = {
    Id: String(mbClientId),
  };

  if (
    updates.salesRep !== undefined &&
    updates.salesRep !== null &&
    Number.isFinite(updates.salesRep)
  ) {
    clientPayload.SalesReps = [
      {
        Id: Number(updates.salesRep),
        SalesRepNumber: 1,
      },
    ];
  }

  if (
    typeof updates.prospectStageDescription === "string" &&
    updates.prospectStageDescription.trim()
  ) {
    // Per docs: setting IsProspect=true alongside ProspectStage.Description
    // is what triggers the Sales Pipeline opportunity create/move on
    // Ultimate-tier accounts.
    clientPayload.IsProspect = true;
    clientPayload.ProspectStage = {
      Description: updates.prospectStageDescription.trim(),
    };
  }

  const res = await client.post(`/client/updateclient`, {
    Client: clientPayload,
    CrossRegionalUpdate: false,
  });
  return res.data?.Client ?? null;
}

/**
 * Add a Contact Log (= "Sales Followup Task" in the Sales Pipeline UI).
 *
 * Creating this immediately after AddClient should fire the trigger:
 *   "If the sales followup task is created immediately after lead creation,
 *    then move it to <destination> stage."
 *
 * Per Mindbody docs (POST /client/addcontactlog), the request body is FLAT
 * (no ContactLog wrapper). Key fields used here:
 *   - ClientId: the new client's Id (string)
 *   - Text: body text of the log/task
 *   - AssignedToStaffId: who the follow-up task is assigned to. We use this
 *     to make Mindbody treat the log as an open follow-up TASK (not just a
 *     log entry) — that's what the Sales Pipeline trigger keys off.
 *   - FollowupByDate: when the task is due. Open follow-up tasks have a
 *     follow-up date; closed log entries don't.
 *   - IsComplete: false — explicitly mark as an open task.
 *   - ContactMethod, ContactName: descriptive metadata.
 */
export async function addContactLog(
  siteId: number,
  input: {
    clientId: string | number;
    text?: string;
    assignedToStaffId?: number;
    followupByDate?: Date; // defaults to "tomorrow" if not provided
    contactMethod?: string; // free-text; e.g. "Phone"
    contactName?: string;   // who to contact (typically the client's name)
  }
) {
  const client = await mbClient(siteId);

  // Default follow-up date: 24 hours from now.
  // The trigger fires on "task created immediately after lead creation",
  // not on the follow-up date, so the exact value doesn't matter much —
  // it just needs to be a valid future date so Mindbody treats this as
  // an open task.
  const followupByDate =
    input.followupByDate ?? new Date(Date.now() + 24 * 60 * 60 * 1000);

  const payload: Record<string, any> = {
    ClientId: String(input.clientId),
    Text: input.text ?? "Auto-created follow-up task from Paid Leads pipeline",
    FollowupByDate: followupByDate.toISOString(),
    IsComplete: false,
  };

  if (
    input.assignedToStaffId !== undefined &&
    input.assignedToStaffId !== null &&
    Number.isFinite(input.assignedToStaffId)
  ) {
    payload.AssignedToStaffId = Number(input.assignedToStaffId);
  }

  if (input.contactMethod) {
    payload.ContactMethod = input.contactMethod;
  }
  if (input.contactName) {
    payload.ContactName = input.contactName;
  }

  const res = await client.post(`/client/addcontactlog`, payload);
  return res.data ?? null;
}

/**
 * List Lead Channels configured for the site.
 *
 * Hits GET /site/sites?includeLeadChannels=true per Mindbody docs.
 * Use the returned Id to populate LeadChannelId in AddClient calls.
 */
export async function listLeadChannels(siteId: number) {
  const client = await mbClient(siteId);
  const res = await client.get(`/site/sites`, {
    params: {
      siteIds: siteId,
      includeLeadChannels: true,
    },
  });
  // Mindbody returns { Sites: [{ ..., LeadChannels: [...] }] }
  const sites = res.data?.Sites ?? [];
  return sites[0]?.LeadChannels ?? [];
}
