import { neon } from "@neondatabase/serverless";

if (!process.env.DATABASE_URL) {
  throw new Error("Missing DATABASE_URL env var");
}

const sql = neon(process.env.DATABASE_URL);

export async function getTenantByFormId(formId: string) {
  const rows = await sql`
    select *
    from tenants
    where form_id = ${formId}
      and is_active = true
    limit 1
  `;
  return rows[0] ?? null;
}

export async function getIdempotency(responseId: string) {
  const rows = await sql`
    select *
    from idempotency
    where response_id = ${responseId}
    limit 1
  `;
  return rows[0] ?? null;
}

export async function insertIdempotency(params: {
  responseId: string;
  formId: string;
  status: string;
  mbClientId?: string | null;
  error?: string | null;
}) {
  await sql`
    insert into idempotency (response_id, form_id, status, mb_client_id, error)
    values (${params.responseId}, ${params.formId}, ${params.status}, ${params.mbClientId ?? null}, ${params.error ?? null})
    on conflict (response_id) do nothing
  `;
}
