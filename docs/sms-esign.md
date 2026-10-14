# SMS E-Sign for Phone Sales (GHL to Mindbody)

Phone-sale signature flow for STRIDE Fitness studios. Staff text the member the membership terms from GoHighLevel, the member replies `AGREE`, and a "Membership Terms Acceptance" PDF is uploaded to the member's Mindbody Documents tab with a contact log entry. The member then gets a confirmation and a welcome text with the STRIDE Fitness app link.

```
Staff tags contact --> GHL Workflow 1 --POST start--> /api/esign --> request id + reply window mapped onto the contact
                                      +-> SMS: terms link, "Reply AGREE"
Member replies -----> GHL Workflow 2 --POST verify-> /api/esign --> if AGREE:
                                                                     1. build certificate PDF
                                                                     2. Mindbody uploadclientdocument
                                                                     3. Mindbody addcontactlog (plain log)
                                      <-- result + status mapped onto the contact
                                      +-> signed: confirmation text, then welcome + app link + bring a friend
                                      +-> anything else: notify staff
Friend reply -------> GHL Workflow 3 --> notify studio staff
```

## Code

| File | Purpose |
|---|---|
| `app/api/esign/route.ts` | `action: start` opens a signature request; `action: verify` checks the reply and writes to Mindbody |
| `lib/esign.ts` | Reply rules, reply modes, certificate PDF (pdf-lib) |
| `lib/esign-mindbody.ts` | Mindbody calls for e-sign only: client lookup by replying phone, document upload, contact log. Separate from `lib/mindbody.ts` so no existing integration is affected; same env vars and auth |

## Vercel env

Existing vars are reused (`MINDBODY_API_KEY`, `MINDBODY_USERNAME`, `MINDBODY_PASSWORD`, `MINDBODY_SITE_ID_{KEY}`). New:

- `ESIGN_WEBHOOK_SECRET` (required): long random string; GHL sends it as header `x-esign-secret`. Kept separate from `SHEETS_WEBHOOK_SECRET`.
- `ESIGN_REPLY_MODE` (default `agree_only`): `agree_only` signs on the reply AGREE. `code` requires a 6-digit code in the reply (see the appendix).
- `ESIGN_CODE_TTL_MINUTES` (default `15`): the reply window in minutes, in both modes.
- `ESIGN_TIMEZONE` (default `America/Los_Angeles`): times printed on the certificate.
- `ESIGN_CONTACT_METHOD` (default `Text Message`): free text on the Mindbody contact log.

Unlike the sheets webhook, `siteKey` is required here (no HB fallback), so a signature can never land on the wrong studio's client.

## Behavior rules

- The member has 15 minutes from the terms text to reply.
- Signs: `AGREE`, `agree`, `I agree`, `Agreed!`, `Yes, agree`.
- Never signs: `I disagree`, `I don't agree`, `do not agree`, `cannot agree`. These and any other reply (`who is this?`) return `unrecognized`, and staff are notified to follow up. There are no attempt limits in this mode.
- A reply after the window returns `expired`; staff send the terms again.
- Client matching: the Mindbody ID on the GHL contact is used only if that client's phone matches the replying phone. Otherwise it searches by phone (then email, then name to break ties). It never uploads to an ambiguous match. When the ID changed, `idCorrected: true` and the new `mbClientId` come back for GHL to map, same as the IG sync.
- Once AGREE is received, the signature is final. If the Mindbody write fails, the status is `signed_writeback_failed` (member is not asked again) and `writebackError` says why.

## GHL setup

### Custom values (per sub-account)

- `esign_terms_url`: versioned terms link, e.g. `https://stridefitness.com/membership-terms?v=2026-10`
- `esign_terms_version`: `2026-10`
- `esign_site_key`: `HB` (the same key used in `MINDBODY_SITE_ID_{KEY}`, e.g. `TUSTIN`)
- `esign_studio_name`: `STRIDE Fitness Huntington Beach`
- `app_link`: `https://www.stridefitness.com/mobile-app`

### Contact custom fields

Staff fill these (folder "Phone Sale Agreement"):

- ESign Plan Name (dropdown of studio plans)
- ESign Price (e.g. `$189.00 per month`)
- ESign Term (e.g. `12 months, billed monthly`)
- ESign Start Date (date)
- ESign Renewal (e.g. `Renews month to month after the initial term until cancelled`)
- ESign Sent By (dropdown of staff names)

Written by the workflows (single-line text, read-only for staff):

- ESign Status (`pending`, `signed`, `signed_writeback_failed`, `expired`)
- ESign Expires At
- ESign Code Sent At (time the terms text went out)
- ESign Request ID
- ESign Signed At
- ESign Record Hash
- Mindbody Client ID (existing field from the IG sync)

Check each field's merge key in the picker after creating it; the bodies below assume keys like `{{contact.esign_request_id}}`.

### Staff process (on the phone)

1. Open the member's contact (do not create a new one; same rule as the IG leads).
2. Fill the six Phone Sale Agreement fields.
3. Add tag `esign-send` and tell the member: "I just texted you your membership terms. Please reply AGREE to sign."
4. Stay on the line until the confirmation text lands. If the window passes, remove and re-add `esign-send` to send the terms again.

### Workflow 1: E-Sign Send Terms

- Trigger: Contact Tag Added, tag `esign-send`
- Allow re-entry: on

Action 1, Custom Webhook:

- Method: POST
- URL: `https://typeform-mindbody-sync.vercel.app/api/esign`
- Headers: `x-esign-secret: <ESIGN_WEBHOOK_SECRET>`, `Content-Type: application/json`
- Body:
  ```json
  { "action": "start", "phone": "{{contact.phone}}", "ghlContactId": "{{contact.id}}" }
  ```
- Response mapping: `esignExpiresAt`, `esignCodeSentAt`, `esignRequestId`, `esignStatus` onto their fields

Action 2, Send SMS: template A

Action 3, tags:

- Remove `esign-send`, `esign-signed`
- Add `esign-pending`

### Workflow 2: E-Sign Verify Reply

- Trigger: Customer Replied, channel SMS
- Filter: contact has tag `esign-pending`
- Allow re-entry: on

Action 1, Custom Webhook (same URL and headers):

```json
{
  "action": "verify",
  "siteKey": "{{custom_values.esign_site_key}}",
  "studioName": "{{custom_values.esign_studio_name}}",
  "ghlContactId": "{{contact.id}}",
  "mbClientId": "{{contact.mindbody_client_id}}",
  "phone": "{{contact.phone}}",
  "email": "{{contact.email}}",
  "firstName": "{{contact.first_name}}",
  "lastName": "{{contact.last_name}}",
  "planName": "{{contact.esign_plan_name}}",
  "price": "{{contact.esign_price}}",
  "term": "{{contact.esign_term}}",
  "startDate": "{{contact.esign_start_date}}",
  "renewal": "{{contact.esign_renewal}}",
  "termsUrl": "{{custom_values.esign_terms_url}}",
  "termsVersion": "{{custom_values.esign_terms_version}}",
  "staffName": "{{contact.esign_sent_by}}",
  "esignStatus": "{{contact.esign_status}}",
  "esignExpiresAt": "{{contact.esign_expires_at}}",
  "esignCodeSentAt": "{{contact.esign_code_sent_at}}",
  "esignRequestId": "{{contact.esign_request_id}}",
  "replyText": "{{message.body}}",
  "messageId": "{{message.id}}"
}
```

Confirm `{{message.body}}` is the inbound message merge field offered on the Customer Replied trigger. Response mapping: `esignStatus`, `esignSignedAt`, `esignRecordHash` to their fields; `mbClientId` to Mindbody Client ID.

Action 2, If/Else on the webhook's `result`:

| result | Member texts | Tags | Staff |
|---|---|---|---|
| `signed` | Template B, Wait 1 minute, Template C | remove `esign-pending`; add `esign-signed`, `friend-ask-sent` | Internal notification: signed |
| `signed` with status `signed_writeback_failed` | same as signed | same as signed | Also notify Matt with `writebackError` |
| `unrecognized` | none | keep `esign-pending` | Notify studio: member replied without AGREE, include the reply |
| `expired` | Template D | remove `esign-pending` | Notify studio to send the terms again |
| `already_signed` | none | remove `esign-pending` | none |

### Workflow 3: Bring a Friend Reply

- Trigger: Customer Replied, channel SMS
- Filter: contact has tag `friend-ask-sent`
- Action 1: Internal notification to the studio: "{{contact.first_name}} replied to bring-a-friend: {{message.body}}". Staff follow up to book the guest.
- Action 2: Remove tag `friend-ask-sent`

So an unanswered question doesn't route a reply weeks later, add to the end of Workflow 2's signed branch: Wait 7 days, then remove tag `friend-ask-sent`.

### Other Customer Replied workflows

Lead nurture, stop-on-reply or auto-responder workflows will also fire on the member's AGREE and friend replies. Add a filter to them excluding contacts tagged `esign-pending` or `friend-ask-sent`.

### SMS templates

Have counsel approve A and B before rollout (ESIGN/UETA consent language, California health studio contract and auto-renewal disclosures).

**A. Terms**
> STRIDE Fitness: Hi {{contact.first_name}}, here are the terms for your {{contact.esign_plan_name}} membership ({{contact.esign_price}}, {{contact.esign_term}}): {{custom_values.esign_terms_url}}
>
> Reply AGREE to accept. Replying AGREE is your electronic signature on these terms.

**B. Signed (confirmation and copy)**
> STRIDE Fitness: Your membership terms are signed. Record {{contact.esign_request_id}}. Keep this link for your records: {{custom_values.esign_terms_url}}

**C. Welcome (1 minute after B)**
> Thank you, {{contact.first_name}}! We're so excited to have you as a STRIDE Fitness member. Here is the link to the STRIDE Fitness app so you can start booking classes now: {{custom_values.app_link}}
>
> By the way, would you like to bring a friend to your next class with you?

**D. Expired**
> STRIDE Fitness: Your signing window has expired. Your studio team will send the terms again.

## First live check (HB)

1. Send a verify call with `"test": true` for a real HB test client. It resolves the client and builds the certificate but writes nothing; the response carries `certificateBase64`.
2. Run one real signature on that test client from your own phone. Confirm the PDF on the client's Documents tab, the contact log entry, texts B and C, and that replying to C notifies the studio.
3. Deactivate the test client afterwards, same as the IG sync test clients.

## Before go-live

- Freeze each terms version (the `?v=` page or a PDF per version) so the certificate always points at the exact terms signed.
- Counsel review of templates A and B.
- Phase 2: a daily backstop that flags contracts sold in Mindbody with no `signed` status in GHL.

## Appendix: code mode

Set `ESIGN_REPLY_MODE=code` to require a one-time code in the reply ("AGREE 482913"). Then:

- `start` also returns `esignCode`; map it to an ESign Code field and add `Reply AGREE {{contact.esign_code}}` to template A.
- Add ESign Code and ESign Attempts fields, and pass `esignCode` and `esignAttempts` in the verify body.
- Extra verify results: `wrong_code` and `need_agree`/`need_code` (keep pending, ask again), `locked` after 3 wrong codes (notify staff).
