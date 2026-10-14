# SMS E-Sign for Phone Sales (GHL to Mindbody)

Phone-sale signature flow for STRIDE Fitness studios. Staff send a one-time code from GoHighLevel, the member replies `AGREE 482913`, and a "Membership Terms Acceptance" PDF is uploaded to the member's Mindbody Documents tab with a contact log entry.

```
Staff tags contact --> GHL Workflow 1 --POST start--> /api/esign --> code + expiry mapped onto the contact
                                      +-> SMS with terms link + code
Member replies -----> GHL Workflow 2 --POST verify-> /api/esign --> if signed:
                                                                     1. build certificate PDF
                                                                     2. Mindbody uploadclientdocument
                                                                     3. Mindbody addcontactlog (plain log)
                                      <-- result + status mapped onto the contact
                                      +-> branch: confirm / retry / notify staff
```

## Code

| File | Purpose |
|---|---|
| `app/api/esign/route.ts` | `action: start` issues a code; `action: verify` checks the reply and writes to Mindbody |
| `lib/esign.ts` | Code generation, reply rules, certificate PDF (pdf-lib) |
| `lib/mindbody.ts` | Adds `resolveClientForEsign` and `uploadClientDocument`; reuses the existing token cache, `searchClients`, phone and name matching, and `addContactLog` |

## Vercel env

Existing vars are reused (`MINDBODY_API_KEY`, `MINDBODY_USERNAME`, `MINDBODY_PASSWORD`, `MINDBODY_SITE_ID_{KEY}`). New:

- `ESIGN_WEBHOOK_SECRET` (required): long random string; GHL sends it as header `x-esign-secret`. Kept separate from `SHEETS_WEBHOOK_SECRET`.
- `ESIGN_CODE_TTL_MINUTES` (default `15`)
- `ESIGN_REQUIRE_AGREE` (default `true`; `false` accepts the bare code)
- `ESIGN_TIMEZONE` (default `America/Los_Angeles`; times printed on the certificate)
- `ESIGN_CONTACT_METHOD` (default `Text Message`; free text on the Mindbody contact log)

Unlike the sheets webhook, `siteKey` is required here (no HB fallback), so a signature can never land on the wrong studio's client.

## First live check (HB)

1. Send a verify call with `"test": true` for a real HB test client. It resolves the client and builds the certificate but writes nothing; the response carries `certificateBase64`.
2. Run one real signature on that test client. Confirm the PDF on the client's Documents tab and the contact log entry.
3. Deactivate the test client afterwards, same as the IG sync test clients.

## Behavior rules

- Codes are 6 digits, random, 15 minute lifetime.
- Accepted replies: `AGREE 482913`, `agree482913`, `I agree 482 913`, `Agreed 482913`. `DISAGREE` never counts.
- Chatter with no 6-digit number (`what is this?`) does not use an attempt; it returns `unrecognized` so staff can answer.
- Wrong code uses an attempt. The third wrong code locks the request; staff must send a new code.
- Client matching: the Mindbody ID on the GHL contact is used only if that client's phone matches the replying phone. Otherwise it searches by phone (then email to break ties). It never uploads to an ambiguous match. When the ID changed, `idCorrected: true` and the new `mbClientId` come back for GHL to map, same as the IG sync.
- Once the reply checks out, the signature is final. If the Mindbody write fails, the status is `signed_writeback_failed` (member is not asked again) and `writebackError` says why.

## GHL setup

### Custom values (per sub-account)

- `esign_terms_url`: versioned terms link, e.g. `https://stridefitness.com/membership-terms?v=2026-10`
- `esign_terms_version`: `2026-10`
- `esign_site_key`: `HB` (the same key used in `MINDBODY_SITE_ID_{KEY}`, e.g. `TUSTIN`)
- `esign_studio_name`: `STRIDE Fitness Huntington Beach`

### Contact custom fields

Staff fill these (one-time setup as a "Phone Sale Agreement" field group so they sit together on the contact record):

- ESign Plan Name (dropdown of studio plans is ideal)
- ESign Price (e.g. `$189.00 per month`)
- ESign Term (e.g. `12 months, billed monthly`)
- ESign Start Date (date)
- ESign Renewal (e.g. `Renews month to month after the initial term until cancelled`)

Written by the workflows (read-only for staff):

- ESign Status (`pending`, `signed`, `signed_writeback_failed`, `expired`, `locked`)
- ESign Code
- ESign Expires At
- ESign Code Sent At
- ESign Request ID
- ESign Attempts
- ESign Sent By
- ESign Signed At
- ESign Record Hash
- Mindbody Client ID (existing field from the IG sync)

### Staff process (on the phone)

1. Open the member's contact (do not create a new one; same rule as the IG leads).
2. Fill the five plan fields.
3. Add tag `esign-send` and tell the member: "I just texted you your membership terms and a code. Please reply AGREE and the code to sign."
4. Stay on the line until the confirmation text lands. If it expires or locks, remove and re-add `esign-send` to send a fresh code.

### Workflow 1: E-Sign Send Code

- **Trigger:** Contact Tag Added, tag = `esign-send`. Allow re-entry ON.
- **Action 1, Custom Webhook:** POST `https://typeform-mindbody-sync.vercel.app/api/esign`
  - Headers: `x-esign-secret: <ESIGN_WEBHOOK_SECRET>`, `Content-Type: application/json`
  - Body:
    ```json
    { "action": "start", "phone": "{{contact.phone}}", "ghlContactId": "{{contact.id}}" }
    ```
  - Response mapping: `esignCode` to ESign Code, `esignExpiresAt` to ESign Expires At, `esignCodeSentAt` to ESign Code Sent At, `esignRequestId` to ESign Request ID, `esignAttempts` to ESign Attempts, `esignStatus` to ESign Status
- **Action 2, Update Contact Field:** ESign Sent By = `{{user.name}}` (the staff member who added the tag; if that merge field isn't available on tag triggers in your account, add a required "Sent by" dropdown to the field group instead)
- **Action 3, Send SMS:** template A below
- **Action 4:** Remove tag `esign-send`, add tag `esign-pending`, remove tags `esign-signed`, `esign-locked`

### Workflow 2: E-Sign Verify Reply

- **Trigger:** Customer Replied, channel SMS, filter: contact has tag `esign-pending`. Allow re-entry ON.
- **Action 1, Custom Webhook:** POST same URL, same headers
  - Body:
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
      "esignCode": "{{contact.esign_code}}",
      "esignExpiresAt": "{{contact.esign_expires_at}}",
      "esignCodeSentAt": "{{contact.esign_code_sent_at}}",
      "esignRequestId": "{{contact.esign_request_id}}",
      "esignAttempts": "{{contact.esign_attempts}}",
      "replyText": "{{message.body}}",
      "messageId": "{{message.id}}"
    }
    ```
    Merge keys depend on the field keys GHL assigns when you create the fields; check each in the field picker. `replyAt` is optional (the server uses receipt time).
  - Response mapping: `esignStatus`, `esignAttempts`, `esignSignedAt`, `esignRecordHash` to their fields; `mbClientId` to Mindbody Client ID
- **Action 2, If/Else on the webhook's `result`:**

| result | Member SMS | Tags | Staff |
|---|---|---|---|
| `signed` | Template B | remove `esign-pending`, add `esign-signed` | Internal notification: "Signed" |
| `signed` + status `signed_writeback_failed` | Template B | same as signed | Notify Matt with `writebackError` |
| `wrong_code` | Template C | keep pending | none |
| `need_agree` | Template D | keep pending | none |
| `need_code` | Template E | keep pending | none |
| `unrecognized` | none | keep pending | Notify staff: member replied with a question |
| `expired` | Template F | remove `esign-pending` | Notify staff to resend |
| `locked` | Template G | remove `esign-pending`, add `esign-locked` | Notify staff |
| `already_signed` | none | remove `esign-pending` | none |

### SMS templates

Have counsel approve A and B before rollout (ESIGN/UETA consent language, California health studio contract and auto-renewal disclosures).

**A. Code (send)**
> STRIDE Fitness: Hi {{contact.first_name}}, here are the terms for your {{contact.esign_plan_name}} membership ({{contact.esign_price}}, {{contact.esign_term}}): {{custom_values.esign_terms_url}}
>
> To sign, reply AGREE {{contact.esign_code}}
>
> Replying AGREE with this code is your electronic signature on these terms. Code expires in 15 minutes.

**B. Signed (confirmation and copy)**
> STRIDE Fitness: Thanks {{contact.first_name}}, your membership terms are signed. Record {{contact.esign_request_id}}. Keep this link for your records: {{custom_values.esign_terms_url}}

**C. Wrong code**
> STRIDE Fitness: That code didn't match. Please reply AGREE followed by the 6-digit code from our earlier text.

**D. Code without AGREE**
> STRIDE Fitness: Almost there. To sign, reply AGREE {{contact.esign_code}}

**E. AGREE without code**
> STRIDE Fitness: Please include your 6-digit code, for example: AGREE {{contact.esign_code}}

**F. Expired**
> STRIDE Fitness: That code has expired. Your studio team will send you a new one.

**G. Locked**
> STRIDE Fitness: We couldn't verify your code. Your studio team will reach out to finish signing.

## Before go-live

- Freeze each terms version (the `?v=` page or a PDF per version) so the certificate always points at the exact terms signed.
- Counsel review of templates A and B.
- Rotate `ESIGN_WEBHOOK_SECRET` if it is ever pasted anywhere other than Vercel and the GHL webhook header.
- Phase 2: a daily backstop that flags contracts sold in Mindbody with no `signed` status in GHL.
