# JMAP MDN Handling - Implementation Reference

Spec: [RFC 9007](https://www.rfc-editor.org/rfc/rfc9007) (published March 2021)

## Capability

```
urn:ietf:params:jmap:mdn
```

Requires `urn:ietf:params:jmap:mail` as well.

## Methods

### MDN/send

Send a disposition notification (read receipt) back to the original sender.

**Request properties:**
- `accountId` (Id)
- `identityId` (Id) - Identity to send the MDN from
- `send` (Id[MDN]) - Map of creation ID → MDN object
- `onSuccessUpdateEmail` (Id[PatchObject]|null) - Patches to apply to the original email after successful send (typically sets `$mdnsent` keyword)

**MDN object properties:**
- `forEmailId` (Id) - Required. The email being acknowledged
- `subject` (String|null) - Subject of the MDN message
- `textBody` (String|null) - Human-readable body
- `reportingUA` (String|null) - User agent identifier
- `disposition` (Disposition) - Required. The disposition object
- `finalRecipient` (String|null) - Server auto-fills from identity if null
- `originalRecipient` (String|null)
- `originalMessageId` (String|null) - Server auto-fills from email if null
- `error` (String[]|null) - Error descriptions
- `extensionFields` (String[String]|null) - Non-standard header fields

**Disposition object:**
- `actionMode` (String) - `"manual-action"` or `"automatic-action"`
- `sendingMode` (String) - `"mdn-sent-manually"` or `"mdn-sent-automatically"`
- `type` (String) - `"deleted"`, `"dispatched"`, `"displayed"`, or `"processed"`

All values must be lowercase.

**Errors:**
- `mdnAlreadySent` - The email already has the `$mdnsent` keyword
- Standard SetError types (notFound, forbidden, etc.)

### MDN/parse

Parse MDN data from message blobs.

**Request:** `accountId`, `blobIds` (Id[])

**Response:**
- `parsed` (Id[MDN]) - Successfully parsed MDN objects keyed by blob ID
- `notParsable` (Id[]) - Blob IDs that couldn't be parsed as MDN
- `notFound` (Id[]) - Missing blob IDs

## Workflow

**Requesting a read receipt (sending side):**
Include `Disposition-Notification-To` header in outgoing email pointing to the address where MDNs should arrive.

**Sending a read receipt (receiving side):**
1. Create MDN object with `forEmailId` referencing the message
2. Set `disposition.type` to `"displayed"` (user viewed it)
3. Set `disposition.actionMode` to `"manual-action"` (user chose to send receipt)
4. Include `onSuccessUpdateEmail` to set `$mdnsent` keyword
5. Call MDN/send

**Processing incoming MDNs:**
1. Detect incoming MDN messages (Content-Type: multipart/report; report-type=disposition-notification)
2. Call MDN/parse with the message's blob ID
3. Display the parsed disposition to the user

## Relevance to Ratatoskr

- "Read receipts (outgoing)" is already in TODO.md - this is the JMAP-native implementation
- Send side: add `Disposition-Notification-To` header in compose when user enables read receipt
- Receive side: detect MDN messages, offer "Send read receipt?" prompt
- Parse side: show "Read by X at Y" status on sent messages that received MDNs
- The `$mdnsent` keyword prevents duplicate receipt sends
