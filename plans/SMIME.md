# JMAP S/MIME Signature Verification - Implementation Reference

Spec: [RFC 9219](https://www.rfc-editor.org/rfc/rfc9219) (published April 2022)

## Capability

```
urn:ietf:params:jmap:smimeverify
```

## What It Does

Extends Email/get with server-side S/MIME signature verification. The server checks signatures and reports status - the client doesn't need to download certificates, check revocation lists, or verify chains. Trade-off: client trusts the server's verification.

## Properties Added to Email/get

These are additional properties that can be requested via Email/get `properties`:

- `smimeStatus` (String|null) - Verification result:
  - `null` - No S/MIME signature present
  - `"unknown"` - S/MIME message but signing protocol not recognized
  - `"signed"` - Signature present, not yet verified
  - `"signed/verified"` - Verified, signer matches From header
  - `"signed/failed"` - Verification failed (expired cert, revoked, address mismatch)
  - `"encrypted+signed/verified"` - Encrypted and verified (future)
  - `"encrypted+signed/failed"` - Encrypted, verification failed (future)

- `smimeStatusAtDelivery` (String|null) - Status at message delivery time. Static unless trust anchors change. Allows comparing "was it valid when received?" vs "is it valid now?"

- `smimeErrors` (String[]|null) - Human-readable error descriptions when status indicates failure. Examples: "Certificate has expired", "CRL not available", "From address doesn't match certificate"

- `smimeVerifiedAt` (UTCDate|null) - When signature was last verified. Server may cache for up to 24 hours.

## Filter Conditions Added to Email/query

- `hasSmime` (Boolean) - Match messages with any S/MIME signature
- `hasVerifiedSmime` (Boolean) - Match messages with valid signatures
- `hasVerifiedSmimeAtDelivery` (Boolean) - Match using delivery-time verification

## Change Tracking

`Email/changes` must include messages when `smimeStatus`, `smimeStatusAtDelivery`, or `smimeErrors` change. Changes to `smimeVerifiedAt` alone do NOT trigger change notifications.

## Relevance to Ratatoskr

- Show a lock/shield icon on verified emails in thread list and reading pane
- Show warning banner on `signed/failed` emails
- "Verified by server at [timestamp]" in message detail
- Filter view: "Show only verified emails"
- Low implementation effort on client side - it's just additional Email/get properties
- Useful for enterprise users who care about email authenticity
- Only relevant when connected to a server that advertises the capability
