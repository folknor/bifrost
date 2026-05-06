# bifrost-jmap roadmap

## Status

| Spec | RFC | Status |
|---|---|---|
| Core | 8620 | Implemented |
| Mail | 8621 | Implemented |
| WebSocket | 8887 | Implemented |
| Sieve | draft-14 | Implemented |
| Blob | 9404 | Implemented |
| Calendars | draft-26 | Implemented |
| Contacts | 9610 | Implemented |
| Quotas | 9425 | Implemented |
| Sharing | 9670 | Implemented |
| MDN | 9007 | Not implemented |
| S/MIME | 9219 | Not implemented |

## Planned

### MDN Handling (RFC 9007)

See `plans/MDN.md`.

- `MDN/send`, `MDN/parse`
- Read receipts over JMAP
- `$mdnsent` keyword tracking

### S/MIME Verification (RFC 9219)

See `plans/SMIME.md`.

- Additional `Email/get` properties: `smimeStatus`, `smimeErrors`, `smimeVerifiedAt`
- Additional `Email/query` filters: `hasSmime`, `hasVerifiedSmime`
- Server-side verification — low client effort.

### Pre-1.0 API stabilization

The JMAP coverage is feature-complete for ratatoskr's needs; the focus before 1.0 is API ergonomics. See `crates/jmap/CLAUDE.md` and recent commit history for the in-flight changes (consumer feedback round, transport abstraction polish, `Field<T>` adoption, etc.).

## Design principles

- **Trait-based method dispatch** — `JmapMethod` trait, no central enums. Adding a method touches only its own module.
- **Transport-generic** — `Client<T: HttpTransport>`, `SseTransport` for EventSource. `ReqwestTransport` as default.
- **JSON map backing for JSCalendar/JSContact** — `CalendarEvent` and `ContactCard` use `serde_json::Map` for extension-property round-trip fidelity.
- **Feature-gated per RFC** — `mail`, `calendars`, `contacts`, `blob`, `quota` features gate modules independently.
- **Implement from the RFCs.** Do not copy from server code under incompatible licenses.
- **Async-only.**
- **Apache-2.0 / MIT dual license.**

Unimplemented spec plans live in `plans/` (`MDN.md`, `SMIME.md`, `SHARING.md`).
