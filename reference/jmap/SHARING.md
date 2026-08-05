# JMAP Sharing - Implementation Reference

Spec: [RFC 9670](https://www.rfc-editor.org/rfc/rfc9670) (published November 2024)

## Capabilities

```
urn:ietf:params:jmap:principals
urn:ietf:params:jmap:principals:owner
```

Session-level `urn:ietf:params:jmap:principals` is empty object.
Account-level contains `currentUserPrincipalId` (String|null).

Account-level `urn:ietf:params:jmap:principals:owner` contains:
- `accountIdForPrincipal` - Account ID holding the Principal object
- `principalId` - Owner's Principal ID

## Object: Principal

Represents an individual, group, resource, location, or other entity.

**Methods:** Principal/get, Principal/changes, Principal/set, Principal/query, Principal/queryChanges

**Properties:**
- `id` (Id) - Immutable
- `type` (String) - `"individual"`, `"group"`, `"resource"`, `"location"`, `"other"`
- `name` (String) - Display name
- `description` (String|null)
- `email` (String|null) - RFC 5322 addr-spec
- `timeZone` (String|null) - IANA timezone
- `capabilities` (Map) - Capability URI → metadata
- `accounts` (Map|null) - Account ID → Account objects accessible to this principal

**Filter properties:**
- `accountIds` - Match principals owning specified accounts
- `email` - Substring match
- `name` - Substring match
- `text` - Substring match on name, email, or description
- `type` - Exact match
- `timeZone` - Exact match

**Set restrictions:** Users can only update their own Principal's `name`, `description`, `timeZone`.

## Object: ShareNotification

Records when permissions change on shared objects. Read-only (destroy only, no create/update).

**Methods:** ShareNotification/get, ShareNotification/changes, ShareNotification/set (destroy only), ShareNotification/query, ShareNotification/queryChanges

**Properties:**
- `id` (Id) - Immutable
- `created` (UTCDate) - Immutable
- `changedBy` (Object) - `{ name, email, principalId }`
- `objectType` (String) - JMAP type name (e.g., `"Calendar"`, `"Mailbox"`)
- `objectAccountId` (String)
- `objectId` (String)
- `oldRights` (String[Boolean]|null) - Previous permissions
- `newRights` (String[Boolean]|null) - New permissions
- `name` (String) - Object name at notification time

**Filter properties:** `after`, `before`, `objectType`, `objectAccountId`
**Comparator:** `created`

## Shared Data Framework

Any shareable JMAP type (Calendar, AddressBook, Mailbox) must include:
- `isSubscribed` (Boolean) - User's subscription preference
- `myRights` (String[Boolean]) - Domain-specific permissions for current user
- `shareWith` (Id[String[Boolean]]|null) - Map of Principal ID → granted rights

## Relevance to Ratatoskr

- Shared calendars - see other users' calendars, accept invitations
- Shared address books - team contact directories
- Shared mailboxes - delegated inbox access
- Share notifications - "Alice shared Calendar 'Team' with you"
- Principal lookup - find users by name/email for sharing UI
- Already partially implemented: Calendar and AddressBook types already have `shareWith`/`myRights` properties. This spec formalizes the Principal and notification layer.

## Note

The Principal type defined here overlaps with the Principal defined in the Calendars spec (ParticipantIdentity, Principal/getAvailability). RFC 9670 is the authoritative source for Principal - the calendars spec extends it with availability.
