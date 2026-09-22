# uSubscription v4

## v3 → v4 Delta Analysis

### 1. API Method Changes

| Operation | v3 method_id | v4 method_id | Change |
|-----------|:---:|:---:|--------|
| Subscribe | 1 | 1 | unchanged |
| Unsubscribe | 2 | 2 | unchanged |
| FetchSubscriptions | 3 | 3 | unchanged |
| RegisterForNotifications | **6** | **4** | **changed** |
| UnregisterForNotifications | **7** | **5** | **changed** |
| **FetchSubscribers** | **8** | — | **removed** |
| Reset | **9** | **6** | **changed** |

### 2. Protobuf Message Changes

| Aspect | v3 | v4 | Delta |
|--------|-----|-----|-------|
| **`SubscriptionStatus`** | Nested message with `State` enum + `message` string | Top-level **enum** (no message field) | Breaking: flatten to enum |
| **`SubscribeRequest`** | `topic` + `SubscribeAttributes` (containing `expire`, `details`, `sample_period_ms`) | `topic` + `expiration` (Timestamp) + `sample_period` (uint32) directly | Flattened; `details` field **removed** |
| **`SubscribeResponse`** | `status: SubscriptionStatus` + `config: EventDeliveryConfig` + `topic` | `topic: UUri` + `status: SubscriptionStatus` (enum) | `EventDeliveryConfig` **removed** |
| **`Subscription`** | `topic` + `subscriber: SubscriberInfo` + `status: SubscriptionStatus` + `attributes: SubscribeAttributes` + `config: EventDeliveryConfig` | `topic` + `subscriber: UUri` + `status: SubscriptionStatus` + `expiration` + `sample_period` | Flattened; `SubscriberInfo` wrapper → plain `UUri`; no `EventDeliveryConfig` |
| **`FetchSubscriptionsRequest`** | `oneof request { topic, subscriber: SubscriberInfo }` | `optional topic_filter` + `optional subscriber_filter` (both UUri, can have wildcards) | Major redesign: dual optional filters replace oneof |
| **`NotificationsRequest`** | Contains `topic: UUri` | **Empty message** | Topic removed; registers for **all** changes |
| **Notification payload** | `Update` message (topic + subscriber: SubscriberInfo + status + attributes) | `Subscription` message directly | Different type |
| **`SubscriberInfo`** wrapper | Used throughout (wraps UUri) | **Removed**; plain `UUri` used directly | Simplification |
| **`SubscribeAttributes`** | Separate message with `expire`, `details`, `sample_period_ms` | **Removed**; fields inlined | Simplification |
| **`EventDeliveryConfig`** | Present in Subscribe response and Subscription | **Removed entirely** | — |
| **`Update`** message | Dedicated notification payload type | **Removed**; replaced by `Subscription` | — |
| **`FetchSubscribersRequest/Response`** | Exists | **Removed entirely** | — |
| **`ResetRequest`** | Contains optional `Reason` message | **Empty message** | Simplified |
| **`PassiveMode`** | Exists | **Removed** | — |

### 3. Behavioral/Semantic Changes

| Aspect | v3 | v4 | Impact |
|--------|-----|-----|--------|
| **Topic validation** | No wildcard authority/ue_id/resource_id; requires specific `ue_version_major` | No wildcard authority/service-ID/resource_id; **allows wildcard service instance ID** (upper 16 bits of ue_id); no explicit `ue_version_major` constraint | Validation logic update |
| **Subscribe auto-registers for notifications** | Implied | **Explicit** req: subscribing auto-registers the client for change notifications on that topic | Currently partially done |
| **Unsubscribe auto-deregisters** | On last subscriber | Explicit per-client: unsubscribing **stops** sending notifications to that client | Notification cleanup |
| **RegisterForNotifications scope** | Per-topic (topic passed in request) | **Global** (empty request → all subscription changes) | Major simplification |
| **Subscription change notification is also Published** | Only Notify + Publish on the `SubscriptionChange` topic | Explicit **3 delivery paths**: (1) direct to subscriber, (2) Publish to dedicated topic, (3) RegisterForNotifications recipients | Ensure all 3 paths implemented |
| **Notification payload triggers** | Status change | `status`, `expiration`, or `samplePeriod` change | Broader trigger conditions |
| **FetchSubscriptions filtering** | Either by topic OR by subscriber (oneof) | By topic **AND/OR** subscriber (both optional, wildcard-capable) | New UURI pattern matching needed |
| **Expiration in the past** | Not explicitly specified | Must return `UNSUBSCRIBED` immediately | New validation |
| **Expiration update to past** | Not specified | Must unsubscribe + return `UNSUBSCRIBED` | New behavior |
| **Reset caller check** | `ue_id != 0x0` → PERMISSION_DENIED | `uEntity type != 0x0` → PERMISSION_DENIED | Same intent, slightly different phrasing |
| **Unsubscribe on SUBSCRIBE_PENDING** | Initiate remote unsub, set UNSUBSCRIBED | Same but explicitly sets `UNSUBSCRIBED` | Clarification |
| **Remote unsubscribe notification stop** | Stops publishing changes for topic when last subscriber leaves | Stops publishing to `subscriptionChange` channel for that topic | Explicit |

---

## Implementation Tasks

### Phase 1: Protobuf & Dependency Update

1. **Update `up-rust` dependency** — Needs a version of `up-rust` that provides the v4 protobuf types (`uprotocol.core.usubscription.v4.*`). This is a prerequisite for everything else.

2. **Update method ID constants** — RegisterForNotifications: 6→4, UnregisterForNotifications: 7→5, Reset: 9→6. FetchSubscribers (method 8) is removed.

3. **Update service version** — `USUBSCRIPTION_VERSION_MAJOR` from 3 to 4.

### Phase 2: Remove Deprecated API Surface

4. **Remove `FetchSubscribers` handler** — The fetch_subscribers.rs handler, its registration, and all related `SubscriptionEvent::FetchSubscribers` variants should be removed.

5. **Remove `SubscriberInfo` usage** — Replace all `SubscriberInfo { uri }` wrappers with plain `UUri`.

6. **Remove `EventDeliveryConfig` handling** — Drop from SubscribeResponse construction and Subscription entries.

7. **Remove `Update` message type** — Replace with `Subscription` message as notification payload.

8. **Remove `SubscribeAttributes` indirection** — Expiry and sample_period are now directly on SubscribeRequest/Subscription.

### Phase 3: Refactor Existing Handlers

9. **Refactor `Subscribe` handler** — Parse `expiration` and `sample_period` directly from `SubscribeRequest` fields (no more `attributes.expire` nesting). Return `SubscribeResponse { topic, status }` without `config`.

10. **Add past-expiration rejection** — If `expiration` is set but already in the past, return `UNSUBSCRIBED` immediately (new req `req~usubscription-past-subscription-expiration~1`).

11. **Add expiration-update-to-past handling** — When re-subscribing with a past expiration, unsubscribe and return `UNSUBSCRIBED` (`req~usubscription-subscribe-expiration-extension-past~1`).

12. **Refactor `FetchSubscriptions` handler** — Change from `oneof { topic, subscriber }` to dual-optional filters (`topic_filter`, `subscriber_filter`). Implement UURI wildcard pattern matching for both filters simultaneously.

13. **Refactor `RegisterForNotifications` handler** — Remove topic parameter; empty request. Register caller for **all** subscription change notifications globally.

14. **Refactor `UnregisterForNotifications` handler** — Remove topic parameter; unregister from all.

15. **Refactor `Reset` handler** — Remove `Reason` from request (empty message). Verify permission check uses uEntity type ID (`0x0000`) not just `ue_id`.

### Phase 4: Notification Manager Overhaul

16. **Change notification payload type** — Replace `Update` with `Subscription` message in all notification sends.

17. **Broaden notification triggers** — Trigger notifications not only on status changes, but also on `expiration` and `samplePeriod` changes.

18. **Implement 3-path notification delivery**:
    - (a) Direct `Notification` to the affected subscriber
    - (b) `Publish` to the dedicated `subscriptionChange` topic (resource 0x8000)
    - (c) `Notification` to all `RegisterForNotifications()` registrants

19. **Subscribe auto-registers for notifications** — Ensure that subscribing automatically registers the subscriber for change notifications on their subscription.

20. **Unsubscribe auto-deregisters** — Unsubscribing should stop sending change notifications to that client.

21. **Global notification registration** — Refactor `NotificationStore` from per-topic to global registration (no longer topic-keyed).

### Phase 5: Subscription Manager / Persistency

22. **Track `sample_period` per subscription** — Store alongside subscriber/topic/expiry.

23. **Include `expiration` and `sample_period` in Subscription responses** — FetchSubscriptions should return full `Subscription` objects with these optional fields.

24. **Topic validation update** — Allow wildcard service instance ID (upper 16 bits of `ue_id`); remove strict `ue_version_major` non-wildcard requirement; enforce `resource_id` in range `[0x8000, 0xFFFE]`.

### Phase 6: Unsubscribe Notification Logic

25. **Stop subscription change publishing on last remote unsub** — When the last subscriber on a remote topic unsubscribes, stop publishing to the `subscriptionChange` channel for that topic.

### Phase 7: Cleanup & Testing

26. **Update all OFT tracing tags** — All `[impl->dsn~...]` and `[impl->req~...]` markers reference v3 requirement IDs; update to v4 IDs.

27. **Update unit tests** — Adapt all tests to new message types, method IDs, and behavior.

28. **Update integration tests** — Adapt `tests/` directory for new API surface.

29. **Update documentation** — README, lib.rs doc comments, workspace Cargo.toml `documentation` field pointing to v4.

---

### Key Blockers / Prerequisites

- **`up-rust` crate update**: The Rust protobuf bindings for `uprotocol.core.usubscription.v4` need to exist in `up-rust`. The current dependency is `up-rust 0.9.0` with `features = ["usubscription"]`. A new version must provide the v4 types.
- **UURI pattern matching**: The `FetchSubscriptions` dual-filter with wildcard support requires UURI pattern matching capability (referenced in the spec as `uuri_pattern_matching.feature`). Check if `up-rust` already provides this, or if it needs implementation.