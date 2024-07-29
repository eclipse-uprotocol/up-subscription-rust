/********************************************************************************
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

/*!
up-subscription is an implementation of the [Eclipse uProtocol&trade; USubscription service](https://github.com/eclipse-uprotocol/up-spec/blob/main/up-l3/usubscription/v3/README.adoc) for the rust programming language.

This crate can be used to configure and run a USubscription service as part of your rust application, implementing the interface defined by the [uProtocol protobuf core API specification](https://github.com/eclipse-uprotocol/up-spec/blob/main/up-core-api/uprotocol/core/usubscription/v3/usubscription.proto).

## Library contents

* `usubscription` service as an frontend for the subscription management and notification handler actors.
* `listeners` module, with UListener trait implementations for all functions defined by the USubscription API

## Note

For a batteries-included approach to running up-subscription-rust, the `up-subscription-cli` module provides a command line frontend for running the USubscription service. It is available via the [project's github repo](https://github.com/eclipse-uprotocol/up-subscription-rust).


## References

* [uProtocol Specification](https://github.com/eclipse-uprotocol/up-spec/tree/v1.6.0-alpha.2)
* [uProtocol USubscription Specification](https://github.com/eclipse-uprotocol/up-spec/blob/main/up-l3/usubscription/v3/README.adoc)
* [uProtocol USubscription API](https://github.com/eclipse-uprotocol/up-spec/blob/main/up-core-api/uprotocol/core/usubscription/v3/usubscription.proto)

*/

mod common {
    pub(crate) mod helpers;
}
pub use common::helpers::init_once;
pub(crate) use common::*;

mod notification_manager;
mod subscription_manager;

mod configuration;
pub use usubscription::*;
mod usubscription;
pub use configuration::{ConfigurationError, USubscriptionConfiguration};

pub mod listeners {
    pub mod fetch_subscribers;
    pub mod fetch_subscriptions;
    pub mod register_for_notifications;
    pub mod subscribe;
    pub mod unregister_for_notifications;
    pub mod unsubscribe;

    pub use fetch_subscribers::FetchSubscribersListener;
    pub use fetch_subscriptions::FetchSubscriptionsListener;
    pub use register_for_notifications::RegisterForNotificationsListener;
    pub use subscribe::SubscribeListener;
    pub use unregister_for_notifications::UnregisterForNotificationsListener;
    pub use unsubscribe::UnsubscribeListener;
}

#[cfg(test)]
mod tests;
#[cfg(test)]
pub(crate) use tests::*;
