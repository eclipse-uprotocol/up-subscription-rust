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
up-subscription is an implementation of the [Eclipse uProtocol&trade; USubscription service](https://github.com/eclipse-uprotocol/up-spec/blob/main/up-l3/usubscription/v4/README.adoc) for the rust programming language.

This crate can be used to configure and run a USubscription service as part of your rust application, implementing the interface defined by the [uProtocol protobuf core API specification](https://github.com/eclipse-uprotocol/up-spec/blob/main/up-core-api/uprotocol/core/usubscription/v4/usubscription.proto).

## Library contents

* `usubscription` service as an frontend for the subscription management and notification handler actors.

## Note

For a batteries-included approach to running up-subscription-rust, the `up-subscription-cli` module provides a command line frontend for running USubscription service. It is available via the [project's github repo](https://github.com/eclipse-uprotocol/up-subscription-rust).

## References

* [uProtocol Specification](https://github.com/eclipse-uprotocol/up-spec)
* [uProtocol USubscription Specification](https://github.com/eclipse-uprotocol/up-spec/blob/main/up-l3/usubscription/v4/README.adoc)
* [uProtocol USubscription API](https://github.com/eclipse-uprotocol/up-spec/blob/main/up-core-api/uprotocol/core/usubscription/v4/usubscription.proto)

*/

// Adapter types for use by all workspace members
pub mod adapters;

// public interface for configuring and starting usubscription service
mod usubscription;
pub use usubscription::*;
mod configuration;
pub use configuration::{
    ConfigurationError, USubscriptionConfiguration, DEFAULT_COMMAND_BUFFER_SIZE,
};

// actors implementing the backend management logic for tracking subscriptions etc
mod notification_manager;
mod subscription_manager;

// persistent storage for backend data
mod persistency;

// testing modules and infra
#[cfg(test)]
mod tests;
#[cfg(test)]
pub(crate) use tests::*;

// helper function(s) used in the crate
mod helpers {
    use std::future::Future;
    use tokio::task;
    use tracing::error;

    // `Send + Sync` is required so the boxed error can cross the `tokio::spawn` task boundary.
    type SpawnResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

    /// Spawns `fut` on a new task, logging (rather than propagating) any error it returns.
    pub(crate) fn spawn_and_log_error<F>(fut: F) -> task::JoinHandle<()>
    where
        F: Future<Output = SpawnResult<()>> + Send + 'static,
    {
        task::spawn(async move {
            if let Err(e) = fut.await {
                error!("{e}")
            }
        })
    }
}
