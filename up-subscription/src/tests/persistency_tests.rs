/********************************************************************************
 * Copyright (c) 2025 Contributors to the Eclipse Foundation
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

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::time::{sleep, Duration};

    use crate::{persistency, test_lib, USubscriptionConfiguration};

    fn get_configuration() -> USubscriptionConfiguration {
        USubscriptionConfiguration::create(
            test_lib::helpers::LOCAL_AUTHORITY.to_string(),
            None,
            None,
            false,
            None,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_get_and_prune_expiring_subscriptions() {
        let mut subscriptions = persistency::SubscriptionsStore::new(&get_configuration());

        // Prepare subscription persistency with two subscriptions, one with and one without expiry timestamp
        let expiry_in_1s = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 1000;
        let _ = subscriptions.add_subscription(
            &test_lib::helpers::subscriber_uri1(),
            &test_lib::helpers::local_topic1_uri(),
            None,
        );
        let _ = subscriptions.add_subscription(
            &test_lib::helpers::subscriber_uri2(),
            &test_lib::helpers::local_topic2_uri(),
            Some(expiry_in_1s),
        );

        // retrieve all persisted subscription relationships - should be the two we added above
        #[allow(clippy::mutable_key_type)]
        let data = subscriptions
            .get_data()
            .expect("Error interacting with subscription persistency");
        assert!(data.len() == 2);

        // get all subscriptions that have an expiration timestamp set - should be one, as added above
        let list = subscriptions
            .get_and_prune_expiring_subscriptions()
            .expect("Error interacting with subscription persistency");
        assert!(list.len() == 1);

        // wait a second, this should result in the one subscription expiration timestamp to pass
        sleep(Duration::from_millis(2000)).await;

        // now, the one timed subscription we had should be pruned by get_and_prune_expiring_subscriptions() function call, so list should be empty
        let list = subscriptions
            .get_and_prune_expiring_subscriptions()
            .expect("Error interacting with subscription persistency");
        assert!(list.is_empty());
    }
}
