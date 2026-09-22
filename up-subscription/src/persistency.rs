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

use redb::{
    Database, MultimapTableDefinition, ReadableDatabase, ReadableMultimapTable, ReadableTable,
    TableDefinition,
};
use redb_derive::{Key, Value};
#[cfg(test)]
use std::collections::HashMap;
use std::{
    path::PathBuf,
    time::{Duration, SystemTime},
};

use up_rust::{communication::SubscriptionStatus, core::usubscription::SubscriptionInfo, UUri};

use crate::{
    usubscription::{SubscriberUUri, TopicUUri},
    USubscriptionConfiguration,
};

// We are using SubscriptionInfo in this module to represent subscription-related information - although in the context of
// Persistency, the existence of a subscription record autmatically implies SubscriptionStatus::Subscribed, which means that
// the status field of SubscriptionInfo is redundant.

// Whether to include 'up:' in serialized UUris
const PERSIST_UP_SCHEMA: bool = true;

// canonical relationship storage: (subscriber, topic) -> metadata
const SUBSCRIPTIONS: TableDefinition<SubscriptionKey, SubscriptionMetadata> =
    TableDefinition::new("subscriptions");
// reverse index: topic -> subscriber (existence only, no duplicated metadata)
const TOPIC_INDEX: MultimapTableDefinition<&str, &str> =
    MultimapTableDefinition::new("subscriber_index");
// for keeping track of the special states of remote topics (SubscribePending, UnsubsribePending)
const REMOTE_TOPICS: TableDefinition<&str, u8> = TableDefinition::new("remote_topics");
// uProtocol entities that registered to directly receive subscription-related notifications
const NOTIFICATIONS: TableDefinition<&str, ()> = TableDefinition::new("notifications");

#[derive(Debug)]
pub(crate) enum PersistencyError {
    InternalError(String),
    SerializationError(String),
}

impl PersistencyError {
    pub(crate) fn serialization_error<T>(message: T) -> PersistencyError
    where
        T: Into<String>,
    {
        Self::SerializationError(message.into())
    }

    pub(crate) fn internal_error<T>(message: T) -> PersistencyError
    where
        T: Into<String>,
    {
        Self::InternalError(message.into())
    }
}

impl std::fmt::Display for PersistencyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SerializationError(e) => f.write_fmt(format_args!("Serialization error: {e}")),
            Self::InternalError(e) => f.write_fmt(format_args!("Internal error: {e}")),
        }
    }
}

impl std::error::Error for PersistencyError {}

// helper function to make redb error mapping a bit more legible
fn internal_err<E: std::fmt::Display>(e: E) -> PersistencyError {
    PersistencyError::internal_error(e.to_string())
}

#[derive(Debug, Key, Value, PartialEq, Eq, PartialOrd, Ord, Clone)]
struct SubscriptionKey {
    subscriber: String,
    topic: String,
}

#[derive(Debug, Value, Clone, Copy, PartialEq, Eq)]
struct SubscriptionMetadata {
    // Option<u128> is one of redb's built-in impls - using nanosecond precision
    // to be able to recreate timestamps exactly as they were persisted.
    expiration_nanos: Option<u128>,
    sample_period_nanos: Option<u128>,
}

impl SubscriptionMetadata {
    fn new(expiration: Option<SystemTime>, sample_period: Option<Duration>) -> Self {
        Self {
            expiration_nanos: expiration.map(|t| {
                t.duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            }),
            sample_period_nanos: sample_period.map(|d| d.as_nanos()),
        }
    }

    fn expiration(&self) -> Option<SystemTime> {
        self.expiration_nanos
            .map(|ms| SystemTime::UNIX_EPOCH + Duration::from_nanos_u128(ms))
    }

    fn sample_period(&self) -> Option<Duration> {
        self.sample_period_nanos.map(Duration::from_nanos_u128)
    }
}

/// Persistent store for tracking subscriber-topic relationships
pub(crate) struct SubscriptionsStore {
    persistency: Database,
}

// [impl->req~usubscription-subscribe-persistency~1]
impl SubscriptionsStore {
    const SUBSCRIPTION_STORE_NAME: &str = ".subscriptions.store";

    pub(crate) fn new(
        configuration: &USubscriptionConfiguration,
    ) -> Result<SubscriptionsStore, PersistencyError> {
        let persistency = get_store(
            SubscriptionsStore::SUBSCRIPTION_STORE_NAME.to_string(),
            configuration.persistency_path.clone(),
            configuration.persistency_enabled,
        );

        // open tables, iniates table creation if necessary
        let write_txn = persistency.begin_write().map_err(internal_err)?;
        write_txn.open_table(SUBSCRIPTIONS).map_err(internal_err)?;
        write_txn
            .open_multimap_table(TOPIC_INDEX)
            .map_err(internal_err)?;
        write_txn.commit().map_err(internal_err)?;

        Ok(SubscriptionsStore { persistency })
    }

    /// Adds a new topic-subscriber relationship to persistent storage
    /// * any such relationship in this store implies a subscription state of SUBSCRIBED, except for remote topics (refer to `RemoteTopicsStore`)
    ///
    /// # Arguments
    ///
    /// * `subscriber` - UUri of the topic subscriber.
    /// * `topic` - UUri of the topic that is being subscribed.
    /// * `expiration` - Optional subscription expiration time.
    /// * `sample_period` - Optional minimum duration between two events.
    ///
    /// # Returns
    ///
    /// * returns `Ok(true)` if this was the first subscriber to the topic, `Ok(false)` otherwise
    /// * returns a `PersistencyError` in case of problems with serialization of data or manipulation of persist storage
    pub(crate) fn add_subscription(
        &mut self,
        subscriber: &SubscriberUUri,
        topic: &TopicUUri,
        expiration: Option<SystemTime>,
        sample_period: Option<Duration>,
    ) -> Result<bool, PersistencyError> {
        // serialize inputs to types used in persistency
        let subscriber_string = subscriber.to_uri(PERSIST_UP_SCHEMA);
        let topic_string = topic.to_uri(PERSIST_UP_SCHEMA);
        let metadata = SubscriptionMetadata::new(expiration, sample_period);

        let write_txn = self.persistency.begin_write().map_err(internal_err)?;
        let is_first_subscriber = {
            // write reverse-index table (first, so we don't have to clone the Strings)
            let mut topic_index = write_txn
                .open_multimap_table(TOPIC_INDEX)
                .map_err(internal_err)?;
            // check if this is the first subscription to `topic``
            let is_first_subscriber = topic_index
                .get(topic_string.as_str())
                .map_err(internal_err)?
                .next()
                .is_none();
            topic_index
                .insert(topic_string.as_str(), subscriber_string.as_str())
                .map_err(internal_err)?;

            // write the primary database entry
            let key = SubscriptionKey {
                subscriber: subscriber_string,
                topic: topic_string,
            };
            write_txn
                .open_table(SUBSCRIPTIONS)
                .map_err(internal_err)?
                .insert(key, metadata)
                .map_err(internal_err)?;

            is_first_subscriber
        };
        write_txn.commit().map_err(internal_err)?;

        Ok(is_first_subscriber)
    }

    /// Removes a topic-subscriber combination from persistent storage
    /// * returns `Ok(true)` if this was the last subscriber to the topic, `Ok(false)` otherwise
    /// * returns a `PersistencyError` in case of problems with serialization of data or manipulation of persist storage
    pub(crate) fn remove_subscription(
        &mut self,
        subscriber: &SubscriberUUri,
        topic: &TopicUUri,
    ) -> Result<bool, PersistencyError> {
        // serialize inputs to types used in persistency
        let topic_string = topic.to_uri(PERSIST_UP_SCHEMA);
        let subscriber_string = subscriber.to_uri(PERSIST_UP_SCHEMA);

        let write_txn = self.persistency.begin_write().map_err(internal_err)?;
        let was_last_subscriber = {
            // write reverse-index table (first, so we don't have to clone the Strings)
            let mut topic_index = write_txn
                .open_multimap_table(TOPIC_INDEX)
                .map_err(internal_err)?;
            topic_index
                .remove(topic_string.as_str(), subscriber_string.as_str())
                .map_err(internal_err)?;
            let was_last_subscriber = topic_index
                .get(topic_string.as_str())
                .map_err(internal_err)?
                .next()
                .is_none();

            // write the primary database entry
            let key = SubscriptionKey {
                topic: topic_string,
                subscriber: subscriber_string,
            };
            write_txn
                .open_table(SUBSCRIPTIONS)
                .map_err(internal_err)?
                .remove(key)
                .map_err(internal_err)?;

            was_last_subscriber
        };
        write_txn.commit().map_err(internal_err)?;

        Ok(was_last_subscriber)
    }

    /// Returns a list of all subscribers of given topic
    /// * returns `Vec<SubscriberUUri>` that contains all subscriber UUris registered for the topic
    /// * returns a `PersistencyError` in case of problems with serialization of data or manipulation of persist storage
    pub(crate) fn get_topic_subscribers(
        &self,
        topic: &TopicUUri,
    ) -> Result<Vec<SubscriberUUri>, PersistencyError> {
        let topic_string = &topic.to_uri(PERSIST_UP_SCHEMA);
        let mut subscribers = vec![];

        let read_txn = self.persistency.begin_read().map_err(internal_err)?;
        {
            let topic_index = read_txn
                .open_multimap_table(TOPIC_INDEX)
                .map_err(internal_err)?;

            // This will get *every* client that subscribed to `topic` - no matter whether (in the case of remote subscriptions)
            // the remote topic is already fully SUBSCRIBED, or still SUBSCRIBE_PENDING
            for entry in topic_index
                .get(topic_string.as_str())
                .map_err(internal_err)?
            {
                let subscriber_string = entry.map_err(internal_err)?.value().to_string();
                subscribers.push(UUri::try_from(subscriber_string).map_err(|e| {
                    PersistencyError::serialization_error(format!(
                        "Error deserializing subscriber uri {e}"
                    ))
                })?);
            }
        };
        read_txn.close().map_err(internal_err)?;

        Ok(subscribers)
    }

    /// Returns a list of all topics subscribed to by given subscriber
    /// * returns `Vec<TopicUUri>` that contains all topics subscribed to by subscriber
    /// * returns a `PersistencyError` in case of problems with serialization of data or manipulation of persist storage
    #[allow(dead_code)] // Keeping this method intentionally, even it's not used at the moment
    pub(crate) fn get_subscriber_topics(
        &self,
        subscriber: &SubscriberUUri,
    ) -> Result<Vec<TopicUUri>, PersistencyError> {
        let subscriber_string = subscriber.to_uri(PERSIST_UP_SCHEMA);
        let mut topics: Vec<TopicUUri> = Vec::new();

        let read_txn = self.persistency.begin_read().map_err(internal_err)?;
        {
            let table = read_txn.open_table(SUBSCRIPTIONS).map_err(internal_err)?;

            // subscriber-major key ordering lets us range-scan instead of a full table scan
            let start = SubscriptionKey {
                subscriber: subscriber_string.clone(),
                topic: String::new(),
            };
            for entry in table.range(start..).map_err(internal_err)? {
                let (key, _) = entry.map_err(internal_err)?;
                let key = key.value();
                if key.subscriber != subscriber_string {
                    break; // moved past this subscriber's range
                }
                topics.push(UUri::try_from(key.topic).map_err(|e| {
                    PersistencyError::serialization_error(format!(
                        "Error deserializing topic uri {e}"
                    ))
                })?);
            }
        };
        read_txn.close().map_err(internal_err)?;

        Ok(topics)
    }

    /// Returns a list of all subscriptions stored in persistency
    /// * returns `Vec<SubscriptionInfo>` that contains all subscribers and their associated subscription topics
    /// * returns a `PersistencyError` in case of problems with serialization of data or manipulation of persist storage
    pub(crate) fn get_all_subscriptions(&self) -> Result<Vec<SubscriptionInfo>, PersistencyError> {
        let mut subscriptions: Vec<SubscriptionInfo> = Vec::new();

        let read_txn = self.persistency.begin_read().map_err(internal_err)?;
        {
            let table = read_txn.open_table(SUBSCRIPTIONS).map_err(internal_err)?;

            for entry in table.iter().map_err(internal_err)? {
                let (key, value) = entry.map_err(internal_err)?;
                let key = key.value();
                let metadata = value.value();

                let subscriber = UUri::try_from(key.subscriber).map_err(|e| {
                    PersistencyError::serialization_error(format!(
                        "Error deserializing subscriber uri {e}"
                    ))
                })?;
                let topic = UUri::try_from(key.topic).map_err(|e| {
                    PersistencyError::serialization_error(format!(
                        "Error deserializing topic uri {e}"
                    ))
                })?;

                subscriptions.push(SubscriptionInfo::new(
                    topic,
                    subscriber,
                    SubscriptionStatus::Subscribed,
                    metadata.expiration(),
                    metadata.sample_period(),
                ));
            }
        };
        read_txn.close().map_err(internal_err)?;

        Ok(subscriptions)
    }

    /// Returns all subscriptions whose topic and subscriber match the given filters
    /// * `topic_filter`/`subscriber_filter` may be UUri patterns (containing wildcards), see [`UUri::matches`]
    /// * a `None` filter matches every topic/subscriber
    /// * returns `Vec<SubscriptionInfo>` for all matching subscriptions
    /// * status is always reported as `Subscribed`, as this store only tracks local subscriptions - callers
    ///   needing remote topic state must reconcile against `RemoteTopicsStore` themselves
    /// * returns a `PersistencyError` in case of problems with serialization of data or manipulation of persist storage
    pub(crate) fn get_subscriptions_by_filter(
        &self,
        topic_filter: &Option<UUri>,
        subscriber_filter: &Option<UUri>,
    ) -> Result<Vec<SubscriptionInfo>, PersistencyError> {
        let mut subcriptions = self.get_all_subscriptions()?;

        // a `None` filter matches everything
        subcriptions.retain(|sub| {
            topic_filter.as_ref().is_none_or(|f| f.matches(sub.topic()))
                && subscriber_filter
                    .as_ref()
                    .is_none_or(|f| f.matches(sub.subscriber()))
        });

        Ok(subcriptions)
    }

    /// This function does two things
    /// - remove any subscription relationships from persistency that have an expiration timestamp that lies in the past
    /// - return all remaining subscription relationships which have an expiration timestamp that has not yet expired
    // [impl->req~usubscription-subscribe-no-expiration~1]
    pub(crate) fn get_and_prune_expiring_subscriptions(
        &mut self,
    ) -> Result<Vec<(SubscriberUUri, TopicUUri, SystemTime)>, PersistencyError> {
        let now = SystemTime::now();
        let mut remaining = Vec::new();

        let write_txn = self.persistency.begin_write().map_err(internal_err)?;
        {
            let mut table = write_txn.open_table(SUBSCRIPTIONS).map_err(internal_err)?;
            let mut topic_index = write_txn
                .open_multimap_table(TOPIC_INDEX)
                .map_err(internal_err)?;

            // snapshot entries first: `table` can't be mutated while its iterator borrows it
            let entries: Vec<(SubscriptionKey, SubscriptionMetadata)> = table
                .iter()
                .map_err(internal_err)?
                .map(|entry| entry.map(|(k, v)| (k.value(), v.value())))
                .collect::<Result<_, _>>()
                .map_err(internal_err)?;

            for (key, metadata) in entries {
                let Some(expiration) = metadata.expiration() else {
                    continue;
                };

                if expiration <= now {
                    topic_index
                        .remove(key.topic.as_str(), key.subscriber.as_str())
                        .map_err(internal_err)?;
                    // remove expired subscriptions from persistency
                    table.remove(key).map_err(internal_err)?;
                } else {
                    let subscriber = UUri::try_from(key.subscriber).map_err(|e| {
                        PersistencyError::serialization_error(format!(
                            "Error deserializing subscriber uri {e}"
                        ))
                    })?;
                    let topic = UUri::try_from(key.topic).map_err(|e| {
                        PersistencyError::serialization_error(format!(
                            "Error deserializing topic uri {e}"
                        ))
                    })?;
                    // prepare to return any remaining ones
                    remaining.push((subscriber, topic, expiration));
                }
            }
        }
        write_txn.commit().map_err(internal_err)?;

        // return remaining subscription entries (all entries with expiration timestamp in the future)
        Ok(remaining)
    }

    /// Clears the subscription database
    // [impl->req~usubscription-reset~1]
    pub(crate) fn reset(&mut self) -> Result<(), PersistencyError> {
        // dropping and recreating the tables is cheaper than deleting entry-by-entry
        let write_txn = self.persistency.begin_write().map_err(internal_err)?;

        write_txn
            .delete_table(SUBSCRIPTIONS)
            .map_err(internal_err)?;
        write_txn
            .delete_multimap_table(TOPIC_INDEX)
            .map_err(internal_err)?;
        // re-create tables
        write_txn.open_table(SUBSCRIPTIONS).map_err(internal_err)?;
        write_txn
            .open_multimap_table(TOPIC_INDEX)
            .map_err(internal_err)?;

        write_txn.commit().map_err(internal_err)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn set_data(&mut self, data: Vec<SubscriptionInfo>) {
        self.reset().expect("expecting database reset to work");
        for entry in data {
            self.add_subscription(
                entry.subscriber(),
                entry.topic(),
                *entry.expiration(),
                *entry.min_sample_period(),
            )
            .expect("expecting adding test data to work");
        }
    }
}

/// Persistent store for tracking remote topic status
pub(crate) struct RemoteTopicsStore {
    persistency: Database,
}

impl RemoteTopicsStore {
    const PERSIST_UP_SCHEMA: bool = true;
    const REMOTE_TOPICS_STORE_NAME: &str = ".remote_topics.store";

    pub(crate) fn new(
        configuration: &USubscriptionConfiguration,
    ) -> Result<RemoteTopicsStore, PersistencyError> {
        let persistency = get_store(
            RemoteTopicsStore::REMOTE_TOPICS_STORE_NAME.to_string(),
            configuration.persistency_path.clone(),
            configuration.persistency_enabled,
        );

        // open table, iniates table creation if necessary
        let write_txn = persistency.begin_write().map_err(internal_err)?;
        write_txn.open_table(REMOTE_TOPICS).map_err(internal_err)?;
        write_txn.commit().map_err(internal_err)?;

        Ok(RemoteTopicsStore { persistency })
    }

    /// Returns subscription state of topic
    /// * returns `Ok(Some(TopicState))` (with current TopicState value) if topic exists in store, otherwise returns `Ok(None)`
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn get_topic_state(
        &self,
        topic: &TopicUUri,
    ) -> Result<Option<SubscriptionStatus>, PersistencyError> {
        let topic_string = topic.to_uri(Self::PERSIST_UP_SCHEMA);

        let read_txn = self.persistency.begin_read().map_err(internal_err)?;
        let state = {
            let table = read_txn.open_table(REMOTE_TOPICS).map_err(internal_err)?;
            table
                .get(topic_string.as_str())
                .map_err(internal_err)?
                .map(|v| v.value())
        };
        read_txn.close().map_err(internal_err)?;

        state
            .map(|bytes| {
                deserialize_topic_status(bytes).map_err(|e| {
                    PersistencyError::serialization_error(format!(
                        "Error deserializing topic state {e}"
                    ))
                })
            })
            .transpose()
    }

    /// Updates subscription state of topic in remote-topics store
    /// * returns `Ok(TopicState)` (with updated TopicState value) if state update is successful
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn set_topic_state(
        &mut self,
        topic: &TopicUUri,
        state: SubscriptionStatus,
    ) -> Result<SubscriptionStatus, PersistencyError> {
        let topic_string = topic.to_uri(Self::PERSIST_UP_SCHEMA);

        let write_txn = self.persistency.begin_write().map_err(internal_err)?;
        {
            let mut table = write_txn.open_table(REMOTE_TOPICS).map_err(internal_err)?;
            table
                .insert(topic_string.as_str(), serialize_topic_status(&state))
                .map_err(internal_err)?;
        }
        write_txn.commit().map_err(internal_err)?;

        Ok(state)
    }

    /// Returns subscription state of remote topic, or adds new remote-topic with state TopicState::SUBSCRIBE_PENDING if topic is new
    /// * returns `Ok(TopicState)` (where TopicState is the new topic state)
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn add_topic_or_get_status(
        &mut self,
        topic: &TopicUUri,
    ) -> Result<SubscriptionStatus, PersistencyError> {
        let topic_string = topic.to_uri(Self::PERSIST_UP_SCHEMA);

        let write_txn = self.persistency.begin_write().map_err(internal_err)?;
        let state = {
            let mut table = write_txn.open_table(REMOTE_TOPICS).map_err(internal_err)?;
            let existing = table
                .get(topic_string.as_str())
                .map_err(internal_err)?
                .map(|v| v.value());

            match existing {
                Some(bytes) => deserialize_topic_status(bytes).map_err(|e| {
                    PersistencyError::serialization_error(format!(
                        "Error deserializing topic state {e}"
                    ))
                })?,
                None => {
                    // [impl->req~usubscription-subscribe-remote-pending~1]
                    let pending = SubscriptionStatus::SubscribePending;
                    table
                        .insert(topic_string.as_str(), serialize_topic_status(&pending))
                        .map_err(internal_err)?;
                    pending
                }
            }
        };
        write_txn.commit().map_err(internal_err)?;

        Ok(state)
    }

    /// Clears the remote subscriptions database
    // [impl->req~usubscription-reset~1]
    pub(crate) fn reset(&mut self) -> Result<(), PersistencyError> {
        // dropping and recreating the table is cheaper than deleting entry-by-entry
        let write_txn = self.persistency.begin_write().map_err(internal_err)?;

        write_txn
            .delete_table(REMOTE_TOPICS)
            .map_err(internal_err)?;
        // re-create table
        write_txn.open_table(REMOTE_TOPICS).map_err(internal_err)?;

        write_txn.commit().map_err(internal_err)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn get_data(
        &self,
    ) -> Result<HashMap<TopicUUri, SubscriptionStatus>, Box<dyn std::error::Error>> {
        let mut map: HashMap<TopicUUri, SubscriptionStatus> = HashMap::new();

        let read_txn = self.persistency.begin_read()?;
        {
            let table = read_txn.open_table(REMOTE_TOPICS)?;
            for entry in table.iter()? {
                let (key, value) = entry?;
                let topic = UUri::try_from(key.value().to_string())?;
                let state = deserialize_topic_status(value.value())?;
                map.insert(topic, state);
            }
        }
        read_txn.close()?;

        Ok(map)
    }

    #[cfg(test)]
    pub(crate) fn set_data(&mut self, map: HashMap<TopicUUri, SubscriptionStatus>) {
        self.reset().expect("expecting database reset to work");

        for entry in map {
            self.set_topic_state(&entry.0, entry.1)
                .expect("expecting adding test data to work");
        }
    }
}

pub(crate) struct NotificationStore {
    persistency: Database,
}

impl NotificationStore {
    const PERSIST_UP_SCHEMA: bool = true;
    const NOTIFICATION_STORE_NAME: &str = ".notification.store";

    pub(crate) fn new(
        configuration: &USubscriptionConfiguration,
    ) -> Result<NotificationStore, PersistencyError> {
        let persistency = get_store(
            NotificationStore::NOTIFICATION_STORE_NAME.to_string(),
            configuration.persistency_path.clone(),
            configuration.persistency_enabled,
        );

        // open table, iniates table creation if necessary
        let write_txn = persistency.begin_write().map_err(internal_err)?;
        write_txn.open_table(NOTIFICATIONS).map_err(internal_err)?;
        write_txn.commit().map_err(internal_err)?;

        Ok(NotificationStore { persistency })
    }

    /// Adds client to direct-notification registry
    /// * return `Ok(())` on success
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn add_notifyee(&mut self, client: &UUri) -> Result<(), PersistencyError> {
        let client_string = client.to_uri(Self::PERSIST_UP_SCHEMA);

        let write_txn = self.persistency.begin_write().map_err(internal_err)?;
        {
            let mut table = write_txn.open_table(NOTIFICATIONS).map_err(internal_err)?;
            table
                .insert(client_string.as_str(), ())
                .map_err(internal_err)?;
        }
        write_txn.commit().map_err(internal_err)?;

        Ok(())
    }

    /// Removes client from direct-notification registry
    /// * return `Ok(())` on success
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn remove_notifyee(&mut self, client: &UUri) -> Result<(), PersistencyError> {
        let client_string = client.to_uri(Self::PERSIST_UP_SCHEMA);

        let write_txn = self.persistency.begin_write().map_err(internal_err)?;
        {
            let mut table = write_txn.open_table(NOTIFICATIONS).map_err(internal_err)?;
            table.remove(client_string.as_str()).map_err(internal_err)?;
        }
        write_txn.commit().map_err(internal_err)?;

        Ok(())
    }

    /// Returns a list of all clients that have registered to be notified of subscription state changes
    ///
    /// # Returns
    ///
    /// * return a `Vec<UUri>` list of client UUris that want to be notified about susbcription changes
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn get_registered_notifyees(&mut self) -> Result<Vec<UUri>, PersistencyError> {
        let mut result = vec![];

        let read_txn = self.persistency.begin_read().map_err(internal_err)?;
        {
            let table = read_txn.open_table(NOTIFICATIONS).map_err(internal_err)?;
            for entry in table.iter().map_err(internal_err)? {
                result.push(
                    UUri::try_from(entry.map_err(internal_err)?.0.value()).map_err(|e| {
                        PersistencyError::serialization_error(format!(
                            "Error deserializing subscriber uri {e}"
                        ))
                    })?,
                );
            }
        };
        read_txn.close().map_err(internal_err)?;

        Ok(result)
    }

    /// Clears the notifications database
    // [impl->req~usubscription-reset~1]
    pub(crate) fn reset(&mut self) -> Result<(), PersistencyError> {
        // dropping and recreating the table is cheaper than deleting entry-by-entry
        let write_txn = self.persistency.begin_write().map_err(internal_err)?;

        write_txn
            .delete_table(NOTIFICATIONS)
            .map_err(internal_err)?;
        // re-create table
        write_txn.open_table(NOTIFICATIONS).map_err(internal_err)?;

        write_txn.commit().map_err(internal_err)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn set_data(&mut self, list: Vec<SubscriberUUri>) {
        self.reset().expect("expecting database reset to work");

        for entry in list {
            self.add_notifyee(&entry)
                .expect("expecting adding test data to work");
        }
    }
}

fn serialize_topic_status(state: &SubscriptionStatus) -> u8 {
    match state {
        SubscriptionStatus::Unsubscribed => 0,
        SubscriptionStatus::SubscribePending => 1,
        SubscriptionStatus::Subscribed => 2,
        SubscriptionStatus::UnsubscribePending => 3,
    }
}

fn deserialize_topic_status(value: u8) -> Result<SubscriptionStatus, PersistencyError> {
    match value {
        0 => Ok(SubscriptionStatus::Unsubscribed),
        1 => Ok(SubscriptionStatus::SubscribePending),
        2 => Ok(SubscriptionStatus::Subscribed),
        3 => Ok(SubscriptionStatus::UnsubscribePending),
        _ => Err(PersistencyError::serialization_error(
            "invalid SubscriptionStatus value",
        )),
    }
}

// Return a persistent storage entity, configured according to a USubscriptionConfiguration
fn get_store(name: String, path: PathBuf, persistency_enabled: bool) -> Database {
    let path = validate_and_append_filename(&path, &name)
        .unwrap_or_else(|e| panic!("Problem with persistency, invalid storage file name: {e}"));

    if persistency_enabled {
        Database::create(&path).expect("failed to open redb database")
    } else {
        // in-memory backend to disable disk persistence
        Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .expect("failed to create in-memory database")
    }
}

// Check whether a filename contains any relative/path traversal characters, combine with directory if all is well
fn validate_and_append_filename(dir: &PathBuf, filename: &str) -> Result<PathBuf, &'static str> {
    // Check if filename contains any path separators or special directory components
    if filename.contains('/')
        || filename.contains('\\')
        || filename == "."
        || filename == ".."
        || filename.is_empty()
    {
        return Err("filename contains path components");
    }

    let mut full_path = dir.clone();
    full_path.push(filename);

    // Additional safety check - verify the resulting path is actually under the original directory
    if !full_path.starts_with(dir) {
        return Err("path traversal attempt detected");
    }

    Ok(full_path)
}

#[cfg(test)]
mod tests {
    // These are tests just for locally used helper functions. More complex and complete tests of the persistency
    // manager business logic are located in tests/persistency_tests.rs
    use super::*;
    use test_case::test_case;

    #[test_case(SubscriptionStatus::Unsubscribed; "State UNSUBSCRIBED")]
    #[test_case(SubscriptionStatus::SubscribePending; "State SUBSCRIBE_PENDING")]
    #[test_case(SubscriptionStatus::Subscribed; "State SUBSCRIBED")]
    #[test_case(SubscriptionStatus::UnsubscribePending; "State UNSUBSCRIBE_PENDING")]
    #[test_log::test(tokio::test)]
    async fn test_serialize_deserialize_topic_state(state: SubscriptionStatus) {
        // One way...
        let serialized_bytes = serialize_topic_status(&state);

        // ... then the other
        let reconstructed_state = deserialize_topic_status(serialized_bytes);
        assert!(reconstructed_state.is_ok());

        let reconstructed_state = reconstructed_state.unwrap();
        assert_eq!(reconstructed_state, state);
    }

    #[test_log::test(tokio::test)]
    async fn test_validate_and_append_filename_success() {
        let mut expected = PathBuf::from(".");
        expected.push("newfile");

        let r = validate_and_append_filename(&PathBuf::from("."), "newfile");
        assert!(r.is_ok());
        assert_eq!(r.unwrap().as_os_str(), expected.as_os_str());
    }

    #[test_case("."; "Relative path 1")]
    #[test_case(".."; "Relative path 2")]
    #[test_case( "\\"; "Relative path 3")]
    #[test_case( ""; "Empty filename")]
    #[test_case( "/dummy/newfile"; "Random path and name")]
    #[test_log::test(tokio::test)]
    async fn test_validate_and_append_filename_relative_paths(file: &str) {
        let r = validate_and_append_filename(&PathBuf::from("."), file);
        assert!(r.is_err());
    }
}
