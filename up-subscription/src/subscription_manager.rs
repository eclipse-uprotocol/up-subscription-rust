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

use log::*;
#[cfg(test)]
use std::collections::HashMap;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, mpsc::Receiver, mpsc::Sender, oneshot, Notify},
    time::sleep,
};

use up_rust::{
    communication::{CallOptions, InMemoryRpcClient, RpcClient},
    core::usubscription::{
        State as TopicState, SubscriptionRequest, SubscriptionResponse, SubscriptionStatus,
        UnsubscribeRequest, RESOURCE_ID_SUBSCRIBE, RESOURCE_ID_UNSUBSCRIBE, USUBSCRIPTION_TYPE_ID,
        USUBSCRIPTION_VERSION_MAJOR,
    },
    LocalUriProvider, UCode, UPriority, UStatus, UTransport, UUri,
};

use crate::{
    helpers, notification_manager,
    notification_manager::NotificationEvent,
    persistency,
    usubscription::{ExpiryTimestamp, SubscriberUUri, TopicUUri, UP_REMOTE_TTL},
    USubscriptionConfiguration,
};

// This is the core business logic for handling and tracking subscriptions. It is currently implemented as a single event-consuming
// function `handle_message()`, which is supposed to be spawned into a task and process the various `Events` that it can receive
// via tokio mpsc channel. This design allows to forgo the use of any synhronization primitives on the subscription-tracking container
// data types, as any access is coordinated/serialized via the Event selection loop.

// Maximum number of `Subscriber` entries to be returned in a `FetchSusbcriptions´ operation.
const UP_MAX_FETCH_SUBSCRIBERS_LEN: usize = 100;
// Maximum number of `Subscriber` entries to be returned in a `FetchSusbcriptions´ operation.
const UP_MAX_FETCH_SUBSCRIPTIONS_LEN: usize = 100;

// Queue size of message channel for internal commands - like subscription status change messages or subscription expiration commands.
const INTERNAL_COMMAND_BUFFER_SIZE: usize = 128;

// Timeout to use when sending subscription removal command after a subscription has expired; if exceeded, subscription won't be removed
// directly but will be cleaned up at next startup.
const SUBSCRIPTION_EXPIRY_REMOVAL_TIMEOUT_SECONDS: u64 = 5;

#[derive(Debug)]
pub(crate) enum RequestKind {
    Subscriber(SubscriberUUri),
    Topic(TopicUUri),
}

#[derive(Debug)]
pub(crate) struct SubscriptionEntry {
    pub(crate) topic: TopicUUri,
    pub(crate) subscriber: SubscriberUUri,
    pub(crate) status: SubscriptionStatus,
}

pub(crate) type SubscribersResponse = (Vec<SubscriberUUri>, bool); // List of subscribers, boolean flag stating if there exist more entries than contained in list
pub(crate) type SubscriptionsResponse = (Vec<SubscriptionEntry>, bool); // List of subscriber entries, boolean flag stating if there exist more entries than contained in list

// This is the 'outside API' of subscription manager, it includes some events that are only to be used in (and only enabled for) testing.
#[derive(Debug)]
pub(crate) enum SubscriptionEvent {
    AddSubscription {
        subscriber: SubscriberUUri,
        topic: TopicUUri,
        expiry: Option<ExpiryTimestamp>,
        respond_to: oneshot::Sender<SubscriptionStatus>,
    },
    RemoveSubscription {
        subscriber: SubscriberUUri,
        topic: TopicUUri,
        respond_to: oneshot::Sender<SubscriptionStatus>,
    },
    FetchSubscribers {
        topic: TopicUUri,
        offset: Option<u32>,
        respond_to: oneshot::Sender<SubscribersResponse>, // return list of subscribers and flag indicating whether there are more
    },
    FetchSubscriptions {
        request: RequestKind,
        offset: Option<u32>,
        respond_to: oneshot::Sender<SubscriptionsResponse>, // return list of Subscriptions and flag indicating whether there are more
    },
    // Purely for use during testing: get copy of current topic-subscriper ledger
    #[cfg(test)]
    GetTopicSubscribers {
        respond_to: oneshot::Sender<persistency::SubscriptionSet>,
    },
    // Purely for use during testing: force-set new topic-subscriber ledger
    #[cfg(test)]
    SetTopicSubscribers {
        topic_subscribers_replacement: persistency::SubscriptionSet,
        respond_to: oneshot::Sender<()>,
    },
    // Purely for use during testing: get copy of current topic-subscriper ledger
    #[cfg(test)]
    GetRemoteTopics {
        respond_to: oneshot::Sender<HashMap<TopicUUri, TopicState>>,
    },
    // Purely for use during testing: force-set new topic-subscriber ledger
    #[cfg(test)]
    SetRemoteTopics {
        topic_subscribers_replacement: HashMap<TopicUUri, TopicState>,
        respond_to: oneshot::Sender<()>,
    },
    // Purely for use during testing: get internal remote-subscription-change command sender
    #[cfg(test)]
    GetRemoteSubscriptionChangeSender {
        respond_to: oneshot::Sender<Sender<InternalSubscriptionEvent>>,
    },
}

// Internal subscription manager API - used to update on remote subscriptions (deal with _PENDING states)
#[derive(Debug)]
pub(crate) enum InternalSubscriptionEvent {
    TopicStateUpdate {
        topic: TopicUUri,
        state: TopicState,
    },
    RemoveExpiredSubscription {
        subscriber: SubscriberUUri,
        topic: TopicUUri,
    },
}

// Wrapper type, include all kinds of actions subscription manager knows
enum Event {
    LocalSubscription(SubscriptionEvent),
    RemoteSubscription(InternalSubscriptionEvent),
}

// Core business logic of subscription management - includes container data types for tracking subscriptions and remote subscriptions.
// Interfacing with this purely works via channels, so we do not have to deal with mutexes and similar concepts.
pub(crate) async fn handle_message(
    configuration: Arc<USubscriptionConfiguration>,
    transport: Arc<dyn UTransport>,
    mut command_receiver: Receiver<SubscriptionEvent>,
    notification_sender: Sender<NotificationEvent>,
    shutdown: Arc<Notify>,
) {
    helpers::init_once();

    // track subscribers for topics - if you're in this list, you have SUBSCRIBED, otherwise you're considered UNSUBSCRIBED
    // [impl->req~usubscription-subscribe-persistency~1]
    let mut subscriptions = persistency::SubscriptionsStore::new(&configuration);

    // for remote topics, we need to additionally deal with _PENDING states, this tracks states of these topics
    let mut remote_topics = persistency::RemoteTopicsStore::new(&configuration);

    let (internal_cmd_sender, mut internal_cmd_receiver) =
        mpsc::channel::<InternalSubscriptionEvent>(INTERNAL_COMMAND_BUFFER_SIZE);

    // At startup, set up timed unsubscribe for any persisted subscriptions that define an expiration timestamp
    match subscriptions.get_and_prune_expiring_subscriptions() {
        Ok(list) => {
            for (subscriber, topic, expiry_millis) in list {
                schedule_unsubscribe(
                    expiry_millis,
                    subscriber.clone(),
                    topic.clone(),
                    internal_cmd_sender.clone(),
                );
            }
        }
        Err(e) => {
            panic!("Persistency failure {e}")
        }
    };

    loop {
        let event: Event = tokio::select! {
            // "Outside" events - actions that need to be performed
            event = command_receiver.recv() => match event {
                None => {
                    error!("Problem with subscription command channel, received None-event");
                    break
                },
                Some(event) => Event::LocalSubscription(event),
            },
            // "Inside" events - updates around remote subscription states
            event = internal_cmd_receiver.recv() => match event {
                None => {
                    error!("Problem with subscription command channel, received None-event");
                    break
                },
                Some(event) => Event::RemoteSubscription(event),
            },
            _ = shutdown.notified() => break,
        };
        match event {
            // These all deal with user-driven interactions (the core usubscription interface functionality)
            Event::LocalSubscription(event) => match event {
                SubscriptionEvent::AddSubscription {
                    subscriber,
                    topic,
                    expiry,
                    respond_to,
                } => {
                    match add_subscription(
                        configuration.clone(),
                        transport.clone(),
                        internal_cmd_sender.clone(),
                        &mut subscriptions,
                        &mut remote_topics,
                        subscriber.clone(),
                        topic.clone(),
                        expiry,
                    ) {
                        Ok(result) => {
                            // Send topic state change notification
                            notification_manager::notify(
                                notification_sender.clone(),
                                Some(subscriber.clone()),
                                topic.clone(),
                                result.clone(),
                            )
                            .await;

                            if respond_to.send(result).is_err() {
                                error!("Problem with internal communication");
                            }
                        }
                        Err(e) => {
                            panic!("Persistency failure {e}")
                        }
                    };
                }
                SubscriptionEvent::RemoveSubscription {
                    subscriber,
                    topic,
                    respond_to,
                } => {
                    match remove_subscription(
                        configuration.clone(),
                        transport.clone(),
                        internal_cmd_sender.clone(),
                        &mut subscriptions,
                        &mut remote_topics,
                        subscriber.clone(),
                        topic.clone(),
                    ) {
                        Ok(result) => {
                            // Send topic state change notification
                            notification_manager::notify(
                                notification_sender.clone(),
                                Some(subscriber),
                                topic,
                                result.clone(),
                            )
                            .await;

                            if respond_to.send(result).is_err() {
                                error!("Problem with internal communication");
                            }
                        }
                        Err(e) => {
                            panic!("Persistency failure {e}")
                        }
                    }
                }
                SubscriptionEvent::FetchSubscribers {
                    topic,
                    offset,
                    respond_to,
                } => match fetch_subscribers(&subscriptions, topic, offset) {
                    Ok(result) => {
                        if respond_to.send(result).is_err() {
                            error!("Problem with internal communication");
                        }
                    }
                    Err(e) => {
                        panic!("Persistency failure {e}")
                    }
                },
                SubscriptionEvent::FetchSubscriptions {
                    request,
                    offset,
                    respond_to,
                } => match fetch_subscriptions(&subscriptions, &remote_topics, request, offset) {
                    Ok(result) => {
                        if respond_to.send(result).is_err() {
                            error!("Problem with internal communication");
                        };
                    }
                    Err(e) => {
                        panic!("Persistency failure {e}")
                    }
                },
                #[cfg(test)]
                SubscriptionEvent::GetTopicSubscribers { respond_to } => {
                    match subscriptions.get_data() {
                        Ok(result) => {
                            let _r = respond_to.send(result);
                        }
                        Err(e) => {
                            panic!("Persistency failure {e}")
                        }
                    }
                }
                #[cfg(test)]
                SubscriptionEvent::SetTopicSubscribers {
                    topic_subscribers_replacement,
                    respond_to,
                } => match subscriptions.set_data(topic_subscribers_replacement) {
                    Ok(_) => {
                        let _r = respond_to.send(());
                    }
                    Err(e) => {
                        panic!("Persistency failure {e}")
                    }
                },
                #[cfg(test)]
                SubscriptionEvent::GetRemoteTopics { respond_to } => {
                    match remote_topics.get_data() {
                        Ok(result) => {
                            let _r = respond_to.send(result);
                        }
                        Err(e) => {
                            panic!("Persistency failure {e}")
                        }
                    }
                }
                #[cfg(test)]
                SubscriptionEvent::SetRemoteTopics {
                    topic_subscribers_replacement: remote_topics_replacement,
                    respond_to,
                } => match remote_topics.set_data(remote_topics_replacement) {
                    Ok(_) => {
                        let _r = respond_to.send(());
                    }
                    Err(e) => {
                        panic!("Persistency failure {e}")
                    }
                },
                #[cfg(test)]
                SubscriptionEvent::GetRemoteSubscriptionChangeSender { respond_to } => {
                    let _ = respond_to.send(internal_cmd_sender.clone());
                }
            },
            // deal with feedback/state updates from the remote subscription handlers
            Event::RemoteSubscription(event) => match event {
                InternalSubscriptionEvent::TopicStateUpdate { topic, state } => {
                    match remote_topics.set_topic_state(&topic, state) {
                        Ok(_) => {
                            // Set up timed unsubscription here, in case state changes to ::PENDING

                            // Send topic state change notification - in the case of remote subscriptions,
                            // the subscriber is usubscription service itself, so leave that field empty.
                            notification_manager::notify(
                                notification_sender.clone(),
                                None,
                                topic,
                                SubscriptionStatus {
                                    state: state.into(),
                                    ..Default::default()
                                },
                            )
                            .await;
                        }
                        Err(e) => {
                            panic!("Persistency failure {e}");
                        }
                    }
                }
                // Remote expired subscriptions, via internal command channel
                InternalSubscriptionEvent::RemoveExpiredSubscription { subscriber, topic } => {
                    match remove_subscription(
                        configuration.clone(),
                        transport.clone(),
                        internal_cmd_sender.clone(),
                        &mut subscriptions,
                        &mut remote_topics,
                        subscriber.clone(),
                        topic.clone(),
                    ) {
                        Ok(result) => {
                            // Send topic state change notification
                            notification_manager::notify(
                                notification_sender.clone(),
                                Some(subscriber),
                                topic,
                                result.clone(),
                            )
                            .await;
                        }
                        Err(e) => {
                            panic!("Persistency failure {e}")
                        }
                    }
                }
            },
        }
    }
}

// Add a subscription relationship to bookkeeping, initiate remote subscribe request if neccessary
#[allow(clippy::too_many_arguments)]
fn add_subscription(
    uri_provider: Arc<dyn LocalUriProvider>,
    transport: Arc<dyn UTransport>,
    internal_cmd_sender: Sender<InternalSubscriptionEvent>,
    topic_subscribers: &mut persistency::SubscriptionsStore,
    remote_topics: &mut persistency::RemoteTopicsStore,
    subscriber: SubscriberUUri,
    topic: TopicUUri,
    expiry: Option<ExpiryTimestamp>,
) -> Result<SubscriptionStatus, persistency::PersistencyError> {
    let _ = topic_subscribers.add_subscription(&subscriber, &topic, expiry)?;

    // For REMOTE topics, we explicitly track state due to _PENDING scenarios
    let state = if topic.is_remote_authority(&uri_provider.get_authority()) {
        let state = remote_topics.add_topic_or_get_state(&topic)?;

        // if this remote topic is not yet SUBSCRIBED, perform remote subscription
        if state != TopicState::SUBSCRIBED {
            let topic_clone = topic.clone();
            let internal_cmd_sender_clone = internal_cmd_sender.clone();
            helpers::spawn_and_log_error(async move {
                remote_subscribe(
                    topic_clone,
                    uri_provider,
                    transport,
                    internal_cmd_sender_clone,
                )
                .await?;
                Ok(())
            });
        }
        state
    } else {
        // Otherwise, LOCAL topics are considered to have status SUBSCRIBED as soon as they are registered here
        TopicState::SUBSCRIBED
    };

    // Set up timed unsubscribe in case expiration timestamp is set
    if let Some(expiry_millis) = expiry {
        schedule_unsubscribe(
            expiry_millis,
            subscriber.clone(),
            topic,
            internal_cmd_sender,
        );
    };

    Ok(SubscriptionStatus {
        state: state.into(),
        ..Default::default()
    })
}

// Remove a subscription relationship to bookkeeping, initiate remote unsubscribe request if neccessary
fn remove_subscription(
    uri_provider: Arc<dyn LocalUriProvider>,
    transport: Arc<dyn UTransport>,
    internal_cmd_sender: Sender<InternalSubscriptionEvent>,
    topic_subscribers: &mut persistency::SubscriptionsStore,
    remote_topics: &mut persistency::RemoteTopicsStore,
    subscriber: SubscriberUUri,
    topic: TopicUUri,
) -> Result<SubscriptionStatus, persistency::PersistencyError> {
    // if this was the last subscriber to topic and topic is remote
    if topic_subscribers.remove_subscription(&subscriber, &topic)?
        && topic.is_remote_authority(&uri_provider.get_authority())
    {
        // set remote topic state tracker to UNSUBSCRIBE_PENDING (until remote ubsubscribe confirmed)
        let _r = remote_topics.set_topic_state(&topic, TopicState::UNSUBSCRIBE_PENDING)?;

        // perform remote unsubscription
        helpers::spawn_and_log_error(async move {
            remote_unsubscribe(topic, uri_provider, transport, internal_cmd_sender).await?;
            Ok(())
        });
    }

    Ok(SubscriptionStatus {
        // Whatever happens with the remote topic state - as far as the local client is concerned, it has now UNSUBSCRIBED from this topic
        state: TopicState::UNSUBSCRIBED.into(),
        ..Default::default()
    })
}

// Fetch all subscribers of a topic
fn fetch_subscribers(
    topic_subscribers: &persistency::SubscriptionsStore,
    topic: TopicUUri,
    offset: Option<u32>,
) -> Result<SubscribersResponse, persistency::PersistencyError> {
    // This will get *every* client that subscribed to `topic` - no matter whether (in the case of remote subscriptions)
    // the remote topic is already fully SUBSCRIBED, of still SUSBCRIBED_PENDING
    let mut subscribers = topic_subscribers.get_topic_subscribers(&topic)?;

    if let Some(offset) = offset {
        subscribers.drain(..offset as usize);
    }

    // split up result list, to make sense of has_more_records field
    let has_more = if subscribers.len() > UP_MAX_FETCH_SUBSCRIBERS_LEN {
        subscribers.truncate(UP_MAX_FETCH_SUBSCRIBERS_LEN);
        true
    } else {
        false
    };

    Ok((subscribers, has_more))
}

// Fetch all subscriptions of a topic or subscribers
fn fetch_subscriptions(
    topic_subscribers: &persistency::SubscriptionsStore,
    remote_topics: &persistency::RemoteTopicsStore,
    request: RequestKind,
    offset: Option<u32>,
) -> Result<SubscriptionsResponse, persistency::PersistencyError> {
    let mut results: Vec<SubscriptionEntry> = match request {
        RequestKind::Subscriber(subscriber) => topic_subscribers
            .get_subscriber_topics(&subscriber)?
            .iter()
            .map(|topic| SubscriptionEntry {
                topic: topic.clone(),
                subscriber: subscriber.clone(),
                status: SubscriptionStatus {
                    state: remote_topics
                        .get_topic_state(topic)
                        .unwrap_or(Some(TopicState::SUBSCRIBED))
                        .unwrap_or(TopicState::SUBSCRIBED)
                        .into(),
                    ..Default::default()
                },
            })
            .collect(),

        RequestKind::Topic(topic) => topic_subscribers
            .get_topic_subscribers(&topic)?
            .iter()
            .map(|subscriber| SubscriptionEntry {
                topic: topic.clone(),
                subscriber: subscriber.clone(),
                status: SubscriptionStatus {
                    state: remote_topics
                        .get_topic_state(&topic)
                        .unwrap_or(Some(TopicState::SUBSCRIBED))
                        .unwrap_or(TopicState::SUBSCRIBED)
                        .into(),
                    ..Default::default()
                },
            })
            .collect(),
    };

    if let Some(offset) = offset {
        results.drain(..offset as usize);
    }

    // split up result list, to make sense of has_more_records field
    let mut has_more = false;
    if results.len() > UP_MAX_FETCH_SUBSCRIPTIONS_LEN {
        results.truncate(UP_MAX_FETCH_SUBSCRIPTIONS_LEN);
        has_more = true;
    }

    Ok((results, has_more))
}

// Perform remote topic subscription
async fn remote_subscribe(
    topic: TopicUUri,
    uri_provider: Arc<dyn LocalUriProvider>,
    transport: Arc<dyn UTransport>,
    internal_cmd_sender: Sender<InternalSubscriptionEvent>,
) -> Result<(), UStatus> {
    let rpc_client: Arc<dyn RpcClient> = Arc::new(
        InMemoryRpcClient::new(transport, uri_provider)
            .await
            .map_err(|e| UStatus::fail_with_code(UCode::INTERNAL, e.to_string()))?,
    );

    // build request
    let subscription_request = SubscriptionRequest {
        topic: Some(topic.clone()).into(),
        ..Default::default()
    };

    // send request
    let subscription_response: SubscriptionResponse = rpc_client
        .invoke_proto_method(
            make_remote_subscribe_uuri(&subscription_request.topic),
            CallOptions::for_rpc_request(UP_REMOTE_TTL, None, None, Some(UPriority::UPRIORITY_CS4)),
            subscription_request,
        )
        .await
        .map_err(|e| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Error invoking remote subscription request: {e}"),
            )
        })?;

    // deal with response
    if subscription_response.is_state(TopicState::SUBSCRIBED) {
        debug!("Got remote subscription response, state SUBSCRIBED");

        let _ = internal_cmd_sender
            .send(InternalSubscriptionEvent::TopicStateUpdate {
                topic,
                state: TopicState::SUBSCRIBED,
            })
            .await;
    } else {
        debug!("Got remote subscription response, some other state");
    }

    Ok(())
}

// Perform remote topic unsubscription
async fn remote_unsubscribe(
    topic: TopicUUri,
    uri_provider: Arc<dyn LocalUriProvider>,
    transport: Arc<dyn UTransport>,
    internal_cmd_sender: Sender<InternalSubscriptionEvent>,
) -> Result<(), UStatus> {
    let rpc_client: Arc<dyn RpcClient> = Arc::new(
        InMemoryRpcClient::new(transport, uri_provider)
            .await
            .map_err(|e| UStatus::fail_with_code(UCode::INTERNAL, e.to_string()))?,
    );

    // build request
    let unsubscribe_request = UnsubscribeRequest {
        topic: Some(topic.clone()).into(),
        ..Default::default()
    };

    // send request
    let unsubscribe_response: UStatus = rpc_client
        .invoke_proto_method(
            make_remote_unsubscribe_uuri(&unsubscribe_request.topic),
            CallOptions::for_rpc_request(UP_REMOTE_TTL, None, None, Some(UPriority::UPRIORITY_CS4)),
            unsubscribe_request,
        )
        .await
        .map_err(|e| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Error invoking remote unsubscribe request: {e}"),
            )
        })?;

    // deal with response
    match unsubscribe_response.code.enum_value_or(UCode::UNKNOWN) {
        UCode::OK => {
            debug!("Got OK remote unsubscribe response");
            let _ = internal_cmd_sender
                .send(InternalSubscriptionEvent::TopicStateUpdate {
                    topic,
                    state: TopicState::UNSUBSCRIBED,
                })
                .await;
        }
        code => {
            debug!("Got {:?} remote unsubscribe response", code);
            return Err(UStatus::fail_with_code(
                code,
                "Error during remote unsubscribe",
            ));
        }
    };

    Ok(())
}

// Internal helper that will spawn a task to unsubscribe a subscriber-topic relationship at timestamp `expiry`.
// In case the expiry timestamp is already in the past, unsubscribe will be initiated immediately.
// This is hopefully good enough for now - in case we get very many expiring subscriptions, might have
// to look for an approach that scales better.
fn schedule_unsubscribe(
    expiry: ExpiryTimestamp,
    subscriber: SubscriberUUri,
    topic: TopicUUri,
    sender: Sender<InternalSubscriptionEvent>,
) {
    helpers::spawn_and_log_error(async move {
        if let Some(delay) = helpers::duration_until_timestamp(expiry) {
            sleep(delay).await;
        }

        sender
            .send_timeout(
                InternalSubscriptionEvent::RemoveExpiredSubscription { subscriber, topic },
                Duration::from_secs(SUBSCRIPTION_EXPIRY_REMOVAL_TIMEOUT_SECONDS),
            )
            .await?;

        Ok(())
    });
}

// Create a remote Subscribe UUri from a (topic) uri; copies the UUri authority and
// replaces id, version and resource IDs with Subscribe-endpoint properties
pub(crate) fn make_remote_subscribe_uuri(uri: &UUri) -> UUri {
    UUri {
        authority_name: uri.authority_name.clone(),
        ue_id: USUBSCRIPTION_TYPE_ID,
        ue_version_major: USUBSCRIPTION_VERSION_MAJOR as u32,
        resource_id: RESOURCE_ID_SUBSCRIBE as u32,
        ..Default::default()
    }
}

// Create a remote Unsubscribe UUri from a (topic) uri; copies the UUri authority and
// replaces id, version and resource IDs with Unsubscribe-endpoint properties
pub(crate) fn make_remote_unsubscribe_uuri(uri: &UUri) -> UUri {
    UUri {
        authority_name: uri.authority_name.clone(),
        ue_id: USUBSCRIPTION_TYPE_ID,
        ue_version_major: USUBSCRIPTION_VERSION_MAJOR as u32,
        resource_id: RESOURCE_ID_UNSUBSCRIBE as u32,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    // These are tests just for the locally used helper functions of subscription manager. More complex and complete
    // tests of the susbcription manager business logic are located in tests/subscription_manager_tests.rs

    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::time::{Duration, Instant};

    use super::*;
    use crate::test_lib::{self};
    use up_rust::MockLocalUriProvider;

    fn get_mock_uri_provider(uri: UUri) -> MockLocalUriProvider {
        let mut mock_provider = MockLocalUriProvider::new();
        mock_provider.expect_get_source_uri().return_const(uri);
        mock_provider
    }

    #[tokio::test]
    async fn test_schedule_future_unsubscribe() {
        let (internal_cmd_sender, mut internal_cmd_receiver) =
            mpsc::channel::<InternalSubscriptionEvent>(INTERNAL_COMMAND_BUFFER_SIZE);

        let start = Instant::now();
        let expiry_in_1s = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 1000;

        schedule_unsubscribe(
            expiry_in_1s,
            test_lib::helpers::subscriber_uri1(),
            test_lib::helpers::local_topic1_uri(),
            internal_cmd_sender,
        );

        let message = internal_cmd_receiver.recv().await;
        if let Some(InternalSubscriptionEvent::RemoveExpiredSubscription { subscriber, topic }) =
            message
        {
            // Dunno how flaky this might turn out to be - but let's test if there is at least a second elapsed between scheduling the
            // unsubscribe task and it's reaction to the expiration timer 1s in the future...
            let elapsed = start.elapsed();
            assert!(elapsed >= Duration::from_millis(1000));

            assert_eq!(topic, test_lib::helpers::local_topic1_uri());
            assert_eq!(subscriber, test_lib::helpers::subscriber_uri1());
        } else {
            panic!("Expected RemoveExpiredSubscription event from scheduled task");
        }
    }

    #[tokio::test]
    async fn test_schedule_past_unsubscribe() {
        let (internal_cmd_sender, mut internal_cmd_receiver) =
            mpsc::channel::<InternalSubscriptionEvent>(INTERNAL_COMMAND_BUFFER_SIZE);

        let start = Instant::now();
        let expiry_in_1s = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            - 1000;

        schedule_unsubscribe(
            expiry_in_1s,
            test_lib::helpers::subscriber_uri1(),
            test_lib::helpers::local_topic1_uri(),
            internal_cmd_sender,
        );

        let message = internal_cmd_receiver.recv().await;
        if let Some(InternalSubscriptionEvent::RemoveExpiredSubscription { subscriber, topic }) =
            message
        {
            // Dunno how flaky this might turn out to be - but let's test if there is at most half a second elapsed between scheduling the
            // unsubscribe task and it's reaction to the expiration timer 1s in the past...
            let elapsed = start.elapsed();
            assert!(elapsed < Duration::from_millis(500));

            assert_eq!(topic, test_lib::helpers::local_topic1_uri());
            assert_eq!(subscriber, test_lib::helpers::subscriber_uri1());
        } else {
            panic!("Expected RemoveExpiredSubscription event from scheduled task");
        }
    }

    #[tokio::test]
    async fn test_remote_subscribe() {
        helpers::init_once();
        let expected_topic = test_lib::helpers::remote_topic1_uri();

        // build request
        let expected_request = SubscriptionRequest {
            topic: Some(expected_topic.clone()).into(),
            ..Default::default()
        };

        // build response
        let expected_response = SubscriptionResponse {
            topic: Some(expected_topic.clone()).into(),
            status: Some(SubscriptionStatus {
                state: TopicState::SUBSCRIBED.into(),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        };

        // set up mocks
        let mock_uri_provider = Arc::new(get_mock_uri_provider(
            test_lib::helpers::local_usubscription_service_uri(),
        ));
        let mock_transport = Arc::new(
            test_lib::mocks::utransport_mock_for_rpc(vec![(expected_request, expected_response)])
                .await,
        );
        let (sender, mut receiver) =
            mpsc::channel::<InternalSubscriptionEvent>(INTERNAL_COMMAND_BUFFER_SIZE);

        // perform operation to test
        let result = remote_subscribe(
            expected_topic.clone(),
            mock_uri_provider,
            mock_transport,
            sender,
        )
        .await;

        // validate response
        assert!(result.is_ok());
        let response = receiver.recv().await;
        assert!(response.is_some());
        if let InternalSubscriptionEvent::TopicStateUpdate { topic, state } = response.unwrap() {
            assert_eq!(topic, expected_topic);
            assert_eq!(state, TopicState::SUBSCRIBED);
        };
    }

    #[tokio::test]
    async fn test_remote_unsubscribe() {
        helpers::init_once();
        let expected_topic = test_lib::helpers::remote_topic1_uri();

        // build request
        let expected_request = UnsubscribeRequest {
            topic: Some(expected_topic.clone()).into(),
            ..Default::default()
        };

        // build response
        let expected_response = UStatus {
            code: UCode::OK.into(),
            ..Default::default()
        };

        // set up mocks
        let (sender, mut receiver) =
            mpsc::channel::<InternalSubscriptionEvent>(INTERNAL_COMMAND_BUFFER_SIZE);
        let mock_transport = Arc::new(
            test_lib::mocks::utransport_mock_for_rpc(vec![(expected_request, expected_response)])
                .await,
        );

        // perform operation to test
        let result: Result<(), UStatus> = remote_unsubscribe(
            expected_topic.clone(),
            Arc::new(get_mock_uri_provider(UUri::default())),
            mock_transport,
            sender,
        )
        .await;

        // validate response
        assert!(result.is_ok());
        let response = receiver.recv().await;
        assert!(response.is_some());
        if let InternalSubscriptionEvent::TopicStateUpdate { topic, state } = response.unwrap() {
            assert_eq!(topic, expected_topic);
            assert_eq!(state, TopicState::UNSUBSCRIBED);
        };
    }

    #[test]
    fn test_make_remote_subscribe_uuri() {
        let expected_uri = UUri {
            authority_name: test_lib::helpers::remote_topic1_uri().authority_name,
            ue_id: USUBSCRIPTION_TYPE_ID,
            ue_version_major: USUBSCRIPTION_VERSION_MAJOR as u32,
            resource_id: RESOURCE_ID_SUBSCRIBE as u32,
            ..Default::default()
        };

        let remote_method = make_remote_subscribe_uuri(&test_lib::helpers::remote_topic1_uri());

        assert_eq!(expected_uri, remote_method);
    }

    #[test]
    fn test_make_remote_unsubscribe_uuri() {
        let expected_uri = UUri {
            authority_name: test_lib::helpers::remote_topic1_uri().authority_name,
            ue_id: USUBSCRIPTION_TYPE_ID,
            ue_version_major: USUBSCRIPTION_VERSION_MAJOR as u32,
            resource_id: RESOURCE_ID_UNSUBSCRIBE as u32,
            ..Default::default()
        };
        let remote_method = make_remote_unsubscribe_uuri(&test_lib::helpers::remote_topic1_uri());

        assert_eq!(expected_uri, remote_method);
    }
}
