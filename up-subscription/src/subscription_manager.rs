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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot, Notify};
use up_rust::{LocalUriProvider, UTransport};

use up_rust::{
    communication::{CallOptions, InMemoryRpcClient, RpcClient},
    core::usubscription::{
        State as TopicState, SubscriptionRequest, SubscriptionResponse, SubscriptionStatus,
        UnsubscribeRequest, RESOURCE_ID_SUBSCRIBE, RESOURCE_ID_UNSUBSCRIBE, USUBSCRIPTION_TYPE_ID,
        USUBSCRIPTION_VERSION_MAJOR,
    },
    UCode, UPriority, UStatus, UUri,
};

use crate::{helpers, usubscription::UP_REMOTE_TTL};

// This is the core business logic for handling and tracking subscriptions. It is currently implemented as a single event-consuming
// function `handle_message()`, which is supposed to be spawned into a task and process the various `Events` that it can receive
// via tokio mpsc channel. This design allows to forgo the use of any synhronization primitives on the subscription-tracking container
// data types, as any access is coordinated/serialized via the Event selection loop.

// Maximum number of `Subscriber` entries to be returned in a `FetchSusbcriptions´ operation
const UP_MAX_FETCH_SUBSCRIBERS_LEN: usize = 100;
// Maximum number of `Subscriber` entries to be returned in a `FetchSusbcriptions´ operation
const UP_MAX_FETCH_SUBSCRIPTIONS_LEN: usize = 100;

#[derive(Debug)]
pub(crate) enum RequestKind {
    Subscriber(UUri),
    Topic(UUri),
}

#[derive(Debug)]
pub(crate) struct SubscriptionEntry {
    pub(crate) topic: UUri,
    pub(crate) subscriber: UUri,
    pub(crate) status: SubscriptionStatus,
}

pub(crate) type SubscribersResponse = (Vec<UUri>, bool); // List of subscribers, boolean flag stating if there exist more entries than contained in list
pub(crate) type SubscriptionsResponse = (Vec<SubscriptionEntry>, bool); // List of subscriber entries, boolean flag stating if there exist more entries than contained in list

// This is the 'outside API' of subscription manager, it includes some events that are only to be used in (and only enabled for) testing.
#[derive(Debug)]
pub(crate) enum SubscriptionEvent {
    AddSubscription {
        subscriber: UUri,
        topic: UUri,
        respond_to: oneshot::Sender<SubscriptionStatus>,
    },
    RemoveSubscription {
        subscriber: UUri,
        topic: UUri,
        respond_to: oneshot::Sender<SubscriptionStatus>,
    },
    FetchSubscribers {
        topic: UUri,
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
        respond_to: oneshot::Sender<HashMap<UUri, HashSet<UUri>>>,
    },
    // Purely for use during testing: force-set new topic-subscriber ledger
    #[cfg(test)]
    SetTopicSubscribers {
        topic_subscribers_replacement: HashMap<UUri, HashSet<UUri>>,
        respond_to: oneshot::Sender<()>,
    },
    // Purely for use during testing: get copy of current topic-subscriper ledger
    #[cfg(test)]
    GetRemoteTopics {
        respond_to: oneshot::Sender<HashMap<UUri, TopicState>>,
    },
    // Purely for use during testing: force-set new topic-subscriber ledger
    #[cfg(test)]
    SetRemoteTopics {
        topic_subscribers_replacement: HashMap<UUri, TopicState>,
        respond_to: oneshot::Sender<()>,
    },
}

// Internal subscription manager API - used to update on remote subscriptions (deal with _PENDING states)
enum RemoteSubscriptionEvent {
    RemoteTopicStateUpdate { topic: UUri, state: TopicState },
}

// Wrapper type, include all kinds of actions subscription manager knows
enum Event {
    LocalSubscription(SubscriptionEvent),
    RemoteSubscription(RemoteSubscriptionEvent),
}

// Core business logic of subscription management - includes container data types for tracking subscriptions and remote subscriptions.
// Interfacing with this purely works via channels, so we do not have to deal with mutexes and similar concepts.
pub(crate) async fn handle_message(
    uri_provider: Arc<dyn LocalUriProvider>,
    transport: Arc<dyn UTransport>,
    mut command_receiver: Receiver<SubscriptionEvent>,
    shutdown: Arc<Notify>,
) {
    helpers::init_once();

    // track subscribers for topics - if you're in this list, you have SUBSCRIBED, otherwise you're considered UNSUBSCRIBED
    #[allow(clippy::mutable_key_type)]
    let mut topic_subscribers: HashMap<UUri, HashSet<UUri>> = HashMap::new();

    // for remote topics, we need to additionally deal with _PENDING states, this tracks states of these topics
    #[allow(clippy::mutable_key_type)]
    let mut remote_topics: HashMap<UUri, TopicState> = HashMap::new();

    let (remote_sub_sender, mut remote_sub_receiver) =
        mpsc::unbounded_channel::<RemoteSubscriptionEvent>();

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
            event = remote_sub_receiver.recv() => match event {
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
                    respond_to,
                } => {
                    if respond_to
                        .send(add_subscription(
                            uri_provider.clone(),
                            transport.clone(),
                            remote_sub_sender.clone(),
                            &mut remote_topics,
                            &mut topic_subscribers,
                            subscriber,
                            topic,
                        ))
                        .is_err()
                    {
                        error!("Problem with internal communication");
                    }
                }
                SubscriptionEvent::RemoveSubscription {
                    subscriber,
                    topic,
                    respond_to,
                } => {
                    if respond_to
                        .send(remove_subscription(
                            uri_provider.clone(),
                            transport.clone(),
                            remote_sub_sender.clone(),
                            &mut remote_topics,
                            &mut topic_subscribers,
                            subscriber,
                            topic,
                        ))
                        .is_err()
                    {
                        error!("Problem with internal communication");
                    }
                }
                SubscriptionEvent::FetchSubscribers {
                    topic,
                    offset,
                    respond_to,
                } => {
                    if respond_to
                        .send(fetch_subscribers(&topic_subscribers, topic, offset))
                        .is_err()
                    {
                        error!("Problem with internal communication");
                    }
                }
                SubscriptionEvent::FetchSubscriptions {
                    request,
                    offset,
                    respond_to,
                } => {
                    if respond_to
                        .send(fetch_subscriptions(
                            &topic_subscribers,
                            &remote_topics,
                            request,
                            offset,
                        ))
                        .is_err()
                    {
                        error!("Problem with internal communication");
                    };
                }
                #[cfg(test)]
                SubscriptionEvent::GetTopicSubscribers { respond_to } => {
                    let _r = respond_to.send(topic_subscribers.clone());
                }
                #[cfg(test)]
                SubscriptionEvent::SetTopicSubscribers {
                    topic_subscribers_replacement,
                    respond_to,
                } => {
                    topic_subscribers = topic_subscribers_replacement;
                    let _r = respond_to.send(());
                }
                #[cfg(test)]
                SubscriptionEvent::GetRemoteTopics { respond_to } => {
                    let _r = respond_to.send(remote_topics.clone());
                }
                #[cfg(test)]
                SubscriptionEvent::SetRemoteTopics {
                    topic_subscribers_replacement: remote_topics_replacement,
                    respond_to,
                } => {
                    remote_topics = remote_topics_replacement;
                    let _r = respond_to.send(());
                }
            },
            // these deal with feedback/state updates from the remote subscription handlers
            Event::RemoteSubscription(event) => match event {
                RemoteSubscriptionEvent::RemoteTopicStateUpdate { topic, state } => {
                    remote_topics.entry(topic).and_modify(|s| *s = state);
                }
            },
        }
    }
}

// Add a subscription relationship to bookkeeping, initiate remote subscribe request if neccessary
#[allow(clippy::mutable_key_type)]
fn add_subscription(
    uri_provider: Arc<dyn LocalUriProvider>,
    transport: Arc<dyn UTransport>,
    remote_sub_sender: mpsc::UnboundedSender<RemoteSubscriptionEvent>,
    remote_topics: &mut HashMap<UUri, TopicState>,
    topic_subscribers: &mut HashMap<UUri, HashSet<UUri>>,
    subscriber: UUri,
    topic: UUri,
) -> SubscriptionStatus {
    // Add new subscriber to topic subscription tracker (create new entries as necessary)
    let is_new = topic_subscribers
        .entry(topic.clone())
        .or_default()
        .insert(subscriber);

    let mut state = TopicState::SUBSCRIBED; // everything in topic_subscribers is considered SUBSCRIBED by default

    if topic.is_remote_authority(&uri_provider.get_authority()) {
        // for remote_topics, we explicitly track state due to the _PENDING scenarios
        state = *remote_topics
            .get(&topic)
            .unwrap_or(&TopicState::SUBSCRIBE_PENDING);

        remote_topics.entry(topic.clone()).or_insert(state);
        if is_new {
            // this is the first subscriber to this (remote) topic, so perform remote subscription
            helpers::spawn_and_log_error(async move {
                remote_subscribe(topic, uri_provider, transport, remote_sub_sender).await?;
                Ok(())
            });
        }
    }
    SubscriptionStatus {
        state: state.into(),
        ..Default::default()
    }
}

// Remove a subscription relationship to bookkeeping, initiate remote unsubscribe request if neccessary
#[allow(clippy::mutable_key_type)]
fn remove_subscription(
    uri_provider: Arc<dyn LocalUriProvider>,
    transport: Arc<dyn UTransport>,
    remote_sub_sender: mpsc::UnboundedSender<RemoteSubscriptionEvent>,
    remote_topics: &mut HashMap<UUri, TopicState>,
    topic_subscribers: &mut HashMap<UUri, HashSet<UUri>>,
    subscriber: UUri,
    topic: UUri,
) -> SubscriptionStatus {
    if let Some(entry) = topic_subscribers.get_mut(&topic) {
        // check if we even know this subscriber-topic combo
        entry.remove(&subscriber);

        // if topic is remote, we were tracking this remote topic already, and this was the last subscriber
        if topic.is_remote_authority(&uri_provider.get_authority())
            && remote_topics.contains_key(&topic)
            && entry.is_empty()
        {
            // until remote ubsubscribe confirmed (below), set remote topics tracker state to UNSUBSCRIBE_PENDING
            if let Some(entry) = remote_topics.get_mut(&topic) {
                *entry = TopicState::UNSUBSCRIBE_PENDING;
            }

            // this was the last subscriber to this (remote) topic, so perform remote unsubscription
            let topic_clone = topic.clone();
            helpers::spawn_and_log_error(async move {
                remote_unsubscribe(topic_clone, uri_provider, transport, remote_sub_sender).await?;
                Ok(())
            });
        }
    }
    // If this was the last subscriber to topic, remote the entire subscription entry from tracker
    if topic_subscribers.get(&topic).is_some_and(|e| e.is_empty()) {
        topic_subscribers.remove(&topic);
    }

    SubscriptionStatus {
        // Whatever happens with the remote topic state - as far as the local client is concerned, it has now UNSUBSCRIBED from this topic
        state: TopicState::UNSUBSCRIBED.into(),
        ..Default::default()
    }
}

// Fetch all subscribers of a topicf
#[allow(clippy::mutable_key_type)]
fn fetch_subscribers(
    topic_subscribers: &HashMap<UUri, HashSet<UUri>>,
    topic: UUri,
    offset: Option<u32>,
) -> SubscribersResponse {
    let mut subscriber_infos = vec![];
    let mut has_more = false;

    // This will get *every* client that subscribed to `topic` - no matter whether (in the case of remote subscriptions)
    // the remote topic is already fully SUBSCRIBED, of still SUSBCRIBED_PENDING
    if let Some(subs) = topic_subscribers.get(&topic) {
        let mut subscribers: Vec<&UUri> = subs.iter().collect();

        if let Some(offset) = offset {
            subscribers.drain(..offset as usize);
        }

        // split up result list, to make sense of has_more_records field
        if subscribers.len() > UP_MAX_FETCH_SUBSCRIBERS_LEN {
            subscribers.truncate(UP_MAX_FETCH_SUBSCRIBERS_LEN);
            has_more = true;
        }

        for subscriber_uri in subscribers {
            subscriber_infos.push(subscriber_uri.clone());
        }
    }
    (subscriber_infos, has_more)
}

// Fetch all subscriptions of a topic or subscribers
#[allow(clippy::mutable_key_type)]
fn fetch_subscriptions(
    topic_subscribers: &HashMap<UUri, HashSet<UUri>>,
    remote_topics: &HashMap<UUri, TopicState>,
    request: RequestKind,
    offset: Option<u32>,
) -> SubscriptionsResponse {
    let mut fetch_subscriptions_response: SubscriptionsResponse = (vec![], false);

    match request {
        RequestKind::Subscriber(subscriber) => {
            // This is where someone wants "all subscriptions of a specific subscriber",
            // which isn't very straighforward with the way we do bookeeping, so
            // first, get all entries from our topic-subscribers ledger that contain the requested SubscriberInfo
            let subscriptions: Vec<(&UUri, &HashSet<UUri>)> = topic_subscribers
                .iter()
                .filter(|entry| entry.1.contains(&subscriber))
                .collect();

            // from that set, we use the topics and build Subscription response objects
            let mut result_subs: Vec<SubscriptionEntry> = Vec::new();
            for (topic, _) in subscriptions {
                // get potentially deviating state for remote topics (e.g. SUBSCRIBE_PENDING),
                // if nothing is available there we fall back to default assumption that any
                // entry in topic_subscribers is there because a client SUBSCRIBED to a topic.
                let state = remote_topics.get(topic).unwrap_or(&TopicState::SUBSCRIBED);

                let subscription = SubscriptionEntry {
                    topic: topic.clone(),
                    subscriber: subscriber.clone(),
                    status: SubscriptionStatus {
                        state: (*state).into(),
                        ..Default::default()
                    },
                };
                result_subs.push(subscription);
            }

            if let Some(offset) = offset {
                result_subs.drain(..offset as usize);
            }

            // split up result list, to make sense of has_more_records field
            let mut has_more = false;
            if result_subs.len() > UP_MAX_FETCH_SUBSCRIPTIONS_LEN {
                result_subs.truncate(UP_MAX_FETCH_SUBSCRIPTIONS_LEN);
                has_more = true;
            }

            fetch_subscriptions_response = (result_subs, has_more);
        }
        RequestKind::Topic(topic) => {
            if let Some(subs) = topic_subscribers.get(&topic) {
                let mut subscribers: Vec<&UUri> = subs.iter().collect();

                if let Some(offset) = offset {
                    subscribers.drain(..offset as usize);
                }

                // split up result list, to make sense of has_more_records field
                let mut has_more = false;
                if subscribers.len() > UP_MAX_FETCH_SUBSCRIPTIONS_LEN {
                    subscribers.truncate(UP_MAX_FETCH_SUBSCRIPTIONS_LEN);
                    has_more = true;
                }

                let mut result_subs: Vec<SubscriptionEntry> = Vec::new();
                for subscriber in subscribers {
                    // get potentially deviating state for remote topics (e.g. SUBSCRIBE_PENDING),
                    // if nothing is available there we fall back to default assumption that any
                    // entry in topic_subscribers is there because a client SUBSCRIBED to a topic.
                    let state = remote_topics.get(&topic).unwrap_or(&TopicState::SUBSCRIBED);

                    let subscription = SubscriptionEntry {
                        topic: topic.clone(),
                        subscriber: subscriber.clone(),
                        status: SubscriptionStatus {
                            state: (*state).into(),
                            ..Default::default()
                        },
                    };
                    result_subs.push(subscription);
                }

                fetch_subscriptions_response = (result_subs, has_more);
            }
        }
    }

    fetch_subscriptions_response
}

// Perform remote topic subscription
async fn remote_subscribe(
    topic: UUri,
    uri_provider: Arc<dyn LocalUriProvider>,
    transport: Arc<dyn UTransport>,
    remote_sub_sender: mpsc::UnboundedSender<RemoteSubscriptionEvent>,
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

        let _ = remote_sub_sender.send(RemoteSubscriptionEvent::RemoteTopicStateUpdate {
            topic,
            state: TopicState::SUBSCRIBED,
        });
    } else {
        debug!("Got remote subscription response, some other state");
    }

    Ok(())
}

// Perform remote topic unsubscription
async fn remote_unsubscribe(
    topic: UUri,
    uri_provider: Arc<dyn LocalUriProvider>,
    transport: Arc<dyn UTransport>,
    remote_sub_sender: mpsc::UnboundedSender<RemoteSubscriptionEvent>,
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
            let _ = remote_sub_sender.send(RemoteSubscriptionEvent::RemoteTopicStateUpdate {
                topic,
                state: TopicState::UNSUBSCRIBED,
            });
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

    use super::*;
    use crate::test_lib::{self};
    use up_rust::MockLocalUriProvider;

    fn get_mock_uri_provider(uri: UUri) -> MockLocalUriProvider {
        let mut mock_provider = MockLocalUriProvider::new();
        mock_provider.expect_get_source_uri().return_const(uri);
        mock_provider
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
        let (sender, mut receiver) = mpsc::unbounded_channel::<RemoteSubscriptionEvent>();

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
        match response.unwrap() {
            RemoteSubscriptionEvent::RemoteTopicStateUpdate { topic, state } => {
                assert_eq!(topic, expected_topic);
                assert_eq!(state, TopicState::SUBSCRIBED);
            }
        }
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
        let (sender, mut receiver) = mpsc::unbounded_channel::<RemoteSubscriptionEvent>();
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
        match response.unwrap() {
            RemoteSubscriptionEvent::RemoteTopicStateUpdate { topic, state } => {
                assert_eq!(topic, expected_topic);
                assert_eq!(state, TopicState::UNSUBSCRIBED);
            }
        }
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
