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

use up_rust::{
    communication::CallOptions,
    communication::RpcClient,
    core::usubscription::{
        FetchSubscribersRequest, FetchSubscribersResponse, FetchSubscriptionsRequest,
        FetchSubscriptionsResponse, Request, State as TopicState, SubscriberInfo, Subscription,
        SubscriptionRequest, SubscriptionResponse, SubscriptionStatus, UnsubscribeRequest,
        RESOURCE_ID_SUBSCRIBE, RESOURCE_ID_UNSUBSCRIBE, USUBSCRIPTION_TYPE_ID,
        USUBSCRIPTION_VERSION_MAJOR,
    },
    UCode, UPriority, UStatus, UUri, UUID,
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

// This is the 'outside API' of subscription manager, it includes some events that are only to be used in (and only enabled for) testing.
#[derive(Debug)]
pub(crate) enum SubscriptionEvent {
    AddSubscription {
        subscriber: SubscriberInfo,
        topic: UUri,
        respond_to: oneshot::Sender<SubscriptionStatus>,
    },
    RemoveSubscription {
        subscriber: SubscriberInfo,
        topic: UUri,
        respond_to: oneshot::Sender<SubscriptionStatus>,
    },
    FetchSubscribers {
        request: FetchSubscribersRequest,
        respond_to: oneshot::Sender<FetchSubscribersResponse>,
    },
    FetchSubscriptions {
        request: FetchSubscriptionsRequest,
        respond_to: oneshot::Sender<FetchSubscriptionsResponse>,
    },
    // Purely for use during testing: get copy of current topic-subscriper ledger
    #[cfg(test)]
    GetTopicSubscribers {
        respond_to: oneshot::Sender<HashMap<UUri, HashSet<SubscriberInfo>>>,
    },
    // Purely for use during testing: force-set new topic-subscriber ledger
    #[cfg(test)]
    SetTopicSubscribers {
        topic_subscribers_replacement: HashMap<UUri, HashSet<SubscriberInfo>>,
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
    own_uri: UUri,
    up_client: Arc<dyn RpcClient>,
    mut command_receiver: Receiver<SubscriptionEvent>,
    shutdown: Arc<Notify>,
) {
    helpers::init_once();

    // track subscribers for topics - if you're in this list, you have SUBSCRIBED, otherwise you're considered UNSUBSCRIBED
    #[allow(clippy::mutable_key_type)]
    let mut topic_subscribers: HashMap<UUri, HashSet<SubscriberInfo>> = HashMap::new();

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
                    // Add new subscriber to topic subscription tracker (create new entries as necessary)
                    topic_subscribers
                        .entry(topic.clone())
                        .or_default()
                        .insert(subscriber);

                    // This really should unwrap() ok, as we just inserted an entry above
                    let subscribers_count =
                        topic_subscribers.get(&topic).map(|e| e.len()).unwrap_or(0);

                    let mut state = TopicState::SUBSCRIBED; // everything in topic_subscribers is considered SUBSCRIBED by default

                    if topic.is_remote_authority(&own_uri) {
                        state = TopicState::SUBSCRIBE_PENDING; // for remote_topics, we explicitly track state due to the _PENDING scenarios
                        remote_topics.entry(topic.clone()).or_insert(state);

                        if subscribers_count == 1 {
                            // this is the first subscriber to this (remote) topic, so perform remote subscription
                            let own_uri_clone = own_uri.clone();
                            let up_client_clone = up_client.clone();
                            let remote_sub_sender_clone = remote_sub_sender.clone();

                            helpers::spawn_and_log_error(async move {
                                remote_subscribe(
                                    own_uri_clone,
                                    topic,
                                    up_client_clone,
                                    remote_sub_sender_clone,
                                )
                                .await?;
                                Ok(())
                            });
                        }
                    }
                    if respond_to
                        .send(SubscriptionStatus {
                            state: state.into(),
                            ..Default::default()
                        })
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
                    if let Some(entry) = topic_subscribers.get_mut(&topic) {
                        // check if we even know this subscriber-topic combo
                        entry.remove(&subscriber);

                        // if topic is remote, we were tracking this remote topic already, and this was the last subscriber
                        if topic.is_remote_authority(&own_uri)
                            && remote_topics.contains_key(&topic)
                            && entry.is_empty()
                        {
                            // until remote ubsubscribe confirmed (below), set remote topics tracker state to UNSUBSCRIBE_PENDING
                            if let Some(entry) = remote_topics.get_mut(&topic) {
                                *entry = TopicState::UNSUBSCRIBE_PENDING;
                            }

                            // this was the last subscriber to this (remote) topic, so perform remote unsubscription
                            let own_uri_clone = own_uri.clone();
                            let up_client_clone = up_client.clone();
                            let remote_sub_sender_clone = remote_sub_sender.clone();
                            let topic_cloned = topic.clone();

                            helpers::spawn_and_log_error(async move {
                                remote_unsubscribe(
                                    own_uri_clone,
                                    topic_cloned,
                                    up_client_clone,
                                    remote_sub_sender_clone,
                                )
                                .await?;
                                Ok(())
                            });
                        }
                    }
                    // If this was the last subscriber to topic, remote the entire subscription entry from tracker
                    if topic_subscribers.get(&topic).is_some_and(|e| e.is_empty()) {
                        topic_subscribers.remove(&topic);
                    }

                    if respond_to
                        .send(SubscriptionStatus {
                            // Whatever happens with the remote topic state - as far as the local client is concerned, it has now UNSUBSCRIBED from this topic
                            state: TopicState::UNSUBSCRIBED.into(),
                            ..Default::default()
                        })
                        .is_err()
                    {
                        error!("Problem with internal communication");
                    }
                }
                SubscriptionEvent::FetchSubscribers {
                    request,
                    respond_to,
                } => {
                    let FetchSubscribersRequest { topic, offset, .. } = request;

                    // This will get *every* client that subscribed to `topic` - no matter whether (in the case of remote subscriptions)
                    // the remote topic is already fully SUBSCRIBED, of still SUSBCRIBED_PENDING
                    if let Some(subs) = topic_subscribers.get(&topic) {
                        let mut subscribers: Vec<&SubscriberInfo> = subs.iter().collect();

                        if let Some(offset) = offset {
                            subscribers.drain(..offset as usize);
                        }

                        // split up result list, to make sense of has_more_records field
                        let mut has_more = false;
                        if subscribers.len() > UP_MAX_FETCH_SUBSCRIBERS_LEN {
                            subscribers.truncate(UP_MAX_FETCH_SUBSCRIBERS_LEN);
                            has_more = true;
                        }

                        if respond_to
                            .send(FetchSubscribersResponse {
                                subscribers: subscribers.iter().map(|s| (*s).clone()).collect(),
                                has_more_records: has_more.into(),
                                ..Default::default()
                            })
                            .is_err()
                        {
                            error!("Problem with internal communication");
                        }
                    } else if respond_to
                        .send(FetchSubscribersResponse::default())
                        .is_err()
                    {
                        error!("Problem with internal communication");
                    }
                }
                SubscriptionEvent::FetchSubscriptions {
                    request,
                    respond_to,
                } => {
                    let FetchSubscriptionsRequest {
                        request, offset, ..
                    } = request;
                    let mut fetch_subscriptions_response = FetchSubscriptionsResponse::default();

                    if let Some(request) = request {
                        match request {
                            Request::Subscriber(subscriber) => {
                                // This is where someone wants "all subscriptions of a specific subscriber",
                                // which isn't very straighforward with the way we do bookeeping, so
                                // first, get all entries from our topic-subscribers ledger that contain the requested SubscriberInfo
                                let subscriptions: Vec<(&UUri, &HashSet<SubscriberInfo>)> =
                                    topic_subscribers
                                        .iter()
                                        .filter(|entry| entry.1.contains(&subscriber))
                                        .collect();

                                // from that set, we use the topics and build Subscription response objects
                                let mut result_subs: Vec<Subscription> = Vec::new();
                                for (topic, _) in subscriptions {
                                    // get potentially deviating state for remote topics (e.g. SUBSCRIBE_PENDING),
                                    // if nothing is available there we fall back to default assumption that any
                                    // entry in topic_subscribers is there because a client SUBSCRIBED to a topic.
                                    let state =
                                        remote_topics.get(topic).unwrap_or(&TopicState::SUBSCRIBED);

                                    let subscription = Subscription {
                                        topic: Some(topic.clone()).into(),
                                        subscriber: Some(subscriber.clone()).into(),
                                        status: Some(SubscriptionStatus {
                                            state: (*state).into(),
                                            ..Default::default()
                                        })
                                        .into(),
                                        ..Default::default()
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

                                fetch_subscriptions_response = FetchSubscriptionsResponse {
                                    subscriptions: result_subs,
                                    has_more_records: Some(has_more),
                                    ..Default::default()
                                };
                            }
                            Request::Topic(topic) => {
                                if let Some(subs) = topic_subscribers.get(&topic) {
                                    let mut subscribers: Vec<&SubscriberInfo> =
                                        subs.iter().collect();

                                    if let Some(offset) = offset {
                                        subscribers.drain(..offset as usize);
                                    }

                                    // split up result list, to make sense of has_more_records field
                                    let mut has_more = false;
                                    if subscribers.len() > UP_MAX_FETCH_SUBSCRIPTIONS_LEN {
                                        subscribers.truncate(UP_MAX_FETCH_SUBSCRIPTIONS_LEN);
                                        has_more = true;
                                    }

                                    let mut result_subs: Vec<Subscription> = Vec::new();
                                    for subscriber in subscribers {
                                        // get potentially deviating state for remote topics (e.g. SUBSCRIBE_PENDING),
                                        // if nothing is available there we fall back to default assumption that any
                                        // entry in topic_subscribers is there because a client SUBSCRIBED to a topic.
                                        let state = remote_topics
                                            .get(&topic)
                                            .unwrap_or(&TopicState::SUBSCRIBED);

                                        let subscription = Subscription {
                                            topic: Some(topic.clone()).into(),
                                            subscriber: Some(subscriber.clone()).into(),
                                            status: Some(SubscriptionStatus {
                                                state: (*state).into(),
                                                ..Default::default()
                                            })
                                            .into(),
                                            ..Default::default()
                                        };
                                        result_subs.push(subscription);
                                    }

                                    fetch_subscriptions_response = FetchSubscriptionsResponse {
                                        subscriptions: result_subs,
                                        has_more_records: Some(has_more),
                                        ..Default::default()
                                    };
                                }
                            }
                            _ => {
                                error!("Invalid Request object - this really should not happen")
                            }
                        }
                    }
                    if respond_to.send(fetch_subscriptions_response).is_err() {
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

// Perform remote topic subscription
async fn remote_subscribe(
    own_uri: UUri,
    topic: UUri,
    up_client: Arc<dyn RpcClient>,
    remote_sub_sender: mpsc::UnboundedSender<RemoteSubscriptionEvent>,
) -> Result<(), UStatus> {
    // build request
    let subscription_request = SubscriptionRequest {
        topic: Some(topic.clone()).into(),
        subscriber: Some(SubscriberInfo {
            uri: Some(own_uri.clone()).into(),
            ..Default::default()
        })
        .into(),
        ..Default::default()
    };

    // send request
    let subscription_response: SubscriptionResponse = up_client
        .invoke_proto_method(
            make_remote_subscribe_uuri(&subscription_request.topic),
            CallOptions::for_rpc_request(
                UP_REMOTE_TTL,
                Some(UUID::build()),
                None,
                Some(UPriority::UPRIORITY_CS4),
            ),
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
    own_uri: UUri,
    topic: UUri,
    up_client: Arc<dyn RpcClient>,
    remote_sub_sender: mpsc::UnboundedSender<RemoteSubscriptionEvent>,
) -> Result<(), UStatus> {
    // build request
    let unsubscribe_request = UnsubscribeRequest {
        topic: Some(topic.clone()).into(),
        subscriber: Some(SubscriberInfo {
            uri: Some(own_uri.clone()).into(),
            ..Default::default()
        })
        .into(),
        ..Default::default()
    };

    // send request
    let unsubscribe_response: UStatus = up_client
        .invoke_proto_method(
            make_remote_unsubscribe_uuri(&unsubscribe_request.topic),
            CallOptions::for_rpc_request(
                UP_REMOTE_TTL,
                Some(UUID::new()),
                None,
                Some(UPriority::UPRIORITY_CS2),
            ),
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
    use protobuf::MessageFull;

    use up_rust::communication::UPayload;

    use crate::test_lib::{self, mocks::MockRpcClientMock};

    fn get_client_mock<R: MessageFull, S: MessageFull>(
        expected_method: UUri,
        expected_options: CallOptions,
        expected_request: R,
        expected_response: S,
    ) -> MockRpcClientMock {
        let mut client_mock = MockRpcClientMock::new();

        let expected_request_payload = UPayload::try_from_protobuf(expected_request).unwrap();
        let expected_response_payload = UPayload::try_from_protobuf(expected_response).unwrap();

        client_mock
            .expect_invoke_method()
            .once()
            .withf(move |method, options, payload| {
                *method == expected_method
                    && test_lib::is_equivalent_calloptions(options, &expected_options)
                    && *payload == Some(expected_request_payload.clone())
            })
            .return_const(Ok(Some(expected_response_payload)));

        client_mock
    }

    #[tokio::test]
    async fn test_remote_subscribe() {
        helpers::init_once();

        // prepare things
        let expected_topic = test_lib::helpers::remote_topic1_uri();
        let expected_method = make_remote_subscribe_uuri(&expected_topic);
        let expected_subscriber = test_lib::helpers::local_usubscription_service_uri();
        let expected_message_id = UUID::build();
        let expected_options = CallOptions::for_rpc_request(
            UP_REMOTE_TTL,
            Some(expected_message_id),
            None,
            Some(UPriority::UPRIORITY_CS4),
        );
        let expected_request = SubscriptionRequest {
            topic: Some(expected_topic.clone()).into(),
            subscriber: Some(SubscriberInfo {
                uri: Some(expected_subscriber).into(),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        };
        let expected_response = SubscriptionResponse {
            topic: Some(expected_topic.clone()).into(),
            status: Some(SubscriptionStatus {
                state: TopicState::SUBSCRIBED.into(),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        };

        let (sender, mut receiver) = mpsc::unbounded_channel::<RemoteSubscriptionEvent>();

        // perform operation to test
        let result = remote_subscribe(
            test_lib::helpers::local_usubscription_service_uri(),
            expected_topic.clone(),
            Arc::new(get_client_mock(
                expected_method,
                expected_options,
                expected_request,
                expected_response,
            )),
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

        // prepare things
        let expected_topic = test_lib::helpers::remote_topic1_uri();
        let expected_method = make_remote_unsubscribe_uuri(&expected_topic);
        let expected_subscriber = test_lib::helpers::local_usubscription_service_uri();

        let expected_options = CallOptions::for_rpc_request(
            UP_REMOTE_TTL,
            Some(UUID::new()),
            None,
            Some(UPriority::UPRIORITY_CS2),
        );
        let expected_request = UnsubscribeRequest {
            topic: Some(expected_topic.clone()).into(),
            subscriber: Some(SubscriberInfo {
                uri: Some(expected_subscriber).into(),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        };
        let expected_response = UStatus {
            code: UCode::OK.into(),
            ..Default::default()
        };

        let (sender, mut receiver) = mpsc::unbounded_channel::<RemoteSubscriptionEvent>();

        // perform operation to test
        let result = remote_unsubscribe(
            test_lib::helpers::local_usubscription_service_uri(),
            expected_topic.clone(),
            Arc::new(get_client_mock(
                expected_method,
                expected_options,
                expected_request,
                expected_response,
            )),
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
