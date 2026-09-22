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

use std::collections::HashMap;
use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{mpsc, mpsc::Receiver, mpsc::Sender, oneshot, Notify},
    time::sleep,
};
use tracing::{debug, error, warn};

use up_rust::communication::CallOptions;
use up_rust::{
    communication::{RpcClient, SubscriptionStatus},
    core::usubscription::{RpcClientUSubscription, SubscriptionInfo, USubscription},
    LocalUriProvider, UCode, UStatus, UUri,
};

use crate::{
    helpers,
    notification_manager::{self, NotificationEvent},
    persistency::{self},
    usubscription::{SubscriberUUri, TopicUUri},
    USubscriptionConfiguration,
};

// This is the core business logic for handling and tracking subscriptions. It is implemented as a single event-consuming
// function `handle_message()`, which is supposed to be spawned into a task and process the various `Events` that it can receive
// via tokio mpsc channel. This design allows to forgo the use of any synhronization primitives on the subscription-tracking container
// data types, as any access is coordinated/serialized via the Event selection loop.

// Remote-subscribe operation ttl; 5 minutes in milliseconds, as per https://github.com/eclipse-uprotocol/up-spec/tree/main/up-l3/usubscription/v3#6-timeout--retry-logic
pub(crate) const UP_REMOTE_TTL: u32 = 300000;

// Queue size of message channel for internal commands - like subscription status change messages or subscription expiration commands.
const INTERNAL_COMMAND_BUFFER_SIZE: usize = 128;

// Timeout to use when sending subscription removal command after a subscription has expired; if exceeded, subscription won't be removed
// directly but will be cleaned up at next startup.
const SUBSCRIPTION_EXPIRY_REMOVAL_TIMEOUT: Duration = Duration::from_secs(5);

// This is the 'outside API' of subscription manager, it includes some events that are only to be used in (and only enabled for) testing.
#[derive(Debug)]
pub(crate) enum SubscriptionEvent {
    AddSubscription {
        subscriber: SubscriberUUri,
        topic: TopicUUri,
        expiration: Option<SystemTime>,
        sample_period: Option<Duration>,
        respond_to: oneshot::Sender<SubscriptionStatus>,
    },
    RemoveSubscription {
        subscriber: SubscriberUUri,
        topic: TopicUUri,
        respond_to: oneshot::Sender<SubscriptionStatus>,
    },
    FetchSubscriptions {
        subscriber_filter: Option<UUri>,
        topic_filter: Option<UUri>,
        respond_to: oneshot::Sender<Vec<SubscriptionInfo>>,
    },
    Reset {
        respond_to: oneshot::Sender<()>,
    },
    // Purely for use during testing: get copy of current topic-subscriper ledger
    #[cfg(test)]
    GetTopicSubscribers {
        respond_to: oneshot::Sender<Vec<SubscriptionInfo>>,
    },
    // Purely for use during testing: force-set new topic-subscriber ledger
    #[cfg(test)]
    SetTopicSubscribers {
        topic_subscribers_replacement: Vec<SubscriptionInfo>,
        respond_to: oneshot::Sender<()>,
    },
    // Purely for use during testing: get copy of current topic-subscriper ledger
    #[cfg(test)]
    GetRemoteTopics {
        respond_to: oneshot::Sender<HashMap<TopicUUri, SubscriptionStatus>>,
    },
    // Purely for use during testing: force-set new topic-subscriber ledger
    #[cfg(test)]
    SetRemoteTopics {
        topic_subscribers_replacement: HashMap<TopicUUri, SubscriptionStatus>,
        respond_to: oneshot::Sender<()>,
    },
    // Purely for use during testing: get internal remote-subscription-change command sender
    #[cfg(test)]
    GetRemoteSubscriptionChangeSender {
        respond_to: oneshot::Sender<Sender<InternalSubscriptionEvent>>,
    },
}

// Internal subscription manager API - used to state-update remote subscriptions and expunge expired subscriptions
#[derive(Debug)]
pub(crate) enum InternalSubscriptionEvent {
    TopicStateUpdate {
        topic: TopicUUri,
        state: SubscriptionStatus,
    },
    RemoveExpiredSubscription {
        subscriber: SubscriberUUri,
        topic: TopicUUri,
    },
}

// Wrapper type, bundle all kinds of action-events that subscription manager supports
enum Event {
    LocalSubscription(SubscriptionEvent),
    RemoteSubscription(InternalSubscriptionEvent),
}

/// Core business logic of subscription management - also holds tracked local and remote subscriptions.
/// Interfacing with this purely happens via channels, so we do not have to deal with Mutexes/RwLocks.
///
/// # Arguments
///
/// * `configuration` - Configuration information, for own Uri and persistency storage properties.
/// * `rpc_client` - Used to perform Subscribe and Unsubscribe calls to remote uSubscription service instances.
/// * `command_receiver` - This is where the service outside API handlers post the subscription operation commands to.
/// * `notification_sender` - Channel to the Notification manager, for issuing notification-related commands.
/// * `shutdown` - When receiving a notification, will shutdown the handler loop.
// [impl->req~usubscription-unsubscribe-notifications~1]
// [impl->dsn~usubscription-state-machine~1]
pub(crate) async fn handle_message(
    configuration: Arc<USubscriptionConfiguration>,
    rpc_client: Arc<dyn RpcClient>,
    mut command_receiver: Receiver<SubscriptionEvent>,
    notification_sender: Sender<NotificationEvent>,
    shutdown: Arc<Notify>,
) {
    // track subscriber-topic relations - if in this list, a subscriber is considered to be SUBSCRIBED to the topic
    // (otherwise subscriber-topic relationship is UNSUBSCRIBED)
    // [impl->req~usubscription-subscribe-persistency~1]
    let mut subscriptions = persistency::SubscriptionsStore::new(&configuration)
        .inspect_err(|e| {
            panic!("Cannot set up persistency for subscription tracking {e}");
        })
        .unwrap();

    // for remote topics, we need to additionally deal with _PENDING states, this tracks these topics
    let mut remote_topics = persistency::RemoteTopicsStore::new(&configuration)
        .inspect_err(|e| {
            panic!("Cannot set up persistency for remote subscription tracking {e}");
        })
        .unwrap();

    let (internal_cmd_sender, mut internal_cmd_receiver) =
        mpsc::channel::<InternalSubscriptionEvent>(INTERNAL_COMMAND_BUFFER_SIZE);

    // At startup, set up timed unsubscribe for any persisted subscriptions that define an expiration time
    // and expunge the ones where expiration is already in the past.
    // [impl->req~usubscription-subscribe-expiration~1]
    // [impl->req~usubscription-subscribe-no-expiration~1]
    match subscriptions.get_and_prune_expiring_subscriptions() {
        Ok(list) => {
            for (subscriber, topic, expiration_millis) in list {
                schedule_unsubscribe(
                    expiration_millis,
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
            // "Outside" events - operations requested from the public service API
            event = command_receiver.recv() => match event {
                None => {
                    error!("Problem with subscription command channel, received None-event");
                    break
                },
                Some(event) => Event::LocalSubscription(event),
            },
            // "Inside" events - updates related to remote subscription state changes
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
            // deal with public API driven interactions (the actual usubscription service functionality)
            Event::LocalSubscription(event) => match event {
                SubscriptionEvent::AddSubscription {
                    subscriber,
                    topic,
                    expiration,
                    sample_period,
                    respond_to,
                } => {
                    // [impl->req~usubscription-subscribe~1]
                    match add_subscription(
                        configuration.clone(),
                        rpc_client.clone(),
                        internal_cmd_sender.clone(),
                        &mut subscriptions,
                        &mut remote_topics,
                        subscriber.clone(),
                        topic.clone(),
                        expiration,
                        sample_period,
                    ) {
                        // [impl->req~usubscription-subscribe-notifications~1]
                        // [impl->dsn~usubscription-change-notification-update~1]
                        Ok(result) => {
                            // Send topic state change notification
                            notification_manager::notify_state_change(
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
                    // [impl->req~usubscription-unsubscribe~1]
                    match remove_subscription(
                        configuration.clone(),
                        rpc_client.clone(),
                        internal_cmd_sender.clone(),
                        &mut subscriptions,
                        &mut remote_topics,
                        subscriber.clone(),
                        topic.clone(),
                    ) {
                        Ok(result) => {
                            // Send topic state change notification
                            // [impl->dsn~usubscription-change-notification-update~1]
                            notification_manager::notify_state_change(
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
                SubscriptionEvent::FetchSubscriptions {
                    subscriber_filter,
                    topic_filter,
                    respond_to,
                } => match fetch_subscriptions(
                    &subscriptions,
                    &remote_topics,
                    &subscriber_filter,
                    &topic_filter,
                ) {
                    Ok(result) => {
                        if respond_to.send(result).is_err() {
                            error!("Problem with internal communication");
                        };
                    }
                    Err(e) => {
                        panic!("Persistency failure {e}")
                    }
                },
                SubscriptionEvent::Reset { respond_to } => {
                    reset(
                        &mut subscriptions,
                        &mut remote_topics,
                        notification_sender.clone(),
                    )
                    .await;
                    if respond_to.send(()).is_err() {
                        error!("Problem with internal communication");
                    };
                }
                #[cfg(test)]
                SubscriptionEvent::GetTopicSubscribers { respond_to } => {
                    match subscriptions.get_all_subscriptions() {
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
                } => {
                    subscriptions.set_data(topic_subscribers_replacement);
                    let _r = respond_to.send(());
                }
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
                } => {
                    remote_topics.set_data(remote_topics_replacement);
                    let _r = respond_to.send(());
                }
                #[cfg(test)]
                SubscriptionEvent::GetRemoteSubscriptionChangeSender { respond_to } => {
                    let _ = respond_to.send(internal_cmd_sender.clone());
                }
            },
            // deal with feedback/state updates from the remote subscription handlers
            Event::RemoteSubscription(event) => match event {
                InternalSubscriptionEvent::TopicStateUpdate { topic, state } => {
                    match remote_topics.set_topic_state(&topic, state.clone()) {
                        Ok(_) => {
                            // We're supposed to send topic change notifications to any subscribers of topic
                            // [impl->dsn~usubscription-change-notification-update~1]
                            if let Ok(subscribers) = subscriptions.get_topic_subscribers(&topic) {
                                let topic_clone = topic.clone();
                                let notification_sender_clone = notification_sender.clone();
                                // Want to do this out off the main control flow
                                helpers::spawn_and_log_error(async move {
                                    for subscriber in subscribers {
                                        notification_manager::notify_state_change(
                                            notification_sender_clone.clone(),
                                            Some(subscriber),
                                            topic_clone.clone(),
                                            state.clone(),
                                        )
                                        .await;
                                    }
                                    Ok(())
                                });
                            } else {
                                warn!("Failed to send topic state change update notification to topic subscribers");
                            }
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
                        rpc_client.clone(),
                        internal_cmd_sender.clone(),
                        &mut subscriptions,
                        &mut remote_topics,
                        subscriber.clone(),
                        topic.clone(),
                    ) {
                        Ok(result) => {
                            // Send topic state change notification
                            // [impl->dsn~usubscription-change-notification-update~1]
                            notification_manager::notify_state_change(
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
    rpc_client: Arc<dyn RpcClient>,
    internal_cmd_sender: Sender<InternalSubscriptionEvent>,
    topic_subscribers: &mut persistency::SubscriptionsStore,
    remote_topics: &mut persistency::RemoteTopicsStore,
    subscriber: SubscriberUUri,
    topic: TopicUUri,
    expiration: Option<SystemTime>,
    sample_period: Option<Duration>,
) -> Result<SubscriptionStatus, persistency::PersistencyError> {
    let _ = topic_subscribers.add_subscription(&subscriber, &topic, expiration, sample_period)?;

    // For REMOTE topics, we explicitly track state due to _PENDING scenarios
    // [impl->req~usubscription-subscribe-multiple~1]
    let state = if topic.is_remote_authority(&uri_provider.get_authority()) {
        let state = remote_topics.add_topic_or_get_status(&topic)?;

        // if this remote topic is not yet SUBSCRIBED, perform remote subscription
        // [impl->req~usubscription-subscribe-remote~1]
        // [impl->req~usubscription-subscribe-unsubscribe-pending~1]
        if state != SubscriptionStatus::Subscribed {
            let topic_clone = topic.clone();
            let internal_cmd_sender_clone = internal_cmd_sender.clone();
            // we can only usefully instantiate a RpcClientUSubscription here, as this is the first time we know the relevant remote authority
            let subscription_client = Arc::new(RpcClientUSubscription::new(
                rpc_client.clone(),
                Some(topic.authority_name().to_string()),
                Some(CallOptions::for_rpc_request(
                    UP_REMOTE_TTL,
                    None,
                    None,
                    None,
                )),
            ));
            helpers::spawn_and_log_error(async move {
                remote_subscribe(
                    topic_clone,
                    expiration,
                    sample_period,
                    subscription_client,
                    internal_cmd_sender_clone,
                )
                .await?;
                Ok(())
            });
        }
        state
    } else {
        // Otherwise, LOCAL topics are considered to have status SUBSCRIBED as soon as they are registered here
        SubscriptionStatus::Subscribed
    };

    // Set up timed unsubscribe in case expiration timestamp is set
    // [impl->req~usubscription-subscribe-expiration~1]
    // [impl->req~usubscription-subscribe-no-expiration~1]
    if let Some(expiration) = expiration {
        schedule_unsubscribe(expiration, subscriber, topic, internal_cmd_sender);
    };

    Ok(state)
}

// Remove a subscription relationship to bookkeeping, initiate remote unsubscribe request if neccessary
fn remove_subscription(
    uri_provider: Arc<dyn LocalUriProvider>,
    rpc_client: Arc<dyn RpcClient>,
    internal_cmd_sender: Sender<InternalSubscriptionEvent>,
    topic_subscribers: &mut persistency::SubscriptionsStore,
    remote_topics: &mut persistency::RemoteTopicsStore,
    subscriber: SubscriberUUri,
    topic: TopicUUri,
) -> Result<SubscriptionStatus, persistency::PersistencyError> {
    // if this was the last subscriber to topic and topic is remote
    // [impl->req~usubscription-unsubscribe-last-remote~1]
    // [impl->req~usubscription-unsubscribe-subscribe-pending~1]
    if topic_subscribers.remove_subscription(&subscriber, &topic)?
        && topic.is_remote_authority(&uri_provider.get_authority())
    {
        // set remote topic state tracker to UNSUBSCRIBE_PENDING (until remote ubsubscribe confirmed)
        let _r = remote_topics.set_topic_state(&topic, SubscriptionStatus::UnsubscribePending)?;
        let subscription_client = Arc::new(RpcClientUSubscription::new(
            rpc_client.clone(),
            Some(topic.authority_name().to_string()),
            Some(CallOptions::for_rpc_request(
                UP_REMOTE_TTL,
                None,
                None,
                None,
            )),
        ));

        // perform remote unsubscription
        helpers::spawn_and_log_error(async move {
            remote_unsubscribe(topic, subscription_client, internal_cmd_sender).await?;
            Ok(())
        });
    }

    // [impl->req~usubscription-unsubscribe-multiple~1]
    // [impl->req~usubscription-unsubscribe-remote-unsubscribed~1]
    Ok(
        // Whatever happens with the remote topic state - as far as the local client is concerned, it has now UNSUBSCRIBED from this topic
        SubscriptionStatus::Unsubscribed,
    )
}

// Fetch all subscriptions of a topic or subscribers
fn fetch_subscriptions(
    topic_subscribers: &persistency::SubscriptionsStore,
    remote_topics: &persistency::RemoteTopicsStore,
    subscriber_filter: &Option<UUri>,
    topic_filter: &Option<UUri>,
) -> Result<Vec<SubscriptionInfo>, persistency::PersistencyError> {
    let mut subscriptions =
        topic_subscribers.get_subscriptions_by_filter(topic_filter, subscriber_filter)?;

    for entry in subscriptions.iter_mut() {
        // This will replace the subscription status of any remote subscription with associated remote-status,
        // or break out of the entire function in case of retrieval errors.
        if let Some(remote_status) = remote_topics.get_topic_state(entry.topic())? {
            *entry = SubscriptionInfo::new(
                entry.topic().clone(),
                entry.subscriber().clone(),
                remote_status,
                *entry.expiration(),
                *entry.min_sample_period(),
            );
        }
    }
    Ok(subscriptions)
}

// [impl->req~usubscription-reset~1]
async fn reset(
    topic_subscribers: &mut persistency::SubscriptionsStore,
    remote_topics: &mut persistency::RemoteTopicsStore,
    notification_sender: Sender<NotificationEvent>,
) {
    let mut notification_map: HashMap<TopicUUri, HashSet<SubscriberUUri>> = HashMap::new();
    let subscriptions = topic_subscribers
        .get_all_subscriptions()
        .unwrap_or_else(|e| {
            error!("Error resetting subscriptions list: {e}");
            Vec::new()
        });

    // 1. Collect all topic-subscriber combinations that we're tracking plus direct-notification clients, as we need to notify each about the reset
    for subscription in subscriptions {
        // first, add all regular topic-subscriber pairs to list of notification targets
        notification_map
            .entry(subscription.topic().clone())
            .or_default()
            .insert(subscription.subscriber().clone());
    }

    // 2. Reset/clear all stored subscriptions, remote subscriptions and notification-registrations
    // We just report errors for now - re-subscribing to existing things should do no harm, in case a reset did now work
    if let Err(e) = topic_subscribers.reset() {
        error!("Error resetting subscription data: {e}");
    }
    if let Err(e) = remote_topics.reset() {
        error!("Error resetting remote subscription data: {e}");
    }

    // 3. Notify every topic subscriber about the reset (all subscriptions are UNSUBSCRIBED now)
    //    Notification manager will use direct-notification client list for this, so we're not resetting that yet
    for (topic, notification_targets) in notification_map {
        for target in notification_targets {
            notification_manager::notify_state_change(
                notification_sender.clone(),
                Some(target.clone()),
                topic.clone(),
                SubscriptionStatus::Unsubscribed,
            )
            .await;
        }
    }

    // 4. Finally also reset direct-notification client list
    if let Err(e) = notification_manager::reset(notification_sender.clone()).await {
        error!("Error resetting direct-notification client data: {e}");
    }
}

// Perform remote topic subscription
async fn remote_subscribe(
    topic: TopicUUri,
    expiration: Option<SystemTime>,
    sample_period: Option<Duration>,
    subscription_client: Arc<dyn USubscription>,
    internal_cmd_sender: Sender<InternalSubscriptionEvent>,
) -> Result<(), UStatus> {
    // [impl->dsn~usubscription-subscribe-remote-subscriber-change~1]

    // send request
    // TODO: should we actually pass on expiration time to remote usubscription service? Or track (and unsubscribe) locally?
    // [impl->req~usubscription-remote-max-timeout~1]
    let subscribe_response = subscription_client
        .subscribe(&topic, expiration, sample_period)
        .await
        .map_err(|e| {
            UStatus::fail_with_code(
                UCode::Internal,
                format!("Error invoking remote subscription request: {e}"),
            )
        });

    // deal with response
    // [impl->req~usubscription-subscribe-remote-response~1]
    match subscribe_response {
        Ok(state) => {
            debug!("Got remote subscription response, state SUBSCRIBED");
            let _ = internal_cmd_sender
                .send(InternalSubscriptionEvent::TopicStateUpdate { topic, state })
                .await;
        }
        Err(status) => {
            debug!("Got {:?} remote unsubscribe response", status.get_code());
            return Err(UStatus::fail_with_code(
                status.get_code(),
                "Error during remote subscribe",
            ));
        }
    };

    Ok(())
}

// Perform remote topic unsubscription
async fn remote_unsubscribe(
    topic: TopicUUri,
    subscription_client: Arc<dyn USubscription>,
    internal_cmd_sender: Sender<InternalSubscriptionEvent>,
) -> Result<(), UStatus> {
    // [impl->dsn~usubscription-unsubscribe-remote-subscriber-change~1]

    // send request
    let unsubscribe_response = subscription_client.unsubscribe(&topic).await.map_err(|e| {
        UStatus::fail_with_code(
            UCode::Internal,
            format!("Error invoking remote unsubscribe request: {e}"),
        )
    });

    // deal with response
    match unsubscribe_response {
        Ok(()) => {
            debug!("Got OK remote unsubscribe response");
            let _ = internal_cmd_sender
                .send(InternalSubscriptionEvent::TopicStateUpdate {
                    topic,
                    state: SubscriptionStatus::Unsubscribed,
                })
                .await;
        }
        Err(status) => {
            debug!("Got {:?} remote unsubscribe response", status.get_code());
            return Err(UStatus::fail_with_code(
                status.get_code(),
                "Error during remote unsubscribe",
            ));
        }
    };

    Ok(())
}

// Internal helper that will spawn a task to unsubscribe a subscriber-topic relationship at timestamp expiration.
// In case the expiration timestamp is already in the past, unsubscribe will be initiated immediately.
// This is hopefully good enough for now - in case we get very many expiring subscriptions, might have
// to look for an approach that scales better.
fn schedule_unsubscribe(
    expiration: SystemTime,
    subscriber: SubscriberUUri,
    topic: TopicUUri,
    sender: Sender<InternalSubscriptionEvent>,
) {
    helpers::spawn_and_log_error(async move {
        if let Ok(delay) = expiration.duration_since(SystemTime::now()) {
            sleep(delay).await;
        }

        sender
            .send_timeout(
                InternalSubscriptionEvent::RemoveExpiredSubscription { subscriber, topic },
                SUBSCRIPTION_EXPIRY_REMOVAL_TIMEOUT,
            )
            .await?;

        Ok(())
    });
}

#[cfg(test)]
mod tests {
    // These are tests just for the locally used helper functions of subscription manager. More complex and complete
    // tests of the susbcription manager business logic are located in tests/subscription_manager_tests.rs
    use super::*;
    use crate::test_lib::{self, mocks::MockRpcClientUSubscriptionMock};
    use tokio::time::{Duration, Instant};
    use up_rust::communication::SubscriptionStatus;

    // [utest->req~usubscription-subscribe-expiration~1]
    #[test_log::test(tokio::test)]
    async fn test_schedule_future_unsubscribe() {
        let (internal_cmd_sender, mut internal_cmd_receiver) =
            mpsc::channel::<InternalSubscriptionEvent>(INTERNAL_COMMAND_BUFFER_SIZE);

        let start: Instant = Instant::now();
        schedule_unsubscribe(
            SystemTime::now() + Duration::from_secs(1),
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

    // [utest->req~usubscription-subscribe-expiration~1]
    #[test_log::test(tokio::test)]
    async fn test_schedule_past_unsubscribe() {
        let (internal_cmd_sender, mut internal_cmd_receiver) =
            mpsc::channel::<InternalSubscriptionEvent>(INTERNAL_COMMAND_BUFFER_SIZE);

        let start = Instant::now();
        schedule_unsubscribe(
            SystemTime::now() - Duration::from_secs(1),
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

    #[test_log::test(tokio::test)]
    async fn test_remote_subscribe() {
        let expected_topic = test_lib::helpers::remote_topic1_uri();
        let expected_topic_clone = expected_topic.clone();
        let mut mock_client = MockRpcClientUSubscriptionMock::default();
        let _r = mock_client
            .expect_subscribe()
            .withf(move |topic, _exp, _sample_period| *topic == expected_topic_clone)
            .returning(|_, _, _| Ok(SubscriptionStatus::Subscribed));
        let (sender, mut receiver) =
            mpsc::channel::<InternalSubscriptionEvent>(INTERNAL_COMMAND_BUFFER_SIZE);

        // perform operation to test
        let result = remote_subscribe(
            expected_topic.clone(),
            None,
            None,
            Arc::new(mock_client),
            sender,
        )
        .await;

        // validate response
        assert!(result.is_ok());

        // validate event activity
        let response = receiver.recv().await;
        assert!(response.is_some());
        if let InternalSubscriptionEvent::TopicStateUpdate { topic, state } = response.unwrap() {
            assert_eq!(topic, expected_topic);
            assert_eq!(state, SubscriptionStatus::Subscribed);
        };
    }

    #[test_log::test(tokio::test)]
    async fn test_remote_unsubscribe() {
        let expected_topic = test_lib::helpers::remote_topic1_uri();
        let expected_topic_clone = expected_topic.clone();
        let mut mock_client = MockRpcClientUSubscriptionMock::default();
        let _r = mock_client
            .expect_unsubscribe()
            .withf(move |topic| *topic == expected_topic_clone)
            .returning(|_| Ok(()));
        let (sender, mut receiver) =
            mpsc::channel::<InternalSubscriptionEvent>(INTERNAL_COMMAND_BUFFER_SIZE);

        // perform operation to test
        let result: Result<(), UStatus> =
            remote_unsubscribe(expected_topic.clone(), Arc::new(mock_client), sender).await;

        // validate response
        assert!(result.is_ok());

        // validate event activity
        let response = receiver.recv().await;
        assert!(response.is_some());
        if let InternalSubscriptionEvent::TopicStateUpdate { topic, state } = response.unwrap() {
            assert_eq!(topic, expected_topic);
            assert_eq!(state, SubscriptionStatus::Unsubscribed);
        };
    }
}
