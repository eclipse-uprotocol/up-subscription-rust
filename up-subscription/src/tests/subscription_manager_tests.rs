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

// [utest->dsn~usubscription-state-machine~1]
#[cfg(test)]
mod tests {
    use protobuf::MessageFull;
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use test_case::test_case;
    use tokio::sync::{mpsc, mpsc::Sender, oneshot, Notify};

    use up_rust::{
        core::usubscription::{
            State, SubscriptionRequest, SubscriptionResponse, SubscriptionStatus,
            UnsubscribeRequest,
        },
        MockTransport, UCode, UStatus, UUri,
    };

    use crate::{
        configuration::DEFAULT_COMMAND_BUFFER_SIZE,
        helpers,
        notification_manager::NotificationEvent,
        persistency,
        subscription_manager::{
            handle_message, InternalSubscriptionEvent, RequestKind, SubscribersResponse,
            SubscriptionEntry, SubscriptionEvent, SubscriptionsResponse,
        },
        test_lib,
        usubscription::{ExpiryTimestamp, SubscriberUUri, TopicUUri},
        USubscriptionConfiguration,
    };

    // Simple subscription-manager-actor front-end to use for testing
    struct CommandSender {
        command_sender: Sender<SubscriptionEvent>,
        shutdowner: Arc<Notify>,
    }

    impl CommandSender {
        fn new() -> Self {
            let config = Arc::new(
                USubscriptionConfiguration::create(
                    test_lib::helpers::LOCAL_AUTHORITY.to_string(),
                    None,
                    None,
                    false,
                    None,
                )
                .unwrap(),
            );
            let transport_mock = MockTransport::default();
            let shutdown_notification = Arc::new(Notify::new());
            let (command_sender, command_receiver) =
                mpsc::channel::<SubscriptionEvent>(DEFAULT_COMMAND_BUFFER_SIZE);
            let (notification_sender, mut notification_receiver) =
                mpsc::channel::<NotificationEvent>(config.notification_command_buffer);

            // Spawn notification receiver task
            let shutdown_notification_cloned = shutdown_notification.clone();
            helpers::spawn_and_log_error(async move {
                loop {
                    tokio::select! {
                        Some(event) = notification_receiver.recv() => {
                            if let NotificationEvent::StateChange { subscriber, topic, status, respond_to } = event {
                               println!(
                                    "Change Notification received: {} - {} - {}",
                                    subscriber.unwrap().to_uri(true),
                                    topic.to_uri(true),
                                    status
                                );

                                let _ = respond_to.send(());
                            } else {
                                panic!("Expected a NotificationEvent::StateChange message, got something else")
                            }
                        },
                        _ = shutdown_notification_cloned.notified() => break,
                    };
                }
                Ok(())
            });

            let shutdown_notification_cloned = shutdown_notification.clone();
            helpers::spawn_and_log_error(async move {
                handle_message(
                    config.clone(),
                    Arc::new(transport_mock),
                    command_receiver,
                    notification_sender,
                    shutdown_notification_cloned,
                )
                .await;

                Ok(())
            });
            CommandSender {
                command_sender,
                shutdowner: shutdown_notification,
            }
        }

        // Allows configuration of expected invoke_method() calls from subscription manager (provide expected request and response for utransport mock)
        async fn new_with_expected_notifications(
            mut expected_notifications: Vec<NotificationEvent>,
        ) -> Self {
            let config = Arc::new(
                USubscriptionConfiguration::create(
                    test_lib::helpers::LOCAL_AUTHORITY.to_string(),
                    None,
                    None,
                    false,
                    None,
                )
                .unwrap(),
            );
            let transport_mock = MockTransport::default();
            let shutdown_notification = Arc::new(Notify::new());
            let (command_sender, command_receiver) =
                mpsc::channel::<SubscriptionEvent>(DEFAULT_COMMAND_BUFFER_SIZE);
            let (notification_sender, mut notification_receiver) =
                mpsc::channel::<NotificationEvent>(config.notification_command_buffer);

            // Spawn notification receiver task
            let shutdown_notification_cloned = shutdown_notification.clone();
            helpers::spawn_and_log_error(async move {
                loop {
                    tokio::select! {
                        Some(event) = notification_receiver.recv() => {
                            if let Some(pos) = expected_notifications.iter().position(|e| e == &event) {
                                if let NotificationEvent::StateChange { subscriber, status, topic, respond_to } = event {
                                    println!(
                                        "Change Notification received: {} - {} - {}",
                                        subscriber.unwrap_or_default().to_uri(true),
                                        topic.to_uri(true),
                                        status
                                    );
                                    let _ = respond_to.send(());
                                }

                                // Send ack back to test case that was providing the expected_notifications back channel
                                let matched = expected_notifications.remove(pos);
                                if let NotificationEvent::StateChange { respond_to, .. } = matched {
                                    let _ = respond_to.send(());
                                }
                            } else {
                                panic!("Received unexpected notification event: {event:?}");
                            }
                        },
                        _ = shutdown_notification_cloned.notified() => {
                            break;
                        },
                    };
                }
                Ok(())
            });

            // Spawn off subscription manager task
            let shutdown_notification_cloned = shutdown_notification.clone();
            helpers::spawn_and_log_error(async move {
                handle_message(
                    config.clone(),
                    Arc::new(transport_mock),
                    command_receiver,
                    notification_sender,
                    shutdown_notification_cloned,
                )
                .await;

                Ok(())
            });

            CommandSender {
                command_sender,
                shutdowner: shutdown_notification,
            }
        }

        // Allows configuration of expected invoke_method() calls from subscription manager (provide expected request and response for utransport mock)
        async fn new_with_client_options<R: MessageFull, S: MessageFull>(
            expected_request: R,
            expected_response: S,
        ) -> Self {
            let config = Arc::new(
                USubscriptionConfiguration::create(
                    test_lib::helpers::LOCAL_AUTHORITY.to_string(),
                    None,
                    None,
                    false,
                    None,
                )
                .unwrap(),
            );
            let shutdown_notification = Arc::new(Notify::new());

            let (command_sender, command_receiver) =
                mpsc::channel::<SubscriptionEvent>(DEFAULT_COMMAND_BUFFER_SIZE);

            let mock_transport = Arc::new(
                test_lib::mocks::utransport_mock_for_rpc(vec![(
                    expected_request,
                    expected_response,
                )])
                .await,
            );
            let (notification_sender, _) =
                mpsc::channel::<NotificationEvent>(config.notification_command_buffer);

            let shutdown_notification_cloned = shutdown_notification.clone();
            helpers::spawn_and_log_error(async move {
                handle_message(
                    config,
                    mock_transport,
                    command_receiver,
                    notification_sender,
                    shutdown_notification_cloned,
                )
                .await;
                Ok(())
            });

            CommandSender {
                command_sender,
                shutdowner: shutdown_notification,
            }
        }

        async fn shutdown(&self) {
            self.shutdowner.notify_waiters();
        }

        async fn subscribe(
            &self,
            topic: TopicUUri,
            subscriber: SubscriberUUri,
            expiry: Option<ExpiryTimestamp>,
        ) -> Result<SubscriptionStatus, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<SubscriptionStatus>();
            let command = SubscriptionEvent::AddSubscription {
                subscriber,
                topic,
                expiry,
                respond_to,
            };
            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn unsubscribe(
            &self,
            topic: TopicUUri,
            subscriber: SubscriberUUri,
        ) -> Result<SubscriptionStatus, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<SubscriptionStatus>();
            let command = SubscriptionEvent::RemoveSubscription {
                subscriber,
                topic,
                respond_to,
            };
            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn fetch_subscribers(
            &self,
            topic: TopicUUri,
            offset: Option<u32>,
        ) -> Result<SubscribersResponse, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<SubscribersResponse>();
            let command = SubscriptionEvent::FetchSubscribers {
                topic,
                offset,
                respond_to,
            };
            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn fetch_subscriptions(
            &self,
            request: RequestKind,
            offset: Option<u32>,
        ) -> Result<SubscriptionsResponse, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<SubscriptionsResponse>();
            let command = SubscriptionEvent::FetchSubscriptions {
                request,
                offset,
                respond_to,
            };
            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn get_topic_subscribers(
            &self,
        ) -> Result<persistency::SubscriptionSet, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<persistency::SubscriptionSet>();
            let command = SubscriptionEvent::GetTopicSubscribers { respond_to };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        #[allow(clippy::mutable_key_type)]
        async fn set_topic_subscribers(
            &self,
            topic_subscribers_replacement: persistency::SubscriptionSet,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            let command = SubscriptionEvent::SetTopicSubscribers {
                topic_subscribers_replacement,
                respond_to,
            };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn get_remote_topics(&self) -> Result<HashMap<TopicUUri, State>, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<HashMap<TopicUUri, State>>();
            let command = SubscriptionEvent::GetRemoteTopics { respond_to };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        #[allow(clippy::mutable_key_type)]
        async fn set_remote_topics(
            &self,
            remote_topics_replacement: HashMap<TopicUUri, State>,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            let command = SubscriptionEvent::SetRemoteTopics {
                topic_subscribers_replacement: remote_topics_replacement,
                respond_to,
            };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn get_remote_subcription_change_sender(
            &self,
        ) -> Result<Sender<InternalSubscriptionEvent>, Box<dyn Error>> {
            let (respond_to, receive_from) =
                oneshot::channel::<Sender<InternalSubscriptionEvent>>();
            let command = SubscriptionEvent::GetRemoteSubscriptionChangeSender { respond_to };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }
    }

    // [utest->req~usubscription-subscribe~1]
    // [utest->req~usubscription-subscribe-multiple~1]
    #[test_case(vec![(UUri::default(), UUri::default())]; "Default susbcriber-topic")]
    #[test_case(vec![(UUri::default(), UUri::default()), (UUri::default(), UUri::default())]; "Multiple default susbcriber-topic")]
    #[test_case(vec![(test_lib::helpers::local_topic1_uri(), test_lib::helpers::subscriber_uri1())]; "One susbcriber-topic")]
    #[test_case(vec![
         (test_lib::helpers::local_topic1_uri(), test_lib::helpers::subscriber_uri1()),
         (test_lib::helpers::local_topic1_uri(), test_lib::helpers::subscriber_uri1())
         ]; "Multiple identical susbcriber-topic combinations")]
    #[test_case(vec![
         (test_lib::helpers::local_topic1_uri(), test_lib::helpers::subscriber_uri1()),
         (test_lib::helpers::local_topic2_uri(), test_lib::helpers::subscriber_uri1()),
         (test_lib::helpers::local_topic1_uri(), test_lib::helpers::subscriber_uri2()),
         (test_lib::helpers::local_topic2_uri(), test_lib::helpers::subscriber_uri2())
         ]; "Multiple susbcriber-topic combinations")]
    #[tokio::test]
    async fn test_subscribe(topic_subscribers: Vec<(TopicUUri, SubscriberUUri)>) {
        helpers::init_once();
        let command_sender = CommandSender::new();

        // Prepare things
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        for (topic, subscriber) in topic_subscribers {
            desired_state
                .entry(topic.clone())
                .or_default()
                .insert(subscriber.clone(), None);

            // Operation to test
            let result = command_sender.subscribe(topic, subscriber, None).await;
            assert!(result.is_ok());

            // Verify operation result content
            assert_eq!(result.unwrap().state.unwrap(), State::SUBSCRIBED);
        }

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        assert_eq!(topic_subscribers.len(), desired_state.len());
        assert_eq!(topic_subscribers, desired_state);
    }

    // [utest->req~usubscription-subscribe-expiration~1]
    // [utest->req~usubscription-subscribe-no-expiration~1]
    #[tokio::test]
    async fn test_subscribe_with_expiry() {
        helpers::init_once();
        let command_sender = CommandSender::new();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Error getting now timestamp")
            .as_millis();

        // Prepare things
        let mut desired_state: Vec<(SubscriberUUri, TopicUUri, Option<ExpiryTimestamp>)> = vec![
            (
                test_lib::helpers::subscriber_uri1(),
                test_lib::helpers::local_topic1_uri(),
                None,
            ),
            (
                test_lib::helpers::subscriber_uri2(),
                test_lib::helpers::local_topic2_uri(),
                Some(1000),
            ),
            (
                test_lib::helpers::subscriber_uri3(),
                test_lib::helpers::local_topic2_uri(),
                Some(now + 1000),
            ),
        ];

        for (subscriber, topic, expiry) in desired_state.iter() {
            // Operation to test
            let result = command_sender
                .subscribe(topic.clone(), subscriber.clone(), *expiry)
                .await;
            assert!(result.is_ok());

            // Verify operation result content
            assert_eq!(result.unwrap().state.unwrap(), State::SUBSCRIBED);
        }

        // Verify iternal bookeeping
        let actual_subscribers = command_sender.get_topic_subscribers().await;
        assert!(actual_subscribers.is_ok());

        let flattened_subscribers: Vec<(SubscriberUUri, TopicUUri, Option<ExpiryTimestamp>)> =
            actual_subscribers
                .unwrap()
                .iter()
                .flat_map(|(outer_key, inner_map)| {
                    inner_map.iter().map(move |(inner_key, value)| {
                        (outer_key.clone(), inner_key.clone(), *value)
                    })
                })
                .collect();

        desired_state.remove(1); // Remote item that has expiry timestamp in the past, so hasn't been added by subscription manager
        assert_eq!(flattened_subscribers.len(), desired_state.len());

        for (topic, subscriber, expiry) in flattened_subscribers {
            assert!(desired_state.contains(&(subscriber, topic, expiry)));
        }
    }

    // [utest->req~usubscription-subscribe-remote~1]
    // [utest->req~usubscription-subscribe-remote-pending~1]
    // [utest->req~usubscription-subscribe-remote-response~1]
    #[test_case(test_lib::helpers::remote_topic1_uri(), State::SUBSCRIBE_PENDING; "Remote topic, remote state SUBSCRIBED_PENDING")]
    #[test_case(test_lib::helpers::remote_topic1_uri(), State::SUBSCRIBED; "Remote topic, remote state SUBSCRIBED")]
    #[tokio::test]
    async fn test_remote_subscribe(remote_topic: TopicUUri, remote_state: State) {
        helpers::init_once();

        // Prepare things
        let remote_subscription_request = SubscriptionRequest {
            topic: Some(remote_topic.clone()).into(),
            ..Default::default()
        };
        let remote_subscription_response = SubscriptionResponse {
            topic: Some(remote_topic.clone()).into(),
            status: Some(SubscriptionStatus {
                state: remote_state.into(),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        };
        let command_sender = CommandSender::new_with_client_options::<
            SubscriptionRequest,
            SubscriptionResponse,
        >(remote_subscription_request, remote_subscription_response)
        .await;

        // Operation to test
        let result = command_sender
            .subscribe(
                remote_topic.clone(),
                test_lib::helpers::subscriber_uri1(),
                None,
            )
            .await;
        assert!(result.is_ok());

        // Verify operation result content
        let subscription_status = result.unwrap();
        // Depending on timing of the various async operations involved in remote subscriptions and bookkeeping updates,
        // this might be SUBSCRIBE_PENDING or SUBSCRIBED
        assert!(
            subscription_status.state.unwrap() == State::SUBSCRIBE_PENDING
                || subscription_status.state.unwrap() == State::SUBSCRIBED
        );

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        assert_eq!(topic_subscribers.len(), 1);

        let remote_topics = command_sender.get_remote_topics().await;
        assert!(remote_topics.is_ok());
        #[allow(clippy::mutable_key_type)]
        let remote_topics = remote_topics.unwrap();
        assert_eq!(remote_topics.len(), 1);
        // Depending on timing of the various async operations involved in remote subscriptions and bookkeeping updates,
        // this might be SUBSCRIBE_PENDING or SUBSCRIBED
        assert!(
            *remote_topics.get(&remote_topic.clone()).unwrap() == State::SUBSCRIBE_PENDING
                || *remote_topics.get(&remote_topic.clone()).unwrap() == State::SUBSCRIBED
        );
    }

    // [utest->req~usubscription-subscribe-remote~1]
    // [utest->req~usubscription-unsubscribe-last-remote~1]
    #[tokio::test]
    async fn test_repeated_remote_subscribe() {
        helpers::init_once();

        // Prepare things
        let remote_topic = test_lib::helpers::remote_topic1_uri();
        let remote_subscription_request = SubscriptionRequest {
            topic: Some(remote_topic.clone()).into(),
            ..Default::default()
        };
        let remote_subscription_response = SubscriptionResponse {
            topic: Some(remote_topic.clone()).into(),
            status: Some(SubscriptionStatus {
                state: State::SUBSCRIBED.into(),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        };
        let command_sender = CommandSender::new_with_client_options::<
            SubscriptionRequest,
            SubscriptionResponse,
        >(remote_subscription_request, remote_subscription_response)
        .await;

        // Operation to test
        let result = command_sender
            .subscribe(
                remote_topic.clone(),
                test_lib::helpers::subscriber_uri1(),
                None,
            )
            .await;
        assert!(result.is_ok());

        let result = command_sender
            .subscribe(
                remote_topic.clone(),
                test_lib::helpers::subscriber_uri2(),
                None,
            )
            .await;
        assert!(result.is_ok());

        // Assert we have two local topic-subscriber entries...
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        let entry = topic_subscribers.get(&remote_topic);
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().len(), 2);

        // ... and one remote topic entry
        let remote_topics = command_sender.get_remote_topics().await;
        assert!(remote_topics.is_ok());
        #[allow(clippy::mutable_key_type)]
        let remote_topics = remote_topics.unwrap();
        assert_eq!(remote_topics.len(), 1);
    }

    // All subscribers for a topic unsubscribe
    // [utest->req~usubscription-unsubscribe~1]
    #[tokio::test]
    async fn test_final_unsubscribe() {
        helpers::init_once();
        let command_sender = CommandSender::new();

        // Prepare things
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic1_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1(), None);

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("Interaction with subscription handler broken");

        // Operation to test
        let result = command_sender
            .unsubscribe(
                test_lib::helpers::local_topic1_uri(),
                test_lib::helpers::subscriber_uri1(),
            )
            .await;
        assert!(result.is_ok());

        // Verify operation result content
        let subscription_status = result.unwrap();
        assert_eq!(subscription_status.state.unwrap(), State::UNSUBSCRIBED);

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        assert_eq!(topic_subscribers.len(), 0);
    }

    // Only some subscribers of a topic unsubscribe
    #[tokio::test]
    async fn test_partial_unsubscribe() {
        helpers::init_once();
        let command_sender = CommandSender::new();

        // Prepare things
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic1_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1(), None);
        entry.insert(test_lib::helpers::subscriber_uri2(), None);

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("Interaction with subscription handler broken");

        // Operation to test
        let result = command_sender
            .unsubscribe(
                test_lib::helpers::local_topic1_uri(),
                test_lib::helpers::subscriber_uri1(),
            )
            .await;
        assert!(result.is_ok());

        // Verify operation result content
        let subscription_status = result.unwrap();
        assert_eq!(subscription_status.state.unwrap(), State::UNSUBSCRIBED);

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        assert_eq!(topic_subscribers.len(), 1);
        assert_eq!(
            topic_subscribers
                .get(&test_lib::helpers::local_topic1_uri())
                .unwrap()
                .len(),
            1
        );
        assert!(topic_subscribers
            .get(&test_lib::helpers::local_topic1_uri())
            .unwrap()
            .contains_key(&test_lib::helpers::subscriber_uri2()));
    }

    // All subscribers for a remote topic unsubscribe
    // [utest->req~usubscription-unsubscribe-last-remote~1]
    #[tokio::test]
    async fn test_final_remote_unsubscribe() {
        helpers::init_once();
        let remote_topic = test_lib::helpers::remote_topic1_uri();

        // Prepare things
        let remote_unsubscribe_request = UnsubscribeRequest {
            topic: Some(remote_topic.clone()).into(),
            ..Default::default()
        };
        let remote_unsubscribe_response = UStatus {
            code: UCode::OK.into(),
            ..Default::default()
        };
        let command_sender = CommandSender::new_with_client_options::<UnsubscribeRequest, UStatus>(
            remote_unsubscribe_request,
            remote_unsubscribe_response,
        )
        .await;

        // set starting state
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state.entry(remote_topic.clone()).or_default();
        entry.insert(test_lib::helpers::subscriber_uri1(), None);

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("Interaction with subscription handler broken");

        #[allow(clippy::mutable_key_type)]
        let mut desired_remote_state: HashMap<TopicUUri, State> = HashMap::new();
        desired_remote_state.insert(remote_topic.clone(), State::SUBSCRIBED);
        command_sender
            .set_remote_topics(desired_remote_state)
            .await
            .expect("Interaction with subscription handler broken");

        // Operation to test
        let result = command_sender
            .unsubscribe(remote_topic.clone(), test_lib::helpers::subscriber_uri1())
            .await;
        assert!(result.is_ok());

        // Verify operation result content
        let subscription_status = result.unwrap();
        assert_eq!(
            subscription_status.state.unwrap(),
            // No matter what happens to the remove topic state, as far as the local client is concerned this is now an UNSUBSCRIBED topic
            State::UNSUBSCRIBED
        );

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        // We're expecting our local topic-subscriber tracker to be empty at this point
        assert_eq!(topic_subscribers.len(), 0);

        let remote_topics = command_sender.get_remote_topics().await;
        assert!(remote_topics.is_ok());
        #[allow(clippy::mutable_key_type)]
        let remote_topics = remote_topics.unwrap();
        // our remote topic status tracker should still track this topic, and...
        assert_eq!(remote_topics.len(), 1);

        let entry = remote_topics.get(&remote_topic);
        assert!(entry.is_some());
        let state = entry.unwrap();
        // Depending on timing of the various async operations involved in remote subscriptions and bookkeeping updates,
        // this might be UNSUBSCRIBE_PENDING or UNSUBSCRIBED
        assert!(*state == State::UNSUBSCRIBED || *state == State::UNSUBSCRIBE_PENDING);
    }

    // Some subscribers for a remote topic unsubscribe, but at least one subscriber is left
    // [utest->req~usubscription-unsubscribe-last-remote~1]
    // [utest->req~usubscription-unsubscribe-remote-unsubscribed~1]
    #[tokio::test]
    async fn test_partial_remote_unsubscribe() {
        helpers::init_once();
        let remote_topic = test_lib::helpers::remote_topic1_uri();

        // Prepare things - we're not expecting any remote-unsubscribe action in this case
        let command_sender = CommandSender::new();

        // set starting state
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state.entry(remote_topic.clone()).or_default();
        entry.insert(test_lib::helpers::subscriber_uri1(), None);
        entry.insert(test_lib::helpers::subscriber_uri2(), None);

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("Interaction with subscription handler broken");

        #[allow(clippy::mutable_key_type)]
        let mut desired_remote_state: HashMap<TopicUUri, State> = HashMap::new();
        desired_remote_state.insert(remote_topic.clone(), State::SUBSCRIBED);
        command_sender
            .set_remote_topics(desired_remote_state)
            .await
            .expect("Interaction with subscription handler broken");

        // Operation to test
        let result = command_sender
            .unsubscribe(remote_topic.clone(), test_lib::helpers::subscriber_uri1())
            .await;
        assert!(result.is_ok());

        // Verify operation result content
        let subscription_status = result.unwrap();
        assert_eq!(
            subscription_status.state.unwrap(),
            // this client immediately is getting UNSUBSCRIBED, no _PENDING, as for it the op is done
            State::UNSUBSCRIBED
        );

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        // We're expecting one of the two original subscribers to still be tracked at this point
        assert_eq!(topic_subscribers.len(), 1);

        let remote_topics = command_sender.get_remote_topics().await;
        assert!(remote_topics.is_ok());
        #[allow(clippy::mutable_key_type)]
        let remote_topics = remote_topics.unwrap();
        // our remote topic status tracker should still track this topic, and...
        assert_eq!(remote_topics.len(), 1);

        let entry = remote_topics.get(&remote_topic);
        assert!(entry.is_some());
        let state = entry.unwrap();
        // ... it should still be in state SUBSCRIBED, as there is still another subscriber left
        assert_eq!(*state, State::SUBSCRIBED);
    }

    // [utest->req~usubscription-subscribe-notifications~1]
    // [utest->dsn~usubscription-change-notification-update~1]
    #[tokio::test]
    async fn test_local_subscribe_notification() {
        helpers::init_once();

        // Prepare things
        let topic = test_lib::helpers::local_topic1_uri();
        let subscriber = test_lib::helpers::subscriber_uri1();
        let (respond_to, state_changed) = oneshot::channel::<()>();

        let expected_notification = NotificationEvent::StateChange {
            subscriber: subscriber.clone().into(),
            topic: topic.clone(),
            status: SubscriptionStatus {
                state: State::SUBSCRIBED.into(),
                ..Default::default()
            },
            respond_to,
        };

        let command_sender =
            CommandSender::new_with_expected_notifications(vec![expected_notification]).await;

        // Operation to test
        let result = command_sender.subscribe(topic, subscriber, None).await;
        assert!(result.is_ok());

        let _ = state_changed.await;
        command_sender.shutdown().await;
    }

    // [utest->dsn~usubscription-change-notification-update~1]
    #[tokio::test]
    async fn test_local_unsubscribe_notification() {
        helpers::init_once();

        // Prepare things
        // Prepare things
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic1_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1(), None);

        let topic = test_lib::helpers::local_topic1_uri();
        let subscriber = test_lib::helpers::subscriber_uri1();
        let (respond_to, state_changed) = oneshot::channel::<()>();

        let expected_notification = NotificationEvent::StateChange {
            subscriber: subscriber.clone().into(),
            topic: topic.clone(),
            status: SubscriptionStatus {
                state: State::UNSUBSCRIBED.into(),
                ..Default::default()
            },
            respond_to,
        };

        let command_sender =
            CommandSender::new_with_expected_notifications(vec![expected_notification]).await;

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("Interaction with subscription handler broken");

        // Operation to test
        let result = command_sender.unsubscribe(topic, subscriber).await;
        assert!(result.is_ok());

        let _ = state_changed.await;
        command_sender.shutdown().await;
    }

    // TODO: Let's see if this is actually covered by a requirement - otherwise it should go
    // #[tokio::test]
    // async fn test_remote_subscribe_notification() {
    //     helpers::init_once();

    //     // Prepare things
    //     let topic = test_lib::helpers::remote_topic1_uri();
    //     let (respond_to, state_changed) = oneshot::channel::<()>();

    //     let expected_notification = NotificationEvent::StateChange {
    //         subscriber: None,
    //         topic: topic.clone(),
    //         status: SubscriptionStatus {
    //             state: State::SUBSCRIBE_PENDING.into(),
    //             ..Default::default()
    //         },
    //         respond_to,
    //     };

    //     let command_sender =
    //         CommandSender::new_with_expected_notifications(vec![expected_notification]).await;

    //     let sender = command_sender
    //         .get_remote_subcription_change_sender()
    //         .await
    //         .expect("Error retrieving remote-subscription change event command channel");

    //     // Initiate notification event
    //     let _ = sender
    //         .send(InternalSubscriptionEvent::TopicStateUpdate {
    //             topic: topic.clone(),
    //             state: State::SUBSCRIBE_PENDING,
    //         })
    //         .await;

    //     // ensure that we have run through all the async layers and reached the notification assertion statements
    //     let _ = state_changed.await;
    //     command_sender.shutdown().await;
    // }

    // [utest->dsn~usubscription-change-notification-update~1]
    #[tokio::test]
    async fn test_state_change_notification() {
        helpers::init_once();

        // Prepare things
        let topic = test_lib::helpers::remote_topic1_uri();
        let subscriber = test_lib::helpers::subscriber_uri1();
        let (respond_to, state_changed) = oneshot::channel::<()>();

        let expected_notification = NotificationEvent::StateChange {
            subscriber: subscriber.clone().into(),
            topic: topic.clone(),
            status: SubscriptionStatus {
                state: State::UNSUBSCRIBE_PENDING.into(),
                ..Default::default()
            },
            respond_to,
        };

        let command_sender =
            CommandSender::new_with_expected_notifications(vec![expected_notification]).await;

        // We need a subscriber to topic, which we're subsequently expecting a state change notification to be sent to
        // let subscribers = HashMap<TopicUUri, HashMap<SubscriberUUri, Option<ExpiryTimestamp>>>::new();
        // set starting state
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state.entry(topic.clone()).or_default();
        entry.insert(subscriber.clone(), None);
        assert!(command_sender
            .set_topic_subscribers(desired_state)
            .await
            .is_ok());

        let sender = command_sender
            .get_remote_subcription_change_sender()
            .await
            .expect("Error retrieving remote-subscription change event command channel");

        // Initiate notification event
        let _ = sender
            .send(InternalSubscriptionEvent::TopicStateUpdate {
                topic: topic.clone(),
                state: State::UNSUBSCRIBE_PENDING,
            })
            .await;

        // ensure that we have run through all the async layers and reached the notification assertion statements
        let _ = state_changed.await;
        command_sender.shutdown().await;
    }

    // [utest->req~usubscription-fetch-subscribers~1]
    // [utest->req~usubscription-fetch-subscribers-has-more-records~1]
    // [utest->req~usubscription-fetch-subscribers-offset~1]
    #[test_case(None; "No offset")]
    #[test_case(Some(0); "Offset 0")]
    #[test_case(Some(1); "Offset 1")]
    #[test_case(Some(2); "Offset 2")]
    #[tokio::test]
    async fn test_fetch_subscribers(offset: Option<u32>) {
        helpers::init_once();
        let command_sender = CommandSender::new();

        // set starting state
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic1_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1(), None);
        entry.insert(test_lib::helpers::subscriber_uri2(), None);

        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic2_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1(), None);
        entry.insert(test_lib::helpers::subscriber_uri3(), None);

        command_sender
            .set_topic_subscribers(desired_state.clone())
            .await
            .expect("Interaction with subscription handler broken");

        // Prepare things
        let desired_topic = test_lib::helpers::local_topic1_uri();

        // Operation to test
        let result = command_sender
            .fetch_subscribers(desired_topic.clone(), offset)
            .await;
        assert!(result.is_ok());

        // Verify operation result
        let (fetch_subscribers_response, _has_more) = result.unwrap();
        assert_eq!(
            fetch_subscribers_response.len(),
            2 - (offset.unwrap_or(0) as usize)
        );

        for subscriber in fetch_subscribers_response {
            #[allow(clippy::mutable_key_type)]
            let expected_subscribers = desired_state.get(&desired_topic).unwrap();
            assert!(expected_subscribers.contains_key(&subscriber));
        }
    }

    // [utest->req~usubscription-fetch-subscriptions-by-subscriber~1]
    // [utest->req~usubscription-fetch-subscriptions-has-more-records~1]
    // [utest->req~usubscription-fetch-subscriptions-offset~1]
    #[test_case(None; "No offset")]
    #[test_case(Some(0); "Offset 0")]
    #[test_case(Some(1); "Offset 1")]
    #[test_case(Some(2); "Offset 2")]
    #[tokio::test]
    async fn test_fetch_subscriptions_by_subscriber(offset: Option<u32>) {
        helpers::init_once();
        let command_sender = CommandSender::new();

        // set starting state
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic1_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1(), None);
        entry.insert(test_lib::helpers::subscriber_uri2(), None);

        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic2_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1(), None);
        entry.insert(test_lib::helpers::subscriber_uri3(), None);

        command_sender
            .set_topic_subscribers(desired_state.clone())
            .await
            .expect("Error during testing/setup of subscription manager");

        // Prepare things
        let desired_subscriber = test_lib::helpers::subscriber_uri1();

        let mut expected_subscribers: Vec<(SubscriberUUri, TopicUUri)> = Vec::new();
        for (topic, subscribers) in desired_state.clone() {
            if subscribers.contains_key(&desired_subscriber) {
                if let Some((subscriber, _expiry)) = subscribers.get_key_value(&desired_subscriber)
                {
                    expected_subscribers.push((subscriber.clone(), topic));
                }
            }
        }

        // Operation to test
        let result = command_sender
            .fetch_subscriptions(RequestKind::Subscriber(desired_subscriber.clone()), offset)
            .await;
        assert!(result.is_ok());

        // Verify operation result
        let (fetch_subscriptions_response, _has_more) = result.unwrap();

        assert_eq!(
            fetch_subscriptions_response.len(),
            expected_subscribers.len() - (offset.unwrap_or(0) as usize),
        );

        for subscription in fetch_subscriptions_response {
            let pair = (subscription.subscriber, subscription.topic);
            assert!(expected_subscribers.contains(&pair));
        }
    }

    // [utest->req~usubscription-fetch-subscriptions-by-topic~1]
    // [utest->req~usubscription-fetch-subscriptions-has-more-records~1]
    // [utest->req~usubscription-fetch-subscriptions-offset~1]
    #[test_case(None; "No offset")]
    #[test_case(Some(0); "Offset 0")]
    #[test_case(Some(1); "Offset 1")]
    #[test_case(Some(2); "Offset 2")]
    #[tokio::test]
    async fn test_fetch_subscriptions_by_topic(offset: Option<u32>) {
        helpers::init_once();
        let command_sender = CommandSender::new();

        // set starting state
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic1_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1(), None);
        entry.insert(test_lib::helpers::subscriber_uri2(), None);

        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic2_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1(), None);
        entry.insert(test_lib::helpers::subscriber_uri3(), None);

        command_sender
            .set_topic_subscribers(desired_state.clone())
            .await
            .expect("Interaction with subscription handler broken");

        // Prepare things
        let desired_topic = test_lib::helpers::local_topic1_uri();

        #[allow(clippy::mutable_key_type)]
        let expected_subscribers = desired_state.get(&desired_topic).unwrap();

        // Operation to test
        let result = command_sender
            .fetch_subscriptions(RequestKind::Topic(desired_topic.clone()), offset)
            .await;
        assert!(result.is_ok());

        // Verify operation result
        let (fetch_subscriptions_response, _has_more) = result.unwrap();

        assert_eq!(
            fetch_subscriptions_response.len(),
            expected_subscribers.len() - (offset.unwrap_or(0) as usize)
        );

        for SubscriptionEntry {
            topic,
            subscriber,
            status: _,
        } in fetch_subscriptions_response
        {
            assert_eq!(topic, desired_topic);
            assert!(expected_subscribers.contains_key(&subscriber));
        }
    }
}
