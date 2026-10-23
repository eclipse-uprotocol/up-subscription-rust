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
    use std::{
        collections::HashMap,
        error::Error,
        sync::Arc,
        time::{Duration, SystemTime},
        vec,
    };
    use test_case::test_case;
    use tokio::sync::{mpsc, mpsc::Sender, oneshot, Notify};
    use tracing::debug;

    use up_rust::{
        communication::{CallOptions, SubscriptionStatus, UPayload},
        core::usubscription::{
            usubscription_uri, SubscribeRequest, SubscribeResponse, SubscriptionInfo,
            UnsubscribeRequest,
        },
        MockTransport, UStatus, UUri,
    };

    use crate::{
        configuration::DEFAULT_COMMAND_BUFFER_SIZE,
        helpers,
        notification_manager::{self, NotificationEvent},
        subscription_manager::{self, InternalSubscriptionEvent, SubscriptionEvent, UP_REMOTE_TTL},
        test_lib::{self, helpers::*, mocks::MockRpcClientMock},
        usubscription::{SubscriberUUri, TopicUUri},
        USubscriptionConfiguration,
    };

    // Simple subscription-manager-actor front-end to use for testing
    struct CommandSender {
        command_sender: Sender<SubscriptionEvent>,
        notification_sender: Option<Sender<NotificationEvent>>,
        shutdowner: Arc<Notify>,
    }
    // Helper for keeping RpcClientMock setup data type simpler
    struct ExpectedInvocation {
        call_options: Option<CallOptions>,
        request_payload: Option<UPayload>,
        response_payload: Option<UPayload>,
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
            let rpc_client = Arc::new(MockRpcClientMock::default());
            let shutdown_notification = Arc::new(Notify::new());
            let (command_sender, command_receiver) =
                mpsc::channel::<SubscriptionEvent>(DEFAULT_COMMAND_BUFFER_SIZE.into());
            let (notification_sender, mut notification_receiver) =
                mpsc::channel::<NotificationEvent>(config.notification_command_buffer.into());

            // Spawn notification receiver task
            let shutdown_notification_cloned = shutdown_notification.clone();
            helpers::spawn_and_log_error(async move {
                loop {
                    tokio::select! {
                        Some(event) = notification_receiver.recv() => {
                            if let NotificationEvent::StateChange { subscriber, topic, status, expiration: _, sample_period: _, respond_to } = event {
                               debug!(
                                    "Change Notification received: {} - {} - {}",
                                    subscriber.to_uri(true),
                                    topic.to_uri(true),
                                    status
                                );

                                let _ = respond_to.send(());
                            }
                            else {
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
                subscription_manager::handle_message(
                    config.clone(),
                    rpc_client,
                    command_receiver,
                    notification_sender,
                    shutdown_notification_cloned,
                )
                .await;

                Ok(())
            });
            CommandSender {
                command_sender,
                notification_sender: None,
                shutdowner: shutdown_notification,
            }
        }

        // Allows configuration of expected notifications during test
        async fn new_with_notification_manager(
            expected_notifications: Vec<NotificationEvent>,
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
            let rpc_client = Arc::new(MockRpcClientMock::default());
            let shutdown_notification = Arc::new(Notify::new());
            let (command_sender, command_receiver) =
                mpsc::channel::<SubscriptionEvent>(DEFAULT_COMMAND_BUFFER_SIZE.into());
            let (notification_sender, notification_receiver) =
                mpsc::channel::<NotificationEvent>(config.notification_command_buffer.into());

            // Set up notification manager actor
            let mut transport = MockTransport::new();
            for expected_message in expected_notifications {
                match expected_message {
                    NotificationEvent::StateChange {
                        subscriber,
                        topic,
                        status,
                        expiration: _,
                        sample_period: _,
                        respond_to,
                    } => {
                        transport
                            .expect_do_send()
                            .withf(move |message| {
                                let payload = message
                                    .extract_protobuf::<SubscriptionInfo>()
                                    .expect("expect message deserialization to work");

                                subscriber.clone().eq(payload.subscriber())
                                    && topic.eq(payload.topic())
                                    && status.eq(payload.status())
                            })
                            .return_const({
                                // this is all a bit hacky, but good enough for now
                                let _r = respond_to.send(());
                                Ok(())
                            });
                    }
                    _ => {
                        panic!("not expecting this NotificationEvent variants ");
                    }
                }
            }

            let config_cloned = config.clone();
            let shutdown_notification_cloned = shutdown_notification.clone();
            helpers::spawn_and_log_error(async move {
                notification_manager::notification_engine(
                    config_cloned,
                    Arc::new(transport),
                    notification_receiver,
                    shutdown_notification_cloned,
                )
                .await;
                Ok(())
            });

            // Spawn off subscription manager task
            let notification_sender_cloned = notification_sender.clone();
            let shutdown_notification_cloned = shutdown_notification.clone();
            helpers::spawn_and_log_error(async move {
                subscription_manager::handle_message(
                    config.clone(),
                    rpc_client,
                    command_receiver,
                    notification_sender_cloned,
                    shutdown_notification_cloned,
                )
                .await;

                Ok(())
            });

            CommandSender {
                command_sender,
                notification_sender: Some(notification_sender),
                shutdowner: shutdown_notification,
            }
        }

        // Allows configuration of expected invoke_method() calls from subscription manager (provide expected request and response for RpcClient mock)
        // Useful e.g. for testing remote subscription operations, where subscription manager is expected to invoke methods on other uEntities
        // expected_invocations is a HashMap associating invoke-method UUris with tuples of (request-CallOptions, request-Payload, response-Payload)
        async fn new_with_rpcclient_mock(
            expected_invocations: HashMap<UUri, ExpectedInvocation>,
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
                mpsc::channel::<SubscriptionEvent>(DEFAULT_COMMAND_BUFFER_SIZE.into());

            let mut rpc_client = MockRpcClientMock::default();
            for (
                request_method,
                ExpectedInvocation {
                    call_options,
                    request_payload,
                    response_payload,
                },
            ) in expected_invocations
            {
                let request_options =
                    call_options.unwrap_or(CallOptions::for_rpc_request(5_000, None, None, None));

                rpc_client
                    .expect_invoke_method()
                    .withf(move |method, calloptions, payload| {
                        request_method.eq(method)
                            && request_options.eq(calloptions)
                            && request_payload.eq(payload)
                    })
                    .returning(move |_, _, _| Ok(response_payload.clone()));
            }

            let (notification_sender, _) =
                mpsc::channel::<NotificationEvent>(config.notification_command_buffer.into());

            let shutdown_notification_cloned = shutdown_notification.clone();
            helpers::spawn_and_log_error(async move {
                subscription_manager::handle_message(
                    config,
                    Arc::new(rpc_client),
                    command_receiver,
                    notification_sender,
                    shutdown_notification_cloned,
                )
                .await;
                Ok(())
            });

            CommandSender {
                command_sender,
                notification_sender: None,
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
            expiration: Option<SystemTime>,
            sample_period: Option<Duration>,
        ) -> Result<SubscriptionInfo, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<SubscriptionInfo>();
            let command = SubscriptionEvent::AddSubscription {
                subscriber,
                topic,
                expiration,
                sample_period,
                respond_to,
            };
            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn unsubscribe(
            &self,
            topic: TopicUUri,
            subscriber: SubscriberUUri,
        ) -> Result<SubscriptionInfo, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<SubscriptionInfo>();
            let command = SubscriptionEvent::RemoveSubscription {
                subscriber,
                topic,
                respond_to,
            };
            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn fetch_subscriptions(
            &self,
            subscriber_filter: Option<UUri>,
            topic_filter: Option<UUri>,
        ) -> Result<Vec<SubscriptionInfo>, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<Vec<SubscriptionInfo>>();
            let command = SubscriptionEvent::FetchSubscriptions {
                subscriber_filter,
                topic_filter,
                respond_to,
            };
            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn get_topic_subscribers(&self) -> Result<Vec<SubscriptionInfo>, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<Vec<SubscriptionInfo>>();
            let command = SubscriptionEvent::GetTopicSubscribers { respond_to };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn set_topic_subscribers(
            &self,
            topic_subscribers_replacement: Vec<SubscriptionInfo>,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            let command = SubscriptionEvent::SetTopicSubscribers {
                topic_subscribers_replacement,
                respond_to,
            };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn get_remote_topics(
            &self,
        ) -> Result<HashMap<TopicUUri, SubscriptionStatus>, Box<dyn Error>> {
            let (respond_to, receive_from) =
                oneshot::channel::<HashMap<TopicUUri, SubscriptionStatus>>();
            let command = SubscriptionEvent::GetRemoteTopics { respond_to };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn set_remote_topics(
            &self,
            remote_topics_replacement: HashMap<TopicUUri, SubscriptionStatus>,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            let command = SubscriptionEvent::SetRemoteTopics {
                topic_subscribers_replacement: remote_topics_replacement,
                respond_to,
            };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn set_notifyees(
            &self,
            notifyee_replacement: Vec<UUri>,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            let command = NotificationEvent::SetNotifyees {
                notifyee_replacement,
                respond_to,
            };
            self.notification_sender
                .as_ref()
                .expect("expect notification manager/sender channel to be set up")
                .send(command)
                .await?;
            Ok(receive_from.await?)
        }
        async fn reset(&self) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            let command = SubscriptionEvent::Reset { respond_to };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn topic_state_update(
            &self,
            topic: TopicUUri,
            state: SubscriptionStatus,
        ) -> Result<(), Box<dyn Error>> {
            let sender = self.get_remote_subcription_change_sender().await?;

            // let (respond_to, receive_from) = oneshot::channel::<()>();
            let command = InternalSubscriptionEvent::TopicStateUpdate { topic, state };
            sender.send(command).await?;

            Ok(())
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
    #[test_case(vec![(local_topic1_uri(), subscriber_uri1())]; "Default subscriber-topic")]
    #[test_case(vec![(local_topic1_uri(), subscriber_uri1()), (local_topic1_uri(), subscriber_uri1())]; "Multiple identical subscriber-topic combinations")]
    #[test_case(vec![
         (local_topic1_uri(), subscriber_uri1()), (local_topic1_uri(), subscriber_uri2()),
         (local_topic2_uri(), subscriber_uri1()), (local_topic2_uri(), subscriber_uri2())
         ]; "Multiple susbcriber-topic combinations")]
    #[test_log::test(tokio::test)]
    async fn test_subscribe(topic_subscribers: Vec<(TopicUUri, SubscriberUUri)>) {
        let command_sender = CommandSender::new();

        // Prepa re things
        let mut desired_state: HashMap<(SubscriberUUri, TopicUUri), SubscriptionInfo> =
            HashMap::new();

        for (topic, subscriber) in topic_subscribers {
            desired_state.insert(
                (subscriber.clone(), topic.clone()),
                SubscriptionInfo::new(
                    topic.clone(),
                    subscriber.clone(),
                    SubscriptionStatus::Subscribed,
                    None,
                    None,
                ),
            );

            // Operation to test
            let result = command_sender
                .subscribe(topic, subscriber, None, None)
                .await
                .expect("expecting message passing to work");

            // Verify operation result content
            assert_eq!(*result.status(), SubscriptionStatus::Subscribed);
        }

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        let topic_subscribers = topic_subscribers.unwrap();
        assert_eq!(topic_subscribers.len(), desired_state.len());
        assert!(desired_state
            .into_values()
            .collect::<Vec<_>>()
            .iter()
            .all(|x| topic_subscribers.contains(x)));
    }

    // [utest->req~usubscription-subscribe-expiration~1]
    // [utest->req~usubscription-subscribe-no-expiration~1]
    // [utest->req~usubscription-past-subscription-expiration~1]
    #[test_log::test(tokio::test)]
    async fn test_subscribe_with_expiration() {
        let command_sender = CommandSender::new();

        // Prepare things
        let mut desired_state: Vec<SubscriptionInfo> = vec![
            SubscriptionInfo::new(
                test_lib::helpers::subscriber_uri1(),
                test_lib::helpers::local_topic1_uri(),
                SubscriptionStatus::Subscribed,
                None, // subscription with no expiration property
                None,
            ),
            SubscriptionInfo::new(
                test_lib::helpers::subscriber_uri2(),
                test_lib::helpers::local_topic2_uri(),
                SubscriptionStatus::Subscribed,
                Some(SystemTime::now() + Duration::from_secs(10)), // yet to expire subscription
                None,
            ),
            SubscriptionInfo::new(
                test_lib::helpers::subscriber_uri3(),
                test_lib::helpers::local_topic2_uri(),
                SubscriptionStatus::Subscribed,
                Some(SystemTime::now() - Duration::from_secs(10)), // expired subscription
                None,
            ),
        ];

        for entry in desired_state.iter() {
            // Operation to test
            command_sender
                .subscribe(
                    entry.topic().clone(),
                    entry.subscriber().clone(),
                    *entry.expiration(),
                    *entry.min_sample_period(),
                )
                .await
                .expect("expecting internal message passing to work");
        }

        // Verify iternal bookeeping
        let actual_subscribers = command_sender
            .get_topic_subscribers()
            .await
            .expect("expecting internal data retrieval to work");
        desired_state.pop(); // Remote item that has expiry timestamp in the past, so hasn't been added by subscription manager

        assert_eq!(desired_state.len(), actual_subscribers.len());
        assert!(desired_state.iter().all(|x| actual_subscribers.contains(x)));
    }

    // [utest->req~usubscription-subscribe-expiration-extension~1]
    #[test_log::test(tokio::test)]
    async fn test_update_expiration() {
        let command_sender = CommandSender::new();

        // set up initial state
        let result = command_sender
            .subscribe(
                test_lib::helpers::local_topic1_uri(),
                test_lib::helpers::subscriber_uri1(),
                Some(SystemTime::now() + Duration::from_secs(10)),
                None,
            )
            .await
            .expect("expecting internal message passing to work");
        assert_eq!(*result.status(), SubscriptionStatus::Subscribed);

        let new_expiration = SystemTime::now() + Duration::from_secs(100);
        let result = command_sender
            .subscribe(
                test_lib::helpers::local_topic1_uri(),
                test_lib::helpers::subscriber_uri1(),
                Some(new_expiration),
                None,
            )
            .await
            .expect("expecting internal message passing to work");

        // Verify operation result content
        assert_eq!(*result.status(), SubscriptionStatus::Subscribed);
        let subs = command_sender
            .fetch_subscriptions(None, None)
            .await
            .expect("expecting internal message passing to work");
        assert_eq!(subs.len(), 1);
        assert_eq!(
            new_expiration,
            subs.first()
                .expect("expecting one subscription entry")
                .expiration()
                .expect("expecting a subscription expiration entry")
        );
    }

    // [utest->req~usubscription-past-subscription-expiration~1]
    #[test_log::test(tokio::test)]
    async fn test_update_expiration_to_past_timestamp() {
        let command_sender = CommandSender::new();

        // set up initial state
        let result = command_sender
            .subscribe(
                test_lib::helpers::local_topic1_uri(),
                test_lib::helpers::subscriber_uri1(),
                Some(SystemTime::now() + Duration::from_secs(10)), // initial subscription has a future expiration timestamp
                None,
            )
            .await
            .expect("expecting internal message passing to work");
        assert_eq!(*result.status(), SubscriptionStatus::Subscribed);

        let result = command_sender
            .subscribe(
                test_lib::helpers::local_topic1_uri(),
                test_lib::helpers::subscriber_uri1(),
                Some(SystemTime::now() - Duration::from_secs(10)), // now set expiration timestamp into the past
                None,
            )
            .await
            .expect("expecting internal message passing to work");

        // Verify operation result content
        assert_eq!(*result.status(), SubscriptionStatus::Unsubscribed);
        let subs = command_sender
            .fetch_subscriptions(None, None)
            .await
            .expect("expecting internal message passing to work");
        assert_eq!(subs.len(), 0);
    }

    // [utest->req~usubscription-subscribe-remote~1]
    // [utest->req~usubscription-subscribe-remote-pending~1]
    // [utest->req~usubscription-subscribe-remote-response~1]
    #[test_case(test_lib::helpers::remote_topic1_uri(), SubscriptionStatus::SubscribePending; "Remote topic, remote state SubscribePending")]
    #[test_case(test_lib::helpers::remote_topic1_uri(), SubscriptionStatus::Subscribed; "Remote topic, remote state Subscribed")]
    #[test_log::test(tokio::test)]
    async fn test_remote_subscribe(remote_topic: TopicUUri, remote_state: SubscriptionStatus) {
        // Prepare things
        let remote_subscribe_request = SubscribeRequest {
            topic: remote_topic.clone(),
            expiration: None,
            sample_period: None,
        };
        let remote_subscribe_response = SubscribeResponse {
            topic: remote_topic.clone(),
            status: remote_state,
        };

        // prepare data for RpcMock for CommandSender
        let mut expected_invocations: HashMap<UUri, ExpectedInvocation> = HashMap::new();
        expected_invocations.insert(
            usubscription_uri(
                Some(remote_topic.authority_name()),
                up_rust::core::usubscription::RESOURCE_ID_SUBSCRIBE,
            ),
            ExpectedInvocation {
                call_options: None,
                request_payload: Some(
                    UPayload::try_from_protobuf(remote_subscribe_request)
                        .expect("expecting payload serialization to work"),
                ),
                response_payload: Some(
                    UPayload::try_from_protobuf(remote_subscribe_response)
                        .expect("expecting payload serialization to work"),
                ),
            },
        );
        let command_sender = CommandSender::new_with_rpcclient_mock(expected_invocations).await;

        // Operation to test
        let result = command_sender
            .subscribe(
                remote_topic.clone(),
                test_lib::helpers::subscriber_uri1(),
                None,
                None,
            )
            .await
            .expect("expecting message passing to work");

        // Verify operation result content
        // Depending on timing of the various async operations involved in remote subscriptions and bookkeeping updates,
        // this might be SUBSCRIBE_PENDING or SUBSCRIBED
        assert!(
            *result.status() == SubscriptionStatus::SubscribePending
                || *result.status() == SubscriptionStatus::Subscribed
        );

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        let topic_subscribers = topic_subscribers.unwrap();
        assert_eq!(topic_subscribers.len(), 1);

        let remote_topics = command_sender.get_remote_topics().await;
        assert!(remote_topics.is_ok());
        let remote_topics = remote_topics.unwrap();
        assert_eq!(remote_topics.len(), 1);
        // Depending on timing of the various async operations involved in remote subscriptions and bookkeeping updates,
        // this might be SUBSCRIBE_PENDING or SUBSCRIBED
        assert!(
            *remote_topics.get(&remote_topic.clone()).unwrap()
                == SubscriptionStatus::SubscribePending
                || *remote_topics.get(&remote_topic.clone()).unwrap()
                    == SubscriptionStatus::Subscribed
        );
    }

    // [utest->req~usubscription-unsubscribe-subscribe-pending~1]
    #[test_log::test(tokio::test)]
    async fn test_remote_pending_unsubscribe() {
        // Prepare things
        let remote_topic = test_lib::helpers::remote_topic1_uri();
        let subscriber = test_lib::helpers::subscriber_uri1();

        // prepare CommandSender that accepts remote unsubscribe request
        let remote_unsubscribe_request = UnsubscribeRequest {
            topic: remote_topic.clone(),
        };
        let remote_unsubscribe_response = UStatus::ok();
        let mut expected_invocations: HashMap<UUri, ExpectedInvocation> = HashMap::new();
        expected_invocations.insert(
            usubscription_uri(
                Some(remote_topic.authority_name()),
                up_rust::core::usubscription::RESOURCE_ID_UNSUBSCRIBE,
            ),
            ExpectedInvocation {
                call_options: Some(CallOptions::for_rpc_request(
                    UP_REMOTE_TTL,
                    None,
                    None,
                    None,
                )),
                request_payload: Some(
                    UPayload::try_from_protobuf(remote_unsubscribe_request)
                        .expect("expecting payload serialization to work"),
                ),
                response_payload: Some(
                    UPayload::try_from_protobuf(remote_unsubscribe_response)
                        .expect("expecting payload serialization to work"),
                ),
            },
        );
        let command_sender = CommandSender::new_with_rpcclient_mock(expected_invocations).await;

        let mut remote_topics_replacement = HashMap::new();
        remote_topics_replacement
            .insert(remote_topic.clone(), SubscriptionStatus::SubscribePending);
        command_sender
            .set_remote_topics(remote_topics_replacement)
            .await
            .expect("expecting test setup preparation to work");

        command_sender
            .set_topic_subscribers(vec![SubscriptionInfo::new(
                remote_topic.clone(),
                subscriber.clone(),
                SubscriptionStatus::Subscribed,
                None,
                None,
            )])
            .await
            .expect("expecting internal message passing to work");

        // Operation to test
        let result = command_sender
            .unsubscribe(remote_topic.clone(), subscriber.clone())
            .await
            .expect("expecting internal data retrieval to work");

        // Depending on timing of the various async operations involved in remote subscriptions and bookkeeping updates,
        // this might be SUBSCRIBE_PENDING or SUBSCRIBED
        assert_eq!(*result.status(), SubscriptionStatus::Unsubscribed);

        // Verify iternal bookeeping
        let topic_subscribers = command_sender
            .get_topic_subscribers()
            .await
            .expect("expecting internal data retrieval to work");
        assert!(topic_subscribers.is_empty());

        let remote_topics = command_sender
            .get_remote_topics()
            .await
            .expect("expecting internal data retrieval to work");
        assert_eq!(remote_topics.len(), 1);
        // Depending on timing of the various async operations involved in remote subscriptions and bookkeeping updates,
        // this might be SUBSCRIBE_PENDING or SUBSCRIBED
        assert_eq!(
            remote_topics
                .get(&remote_topic)
                .expect("expecting a value for this key"),
            &SubscriptionStatus::UnsubscribePending
        );
    }

    // [utest->req~usubscription-subscribe-remote~1]
    // [utest->req~usubscription-unsubscribe-last-remote~1]
    #[test_log::test(tokio::test)]
    async fn test_repeated_remote_subscribe() {
        // Prepare things
        let remote_topic = test_lib::helpers::remote_topic1_uri();
        let remote_subscribe_request = SubscribeRequest {
            topic: remote_topic.clone(),
            expiration: None,
            sample_period: None,
        };
        let remote_subscribe_response = SubscribeResponse {
            topic: remote_topic.clone(),
            status: SubscriptionStatus::Subscribed,
        };

        // prepare data for RpcMock for CommandSender
        let mut expected_invocations: HashMap<UUri, ExpectedInvocation> = HashMap::new();
        expected_invocations.insert(
            usubscription_uri(
                Some(remote_topic.authority_name()),
                up_rust::core::usubscription::RESOURCE_ID_SUBSCRIBE,
            ),
            ExpectedInvocation {
                call_options: None,
                request_payload: Some(
                    UPayload::try_from_protobuf(remote_subscribe_request)
                        .expect("expecting payload serialization to work"),
                ),
                response_payload: Some(
                    UPayload::try_from_protobuf(remote_subscribe_response)
                        .expect("expecting payload serialization to work"),
                ),
            },
        );
        let command_sender = CommandSender::new_with_rpcclient_mock(expected_invocations).await;

        // Operations to test
        let result = command_sender
            .subscribe(
                remote_topic.clone(),
                test_lib::helpers::subscriber_uri1(),
                None,
                None,
            )
            .await;
        assert!(result.is_ok());

        let result = command_sender
            .subscribe(
                remote_topic.clone(),
                test_lib::helpers::subscriber_uri2(),
                None,
                None,
            )
            .await;
        assert!(result.is_ok());

        // Assert we have two local topic-subscriber entries...
        let topic_subscribers = command_sender
            .get_topic_subscribers()
            .await
            .expect("expecting internal data retrieval to work");
        assert_eq!(topic_subscribers.len(), 2);

        // ... and one remote topic entry
        let remote_topics = command_sender
            .get_remote_topics()
            .await
            .expect("expecting internal data retrieval to work");
        assert_eq!(remote_topics.len(), 1);
    }

    // All subscribers for a topic unsubscribe
    // [utest->req~usubscription-unsubscribe~1]
    #[test_log::test(tokio::test)]
    async fn test_final_unsubscribe() {
        let command_sender = CommandSender::new();

        // Prepare things
        let desired_state: Vec<SubscriptionInfo> = vec![SubscriptionInfo::new(
            test_lib::helpers::local_topic1_uri(),
            test_lib::helpers::subscriber_uri1(),
            SubscriptionStatus::Subscribed,
            None,
            None,
        )];

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("expecting internal message passing to work");

        // Operation to test
        let result = command_sender
            .unsubscribe(
                test_lib::helpers::local_topic1_uri(),
                test_lib::helpers::subscriber_uri1(),
            )
            .await
            .expect("expecting internal message passing to work");

        // Verify operation result content
        assert_eq!(*result.status(), SubscriptionStatus::Unsubscribed);

        // Verify iternal bookeeping
        let topic_subscribers = command_sender
            .get_topic_subscribers()
            .await
            .expect("expecting internal data retrieval to work");
        assert_eq!(topic_subscribers.len(), 0);
    }

    // Only some subscribers of a topic unsubscribe
    #[test_log::test(tokio::test)]
    async fn test_partial_unsubscribe() {
        let command_sender = CommandSender::new();

        // Prepare things
        let desired_state: Vec<SubscriptionInfo> = vec![
            SubscriptionInfo::new(
                test_lib::helpers::local_topic1_uri(),
                test_lib::helpers::subscriber_uri1(),
                SubscriptionStatus::Subscribed,
                None,
                None,
            ),
            SubscriptionInfo::new(
                test_lib::helpers::local_topic1_uri(),
                test_lib::helpers::subscriber_uri2(),
                SubscriptionStatus::Subscribed,
                None,
                None,
            ),
        ];

        command_sender
            .set_topic_subscribers(desired_state.clone())
            .await
            .expect("expecting internal message passing to work");

        // Operation to test
        let result = command_sender
            .unsubscribe(
                test_lib::helpers::local_topic1_uri(),
                test_lib::helpers::subscriber_uri1(),
            )
            .await
            .expect("expecting internal message passing to work");

        // Verify operation result content
        assert_eq!(*result.status(), SubscriptionStatus::Unsubscribed);

        // Verify iternal bookeeping
        let topic_subscribers = command_sender
            .get_topic_subscribers()
            .await
            .expect("expecting internal data retrieval to work");
        assert_eq!(topic_subscribers.len(), 1);

        assert_eq!(topic_subscribers[0], desired_state[1]);
    }

    // All subscribers for a remote topic unsubscribe
    // [utest->req~usubscription-unsubscribe-last-remote~1]
    #[test_log::test(tokio::test)]
    async fn test_final_remote_unsubscribe() {
        let remote_topic = test_lib::helpers::remote_topic1_uri();

        // Prepare things
        let remote_unsubscribe_request = UnsubscribeRequest {
            topic: remote_topic.clone(),
        };
        let remote_unsubscribe_response = UStatus::ok();

        // prepare data for RpcMock for CommandSender
        let mut expected_invocations: HashMap<UUri, ExpectedInvocation> = HashMap::new();
        expected_invocations.insert(
            usubscription_uri(
                Some(remote_topic.authority_name()),
                up_rust::core::usubscription::RESOURCE_ID_UNSUBSCRIBE,
            ),
            ExpectedInvocation {
                call_options: Some(CallOptions::for_rpc_request(
                    UP_REMOTE_TTL,
                    None,
                    None,
                    None,
                )),
                request_payload: Some(
                    UPayload::try_from_protobuf(remote_unsubscribe_request)
                        .expect("expecting payload serialization to work"),
                ),
                response_payload: Some(
                    UPayload::try_from_protobuf(remote_unsubscribe_response)
                        .expect("expecting payload serialization to work"),
                ),
            },
        );
        let command_sender = CommandSender::new_with_rpcclient_mock(expected_invocations).await;
        // set starting state
        let desired_state: Vec<SubscriptionInfo> = vec![SubscriptionInfo::new(
            remote_topic.clone(),
            test_lib::helpers::subscriber_uri1(),
            SubscriptionStatus::Subscribed,
            None,
            None,
        )];

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("expecting internal message passing to work");

        let mut desired_remote_state: HashMap<TopicUUri, SubscriptionStatus> = HashMap::new();
        desired_remote_state.insert(remote_topic.clone(), SubscriptionStatus::Subscribed);

        command_sender
            .set_remote_topics(desired_remote_state)
            .await
            .expect("expecting internal message passing to work");

        // Operation to test
        let result = command_sender
            .unsubscribe(remote_topic.clone(), test_lib::helpers::subscriber_uri1())
            .await
            .expect("expecting message passing to work");

        // Verify operation result content
        assert_eq!(
            *result.status(),
            // No matter what happens to the remove topic state, as far as the local client is concerned this is now an UNSUBSCRIBED topic
            SubscriptionStatus::Unsubscribed
        );

        // Verify iternal bookeeping
        let topic_subscribers = command_sender
            .get_topic_subscribers()
            .await
            .expect("expecting internal data retrieval to work");
        // We're expecting our local topic-subscriber tracker to be empty at this point
        assert_eq!(topic_subscribers.len(), 0);

        let remote_topics = command_sender
            .get_remote_topics()
            .await
            .expect("expecting internal data retrieval to work");
        // our remote topic status tracker should still track this topic, and...
        assert_eq!(remote_topics.len(), 1);

        let state = remote_topics
            .get(&remote_topic)
            .expect("expecting internal data retrieval to work");
        // Depending on timing of the various async operations involved in remote subscriptions and bookkeeping updates,
        // this might be UNSUBSCRIBE_PENDING or UNSUBSCRIBED
        assert!(
            *state == SubscriptionStatus::Unsubscribed
                || *state == SubscriptionStatus::UnsubscribePending
        );
    }

    // Some subscribers for a remote topic unsubscribe, but at least one subscriber is left
    // [utest->req~usubscription-unsubscribe-last-remote~1]
    // [utest->req~usubscription-unsubscribe-remote-unsubscribed~1]
    #[test_log::test(tokio::test)]
    async fn test_partial_remote_unsubscribe() {
        let remote_topic = test_lib::helpers::remote_topic1_uri();

        // Prepare things - we're not expecting any remote-unsubscribe action in this case
        let command_sender = CommandSender::new();

        // set starting state
        let desired_state: Vec<SubscriptionInfo> = vec![
            SubscriptionInfo::new(
                remote_topic.clone(),
                test_lib::helpers::subscriber_uri1(),
                SubscriptionStatus::Subscribed,
                None,
                None,
            ),
            SubscriptionInfo::new(
                remote_topic.clone(),
                test_lib::helpers::subscriber_uri2(),
                SubscriptionStatus::Subscribed,
                None,
                None,
            ),
        ];

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("expecting internal message passing to work");

        let mut desired_remote_state: HashMap<TopicUUri, SubscriptionStatus> = HashMap::new();
        desired_remote_state.insert(remote_topic.clone(), SubscriptionStatus::Subscribed);
        command_sender
            .set_remote_topics(desired_remote_state)
            .await
            .expect("expecting internal message passing to work");

        // Operation to test
        let result = command_sender
            .unsubscribe(remote_topic.clone(), test_lib::helpers::subscriber_uri1())
            .await
            .expect("expecting internal data retrieval to work");

        assert_eq!(
            *result.status(),
            // this client immediately is getting UNSUBSCRIBED, no _PENDING, as for it the op is done
            SubscriptionStatus::Unsubscribed
        );

        // Verify iternal bookeeping
        let topic_subscribers = command_sender
            .get_topic_subscribers()
            .await
            .expect("expecting internal data retrieval to work");
        // We're expecting one of the two original subscribers to still be tracked at this point
        assert_eq!(topic_subscribers.len(), 1);

        let remote_topics = command_sender
            .get_remote_topics()
            .await
            .expect("expecting internal data retrieval to work");
        // our remote topic status tracker should still track this topic, and...
        assert_eq!(remote_topics.len(), 1);

        let state = remote_topics
            .get(&remote_topic)
            .expect("expecting internal data retrieval to work");
        // ... it should still be in state SUBSCRIBED, as there is still another subscriber left
        assert_eq!(*state, SubscriptionStatus::Subscribed);
    }

    #[test_case(None, None, 7; "No filters, fetch all registered subscriptions")]
    #[test_case(Some(test_lib::helpers::subscriber_uri1()), None, 3; "All subscriber1 subscriptions")]
    #[test_case(None, Some(test_lib::helpers::local_topic1_uri()), 3; "All subscribers of topic1")]
    #[test_case(Some(test_lib::helpers::subscriber_uri1()), Some(test_lib::helpers::local_topic1_uri()), 1; "All subscriptions of subscriber1 to topic1")]
    #[test_case(Some(UUri::try_from_parts(LOCAL_AUTHORITY, 0xFFFF_FFFF, 0xFF, 0x0000).expect("expecting test UUri setup to work")), None, 7; "All local subscribers")]
    #[test_case(None, Some(UUri::try_from_parts(LOCAL_AUTHORITY, 0xFFFF_FFFF, 0xFF, 0xFFFF).expect("expecting test UUri setup to work")), 6; "All subscriptions to local topics")]
    #[test_case(None, Some(UUri::try_from_parts(REMOTE_AUTHORITY, 0xFFFF_FFFF, 0xFF, 0xFFFF).expect("expecting test UUri setup to work")), 1; "All subscriptions to remote topics")]
    #[test_log::test(tokio::test)]
    // [utest->req~usubscription-fetch-subscriptions~1]
    // [utest->req~usubscription-fetch-subscriptions-topic-filter~1]
    // [utest->req~usubscription-fetch-subscriptions-subscriber-filter~1]
    async fn test_fetch_subscriptions(
        subscriber_filter: Option<UUri>,
        topic_filter: Option<UUri>,
        expected_len: usize,
    ) {
        let command_sender = CommandSender::new();

        // set starting state
        let desired_state: Vec<SubscriptionInfo> = vec![
            SubscriptionInfo::new(
                test_lib::helpers::local_topic1_uri(),
                test_lib::helpers::subscriber_uri1(),
                SubscriptionStatus::Subscribed,
                None,
                None,
            ),
            SubscriptionInfo::new(
                test_lib::helpers::local_topic1_uri(),
                test_lib::helpers::subscriber_uri2(),
                SubscriptionStatus::Subscribed,
                None,
                None,
            ),
            SubscriptionInfo::new(
                test_lib::helpers::local_topic1_uri(),
                test_lib::helpers::subscriber_uri3(),
                SubscriptionStatus::Subscribed,
                None,
                None,
            ),
            SubscriptionInfo::new(
                test_lib::helpers::local_topic2_uri(),
                test_lib::helpers::subscriber_uri1(),
                SubscriptionStatus::Subscribed,
                None,
                None,
            ),
            SubscriptionInfo::new(
                test_lib::helpers::local_topic2_uri(),
                test_lib::helpers::subscriber_uri2(),
                SubscriptionStatus::Subscribed,
                None,
                None,
            ),
            SubscriptionInfo::new(
                test_lib::helpers::local_topic2_uri(),
                test_lib::helpers::subscriber_uri3(),
                SubscriptionStatus::Subscribed,
                None,
                None,
            ),
            SubscriptionInfo::new(
                test_lib::helpers::remote_topic1_uri(),
                test_lib::helpers::subscriber_uri1(),
                SubscriptionStatus::Subscribed,
                None,
                None,
            ),
        ];
        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("expecting internal message passing to work");

        // test fetch
        let subscriptions = command_sender
            .fetch_subscriptions(subscriber_filter, topic_filter)
            .await
            .expect("expecting data retrieval to work");
        assert_eq!(subscriptions.len(), expected_len);
    }

    // [utest->req~usubscription-subscribe-notifications~1]
    // [utest->req~usubscription-change-notification~1]
    // [utest->req~usubscription-change-message-type~1]
    #[test_log::test(tokio::test)]
    async fn test_subscriber_notifications() {
        // Prepare things
        let topic = test_lib::helpers::local_topic1_uri();
        let subscriber1 = test_lib::helpers::subscriber_uri1();
        let subscriber2 = test_lib::helpers::subscriber_uri2();
        let (respond_to1, state_changed1) = oneshot::channel::<()>();
        let (respond_to2, state_changed2) = oneshot::channel::<()>();

        let expected_notifications = vec![
            NotificationEvent::StateChange {
                subscriber: subscriber1.clone(),
                topic: topic.clone(),
                status: SubscriptionStatus::UnsubscribePending,
                expiration: None,
                sample_period: None,
                respond_to: respond_to1,
            },
            NotificationEvent::StateChange {
                subscriber: subscriber2.clone(),
                topic: topic.clone(),
                status: SubscriptionStatus::UnsubscribePending,
                expiration: None,
                sample_period: None,

                respond_to: respond_to2,
            },
        ];
        let command_sender =
            CommandSender::new_with_notification_manager(expected_notifications).await;

        // set starting state
        let desired_state: Vec<SubscriptionInfo> = vec![
            SubscriptionInfo::new(
                topic.clone(),
                subscriber1.clone(),
                SubscriptionStatus::Subscribed,
                None,
                None,
            ),
            SubscriptionInfo::new(
                topic.clone(),
                subscriber2.clone(),
                SubscriptionStatus::Subscribed,
                None,
                None,
            ),
        ];
        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("expecting internal message passing to work");

        // initiate topic state change - should result in notifications to both subscribed clients
        command_sender
            .topic_state_update(topic.clone(), SubscriptionStatus::UnsubscribePending)
            .await
            .expect("expecting internal command passing to work");

        state_changed1
            .await
            .expect("expecting a state change notification");
        state_changed2
            .await
            .expect("expecting a state change notification");

        command_sender.shutdown().await;
    }

    // [utest->req~usubscription-unsubscribe-notifications~1]
    #[test_log::test(tokio::test)]
    async fn test_unsubscribe_stops_notifications() {
        // Prepare things
        let topic = test_lib::helpers::local_topic1_uri();
        let subscriber1 = test_lib::helpers::subscriber_uri1();
        let subscriber2 = test_lib::helpers::subscriber_uri2();
        let (respond_to1, state_changed1) = oneshot::channel::<()>();
        let (respond_to2, state_changed2) = oneshot::channel::<()>();

        let expected_notifications = vec![
            NotificationEvent::StateChange {
                subscriber: subscriber1.clone(),
                topic: topic.clone(),
                status: SubscriptionStatus::Unsubscribed,
                expiration: None,
                sample_period: None,

                respond_to: respond_to1,
            },
            NotificationEvent::StateChange {
                subscriber: subscriber2.clone(),
                topic: topic.clone(),
                status: SubscriptionStatus::UnsubscribePending,
                expiration: None,
                sample_period: None,

                respond_to: respond_to2,
            },
        ];
        let command_sender =
            CommandSender::new_with_notification_manager(expected_notifications).await;

        // set starting state
        let desired_state: Vec<SubscriptionInfo> = vec![
            SubscriptionInfo::new(
                topic.clone(),
                subscriber1.clone(),
                SubscriptionStatus::Subscribed,
                None,
                None,
            ),
            SubscriptionInfo::new(
                topic.clone(),
                subscriber2.clone(),
                SubscriptionStatus::Subscribed,
                None,
                None,
            ),
        ];
        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("expecting internal message passing to work");

        // we have set up two subscribers of a topic, both are notified on changes (tested by utest->req~usubscription-subscribe-notifications~1)
        // so we unsubscribe subscriber1, and now only subscriber2 should get a notification on topic-subscription related chages
        let _status = command_sender
            .unsubscribe(topic.clone(), subscriber1.clone())
            .await
            .expect("expecting internal message passing to work");
        // the first change notification is about the unsubscribe operation - take that one, but this is not the object of test
        state_changed1
            .await
            .expect("expecting a state change notification");

        // initiate first topic state change - should result in notifications to both subscribed clients
        command_sender
            .topic_state_update(topic.clone(), SubscriptionStatus::UnsubscribePending)
            .await
            .expect("expecting internal command passing to work");
        // at this point, we're only expecting subscriber2 to be notified of the topic change, as subscriber1 has already unsubscribed
        state_changed2
            .await
            .expect("expecting a state change notification");

        command_sender.shutdown().await;
    }

    // [utest->req~usubscription-subscribe-notifications~1]
    // [utest->dsn~usubscription-change-notification-update~1]
    #[test_log::test(tokio::test)]
    async fn test_local_unsubscribe_notification() {
        // Prepare things
        let desired_state: Vec<SubscriptionInfo> = vec![SubscriptionInfo::new(
            test_lib::helpers::local_topic1_uri(),
            test_lib::helpers::subscriber_uri1(),
            SubscriptionStatus::Subscribed,
            None,
            None,
        )];

        let topic = test_lib::helpers::local_topic1_uri();
        let subscriber = test_lib::helpers::subscriber_uri1();
        let (respond_to, state_changed) = oneshot::channel::<()>();

        let expected_notification = NotificationEvent::StateChange {
            subscriber: subscriber.clone(),
            topic: topic.clone(),
            status: SubscriptionStatus::Unsubscribed,
            expiration: None,
            sample_period: None,
            respond_to,
        };

        let command_sender =
            CommandSender::new_with_notification_manager(vec![expected_notification]).await;

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("expecting internal message passing to work");

        // Operation to test
        let result = command_sender.unsubscribe(topic, subscriber).await;
        assert!(result.is_ok());

        let _ = state_changed.await;
        command_sender.shutdown().await;
    }

    // [utest->dsn~usubscription-change-notification-update~1]
    #[test_log::test(tokio::test)]
    async fn test_state_change_notification() {
        // Prepare things
        let topic = test_lib::helpers::remote_topic1_uri();
        let subscriber = test_lib::helpers::subscriber_uri1();
        let (respond_to, state_changed) = oneshot::channel::<()>();

        let expected_notification = NotificationEvent::StateChange {
            subscriber: subscriber.clone(),
            topic: topic.clone(),
            status: SubscriptionStatus::UnsubscribePending,
            expiration: None,
            sample_period: None,

            respond_to,
        };

        let command_sender =
            CommandSender::new_with_notification_manager(vec![expected_notification]).await;

        // We need a subscriber to topic, which we're subsequently expecting a state change notification to be sent to
        let desired_state: Vec<SubscriptionInfo> = vec![SubscriptionInfo::new(
            topic.clone(),
            subscriber.clone(),
            SubscriptionStatus::Subscribed,
            None,
            None,
        )];

        assert!(command_sender
            .set_topic_subscribers(desired_state)
            .await
            .is_ok());

        let sender = command_sender
            .get_remote_subcription_change_sender()
            .await
            .expect("expecting internal data retrieval to work");

        // Initiate notification event
        let _ = sender
            .send(InternalSubscriptionEvent::TopicStateUpdate {
                topic: topic.clone(),
                state: SubscriptionStatus::UnsubscribePending,
            })
            .await;

        // ensure that we have run through all the async layers and reached the notification assertion statements
        let _ = state_changed.await;
        command_sender.shutdown().await;
    }

    // [utest->req~usubscription-reset~1]
    #[test_log::test(tokio::test)]
    async fn test_reset_notifications() {
        // Prepare things
        let topic = test_lib::helpers::remote_topic1_uri();
        let subscriber = test_lib::helpers::subscriber_uri1();
        let notification_client = test_lib::helpers::subscriber_uri2();

        let (respond_to_subscriber_notification, state_changed_subscriber_notification) =
            oneshot::channel::<()>();
        let (respond_to_direct_notification, state_changed_direct_notification) =
            oneshot::channel::<()>();

        let expected_subscriber_notification = NotificationEvent::StateChange {
            subscriber: subscriber.clone(),
            topic: topic.clone(),
            status: SubscriptionStatus::Unsubscribed,
            expiration: None,
            sample_period: None,

            respond_to: respond_to_subscriber_notification,
        };
        let expected_direct_notification = NotificationEvent::StateChange {
            subscriber: notification_client.clone(),
            topic: topic.clone(),
            status: SubscriptionStatus::Unsubscribed,
            expiration: None,
            sample_period: None,

            respond_to: respond_to_direct_notification,
        };

        let command_sender = CommandSender::new_with_notification_manager(vec![
            expected_subscriber_notification,
            expected_direct_notification,
        ])
        .await;

        // We need a subscriber to topic, which we're subsequently expecting a state change notification to be sent about on reset
        command_sender
            .set_topic_subscribers(vec![SubscriptionInfo::new(
                topic.clone(),
                subscriber.clone(),
                SubscriptionStatus::Subscribed,
                None,
                None,
            )])
            .await
            .expect("expecting internal message passing to work");

        // Add a notification-client registration for which we subsequently expecting a notification on reset
        command_sender
            .set_notifyees(vec![notification_client])
            .await
            .expect("expecting internal message passing to work");

        // Perform reset
        command_sender
            .reset()
            .await
            .expect("expecting internal message passing to work");

        // these will come back once/if our expected notifications (above) have been sent() to the mock transport
        assert!(state_changed_subscriber_notification.await.is_ok());
        assert!(state_changed_direct_notification.await.is_ok());

        // Assert that topic subscriber lists is empty after reset
        // (We don't do the same thing for notification manager, because that is entirely mocked in the context of this suite of tests)
        let subscribers = command_sender
            .get_topic_subscribers()
            .await
            .expect("expecting internal data retrieval to work");
        assert!(subscribers.is_empty());

        command_sender.shutdown().await;
    }
}
