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

#[cfg(test)]
mod tests {
    use protobuf::MessageFull;
    use std::collections::{HashMap, HashSet};
    use std::error::Error;
    use std::sync::Arc;
    use test_case::test_case;
    use tokio::sync::{mpsc, mpsc::Sender, oneshot, Notify};
    use up_rust::MockTransport;

    use up_rust::core::usubscription::{
        State, SubscriptionRequest, SubscriptionResponse, SubscriptionStatus, UnsubscribeRequest,
    };
    use up_rust::{UCode, UStatus, UUri};

    use crate::configuration::DEFAULT_COMMAND_BUFFER_SIZE;
    use crate::subscription_manager::{
        handle_message, RequestKind, SubscribersResponse, SubscriptionEntry, SubscriptionEvent,
        SubscriptionsResponse,
    };
    use crate::{helpers, test_lib, USubscriptionConfiguration};

    // Simple subscription-manager-actor front-end to use for testing
    struct CommandSender {
        command_sender: Sender<SubscriptionEvent>,
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

            helpers::spawn_and_log_error(async move {
                handle_message(
                    config.clone(),
                    Arc::new(transport_mock),
                    command_receiver,
                    shutdown_notification,
                )
                .await;

                Ok(())
            });
            CommandSender { command_sender }
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

            helpers::spawn_and_log_error(async move {
                handle_message(
                    config,
                    mock_transport,
                    command_receiver,
                    shutdown_notification,
                )
                .await;
                Ok(())
            });
            CommandSender { command_sender }
        }

        async fn subscribe(
            &self,
            topic: UUri,
            subscriber: UUri,
        ) -> Result<SubscriptionStatus, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<SubscriptionStatus>();
            let command = SubscriptionEvent::AddSubscription {
                subscriber,
                topic,
                respond_to,
            };
            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn unsubscribe(
            &self,
            topic: UUri,
            subscriber: UUri,
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
            topic: UUri,
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
        ) -> Result<HashMap<UUri, HashSet<UUri>>, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<HashMap<UUri, HashSet<UUri>>>();
            let command = SubscriptionEvent::GetTopicSubscribers { respond_to };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        #[allow(clippy::mutable_key_type)]
        async fn set_topic_subscribers(
            &self,
            topic_subscribers_replacement: HashMap<UUri, HashSet<UUri>>,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            let command = SubscriptionEvent::SetTopicSubscribers {
                topic_subscribers_replacement,
                respond_to,
            };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn get_remote_topics(&self) -> Result<HashMap<UUri, State>, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<HashMap<UUri, State>>();
            let command = SubscriptionEvent::GetRemoteTopics { respond_to };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        #[allow(clippy::mutable_key_type)]
        async fn set_remote_topics(
            &self,
            remote_topics_replacement: HashMap<UUri, State>,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            let command = SubscriptionEvent::SetRemoteTopics {
                topic_subscribers_replacement: remote_topics_replacement,
                respond_to,
            };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }
    }

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
    async fn test_subscribe(topic_subscribers: Vec<(UUri, UUri)>) {
        helpers::init_once();
        let command_sender = CommandSender::new();

        // Prepare things
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: HashMap<UUri, HashSet<UUri>> = HashMap::new();
        for (topic, subscriber) in topic_subscribers {
            desired_state
                .entry(topic.clone())
                .or_default()
                .insert(subscriber.clone());

            // Operation to test
            let result = command_sender.subscribe(topic, subscriber).await;
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

    #[test_case(test_lib::helpers::remote_topic1_uri(), State::SUBSCRIBE_PENDING; "Remote topic, remote state SUBSCRIBED_PENDING")]
    #[test_case(test_lib::helpers::remote_topic1_uri(), State::SUBSCRIBED; "Remote topic, remote state SUBSCRIBED")]
    #[tokio::test]
    async fn test_remote_subscribe(remote_topic: UUri, remote_state: State) {
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
            .subscribe(remote_topic.clone(), test_lib::helpers::subscriber_uri1())
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
            .subscribe(remote_topic.clone(), test_lib::helpers::subscriber_uri1())
            .await;
        assert!(result.is_ok());

        let result = command_sender
            .subscribe(remote_topic.clone(), test_lib::helpers::subscriber_uri2())
            .await;
        assert!(result.is_ok());

        // Assert we have to local topic-subscriber entries...
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
    #[tokio::test]
    async fn test_final_unsubscribe() {
        helpers::init_once();
        let command_sender = CommandSender::new();

        // Prepare things
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: HashMap<UUri, HashSet<UUri>> = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic1_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1());

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
        let mut desired_state: HashMap<UUri, HashSet<UUri>> = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic1_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1());
        entry.insert(test_lib::helpers::subscriber_uri2());

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
            .contains(&test_lib::helpers::subscriber_uri2()));
    }

    // All subscribers for a remote topic unsubscribe
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
        let mut desired_state: HashMap<UUri, HashSet<UUri>> = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state.entry(remote_topic.clone()).or_default();
        entry.insert(test_lib::helpers::subscriber_uri1());

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("Interaction with subscription handler broken");

        #[allow(clippy::mutable_key_type)]
        let mut desired_remote_state: HashMap<UUri, State> = HashMap::new();
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
    #[tokio::test]
    async fn test_partial_remote_unsubscribe() {
        helpers::init_once();
        let remote_topic = test_lib::helpers::remote_topic1_uri();

        // Prepare things - we're not expecting any remote-unsubscribe action in this case
        let command_sender = CommandSender::new();

        // set starting state
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: HashMap<UUri, HashSet<UUri>> = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state.entry(remote_topic.clone()).or_default();
        entry.insert(test_lib::helpers::subscriber_uri1());
        entry.insert(test_lib::helpers::subscriber_uri2());

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("Interaction with subscription handler broken");

        #[allow(clippy::mutable_key_type)]
        let mut desired_remote_state: HashMap<UUri, State> = HashMap::new();
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
        let mut desired_state: HashMap<UUri, HashSet<UUri>> = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic1_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1());
        entry.insert(test_lib::helpers::subscriber_uri2());

        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic2_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1());
        entry.insert(test_lib::helpers::subscriber_uri3());

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
            assert!(expected_subscribers.contains(&subscriber));
        }
    }

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
        let mut desired_state: HashMap<UUri, HashSet<UUri>> = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic1_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1());
        entry.insert(test_lib::helpers::subscriber_uri2());

        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic2_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1());
        entry.insert(test_lib::helpers::subscriber_uri3());

        command_sender
            .set_topic_subscribers(desired_state.clone())
            .await
            .expect("Error during testing/setup of subscription manager");

        // Prepare things
        let desired_subscriber = test_lib::helpers::subscriber_uri1();

        let mut expected_subscribers: Vec<(UUri, UUri)> = Vec::new();
        for (topic, subscribers) in desired_state.clone() {
            if subscribers.contains(&desired_subscriber) {
                expected_subscribers.push((
                    subscribers
                        .get(&desired_subscriber)
                        .unwrap_or_default()
                        .clone(),
                    topic,
                ));
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
        let mut desired_state: HashMap<UUri, HashSet<UUri>> = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic1_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1());
        entry.insert(test_lib::helpers::subscriber_uri2());

        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic2_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1());
        entry.insert(test_lib::helpers::subscriber_uri3());

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
            assert!(expected_subscribers.contains(&subscriber));
        }
    }
}
