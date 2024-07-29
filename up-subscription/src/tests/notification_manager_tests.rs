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
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::Arc;
    use tokio::sync::{
        mpsc::{self, Sender},
        oneshot, Notify,
    };

    use up_rust::core::usubscription::{
        usubscription_uri, State, SubscriberInfo, SubscriptionStatus, Update,
        RESOURCE_ID_SUBSCRIPTION_CHANGE,
    };
    use up_rust::{UMessage, UMessageBuilder, UUri, UUID};

    use crate::configuration::DEFAULT_COMMAND_BUFFER_SIZE;
    use crate::notification_manager::{notification_engine, NotificationEvent};
    use crate::{helpers, test_lib};

    // Simple subscription-manager-actor front-end to use for testing
    struct CommandSender {
        command_sender: Sender<NotificationEvent>,
    }

    impl CommandSender {
        fn new(expected_message: Vec<UMessage>) -> Self {
            let shutdown_notification = Arc::new(Notify::new());

            let (command_sender, command_receiver) =
                mpsc::channel::<NotificationEvent>(DEFAULT_COMMAND_BUFFER_SIZE);
            let transport_mock =
                test_lib::mocks::utransport_mock_for_notification_manager(expected_message);

            helpers::spawn_and_log_error(async move {
                notification_engine(
                    Arc::new(transport_mock),
                    command_receiver,
                    shutdown_notification,
                )
                .await;
                Ok(())
            });
            CommandSender { command_sender }
        }

        async fn add_notifyee(&self, subscriber: UUri, topic: UUri) -> Result<(), Box<dyn Error>> {
            Ok(self
                .command_sender
                .send(NotificationEvent::AddNotifyee { subscriber, topic })
                .await?)
        }

        async fn remove_notifyee(&self, subscriber: UUri) -> Result<(), Box<dyn Error>> {
            Ok(self
                .command_sender
                .send(NotificationEvent::RemoveNotifyee { subscriber })
                .await?)
        }

        async fn state_change(
            &self,
            subscriber: SubscriberInfo,
            topic: UUri,
            status: SubscriptionStatus,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();

            self.command_sender
                .send(NotificationEvent::StateChange {
                    subscriber,
                    topic,
                    status,
                    respond_to,
                })
                .await?;

            Ok(receive_from.await?)
        }

        async fn get_notification_topics(&self) -> Result<HashMap<UUri, UUri>, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<HashMap<UUri, UUri>>();
            self.command_sender
                .send(NotificationEvent::GetNotificationTopics { respond_to })
                .await?;

            Ok(receive_from.await?)
        }

        #[allow(clippy::mutable_key_type)]
        async fn set_notification_topics(
            &self,
            notification_topics_replacement: HashMap<UUri, UUri>,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            self.command_sender
                .send(NotificationEvent::SetNotificationTopics {
                    respond_to,
                    notification_topics_replacement,
                })
                .await?;

            Ok(receive_from.await?)
        }
    }

    #[tokio::test]
    async fn test_add_notifyee() {
        helpers::init_once();
        let command_sender = CommandSender::new(vec![]);

        let expected_subscriber = test_lib::helpers::subscriber_info1().uri.unwrap();
        let expected_topic = test_lib::helpers::local_topic1_uri();

        command_sender
            .add_notifyee(expected_subscriber.clone(), expected_topic.clone())
            .await
            .expect("Error communicating with subscription manager");

        #[allow(clippy::mutable_key_type)]
        let notification_topics = command_sender
            .get_notification_topics()
            .await
            .expect("Error communicating with subscription manager");

        assert_eq!(notification_topics.len(), 1);
        assert!(notification_topics.contains_key(&expected_subscriber));
        assert_eq!(
            *notification_topics.get(&expected_subscriber).unwrap(),
            expected_topic
        );
    }

    #[tokio::test]
    async fn test_remove_notifyee() {
        helpers::init_once();
        let command_sender = CommandSender::new(vec![]);

        // prepare things
        let expected_subscriber = test_lib::helpers::subscriber_info1().uri.unwrap();
        let expected_topic = test_lib::helpers::local_topic1_uri();

        #[allow(clippy::mutable_key_type)]
        let mut notification_topics_replacement: HashMap<UUri, UUri> = HashMap::new();
        notification_topics_replacement.insert(expected_subscriber.clone(), expected_topic.clone());

        command_sender
            .set_notification_topics(notification_topics_replacement)
            .await
            .expect("Error communicating with subscription manager");

        // operation to test
        command_sender
            .remove_notifyee(expected_subscriber.clone())
            .await
            .expect("Error communicating with subscription manager");

        #[allow(clippy::mutable_key_type)]
        let notification_topics = command_sender
            .get_notification_topics()
            .await
            .expect("Error communicating with subscription manager");

        assert_eq!(notification_topics.len(), 0);
    }

    // This test expects a state change notification to be send to the generic Notification Update channel
    #[tokio::test]
    async fn test_state_change() {
        helpers::init_once();

        // prepare things
        // this is the status&topic&subscriber that the notification is about
        let changing_status = SubscriptionStatus {
            state: State::SUBSCRIBED.into(),
            ..Default::default()
        };
        let changing_topic = test_lib::helpers::local_topic1_uri();
        let changing_subscriber = test_lib::helpers::subscriber_info1();

        // the update message that we're expecting
        let expected_update = Update {
            topic: Some(changing_topic.clone()).into(),
            subscriber: Some(changing_subscriber.clone()).into(),
            status: Some(changing_status.clone()).into(),
            ..Default::default()
        };

        // this is the generic update channel notification, that always is sent
        let expected_message_general_channel =
            UMessageBuilder::publish(usubscription_uri(RESOURCE_ID_SUBSCRIPTION_CHANGE))
                .with_message_id(UUID::build())
                .build_with_protobuf_payload(&expected_update)
                .unwrap();

        let command_sender = CommandSender::new(vec![expected_message_general_channel]);

        // operation to test
        let r = command_sender
            .state_change(
                changing_subscriber.clone(),
                changing_topic.clone(),
                changing_status,
            )
            .await;
        assert!(r.is_ok())
    }

    // This test expects a state change notification to be send to the generic Notification Update channel,
    // as well as to the custom notification topics registered by susbcribers who like things complicated.
    #[tokio::test]
    async fn test_state_change_custom() {
        helpers::init_once();

        // prepare things
        // this is the status&topic&subscriber that the notification is about
        let changing_status = SubscriptionStatus {
            state: State::SUBSCRIBED.into(),
            ..Default::default()
        };
        let changing_topic = test_lib::helpers::local_topic1_uri();
        let changing_subscriber = test_lib::helpers::subscriber_info1();

        // first subscriber that set a custom notification topic
        let expected_subscriber_1 = test_lib::helpers::subscriber_info2();
        let expected_topic_1 = test_lib::helpers::local_topic2_uri();

        // second subscriber that set a custom notification topic
        let expected_subscriber_2 = test_lib::helpers::subscriber_info3();
        let expected_topic_2 = test_lib::helpers::local_topic3_uri();

        // custom notification expectations
        #[allow(clippy::mutable_key_type)]
        let mut notification_topics_replacement: HashMap<UUri, UUri> = HashMap::new();
        notification_topics_replacement.insert(
            expected_subscriber_1.uri.clone().unwrap(),
            expected_topic_1.clone(),
        );
        notification_topics_replacement.insert(
            expected_subscriber_2.uri.clone().unwrap(),
            expected_topic_2.clone(),
        );

        // the update message that we're expecting
        let expected_update = Update {
            topic: Some(changing_topic.clone()).into(),
            subscriber: Some(changing_subscriber.clone()).into(),
            status: Some(changing_status.clone()).into(),
            ..Default::default()
        };

        // this is the generic update channel notification, that always is sent
        let expected_message_general_channel =
            UMessageBuilder::publish(usubscription_uri(RESOURCE_ID_SUBSCRIPTION_CHANGE))
                .with_message_id(UUID::build())
                .build_with_protobuf_payload(&expected_update)
                .unwrap();

        // custom update messages, expected by subscribers who registered for notification on custom topics
        let expected_message_custom_1 = UMessageBuilder::publish(expected_topic_1)
            .with_message_id(UUID::build())
            .build_with_protobuf_payload(&expected_update)
            .unwrap();
        let expected_message_custom_2 = UMessageBuilder::publish(expected_topic_2)
            .with_message_id(UUID::build())
            .build_with_protobuf_payload(&expected_update)
            .unwrap();

        // put all of this into our mock
        let command_sender = CommandSender::new(vec![
            expected_message_general_channel,
            expected_message_custom_1,
            expected_message_custom_2,
        ]);

        // set custom notification config
        command_sender
            .set_notification_topics(notification_topics_replacement)
            .await
            .expect("Error communicating with subscription manager");

        // operation to test
        let r = command_sender
            .state_change(
                changing_subscriber.clone(),
                changing_topic.clone(),
                changing_status,
            )
            .await;
        assert!(r.is_ok())
    }
}
