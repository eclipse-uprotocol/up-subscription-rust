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
    use mockall::predicate::eq;
    use std::sync::Arc;
    use test_case::test_case;

    use up_rust::{
        communication::{CallOptions, UPayload},
        core::usubscription::{
            FetchSubscribersRequest, FetchSubscriptionsRequest, NotificationsRequest, Request,
            State, SubscriberInfo, SubscriptionRequest, SubscriptionResponse, SubscriptionStatus,
            UnsubscribeRequest, UnsubscribeResponse, RESOURCE_ID_SUBSCRIBE,
            RESOURCE_ID_UNSUBSCRIBE, USUBSCRIPTION_TYPE_ID, USUBSCRIPTION_VERSION_MAJOR,
        },
        UCode, UPriority, UUri, UUID,
    };

    use crate::{
        helpers,
        test_lib::{self},
        USubscriptionConfiguration, USubscriptionService,
    };

    #[test_case(UUri::default(), SubscriberInfo::default(); "Default topic, Default SubscriberInfo")]
    #[test_case(test_lib::helpers::local_topic1_uri(), SubscriberInfo::default(); "Good topic, Default SubscriberInfo")]
    #[test_case(UUri::default(), test_lib::helpers::subscriber_info1(); "Default topic, good SubscriberInfo")]
    #[tokio::test]
    async fn test_subscribe_input_validation(topic: UUri, subscriber: SubscriberInfo) {
        helpers::init_once();

        // Prepare things
        let usubscription = test_lib::mocks::usubscription_default_mock(0);

        let subscription_request = SubscriptionRequest {
            topic: Some(topic).into(),
            subscriber: Some(subscriber).into(),
            ..Default::default()
        };

        // Operation to test
        let result = usubscription.subscribe(subscription_request).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code, UCode::INVALID_ARGUMENT.into());
    }

    #[test_case(test_lib::helpers::local_topic1_uri(), State::SUBSCRIBED; "Local topic")]
    #[test_case(test_lib::helpers::remote_topic1_uri(), State::SUBSCRIBE_PENDING; "Remote topic")]
    #[tokio::test]
    async fn test_subscribe(topic: UUri, state: State) {
        helpers::init_once();

        // Prepare things
        let (mock_transport, mut notification_receiver) =
            test_lib::mocks::utransport_mock_for_notifications();
        let mut mock_client = test_lib::mocks::MockRpcClientMock::default();

        // if subscription is for remote topic, we expect a call to a remote usubscription service
        if test_lib::helpers::local_usubscription_service_uri().is_remote_authority(&topic) {
            let expected_remote_uri = UUri {
                authority_name: topic.authority_name.clone(),
                ue_id: USUBSCRIPTION_TYPE_ID,
                ue_version_major: USUBSCRIPTION_VERSION_MAJOR as u32,
                resource_id: RESOURCE_ID_SUBSCRIBE as u32,
                ..Default::default()
            };
            let expected_call_options = CallOptions::for_rpc_request(
                crate::UP_REMOTE_TTL,
                Some(UUID::new()),
                None,
                Some(UPriority::UPRIORITY_CS2),
            );
            let remote_subscription_request = SubscriptionRequest {
                topic: Some(topic.clone()).into(),
                subscriber: Some(SubscriberInfo {
                    uri: Some(test_lib::helpers::local_usubscription_service_uri()).into(),
                    ..Default::default()
                })
                .into(),
                ..Default::default()
            };
            let expected_payload =
                UPayload::try_from_protobuf(remote_subscription_request).unwrap();

            let expected_result = SubscriptionResponse {
                topic: Some(topic.clone()).into(),
                status: Some(SubscriptionStatus {
                    state: State::SUBSCRIBED.into(),
                    ..Default::default()
                })
                .into(),
                ..Default::default()
            };
            let expected_result_payload = UPayload::try_from_protobuf(expected_result).unwrap();

            mock_client
                .expect_invoke_method()
                .with(
                    eq(expected_remote_uri),
                    eq(expected_call_options),
                    eq(Some(expected_payload)),
                )
                .returning(move |_u, _o, _p| Ok(Some(expected_result_payload.clone())));
        }
        let (usubscription, _ustop) = USubscriptionService::run(
            USubscriptionConfiguration::create(
                test_lib::helpers::LOCAL_AUTHORITY.to_string(),
                None,
                None,
            )
            .unwrap(),
            Arc::new(mock_transport),
            Arc::new(mock_client),
        )
        .unwrap();

        let subscription_request = SubscriptionRequest {
            topic: Some(topic.clone()).into(),
            subscriber: Some(test_lib::helpers::subscriber_info1()).into(),
            ..Default::default()
        };

        // Operation to test
        let result = usubscription.subscribe(subscription_request.clone()).await;

        // Check subscribe operation result
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.topic.unwrap(), topic);
        assert_eq!(result.status.state.unwrap(), state);

        // Check subscription change notification
        let received = notification_receiver.recv().await;
        assert!(received.is_some());
        let (notification_subscriber, notification_topic, notification_status) = received.unwrap();
        assert_eq!(
            notification_subscriber,
            test_lib::helpers::subscriber_info1()
        );
        assert_eq!(notification_topic, topic);
        assert_eq!(notification_status.state.unwrap(), state);
    }

    #[test_case(UUri::default(), SubscriberInfo::default(); "Default topic, Default SubscriberInfo")]
    #[test_case(test_lib::helpers::local_topic1_uri(), SubscriberInfo::default(); "Good topic, Default SubscriberInfo")]
    #[test_case(UUri::default(), test_lib::helpers::subscriber_info1(); "Default topic, good SubscriberInfo")]
    #[tokio::test]
    async fn test_unsubscribe_input_validation(topic: UUri, subscriber: SubscriberInfo) {
        helpers::init_once();

        // Prepare things
        let usubscription = test_lib::mocks::usubscription_default_mock(0);

        let unsubscribe_request = UnsubscribeRequest {
            topic: Some(topic).into(),
            subscriber: Some(subscriber).into(),
            ..Default::default()
        };

        // Operation to test
        let result = usubscription.unsubscribe(unsubscribe_request).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code, UCode::INVALID_ARGUMENT.into());
    }

    #[test_case(test_lib::helpers::local_topic1_uri(); "Local topic")]
    #[test_case(test_lib::helpers::remote_topic1_uri(); "Remote topic")]
    #[tokio::test]
    async fn test_unsubscribe(topic: UUri) {
        helpers::init_once();

        // Prepare things
        let (mock_transport, mut notification_receiver) =
            test_lib::mocks::utransport_mock_for_notifications();
        let mut mock_client = test_lib::mocks::MockRpcClientMock::default();

        // if subscription is for remote topic, we expect a call to a remote usubscription service
        if test_lib::helpers::local_usubscription_service_uri().is_remote_authority(&topic) {
            let expected_remote_uri = UUri {
                authority_name: topic.authority_name.clone(),
                ue_id: USUBSCRIPTION_TYPE_ID,
                ue_version_major: USUBSCRIPTION_VERSION_MAJOR as u32,
                resource_id: RESOURCE_ID_UNSUBSCRIBE as u32,
                ..Default::default()
            };
            let expected_call_options = CallOptions::for_rpc_request(
                crate::UP_REMOTE_TTL,
                Some(UUID::new()),
                None,
                Some(UPriority::UPRIORITY_CS2),
            );
            let remote_unsubscribe_request = UnsubscribeRequest {
                topic: Some(topic.clone()).into(),
                subscriber: Some(SubscriberInfo {
                    uri: Some(test_lib::helpers::local_usubscription_service_uri()).into(),
                    ..Default::default()
                })
                .into(),
                ..Default::default()
            };
            let expected_payload = UPayload::try_from_protobuf(remote_unsubscribe_request).unwrap();

            let expected_result = UnsubscribeResponse::default();
            let expected_result_payload = UPayload::try_from_protobuf(expected_result).unwrap();

            mock_client
                .expect_invoke_method()
                .with(
                    eq(expected_remote_uri),
                    eq(expected_call_options),
                    eq(Some(expected_payload)),
                )
                .returning(move |_u, _o, _p| Ok(Some(expected_result_payload.clone())));
        }
        let (usubscription, _ustop) = USubscriptionService::run(
            USubscriptionConfiguration::create(
                test_lib::helpers::LOCAL_AUTHORITY.to_string(),
                None,
                None,
            )
            .unwrap(),
            Arc::new(mock_transport),
            Arc::new(mock_client),
        )
        .unwrap();

        let unsubscribe_request = UnsubscribeRequest {
            topic: Some(topic.clone()).into(),
            subscriber: Some(test_lib::helpers::subscriber_info1()).into(),
            ..Default::default()
        };

        // Operation to test
        let result = usubscription.unsubscribe(unsubscribe_request.clone()).await;

        // Check subscribe operation result
        assert!(result.is_ok());

        // Check subscription change notification
        let received = notification_receiver.recv().await;
        assert!(received.is_some());
        let (notification_subscriber, notification_topic, notification_status) = received.unwrap();
        assert_eq!(
            notification_subscriber,
            test_lib::helpers::subscriber_info1()
        );
        assert_eq!(notification_topic, topic);
        // As a client, we expect that our subscription-relation is UNSUBSCRIBED immediately, nonwithstanding whether a remote topic might still be in state
        // UNSUBSCRIBE_PENDING between the two usubscription services
        assert_eq!(notification_status.state.unwrap(), State::UNSUBSCRIBED);
    }

    #[test_case(UUri::default(), SubscriberInfo::default(); "Default topic, Default SubscriberInfo")]
    #[test_case(test_lib::helpers::notification_topic_uri(), SubscriberInfo::default(); "Good topic, Default SubscriberInfo")]
    #[test_case(test_lib::helpers::remote_topic1_uri(), SubscriberInfo::default(); "Remote topic, Default SubscriberInfo")]
    #[test_case(test_lib::helpers::remote_topic1_uri(), SubscriberInfo::default(); "Invalid topic, Default SubscriberInfo")]
    #[test_case(UUri::default(), test_lib::helpers::subscriber_info1(); "Default topic, good SubscriberInfo")]
    #[tokio::test]
    async fn test_register_for_notifications_input_validation(
        topic: UUri,
        subscriber: SubscriberInfo,
    ) {
        helpers::init_once();

        // Prepare things
        let usubscription = test_lib::mocks::usubscription_default_mock(0);

        let notification_request = NotificationsRequest {
            topic: Some(topic.clone()).into(),
            subscriber: Some(subscriber).into(),
            ..Default::default()
        };

        // Operation to test
        let result = usubscription
            .register_for_notifications(notification_request)
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code, UCode::INVALID_ARGUMENT.into());
    }

    #[tokio::test]
    async fn test_register_for_notifications() {
        helpers::init_once();

        // Prepare things
        let usubscription = test_lib::mocks::usubscription_default_mock(0);

        let notification_request = NotificationsRequest {
            topic: Some(test_lib::helpers::notification_topic_uri()).into(),
            subscriber: Some(test_lib::helpers::subscriber_info1()).into(),
            ..Default::default()
        };

        // Operation to test
        let result = usubscription
            .register_for_notifications(notification_request.clone())
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_unregister_for_notifications_input_validation() {
        helpers::init_once();

        // Prepare things
        let usubscription = test_lib::mocks::usubscription_default_mock(0);

        let notification_request = NotificationsRequest::default();

        // Operation to test
        let result = usubscription
            .unregister_for_notifications(notification_request)
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code, UCode::INVALID_ARGUMENT.into());
    }

    #[tokio::test]
    async fn test_unregister_for_notifications() {
        helpers::init_once();

        // Prepare things
        let usubscription = test_lib::mocks::usubscription_default_mock(0);

        let notification_request = NotificationsRequest {
            topic: Some(test_lib::helpers::local_topic1_uri()).into(),
            subscriber: Some(test_lib::helpers::subscriber_info1()).into(),
            ..Default::default()
        };

        // Operation to test
        let result = usubscription
            .unregister_for_notifications(notification_request.clone())
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fetch_subscribers_input_validation() {
        helpers::init_once();

        // Prepare things
        let usubscription = test_lib::mocks::usubscription_default_mock(0);

        let fetch_subscribers_request = FetchSubscribersRequest {
            topic: Some(UUri::default()).into(),
            ..Default::default()
        };

        // Operation to test
        let result = usubscription
            .fetch_subscribers(fetch_subscribers_request)
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code, UCode::INVALID_ARGUMENT.into());
    }

    #[test_case(vec![], 0; "No subscription")]
    #[test_case(vec![SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic1_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info1()).into(),
                        ..Default::default()
                    }], 1; "1 subscription")]
    #[test_case(vec![SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic1_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info1()).into(),
                        ..Default::default()
                    },
                    SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic1_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info2()).into(),
                        ..Default::default()
                    }], 2; "2 subscriptions")]
    #[test_case(vec![SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic1_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info1()).into(),
                        ..Default::default()
                    },
                    SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic1_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info2()).into(),
                        ..Default::default()
                    },
                    SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic2_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info1()).into(),
                        ..Default::default()
                    }], 2; "2 subscriptions for same topic, 1 irrelevant")]
    #[tokio::test]
    async fn test_fetch_subscribers(
        subscriptions: Vec<SubscriptionRequest>,
        expected_count: usize,
    ) {
        helpers::init_once();

        // Prepare things
        let usubscription = test_lib::mocks::usubscription_default_mock(subscriptions.len());

        for subscription_request in subscriptions {
            let _ = usubscription.subscribe(subscription_request).await;
        }

        let fetch_subscribers_request = FetchSubscribersRequest {
            topic: Some(test_lib::helpers::local_topic1_uri()).into(),
            ..Default::default()
        };

        // Operation to test
        let result = usubscription
            .fetch_subscribers(fetch_subscribers_request.clone())
            .await;

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.subscribers.len(), expected_count);
    }

    #[test_case(Request::Topic(UUri::default()); "Default Request:Topic UUri")]
    #[test_case(Request::Subscriber(SubscriberInfo::default()); "Default Request:Susbcriber SusbcriberInfo")]
    #[tokio::test]
    async fn test_fetch_subscriptions_input_validation(request: Request) {
        helpers::init_once();

        // Prepare things
        let usubscription = test_lib::mocks::usubscription_default_mock(0);

        let fetch_subscriptions_request = FetchSubscriptionsRequest {
            request: Some(request),
            ..Default::default()
        };

        // Operation to test
        let result = usubscription
            .fetch_subscriptions(fetch_subscriptions_request)
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code, UCode::INVALID_ARGUMENT.into());
    }

    #[test_case(vec![], Request::Topic(test_lib::helpers::local_topic1_uri()), 0; "No subscription")]
    #[test_case(vec![SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic1_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info1()).into(),
                        ..Default::default()
                    }],
                    Request::Topic(test_lib::helpers::local_topic1_uri()),
                    1; "1 subscription, request by topic")]
    #[test_case(vec![SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic1_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info1()).into(),
                        ..Default::default()
                    },
                    SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic1_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info2()).into(),
                        ..Default::default()
                    }],
                    Request::Topic(test_lib::helpers::local_topic1_uri()),
                    2; "2 subscriptions, request by topic")]
    #[test_case(vec![SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic1_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info1()).into(),
                        ..Default::default()
                    },
                    SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic1_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info2()).into(),
                        ..Default::default()
                    },
                    SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic2_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info1()).into(),
                        ..Default::default()
                    }],
                    Request::Topic(test_lib::helpers::local_topic1_uri()),
                    2; "2 subscriptions for same topic, 1 irrelevant, request by topic")]
    #[test_case(vec![SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic1_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info1()).into(),
                        ..Default::default()
                    }],
                    Request::Subscriber(test_lib::helpers::subscriber_info1()),
                    1; "1 subscription, request by subscriber")]
    #[test_case(vec![SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic1_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info1()).into(),
                        ..Default::default()
                    },
                    SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic2_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info1()).into(),
                        ..Default::default()
                    }],
                    Request::Subscriber(test_lib::helpers::subscriber_info1()),
                    2; "2 subscriptions, request by subscriber")]
    #[test_case(vec![SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic1_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info1()).into(),
                        ..Default::default()
                    },
                    SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic1_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info2()).into(),
                        ..Default::default()
                    },
                    SubscriptionRequest {
                        topic: Some(test_lib::helpers::local_topic2_uri()).into(),
                        subscriber: Some(test_lib::helpers::subscriber_info2()).into(),
                        ..Default::default()
                    }],
                    Request::Subscriber(test_lib::helpers::subscriber_info2()),
                    2; "2 subscriptions for same topic, 1 irrelevant, request by subscriber")]
    #[tokio::test]
    async fn test_fetch_subscriptions(
        subscriptions: Vec<SubscriptionRequest>,
        request: Request,
        expected_count: usize,
    ) {
        helpers::init_once();

        // Prepare things
        let usubscription = test_lib::mocks::usubscription_default_mock(subscriptions.len());

        for subscription_request in subscriptions {
            let _ = usubscription.subscribe(subscription_request).await;
        }

        let fetch_subscriptions_request = FetchSubscriptionsRequest {
            request: Some(request),
            ..Default::default()
        };

        // Operation to test
        let result = usubscription
            .fetch_subscriptions(fetch_subscriptions_request)
            .await;

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.subscriptions.len(), expected_count);
    }
}
