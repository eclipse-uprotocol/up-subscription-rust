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

use async_trait::async_trait;
use log::*;
use tokio::{sync::mpsc::Sender, sync::oneshot};

use up_rust::{
    communication::{RequestHandler, ServiceInvocationError, UPayload},
    core::usubscription::{
        FetchSubscriptionsRequest, FetchSubscriptionsResponse, Request, SubscriberInfo,
        Subscription, RESOURCE_ID_FETCH_SUBSCRIPTIONS,
    },
    UAttributes,
};

use crate::{
    helpers,
    subscription_manager::{
        RequestKind, SubscriptionEntry, SubscriptionEvent, SubscriptionsResponse,
    },
};

pub(crate) struct FetchSubscriptionsRequestHandler {
    subscription_sender: Sender<SubscriptionEvent>,
}

impl FetchSubscriptionsRequestHandler {
    pub(crate) fn new(subscription_sender: Sender<SubscriptionEvent>) -> Self {
        Self {
            subscription_sender,
        }
    }
}

#[async_trait]
impl RequestHandler for FetchSubscriptionsRequestHandler {
    async fn handle_request(
        &self,
        resource_id: u16,
        message_attributes: &UAttributes,
        request_payload: Option<UPayload>,
    ) -> Result<Option<UPayload>, ServiceInvocationError> {
        // [impl->dsn~usubscription-fetch-subscriptions-protobuf~1]
        let (fetch_subscriptions_request, _source) =
            helpers::extract_inputs::<FetchSubscriptionsRequest>(
                RESOURCE_ID_FETCH_SUBSCRIPTIONS,
                resource_id,
                &request_payload,
                message_attributes,
            )?;

        let FetchSubscriptionsRequest {
            request, offset, ..
        } = fetch_subscriptions_request;
        let request_kind = match request {
            Some(Request::Topic(topic)) => {
                if !topic.is_empty() {
                    RequestKind::Topic(topic)
                } else {
                    return Err(ServiceInvocationError::InvalidArgument(
                        "Empty topic in Request::Topic".to_string(),
                    ));
                }
            }
            Some(Request::Subscriber(subscriber)) => {
                if let Some(subscriber) = subscriber.uri.into_option() {
                    // [impl->dsn~usubscription-fetch-subscriptions-invalid-subscriber~1]
                    helpers::validate_uri(&subscriber)?;
                    RequestKind::Subscriber(subscriber)
                } else {
                    return Err(ServiceInvocationError::InvalidArgument(
                        "Missing subscriber uri in Request::Subscriber".to_string(),
                    ));
                }
            }
            _ => {
                return Err(ServiceInvocationError::InvalidArgument(
                    "Missing or bad Request parameter".to_string(),
                ));
            }
        };

        // Interact with subscription manager backend
        let (respond_to, receive_from) = oneshot::channel::<SubscriptionsResponse>();
        let se = SubscriptionEvent::FetchSubscriptions {
            request: request_kind,
            offset,
            respond_to,
        };

        if let Err(e) = self.subscription_sender.send(se).await {
            error!("Error communicating with subscription manager: {e}");
            return Err(ServiceInvocationError::Internal(
                "Error processing request".to_string(),
            ));
        }
        let Ok(fetch_subscriptions_response) = receive_from.await else {
            return Err(ServiceInvocationError::Internal(
                "Error processing request".to_string(),
            ));
        };

        // Build and return result
        let (subscriptions, has_more) = fetch_subscriptions_response;
        let subscription_list: Vec<Subscription> = subscriptions
            .iter()
            .map(
                |SubscriptionEntry {
                     topic,
                     subscriber,
                     status,
                 }| Subscription {
                    topic: Some(topic.clone()).into(),
                    subscriber: Some(SubscriberInfo {
                        uri: Some(subscriber.clone()).into(),
                        ..Default::default()
                    })
                    .into(),
                    status: Some(status.clone()).into(),
                    ..Default::default()
                },
            )
            .collect();

        let fetch_subscriptions_response = FetchSubscriptionsResponse {
            subscriptions: subscription_list,
            has_more_records: Some(has_more),
            ..Default::default()
        };

        let response_payload =
            UPayload::try_from_protobuf(fetch_subscriptions_response).map_err(|e| {
                ServiceInvocationError::Internal(format!("Error building response payload: {e}"))
            })?;

        Ok(Some(response_payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;
    use tokio::sync::mpsc::{self};

    use up_rust::UUri;

    use crate::{helpers, tests::test_lib};

    // [utest->dsn~usubscription-fetch-subscriptions-protobuf~1]
    #[tokio::test]
    async fn test_fetch_subscriptions_success() {
        helpers::init_once();

        // create request and other required object(s)
        let fetch_subscriptions_request = FetchSubscriptionsRequest {
            request: Some(up_rust::core::usubscription::Request::Subscriber(
                test_lib::helpers::subscriber_info1(),
            )),
            offset: Some(42),
            ..Default::default()
        };
        let request_payload =
            UPayload::try_from_protobuf(fetch_subscriptions_request.clone()).unwrap();
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };

        let (subscription_sender, mut subscription_receiver) =
            mpsc::channel::<SubscriptionEvent>(1);

        // create and spawn off handler, to make all the asnync goodness work
        let request_handler = FetchSubscriptionsRequestHandler::new(subscription_sender);
        tokio::spawn(async move {
            let result = request_handler
                .handle_request(
                    RESOURCE_ID_FETCH_SUBSCRIPTIONS,
                    &message_attributes,
                    Some(request_payload),
                )
                .await
                .unwrap();

            let response: FetchSubscriptionsResponse = result.unwrap().extract_protobuf().unwrap();
            assert_eq!(response, FetchSubscriptionsResponse::default());
        });

        // validate subscription manager interaction
        let subscription_event = subscription_receiver.recv().await.unwrap();
        match subscription_event {
            SubscriptionEvent::FetchSubscriptions {
                request,
                offset,
                respond_to,
            } => {
                match request {
                    RequestKind::Subscriber(subscriber_info) => {
                        assert_eq!(
                            subscriber_info,
                            test_lib::helpers::subscriber_info1()
                                .uri
                                .unwrap_or_default()
                        );
                    }
                    _ => panic!("Wrong request details"),
                }
                assert_eq!(offset.unwrap_or_default(), 42);

                let _ = respond_to.send(SubscriptionsResponse::default());
            }

            _ => panic!("Wrong event type"),
        }
    }

    #[tokio::test]
    async fn test_wrong_resource_id() {
        helpers::init_once();

        // create request and other required object(s)
        let subscribe_request =
            test_lib::helpers::subscription_request(test_lib::helpers::local_topic1_uri(), None);
        let request_payload = UPayload::try_from_protobuf(subscribe_request.clone()).unwrap();
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };
        let (subscription_sender, _) = mpsc::channel::<SubscriptionEvent>(1);

        // create handler and perform tested operation
        let request_handler = FetchSubscriptionsRequestHandler::new(subscription_sender);

        let result = request_handler
            .handle_request(
                up_rust::core::usubscription::RESOURCE_ID_UNSUBSCRIBE,
                &message_attributes,
                Some(request_payload),
            )
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ServiceInvocationError::InvalidArgument(_) => {}
            _ => panic!("Wrong error type"),
        }
    }

    #[tokio::test]
    async fn test_no_source_uri() {
        helpers::init_once();

        // create request and other required object(s)
        let subscribe_request =
            test_lib::helpers::subscription_request(test_lib::helpers::local_topic1_uri(), None);
        let request_payload = UPayload::try_from_protobuf(subscribe_request.clone()).unwrap();

        let (subscription_sender, _) = mpsc::channel::<SubscriptionEvent>(1);

        // create handler and perform tested operation
        let request_handler = FetchSubscriptionsRequestHandler::new(subscription_sender);

        let result = request_handler
            .handle_request(
                RESOURCE_ID_FETCH_SUBSCRIPTIONS,
                &UAttributes::default(),
                Some(request_payload),
            )
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ServiceInvocationError::InvalidArgument(_) => {}
            _ => panic!("Wrong error type"),
        }
    }

    #[tokio::test]
    async fn test_no_request_payload() {
        helpers::init_once();

        // create request and other required object(s)
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };

        let (subscription_sender, _) = mpsc::channel::<SubscriptionEvent>(1);

        // create handler and perform tested operation
        let request_handler = FetchSubscriptionsRequestHandler::new(subscription_sender);

        let result = request_handler
            .handle_request(RESOURCE_ID_FETCH_SUBSCRIPTIONS, &message_attributes, None)
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ServiceInvocationError::InvalidArgument(_) => {}
            _ => panic!("Wrong error type"),
        }
    }

    #[tokio::test]
    async fn test_wrong_request_payload_type() {
        helpers::init_once();

        // create request and other required object(s)
        let subscribe_request =
            test_lib::helpers::unsubscribe_request(test_lib::helpers::local_topic1_uri());
        let request_payload = UPayload::try_from_protobuf(subscribe_request.clone()).unwrap();
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };

        let (subscription_sender, _) = mpsc::channel::<SubscriptionEvent>(1);

        // create handler and perform tested operation
        let request_handler = FetchSubscriptionsRequestHandler::new(subscription_sender);

        let result = request_handler
            .handle_request(
                RESOURCE_ID_FETCH_SUBSCRIPTIONS,
                &message_attributes,
                Some(request_payload),
            )
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ServiceInvocationError::InvalidArgument(_) => {}
            _ => panic!("Wrong error type"),
        }
    }

    // [utest->dsn~usubscription-fetch-subscriptions-invalid-subscriber~1]
    #[test_case(UUri::default(); "Bad subscriber UUri")]
    #[test_case(UUri {
            authority_name: String::from("*"),
            ue_id: test_lib::helpers::SUBSCRIBER1_ID,
            ue_version_major: test_lib::helpers::SUBSCRIBER1_VERSION as u32,
            resource_id: test_lib::helpers::SUBSCRIBER1_RESOURCE as u32,
            ..Default::default()
        }; "Wildcard authority in subscriber UUri")]
    #[test_case(UUri {
            authority_name: test_lib::helpers::LOCAL_AUTHORITY.into(),
            ue_id: 0xFFFF_0000,
            ue_version_major: test_lib::helpers::SUBSCRIBER1_VERSION as u32,
            resource_id: test_lib::helpers::SUBSCRIBER1_RESOURCE as u32,
            ..Default::default()
        }; "Wildcard entity id in subscriber UUri")]
    #[test_case(UUri {
            authority_name: test_lib::helpers::LOCAL_AUTHORITY.into(),
            ue_id: test_lib::helpers::SUBSCRIBER1_ID,
            ue_version_major: test_lib::helpers::SUBSCRIBER1_VERSION as u32,
            resource_id: 0x0000_FFFF,
            ..Default::default()
        }; "Wildcard resource id in subscriber UUri")]
    #[tokio::test]
    async fn test_invalid_subscriber_uri(subscriber: UUri) {
        helpers::init_once();

        // create request and other required object(s)
        let fetch_subscriptions_request = FetchSubscriptionsRequest {
            request: Some(up_rust::core::usubscription::Request::Subscriber(
                SubscriberInfo {
                    uri: Some(subscriber).into(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let request_payload =
            UPayload::try_from_protobuf(fetch_subscriptions_request.clone()).unwrap();
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };
        let (subscription_sender, _) = mpsc::channel::<SubscriptionEvent>(1);

        // create handler and perform tested operation
        let request_handler = FetchSubscriptionsRequestHandler::new(subscription_sender);

        let result = request_handler
            .handle_request(
                up_rust::core::usubscription::RESOURCE_ID_FETCH_SUBSCRIPTIONS,
                &message_attributes,
                Some(request_payload),
            )
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ServiceInvocationError::InvalidArgument(_) => {}
            _ => panic!("Wrong error type"),
        }
    }
}
