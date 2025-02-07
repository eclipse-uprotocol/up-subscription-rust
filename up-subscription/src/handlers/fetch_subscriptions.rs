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
use std::sync::Arc;
use tokio::{sync::mpsc::Sender, sync::oneshot};

use crate::subscription_manager::SubscriptionEvent;

use up_rust::{
    communication::{RequestHandler, ServiceInvocationError, UPayload},
    core::usubscription::{
        FetchSubscriptionsRequest, FetchSubscriptionsResponse, RESOURCE_ID_FETCH_SUBSCRIPTIONS,
    },
    UAttributes,
};

pub(crate) struct FetchSubscriptionsRequestHandler {
    subscription_sender: Arc<Sender<SubscriptionEvent>>,
}

impl FetchSubscriptionsRequestHandler {
    pub(crate) fn new(subscription_sender: Arc<Sender<SubscriptionEvent>>) -> Self {
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
        _message_attributes: &UAttributes,
        request_payload: Option<UPayload>,
    ) -> Result<Option<UPayload>, ServiceInvocationError> {
        // Some input validation
        if resource_id != RESOURCE_ID_FETCH_SUBSCRIPTIONS {
            return Err(ServiceInvocationError::InvalidArgument(format!(
                "Wrong resource ID (expected {}, got {})",
                RESOURCE_ID_FETCH_SUBSCRIPTIONS, resource_id
            )));
        }
        let Some(payload) = request_payload else {
            return Err(ServiceInvocationError::InvalidArgument(
                "No request payload".to_string(),
            ));
        };
        let fetch_subscriptions_request: FetchSubscriptionsRequest =
            payload.extract_protobuf().map_err(|e| {
                ServiceInvocationError::InvalidArgument(
                    format!("Expected FetchSubscriptionsRequest payload, error when unpacking {e}")
                        .to_string(),
                )
            })?;

        // Interact with subscription manager backend
        let (respond_to, receive_from) = oneshot::channel::<FetchSubscriptionsResponse>();
        let se = SubscriptionEvent::FetchSubscriptions {
            request: fetch_subscriptions_request,
            respond_to,
        };

        if let Err(e) = self.subscription_sender.send(se).await {
            return Err(ServiceInvocationError::Internal(format!(
                "Error communicating with subscription manager: {e}"
            )));
        }
        let Ok(fetch_subscriptions_response) = receive_from.await else {
            return Err(ServiceInvocationError::Internal(
                "Error communicating with subscription manager".to_string(),
            ));
        };

        // Build and return result
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
    use tokio::sync::mpsc::{self};

    use crate::{helpers, tests::test_lib};

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
        let request_handler = FetchSubscriptionsRequestHandler::new(Arc::new(subscription_sender));
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
                respond_to,
            } => {
                match request.request.as_ref().unwrap() {
                    up_rust::core::usubscription::Request::Subscriber(subscriber_info) => {
                        assert_eq!(subscriber_info, &test_lib::helpers::subscriber_info1());
                    }
                    _ => panic!("Wrong request details"),
                }
                assert_eq!(request.offset.unwrap_or_default(), 42);

                let _ = respond_to.send(FetchSubscriptionsResponse::default());
            }
            _ => panic!("Wrong event type"),
        }
    }

    #[tokio::test]
    async fn test_wrong_resource_id() {
        helpers::init_once();

        // create request and other required object(s)
        let subscribe_request =
            test_lib::helpers::subscription_request(test_lib::helpers::local_topic1_uri());
        let request_payload = UPayload::try_from_protobuf(subscribe_request.clone()).unwrap();
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };
        let (subscription_sender, _) = mpsc::channel::<SubscriptionEvent>(1);

        // create handler and perform tested operation
        let request_handler = FetchSubscriptionsRequestHandler::new(Arc::new(subscription_sender));

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
            test_lib::helpers::subscription_request(test_lib::helpers::local_topic1_uri());
        let request_payload = UPayload::try_from_protobuf(subscribe_request.clone()).unwrap();

        let (subscription_sender, _) = mpsc::channel::<SubscriptionEvent>(1);

        // create handler and perform tested operation
        let request_handler = FetchSubscriptionsRequestHandler::new(Arc::new(subscription_sender));

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
        let request_handler = FetchSubscriptionsRequestHandler::new(Arc::new(subscription_sender));

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
        let request_handler = FetchSubscriptionsRequestHandler::new(Arc::new(subscription_sender));

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
}
