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
        FetchSubscribersRequest, FetchSubscribersResponse, RESOURCE_ID_FETCH_SUBSCRIBERS,
    },
    UAttributes,
};

pub(crate) struct FetchSubscribersRequestHandler {
    subscription_sender: Arc<Sender<SubscriptionEvent>>,
}

impl FetchSubscribersRequestHandler {
    pub(crate) fn new(subscription_sender: Arc<Sender<SubscriptionEvent>>) -> Self {
        Self {
            subscription_sender,
        }
    }
}

#[async_trait]
impl RequestHandler for FetchSubscribersRequestHandler {
    async fn handle_request(
        &self,
        resource_id: u16,
        _message_attributes: &UAttributes,
        request_payload: Option<UPayload>,
    ) -> Result<Option<UPayload>, ServiceInvocationError> {
        // Some input validation
        if resource_id != RESOURCE_ID_FETCH_SUBSCRIBERS {
            return Err(ServiceInvocationError::InvalidArgument(
                "Wrong resource ID".to_string(),
            ));
        }
        let Some(payload) = request_payload else {
            return Err(ServiceInvocationError::InvalidArgument(
                "No request payload".to_string(),
            ));
        };
        let fetch_subscribers_request: FetchSubscribersRequest =
            payload.extract_protobuf().map_err(|e| {
                ServiceInvocationError::InvalidArgument(
                    format!("Expected FetchSubscribersRequest payload, error when unpacking {e}")
                        .to_string(),
                )
            })?;

        // Interact with subscription manager backend
        let (respond_to, receive_from) = oneshot::channel::<FetchSubscribersResponse>();
        let se = SubscriptionEvent::FetchSubscribers {
            request: fetch_subscribers_request,
            respond_to,
        };

        if let Err(e) = self.subscription_sender.send(se).await {
            return Err(ServiceInvocationError::Internal(format!(
                "Error communicating with subscription manager: {e}"
            )));
        }
        let Ok(fetch_subscribers_response) = receive_from.await else {
            return Err(ServiceInvocationError::Internal(
                "Error communicating with subscription manager".to_string(),
            ));
        };

        // Build and return result
        let response_payload =
            UPayload::try_from_protobuf(fetch_subscribers_response).map_err(|e| {
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
    async fn test_subscribe_success() {
        helpers::init_once();

        // create request and other required object(s)
        let fetch_subscribers_request = FetchSubscribersRequest {
            topic: Some(test_lib::helpers::local_topic1_uri()).into(),
            offset: Some(42),
            ..Default::default()
        };
        let request_payload =
            UPayload::try_from_protobuf(fetch_subscribers_request.clone()).unwrap();
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };

        let (subscription_sender, mut subscription_receiver) =
            mpsc::channel::<SubscriptionEvent>(1);

        // create and spawn off handler, to make all the asnync goodness work
        let request_handler = FetchSubscribersRequestHandler::new(Arc::new(subscription_sender));
        tokio::spawn(async move {
            let result = request_handler
                .handle_request(
                    RESOURCE_ID_FETCH_SUBSCRIBERS,
                    &message_attributes,
                    Some(request_payload),
                )
                .await
                .unwrap();

            let response: FetchSubscribersResponse = result.unwrap().extract_protobuf().unwrap();
            assert_eq!(response, FetchSubscribersResponse::default());
        });

        // validate subscription manager interaction
        let subscription_event = subscription_receiver.recv().await.unwrap();
        match subscription_event {
            SubscriptionEvent::FetchSubscribers {
                request,
                respond_to,
            } => {
                assert_eq!(
                    request.topic.unwrap_or_default(),
                    test_lib::helpers::local_topic1_uri()
                );
                assert_eq!(request.offset.unwrap_or_default(), 42);

                let _ = respond_to.send(FetchSubscribersResponse::default());
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
        let request_handler = FetchSubscribersRequestHandler::new(Arc::new(subscription_sender));

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
        let request_handler = FetchSubscribersRequestHandler::new(Arc::new(subscription_sender));

        let result = request_handler
            .handle_request(
                RESOURCE_ID_FETCH_SUBSCRIBERS,
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
        let request_handler = FetchSubscribersRequestHandler::new(Arc::new(subscription_sender));

        let result = request_handler
            .handle_request(RESOURCE_ID_FETCH_SUBSCRIBERS, &message_attributes, None)
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
        let request_handler = FetchSubscribersRequestHandler::new(Arc::new(subscription_sender));

        let result = request_handler
            .handle_request(
                RESOURCE_ID_FETCH_SUBSCRIBERS,
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
