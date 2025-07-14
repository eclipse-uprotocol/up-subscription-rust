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
        FetchSubscribersRequest, FetchSubscribersResponse, SubscriberInfo,
        RESOURCE_ID_FETCH_SUBSCRIBERS,
    },
    UAttributes,
};

use crate::{helpers, subscription_manager::SubscriptionEvent, usubscription::SubscriberUUri};

pub(crate) struct FetchSubscribersRequestHandler {
    subscription_sender: Sender<SubscriptionEvent>,
}

impl FetchSubscribersRequestHandler {
    pub(crate) fn new(subscription_sender: Sender<SubscriptionEvent>) -> Self {
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
        message_attributes: &UAttributes,
        request_payload: Option<UPayload>,
    ) -> Result<Option<UPayload>, ServiceInvocationError> {
        // [impl->dsn~usubscription-fetch-subscribers-protobuf~1]
        let (fetch_subscribers_request, _source) =
            helpers::extract_inputs::<FetchSubscribersRequest>(
                RESOURCE_ID_FETCH_SUBSCRIBERS,
                resource_id,
                &request_payload,
                message_attributes,
            )?;
        let FetchSubscribersRequest { topic, .. } = fetch_subscribers_request;
        let Some(topic) = topic.into_option() else {
            return Err(ServiceInvocationError::InvalidArgument(
                "No topic defined in request".to_string(),
            ));
        };

        // topic input validation
        // [impl->dsn~usubscription-fetch-subscribers-invalid-topic~1]
        helpers::validate_uri(&topic)?;

        // Interact with subscription manager backend
        let (respond_to, receive_from) = oneshot::channel::<Vec<SubscriberUUri>>();
        let se = SubscriptionEvent::FetchSubscribers { topic, respond_to };

        if let Err(e) = self.subscription_sender.send(se).await {
            error!("Error communicating with subscription manager: {e}");
            return Err(ServiceInvocationError::Internal(
                "Error processing request".to_string(),
            ));
        }
        let Ok(fetch_subscribers_response) = receive_from.await else {
            return Err(ServiceInvocationError::Internal(
                "Error processing request".to_string(),
            ));
        };

        // Build and return result
        let subscriber_infos = fetch_subscribers_response
            .iter()
            .map(|subscriber| SubscriberInfo {
                uri: Some(subscriber.clone()).into(),
                ..Default::default()
            })
            .collect();
        let fetch_subscribers_response = FetchSubscribersResponse {
            subscribers: subscriber_infos,
            ..Default::default()
        };

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
    use test_case::test_case;
    use tokio::sync::mpsc::{self};

    use up_rust::UUri;

    use crate::{helpers, tests::test_lib};

    // [utest->dsn~usubscription-fetch-subscribers-protobuf~1]
    #[tokio::test]
    async fn test_subscribe_success() {
        helpers::init_once();

        // create request and other required object(s)
        let fetch_subscribers_request = FetchSubscribersRequest {
            topic: Some(test_lib::helpers::local_topic1_uri()).into(),
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
        let request_handler = FetchSubscribersRequestHandler::new(subscription_sender);
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
                topic, respond_to, ..
            } => {
                assert_eq!(topic, test_lib::helpers::local_topic1_uri());
                let _ = respond_to.send(Vec::default());
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
        let request_handler = FetchSubscribersRequestHandler::new(subscription_sender);

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
        let request_handler = FetchSubscribersRequestHandler::new(subscription_sender);

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
        let request_handler = FetchSubscribersRequestHandler::new(subscription_sender);

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
        let request_handler = FetchSubscribersRequestHandler::new(subscription_sender);

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

    // [utest->dsn~usubscription-fetch-subscribers-invalid-topic~1]
    #[test_case(UUri::default(); "Bad topic UUri")]
    #[test_case(UUri {
            authority_name: String::from("*"),
            ue_id: test_lib::helpers::TOPIC_LOCAL1_ID,
            ue_version_major: test_lib::helpers::TOPIC_LOCAL1_VERSION as u32,
            resource_id: test_lib::helpers::TOPIC_LOCAL1_RESOURCE as u32,
            ..Default::default()
        }; "Wildcard authority in topic UUri")]
    #[test_case(UUri {
            authority_name: test_lib::helpers::LOCAL_AUTHORITY.into(),
            ue_id: 0xFFFF_0000,
            ue_version_major: test_lib::helpers::TOPIC_LOCAL1_VERSION as u32,
            resource_id: test_lib::helpers::TOPIC_LOCAL1_RESOURCE as u32,
            ..Default::default()
        }; "Wildcard entity id in topic UUri")]
    #[test_case(UUri {
            authority_name: test_lib::helpers::LOCAL_AUTHORITY.into(),
            ue_id: test_lib::helpers::TOPIC_LOCAL1_ID,
            ue_version_major: test_lib::helpers::TOPIC_LOCAL1_VERSION as u32,
            resource_id: 0x0000_FFFF,
            ..Default::default()
        }; "Wildcard resource id in topic UUri")]
    #[tokio::test]
    async fn test_invalid_topic_uri(topic: UUri) {
        helpers::init_once();

        // create request and other required object(s)
        let subscribe_request = test_lib::helpers::subscription_request(topic, None);
        let request_payload = UPayload::try_from_protobuf(subscribe_request.clone()).unwrap();
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };
        let (subscription_sender, _) = mpsc::channel::<SubscriptionEvent>(1);

        // create handler and perform tested operation
        let request_handler = FetchSubscribersRequestHandler::new(subscription_sender);

        let result = request_handler
            .handle_request(
                up_rust::core::usubscription::RESOURCE_ID_FETCH_SUBSCRIBERS,
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
