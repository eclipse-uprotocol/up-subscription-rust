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
        SubscriptionStatus, UnsubscribeRequest, UnsubscribeResponse, RESOURCE_ID_UNSUBSCRIBE,
    },
    UAttributes,
};

use crate::{helpers, subscription_manager::SubscriptionEvent};

pub(crate) struct UnubscribeRequestHandler {
    subscription_sender: Sender<SubscriptionEvent>,
}

impl UnubscribeRequestHandler {
    pub(crate) fn new(subscription_sender: Sender<SubscriptionEvent>) -> Self {
        Self {
            subscription_sender,
        }
    }
}

#[async_trait]
impl RequestHandler for UnubscribeRequestHandler {
    async fn handle_request(
        &self,
        resource_id: u16,
        message_attributes: &UAttributes,
        request_payload: Option<UPayload>,
    ) -> Result<Option<UPayload>, ServiceInvocationError> {
        // [impl->dsn~usubscription-unsubscribe-protobuf~1]
        let (unsubscribe_request, source) = helpers::extract_inputs::<UnsubscribeRequest>(
            RESOURCE_ID_UNSUBSCRIBE,
            resource_id,
            &request_payload,
            message_attributes,
        )?;
        let Some(topic) = unsubscribe_request.topic.as_ref() else {
            return Err(ServiceInvocationError::InvalidArgument(
                "No topic defined in request".to_string(),
            ));
        };

        let (respond_to, receive_from) = oneshot::channel::<SubscriptionStatus>();
        let se = SubscriptionEvent::RemoveSubscription {
            subscriber: source.clone(),
            topic: topic.clone(),
            respond_to,
        };

        // Interact with subscription manager backend
        if let Err(e) = self.subscription_sender.send(se).await {
            error!("Error communicating with subscription manager: {e}");
            return Err(ServiceInvocationError::Internal(
                "Error processing request".to_string(),
            ));
        }
        let Ok(_status) = receive_from.await else {
            return Err(ServiceInvocationError::Internal(
                "Error processing request".to_string(),
            ));
        };

        // Build and return result
        let response_payload = UPayload::try_from_protobuf(UnsubscribeResponse::default())
            .map_err(|e| {
                ServiceInvocationError::Internal(format!("Error building response payload: {e}"))
            })?;

        Ok(Some(response_payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::{self};
    use up_rust::core::usubscription::State;

    use crate::{helpers, tests::test_lib};

    // [utest->dsn~usubscription-unsubscribe-protobuf~1]
    #[tokio::test]
    async fn test_unsubscribe_success() {
        helpers::init_once();

        // create request and other required object(s)
        let unsubscribe_request =
            test_lib::helpers::unsubscribe_request(test_lib::helpers::local_topic1_uri());
        let request_payload = UPayload::try_from_protobuf(unsubscribe_request.clone()).unwrap();
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };

        let (subscription_sender, mut subscription_receiver) =
            mpsc::channel::<SubscriptionEvent>(1);

        // create and spawn off handler, to make all the asnync goodness work
        let request_handler = UnubscribeRequestHandler::new(subscription_sender);
        tokio::spawn(async move {
            let result = request_handler
                .handle_request(
                    RESOURCE_ID_UNSUBSCRIBE,
                    &message_attributes,
                    Some(request_payload),
                )
                .await
                .unwrap();

            assert_eq!(
                result
                    .unwrap()
                    .extract_protobuf::<UnsubscribeResponse>()
                    .unwrap(),
                UnsubscribeResponse::default()
            );
        });

        // validate subscription manager interaction
        let subscription_event = subscription_receiver.recv().await.unwrap();
        match subscription_event {
            SubscriptionEvent::RemoveSubscription {
                subscriber,
                topic,
                respond_to,
            } => {
                assert_eq!(subscriber, test_lib::helpers::subscriber_uri1());
                assert_eq!(topic, test_lib::helpers::local_topic1_uri());

                let _ = respond_to.send(SubscriptionStatus {
                    state: State::UNSUBSCRIBED.into(),
                    ..Default::default()
                });
            }
            _ => panic!("Wrong event type"),
        }
    }

    #[tokio::test]
    async fn test_wrong_resource_id() {
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
        let request_handler = UnubscribeRequestHandler::new(subscription_sender);

        let result = request_handler
            .handle_request(
                up_rust::core::usubscription::RESOURCE_ID_SUBSCRIBE,
                &message_attributes,
                Some(request_payload),
            )
            .await;

        assert!(result.is_err_and(|err| matches!(err, ServiceInvocationError::InvalidArgument(_))));
    }

    #[tokio::test]
    async fn test_no_source_uri() {
        helpers::init_once();

        // create request and other required object(s)
        let subscribe_request =
            test_lib::helpers::unsubscribe_request(test_lib::helpers::local_topic1_uri());
        let request_payload = UPayload::try_from_protobuf(subscribe_request.clone()).unwrap();

        let (subscription_sender, _) = mpsc::channel::<SubscriptionEvent>(1);

        // create handler and perform tested operation
        let request_handler = UnubscribeRequestHandler::new(subscription_sender);

        let result = request_handler
            .handle_request(
                RESOURCE_ID_UNSUBSCRIBE,
                &UAttributes::default(),
                Some(request_payload),
            )
            .await;

        assert!(result.is_err_and(|err| matches!(err, ServiceInvocationError::InvalidArgument(_))));
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
        let request_handler = UnubscribeRequestHandler::new(subscription_sender);

        let result = request_handler
            .handle_request(RESOURCE_ID_UNSUBSCRIBE, &message_attributes, None)
            .await;

        assert!(result.is_err_and(|err| matches!(err, ServiceInvocationError::InvalidArgument(_))));
    }

    #[tokio::test]
    async fn test_wrong_request_payload_type() {
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
        let request_handler = UnubscribeRequestHandler::new(subscription_sender);

        let result = request_handler
            .handle_request(
                RESOURCE_ID_UNSUBSCRIBE,
                &message_attributes,
                Some(request_payload),
            )
            .await;

        assert!(result.is_err_and(|err| matches!(err, ServiceInvocationError::InvalidArgument(_))));
    }
}
