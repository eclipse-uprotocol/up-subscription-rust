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
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::{sync::mpsc::Sender, sync::oneshot};

use up_rust::{
    communication::{RequestHandler, ServiceInvocationError, UPayload},
    core::usubscription::{
        SubscriptionRequest, SubscriptionResponse, SubscriptionStatus, RESOURCE_ID_SUBSCRIBE,
    },
    UAttributes,
};

use crate::{helpers, subscription_manager::SubscriptionEvent, usubscription};

pub(crate) struct SubscriptionRequestHandler {
    subscription_sender: Sender<SubscriptionEvent>,
}

impl SubscriptionRequestHandler {
    pub(crate) fn new(subscription_sender: Sender<SubscriptionEvent>) -> Self {
        Self {
            subscription_sender,
        }
    }
}

#[async_trait]
impl RequestHandler for SubscriptionRequestHandler {
    async fn handle_request(
        &self,
        resource_id: u16,
        message_attributes: &UAttributes,
        request_payload: Option<UPayload>,
    ) -> Result<Option<UPayload>, ServiceInvocationError> {
        // [impl->dsn~usubscription-subscribe-protobuf~1]
        let (subscription_request, source) = helpers::extract_inputs::<SubscriptionRequest>(
            RESOURCE_ID_SUBSCRIBE,
            resource_id,
            &request_payload,
            message_attributes,
        )?;
        let Some(topic) = subscription_request.topic.as_ref() else {
            return Err(ServiceInvocationError::InvalidArgument(
                "No topic defined in request".to_string(),
            ));
        };

        // Provisionally compute milliseconds to subscription expiry, from protobuf.google.Timestamp input in second granularity (we ignore the nanos).
        // Likely to change in the future, when we get rid of the protobuf.google.Timestamp type and track in milliseconds throughought.
        let expiry: Option<usubscription::ExpiryTimestamp> =
            match subscription_request.attributes.expire.seconds.try_into() {
                Ok(0) => None,
                Ok(seconds) => Some(seconds * 1000),
                Err(_) => None,
            };
        // Check if the expiry timestamp is in the future
        if let Some(expiry_ms) = expiry {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis();
            if now_ms > expiry_ms {
                return Err(ServiceInvocationError::InvalidArgument(
                    "Subscription Expiry time already passed".to_string(),
                ));
            }
        }
        // Interact with subscription manager backend
        let (respond_to, receive_from) = oneshot::channel::<SubscriptionStatus>();
        let se = SubscriptionEvent::AddSubscription {
            subscriber: source.clone(),
            topic: topic.clone(),
            expiry,
            respond_to,
        };

        if let Err(e) = self.subscription_sender.send(se).await {
            error!("Error communicating with subscription manager: {e}");
            return Err(ServiceInvocationError::Internal(
                "Error processing request".to_string(),
            ));
        }
        let Ok(status) = receive_from.await else {
            return Err(ServiceInvocationError::Internal(
                "Error processing request".to_string(),
            ));
        };

        // Build and return result
        let response = SubscriptionResponse {
            topic: Some(subscription_request.topic.unwrap_or_default()).into(),
            status: Some(status).into(),
            ..Default::default()
        };
        let response_payload = UPayload::try_from_protobuf(response).map_err(|e| {
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

    #[tokio::test]
    async fn test_subscribe_success() {
        helpers::init_once();

        // create request and other required object(s)
        let subscribe_request =
            test_lib::helpers::subscription_request(test_lib::helpers::local_topic1_uri(), None);
        let request_payload = UPayload::try_from_protobuf(subscribe_request.clone()).unwrap();
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };

        let (subscription_sender, mut subscription_receiver) =
            mpsc::channel::<SubscriptionEvent>(1);

        // create and spawn off handler, to make all the asnync goodness work
        let request_handler = SubscriptionRequestHandler::new(subscription_sender);
        tokio::spawn(async move {
            let result = request_handler
                .handle_request(
                    RESOURCE_ID_SUBSCRIBE,
                    &message_attributes,
                    Some(request_payload),
                )
                .await
                .unwrap();

            let response: SubscriptionResponse = result.unwrap().extract_protobuf().unwrap();
            assert_eq!(
                response.topic.unwrap_or_default(),
                test_lib::helpers::local_topic1_uri()
            );
            assert_eq!(response.status.unwrap().state, State::SUBSCRIBED.into());
        });

        // validate subscription manager interaction
        let subscription_event = subscription_receiver.recv().await.unwrap();
        match subscription_event {
            SubscriptionEvent::AddSubscription {
                subscriber,
                topic,
                expiry: None,
                respond_to,
            } => {
                assert_eq!(subscriber, test_lib::helpers::subscriber_uri1());
                assert_eq!(topic, test_lib::helpers::local_topic1_uri());

                let _ = respond_to.send(SubscriptionStatus {
                    state: State::SUBSCRIBED.into(),
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
            test_lib::helpers::subscription_request(test_lib::helpers::local_topic1_uri(), None);
        let request_payload = UPayload::try_from_protobuf(subscribe_request.clone()).unwrap();
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };

        let (subscription_sender, _) = mpsc::channel::<SubscriptionEvent>(1);

        // create handler and perform tested operation
        let request_handler = SubscriptionRequestHandler::new(subscription_sender);

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
        let request_handler = SubscriptionRequestHandler::new(subscription_sender);

        let result = request_handler
            .handle_request(
                RESOURCE_ID_SUBSCRIBE,
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
        let request_handler = SubscriptionRequestHandler::new(subscription_sender);

        let result = request_handler
            .handle_request(RESOURCE_ID_SUBSCRIBE, &message_attributes, None)
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
        let request_handler = SubscriptionRequestHandler::new(subscription_sender);

        let result = request_handler
            .handle_request(
                RESOURCE_ID_SUBSCRIBE,
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
    async fn test_future_subscription() {
        helpers::init_once();

        let future_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 600;

        // create request and other required object(s)
        // SubscriptionRequest currently uses protobuf.google.Timestamp for expiry attribute, which tracks timestamp in second granularity (we ignore the nanos)
        // Also, protobuf can only do signed ints, and uses an i64 for the seconds... so we have to force our timestamp into that.
        let subscribe_request = test_lib::helpers::subscription_request(
            test_lib::helpers::local_topic1_uri(),
            Some(u32::try_from(future_secs).unwrap()),
        );
        let request_payload = UPayload::try_from_protobuf(subscribe_request.clone()).unwrap();
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };

        let (subscription_sender, mut subscription_receiver) =
            mpsc::channel::<SubscriptionEvent>(1);

        // create and spawn off handler, to make all the asnync goodness work
        let request_handler = SubscriptionRequestHandler::new(subscription_sender);
        tokio::spawn(async move {
            let result = request_handler
                .handle_request(
                    RESOURCE_ID_SUBSCRIBE,
                    &message_attributes,
                    Some(request_payload),
                )
                .await
                .unwrap();

            let response: SubscriptionResponse = result.unwrap().extract_protobuf().unwrap();
            assert_eq!(
                response.topic.unwrap_or_default(),
                test_lib::helpers::local_topic1_uri()
            );
            assert_eq!(response.status.unwrap().state, State::SUBSCRIBED.into());
        });

        // validate subscription manager interaction
        let subscription_event = subscription_receiver.recv().await.unwrap();
        match subscription_event {
            SubscriptionEvent::AddSubscription {
                subscriber,
                topic,
                expiry,
                respond_to,
            } => {
                assert_eq!(subscriber, test_lib::helpers::subscriber_uri1());
                assert_eq!(topic, test_lib::helpers::local_topic1_uri());
                // we're passing in seconds above, because of how SubscriptionRequest is defined - but internally we are handling milliseconds; therefore *1000
                assert_eq!(
                    expiry,
                    Some((future_secs as usubscription::ExpiryTimestamp) * 1000)
                );

                let _ = respond_to.send(SubscriptionStatus {
                    state: State::SUBSCRIBED.into(),
                    ..Default::default()
                });
            }
            _ => panic!("Wrong event type"),
        }
    }

    #[tokio::test]
    async fn test_expired_subscription() {
        helpers::init_once();

        // create request and other required object(s)
        let subscribe_request = test_lib::helpers::subscription_request(
            test_lib::helpers::local_topic1_uri(),
            Some(10),
        );
        let request_payload = UPayload::try_from_protobuf(subscribe_request.clone()).unwrap();
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };

        let (subscription_sender, _) = mpsc::channel::<SubscriptionEvent>(1);

        // create handler and perform tested operation
        let request_handler = SubscriptionRequestHandler::new(subscription_sender);

        let result = request_handler
            .handle_request(
                up_rust::core::usubscription::RESOURCE_ID_SUBSCRIBE,
                &message_attributes,
                Some(request_payload),
            )
            .await;

        assert!(result.is_err_and(|e| matches!(e, ServiceInvocationError::InvalidArgument(_))));
    }
}
