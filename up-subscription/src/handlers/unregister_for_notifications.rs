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
use tokio::sync::mpsc::Sender;

use up_rust::{
    communication::{RequestHandler, ServiceInvocationError, UPayload},
    core::usubscription::{
        NotificationsRequest, NotificationsResponse, RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS,
    },
    UAttributes,
};

use crate::{helpers, notification_manager::NotificationEvent};

pub(crate) struct UnregisterNotificationsRequestHandler {
    notification_sender: Sender<NotificationEvent>,
}

impl UnregisterNotificationsRequestHandler {
    pub(crate) fn new(notification_sender: Sender<NotificationEvent>) -> Self {
        Self {
            notification_sender,
        }
    }
}

#[async_trait]
impl RequestHandler for UnregisterNotificationsRequestHandler {
    async fn handle_request(
        &self,
        resource_id: u16,
        message_attributes: &UAttributes,
        request_payload: Option<UPayload>,
    ) -> Result<Option<UPayload>, ServiceInvocationError> {
        // [impl->dsn~usubscription-unregister-notifications-protobuf~1]
        let (_subscription_request, source) = helpers::extract_inputs::<NotificationsRequest>(
            RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS,
            resource_id,
            &request_payload,
            message_attributes,
        )?;

        // Interact with notification manager backend
        let se = NotificationEvent::RemoveNotifyee {
            subscriber: source.clone(),
        };

        if let Err(e) = self.notification_sender.send(se).await {
            error!("Error communicating with notification manager: {e}");
            return Err(ServiceInvocationError::Internal(
                "Error processing requestr".to_string(),
            ));
        }

        // Build and return result
        let response_payload = UPayload::try_from_protobuf(NotificationsResponse::default())
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

    use crate::{helpers, tests::test_lib};

    // [utest->dsn~usubscription-unregister-notifications-protobuf~1]
    #[tokio::test]
    async fn test_unregister_notification_success() {
        helpers::init_once();

        // create request and other required object(s)
        let notification_request = NotificationsRequest {
            topic: Some(test_lib::helpers::local_topic1_uri()).into(),
            ..Default::default()
        };
        let request_payload = UPayload::try_from_protobuf(notification_request.clone()).unwrap();
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };
        let (notification_sender, mut notification_receiver) =
            mpsc::channel::<NotificationEvent>(1);

        // create and spawn off handler, to make all the asnync goodness work
        let request_handler = UnregisterNotificationsRequestHandler::new(notification_sender);
        tokio::spawn(async move {
            let result = request_handler
                .handle_request(
                    RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS,
                    &message_attributes,
                    Some(request_payload),
                )
                .await
                .unwrap();

            assert!(result
                .unwrap()
                .extract_protobuf::<NotificationsResponse>()
                .is_ok());
        });

        // validate subscription manager interaction
        let notification_event = notification_receiver.recv().await.unwrap();
        match notification_event {
            NotificationEvent::RemoveNotifyee { subscriber } => {
                assert_eq!(subscriber, test_lib::helpers::subscriber_uri1());
            }
            _ => panic!("Wrong event type"),
        }
    }

    #[tokio::test]
    async fn test_wrong_resource_id() {
        helpers::init_once();

        // create request and other required object(s)
        let notification_request = NotificationsRequest {
            topic: Some(test_lib::helpers::local_topic1_uri()).into(),
            ..Default::default()
        };
        let request_payload = UPayload::try_from_protobuf(notification_request.clone()).unwrap();
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };
        let (notification_sender, _) = mpsc::channel::<NotificationEvent>(1);

        // create handler and perform tested operation
        let request_handler = UnregisterNotificationsRequestHandler::new(notification_sender);

        let result = request_handler
            .handle_request(
                up_rust::core::usubscription::RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS,
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
        let notification_request = NotificationsRequest {
            topic: Some(test_lib::helpers::local_topic1_uri()).into(),
            ..Default::default()
        };
        let request_payload = UPayload::try_from_protobuf(notification_request.clone()).unwrap();
        let (notification_sender, _) = mpsc::channel::<NotificationEvent>(1);

        // create handler and perform tested operation
        let request_handler = UnregisterNotificationsRequestHandler::new(notification_sender);

        let result = request_handler
            .handle_request(
                RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS,
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
        let (notification_sender, _) = mpsc::channel::<NotificationEvent>(1);

        // create handler and perform tested operation
        let request_handler = UnregisterNotificationsRequestHandler::new(notification_sender);

        let result = request_handler
            .handle_request(
                RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS,
                &message_attributes,
                None,
            )
            .await;

        assert!(result.is_err_and(|err| matches!(err, ServiceInvocationError::InvalidArgument(_))));
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
        let (notification_sender, _) = mpsc::channel::<NotificationEvent>(1);

        // create handler and perform tested operation
        let request_handler = UnregisterNotificationsRequestHandler::new(notification_sender);

        let result = request_handler
            .handle_request(
                RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS,
                &message_attributes,
                Some(request_payload),
            )
            .await;

        assert!(result.is_err_and(|err| matches!(err, ServiceInvocationError::InvalidArgument(_))));
    }
}
