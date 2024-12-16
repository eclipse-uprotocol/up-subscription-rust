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
use std::str::FromStr;
use std::sync::Arc;
use tokio::{
    sync::{
        mpsc::{self, Sender},
        oneshot, Notify,
    },
    task::JoinHandle,
};

use crate::{
    helpers,
    notification_manager::{self, NotificationEvent},
    subscription_manager::{self, SubscriptionEvent},
    USubscriptionConfiguration,
};

use up_rust::{communication::RpcClient, LocalUriProvider, UCode, UStatus, UTransport, UUri};
use up_rust::{
    core::usubscription::{
        FetchSubscribersRequest, FetchSubscribersResponse, FetchSubscriptionsRequest,
        FetchSubscriptionsResponse, NotificationsRequest, Request, SubscriptionRequest,
        SubscriptionResponse, SubscriptionStatus, USubscription, UnsubscribeRequest,
        RESOURCE_ID_FETCH_SUBSCRIBERS, RESOURCE_ID_FETCH_SUBSCRIPTIONS,
        RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS, RESOURCE_ID_SUBSCRIBE,
        RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS, RESOURCE_ID_UNSUBSCRIBE, USUBSCRIPTION_TYPE_ID,
    },
    UAttributes,
};

use up_rust::communication::{
    InMemoryRpcServer, RequestHandler, RpcServer, ServiceInvocationError, UPayload,
};

/// Whether to include 'up:' uProtocol schema prefix in URIs in log and error messages
pub const INCLUDE_SCHEMA: bool = false;

// Remote-subscribe operation ttl; 5 minutes in milliseconds, as per https://github.com/eclipse-uprotocol/up-spec/tree/main/up-l3/usubscription/v3#6-timeout--retry-logic
pub(crate) const UP_REMOTE_TTL: u32 = 300000;

/// This trait primarily serves to provide a hook-point for using the mockall crate, for mocking USubscriptionService objects
/// where we also need/want to inject custom/mock UTransport implementations that subsequently get used in test cases.
pub trait UTransportHolder {
    fn get_transport(&self) -> Arc<dyn UTransport>;
}

impl UTransportHolder for USubscriptionService {
    fn get_transport(&self) -> Arc<dyn UTransport> {
        self.transport.clone()
    }
}

/// This object holds all mutable content associated with a running `USubscriptionService`, and is populated and returned when
/// calling `USubscriptionService::run()`. It exists for two reasons: a) allow `USubscriptionService` to remain useable as an immutable
/// object that can be put into `Arc`s and passed around freely, while b) offering a well-defined way to stop a running `USubscriptionService`
/// by simply calling `USubscriptionStopper::stop()`.
pub struct USubscriptionStopper {
    shutdown_notification: Arc<Notify>,
    subscription_joiner: Option<JoinHandle<()>>,
    notification_joiner: Option<JoinHandle<()>>,
}

impl USubscriptionStopper {
    pub async fn stop(&mut self) {
        self.shutdown_notification.notify_waiters();

        self.subscription_joiner
            .take()
            .expect("Has this USubscription instance already been stopped?")
            .await
            .expect("Error shutting down subscription manager");
        self.notification_joiner
            .take()
            .expect("Has this USubscription instance already been stopped?")
            .await
            .expect("Error shutting down notification manager");
    }
}

/// Core landing point and coordination of business logic of the uProtocol USubscription service. This implementation usually would be
/// front-ended by the various `listeners` to connect with corresponding uProtocol RPC server endpoints.
///
/// Functionally, the code in this context primarily cares about:
/// - input validation
/// - interaction with / orchestration of backends for managing subscriptions (`usubscription_manager.rs`) and dealing with notifications (`usubscription_notification.rs`)
#[derive(Clone)]
pub struct USubscriptionService {
    config: Arc<USubscriptionConfiguration>,
    server: Arc<InMemoryRpcServer>,

    transport: Arc<dyn UTransport>,
    subscription_sender: Sender<SubscriptionEvent>,
    // notification_sender: Sender<notification_manager::NotificationEvent>,
}

impl USubscriptionService {
    pub fn run(
        config: USubscriptionConfiguration,
        transport: Arc<dyn UTransport>,
    ) -> Result<(Arc<USubscriptionService>, USubscriptionStopper), UStatus> {
        helpers::init_once();

        let config = Arc::new(config);
        let server = Arc::new(InMemoryRpcServer::new(transport.clone(), config.clone()));

        let shutdown_notification = Arc::new(Notify::new());

        // Set up subscription manager actor
        let config_cloned = config.clone();
        let transport_cloned = transport.clone();
        let shutdown_notification_cloned = shutdown_notification.clone();
        let (subscription_sender, subscription_receiver) =
            mpsc::channel::<SubscriptionEvent>(config.subscription_command_buffer);
        let subscription_joiner = helpers::spawn_and_log_error(async move {
            subscription_manager::handle_message(
                config_cloned,
                transport_cloned,
                subscription_receiver,
                shutdown_notification_cloned,
            )
            .await;
            Ok(())
        });

        Ok((
            Arc::new(USubscriptionService {
                config,
                transport,
                server,
                subscription_sender,
            }),
            USubscriptionStopper {
                subscription_joiner: Some(subscription_joiner),
                // notification_joiner: Some(notification_joiner),
                notification_joiner: None,
                shutdown_notification,
            },
        ))
    }
}

struct USubscriptionRequestHandler;

#[async_trait]
impl RequestHandler for USubscriptionRequestHandler {
    async fn handle_request(
        &self,
        resource_id: u16,
        message_attributes: &UAttributes,
        request_payload: Option<UPayload>,
    ) -> Result<Option<UPayload>, ServiceInvocationError> {
        let Some(payload) = request_payload else {
            return Err(ServiceInvocationError::InvalidArgument(
                "No request payload".to_string(),
            ));
        };

        match resource_id {
            RESOURCE_ID_SUBSCRIBE => {
                let subscription_request: SubscriptionRequest =
                    payload.extract_protobuf().map_err(|e| {
                        ServiceInvocationError::InvalidArgument(
                            "Expected SubscriptionRequest payload".to_string(),
                        )
                    })?;

                let source = message_attributes.source.get_or_default();

                // Need message source UUri here
            }

            RESOURCE_ID_UNSUBSCRIBE => {}
            RESOURCE_ID_FETCH_SUBSCRIBERS => {}
            RESOURCE_ID_FETCH_SUBSCRIPTIONS => {}
            RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS => {}
            RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS => {}
            _ => {}
        }

        todo!()
    }
}
