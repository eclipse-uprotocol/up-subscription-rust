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

use std::sync::Arc;
use tokio::{
    sync::{
        mpsc::{self, Sender},
        oneshot, Notify,
    },
    task::JoinHandle,
};
use tracing::{error, info};

use up_rust::{
    communication::{
        InMemoryRpcClient, RequestHandler, ServiceInvocationError, SubscriptionStatus, UPayload,
    },
    core::usubscription::{
        extract_usubscription_request, FetchSubscriptionsResponse, SubscribeResponse,
        SubscriptionInfo, USubscriptionRequest, USubscriptionResponse,
    },
    UAttributes, UCode, UStatus, UTransport, UUri,
};

use crate::{
    adapters::{DynTransport, DynUriProvider},
    helpers,
    notification_manager::{self, NotificationEvent},
    subscription_manager::{self, SubscriptionEvent},
    USubscriptionConfiguration,
};

// Alias definitions to provide more clarity, and make it easier to accomodate potential changes to expiry type in up-spec
pub(crate) type SubscriberUUri = UUri;
pub(crate) type TopicUUri = UUri;

/// This trait primarily serves to provide a hook-point for using the mockall crate, for mocking USubscriptionService objects
/// where we also need/want to inject custom/mock UTransport implementations that subsequently get used in test cases.
pub trait UTransportHolder {
    fn get_transport(&self) -> Arc<dyn UTransport>;
}
impl<S> UTransportHolder for USubscriptionService<S> {
    fn get_transport(&self) -> Arc<dyn UTransport> {
        self.transport.clone()
    }
}

/// USubscriptionService, compile-time enforced type-state pattern
pub struct Idle;
pub struct Running {
    subscription_sender: Sender<SubscriptionEvent>,
    notification_sender: Sender<NotificationEvent>,
    shutdown_notification: Arc<Notify>,
    subscription_joiner: JoinHandle<()>,
    notification_joiner: JoinHandle<()>,
}
pub struct USubscriptionService<S = Idle> {
    config: Arc<USubscriptionConfiguration>,
    transport: Arc<dyn UTransport>,
    state: S,
}

impl USubscriptionService<Idle> {
    pub fn new(config: Arc<USubscriptionConfiguration>, transport: Arc<dyn UTransport>) -> Self {
        Self {
            config,
            transport,
            state: Idle,
        }
    }

    pub async fn run(self) -> Result<USubscriptionService<Running>, UStatus> {
        let shutdown_notification = Arc::new(Notify::new());
        let (notification_sender, notification_receiver) =
            mpsc::channel::<NotificationEvent>(self.config.notification_command_buffer.into());
        let (subscription_sender, subscription_receiver) =
            mpsc::channel::<SubscriptionEvent>(self.config.subscription_command_buffer.into());

        // RpcClient for handling remote subscriptions
        let rpc_client = Arc::new(
            InMemoryRpcClient::new(
                Arc::new(DynTransport::new(self.transport.clone())),
                Arc::new(DynUriProvider::new(self.config.clone())),
            )
            .await
            .map_err(|e| UStatus::fail_with_code(UCode::Internal, e.to_string()))?,
        );

        // Set up notification manager actor
        let config_cloned = self.config.clone();
        let transport_cloned = self.transport.clone();
        let shutdown_notification_cloned = shutdown_notification.clone();
        let notification_joiner = helpers::spawn_and_log_error(async move {
            notification_manager::notification_engine(
                config_cloned,
                transport_cloned,
                notification_receiver,
                shutdown_notification_cloned,
            )
            .await;
            Ok(())
        });

        // Set up subscription manager actor
        let config_cloned = self.config.clone();
        let shutdown_notification_cloned = shutdown_notification.clone();
        let notification_sender_cloned = notification_sender.clone();
        let subscription_joiner = helpers::spawn_and_log_error(async move {
            subscription_manager::handle_message(
                config_cloned,
                rpc_client,
                subscription_receiver,
                notification_sender_cloned,
                shutdown_notification_cloned,
            )
            .await;
            Ok(())
        });

        Ok(USubscriptionService {
            config: self.config,
            transport: self.transport,
            state: Running {
                subscription_sender,
                notification_sender,
                subscription_joiner,
                notification_joiner,
                shutdown_notification,
            },
        })
    }
}

impl USubscriptionService<Running> {
    pub async fn stop(self) {
        info!("Stopping uSubscription service");
        self.state.shutdown_notification.notify_waiters();
        self.state
            .subscription_joiner
            .await
            .expect("Error shutting down subscription manager");
        self.state
            .notification_joiner
            .await
            .expect("Error shutting down notification manager");
    }
}

#[async_trait::async_trait]
impl RequestHandler for USubscriptionService<Running> {
    async fn handle_request(
        &self,
        resource_id: u16,
        message_attributes: &UAttributes,
        request_payload: Option<UPayload>,
    ) -> Result<Option<UPayload>, ServiceInvocationError> {
        // Decode the payload of uSubscription operations into a typed request. Malformed or
        // unsupported requests result in a `ServiceInvocationError`.
        let request = extract_usubscription_request(resource_id, request_payload)?;

        #[allow(clippy::wildcard_enum_match_arm)]
        match request {
            USubscriptionRequest::Subscribe(req) => {
                let (respond_to, receive_from) = oneshot::channel::<SubscriptionStatus>();
                let se = SubscriptionEvent::AddSubscription {
                    subscriber: message_attributes.source().clone(),
                    topic: req.topic.clone(),
                    expiration: req.expiration,
                    sample_period: req.sample_period,
                    respond_to,
                };
                if let Err(e) = self.state.subscription_sender.send(se).await {
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
                USubscriptionResponse::Subscribe(SubscribeResponse {
                    topic: req.topic,
                    status,
                })
            }
            USubscriptionRequest::Unsubscribe(req) => {
                let (respond_to, receive_from) = oneshot::channel::<SubscriptionStatus>();
                let se = SubscriptionEvent::RemoveSubscription {
                    subscriber: message_attributes.source().clone(),
                    topic: req.topic.clone(),
                    respond_to,
                };
                if let Err(e) = self.state.subscription_sender.send(se).await {
                    error!("Error communicating with subscription manager: {e}");
                    return Err(ServiceInvocationError::Internal(
                        "Error processing request".to_string(),
                    ));
                }
                if receive_from.await.is_err() {
                    return Err(ServiceInvocationError::Internal(
                        "Error processing request".to_string(),
                    ));
                };
                USubscriptionResponse::Unsubscribe(())
            }
            USubscriptionRequest::FetchSubscriptions(req) => {
                let (respond_to, receive_from) = oneshot::channel::<Vec<SubscriptionInfo>>();
                let se = SubscriptionEvent::FetchSubscriptions {
                    subscriber_filter: req.subscriber_filter.clone(),
                    topic_filter: req.topic_filter.clone(),
                    respond_to,
                };
                if let Err(e) = self.state.subscription_sender.send(se).await {
                    error!("Error communicating with subscription manager: {e}");
                    return Err(ServiceInvocationError::Internal(
                        "Error processing request".to_string(),
                    ));
                }
                let Ok(subscriptions) = receive_from.await else {
                    return Err(ServiceInvocationError::Internal(
                        "Error processing request".to_string(),
                    ));
                };
                USubscriptionResponse::FetchSubscriptions(FetchSubscriptionsResponse {
                    subscriptions,
                })
            }
            USubscriptionRequest::RegisterForNotification(()) => {
                let se = NotificationEvent::AddNotifyee {
                    subscriber: message_attributes.source().clone(),
                };
                if let Err(e) = self.state.notification_sender.send(se).await {
                    error!("Error communicating with subscription manager: {e}");
                    return Err(ServiceInvocationError::Internal(
                        "Error processing request".to_string(),
                    ));
                }
                USubscriptionResponse::RegisterForNotification(())
            }
            USubscriptionRequest::UnregisterForNotification(()) => {
                let se = NotificationEvent::RemoveNotifyee {
                    subscriber: message_attributes.source().clone(),
                };
                if let Err(e) = self.state.notification_sender.send(se).await {
                    error!("Error communicating with subscription manager: {e}");
                    return Err(ServiceInvocationError::Internal(
                        "Error processing request".to_string(),
                    ));
                }
                USubscriptionResponse::UnregisterForNotification(())
            }
            USubscriptionRequest::Reset(()) => {
                let (respond_to, receive_from) = oneshot::channel::<()>();
                let se = SubscriptionEvent::Reset { respond_to };
                if let Err(e) = self.state.subscription_sender.send(se).await {
                    error!("Error communicating with subscription manager: {e}");
                    return Err(ServiceInvocationError::Internal(
                        "Error processing request".to_string(),
                    ));
                }
                if receive_from.await.is_err() {
                    return Err(ServiceInvocationError::Internal(
                        "Error processing request".to_string(),
                    ));
                };
                USubscriptionResponse::Reset(())
            }
            // `USubscriptionRequest` is `#[non_exhaustive]`, so new operations can be
            // added in future releases without breaking this code.
            other => {
                return Err(ServiceInvocationError::Unimplemented(format!(
                    "operation not supported by this service: {other:?}"
                )))
            }
        };

        Ok(None)
    }
}
