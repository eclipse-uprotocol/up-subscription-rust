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
    helpers, listeners,
    notification_manager::{self, NotificationEvent},
    subscription_manager::{self, SubscriptionEvent},
    USubscriptionConfiguration,
};

use up_rust::core::usubscription::{
    FetchSubscribersRequest, FetchSubscribersResponse, FetchSubscriptionsRequest,
    FetchSubscriptionsResponse, NotificationsRequest, Request, SubscriptionRequest,
    SubscriptionResponse, SubscriptionStatus, USubscription, UnsubscribeRequest,
    RESOURCE_ID_FETCH_SUBSCRIBERS, RESOURCE_ID_FETCH_SUBSCRIPTIONS,
    RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS, RESOURCE_ID_SUBSCRIBE,
    RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS, RESOURCE_ID_UNSUBSCRIBE, USUBSCRIPTION_TYPE_ID,
};
use up_rust::{communication::RpcClient, LocalUriProvider, UCode, UStatus, UTransport, UUri};

/// Whether to include 'up:' uProtocol schema prefix in URIs in log and error messages
pub const INCLUDE_SCHEMA: bool = false;

// Remote-subscribe operation ttl; 5 minutes in milliseconds, as per https://github.com/eclipse-uprotocol/up-spec/tree/main/up-l3/usubscription/v3#6-timeout--retry-logic
pub(crate) const UP_REMOTE_TTL: u32 = 300000;

impl LocalUriProvider for USubscriptionService {
    fn get_authority(&self) -> String {
        self.config.authority_name.clone()
    }
    fn get_resource_uri(&self, resource_id: u16) -> UUri {
        self.config.get_resource_uri(resource_id)
    }
    fn get_source_uri(&self) -> UUri {
        self.get_resource_uri(0x0000)
    }
}

/// This trait (and the comprised UTransportHolder trait) is simply there to have a generic type that
/// usubscription Listeners deal with, so that USubscriptionService can be properly mocked.
pub trait USubscriptionServiceAbstract:
    USubscription + LocalUriProvider + UTransportHolder
{
}

/// This trait primarily serves to provide a hook-point for using the mockall crate, for mocking USubscriptionService objects
/// where we also need/want to inject custom/mock UTransport implementations that subsequently get used in test cases.
pub trait UTransportHolder {
    fn get_transport(&self) -> Arc<dyn UTransport>;
}

impl UTransportHolder for USubscriptionService {
    fn get_transport(&self) -> Arc<dyn UTransport> {
        self.up_transport.clone()
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

    pub(crate) up_transport: Arc<dyn UTransport>,

    subscription_sender: Sender<SubscriptionEvent>,
    notification_sender: Sender<notification_manager::NotificationEvent>,
}

impl USubscriptionServiceAbstract for USubscriptionService {}

/// Implementation of uProtocol L3 USubscription service
impl USubscriptionService {
    /// Start a new USubscriptionService
    /// Atm this will directly spin up two tasks which deal with subscription and notification management, with no further action
    /// required, but also no explicit shutdown operation yet - that's a TODO.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration details for this USUbscription service
    /// * `up_transport` - Implementation of UTransport to be used by this USUbscription instance, for sending Listener-responses and Notifications
    /// * `up_client` - Implementation of RpcClient to be used by this USUbscription instance, for performing remote-subscribe operations
    ///
    /// # Returns
    ///
    /// * the immutable parts of the USubscription service inside an Arc
    /// * a `USubscriptionStopper` object which can be used to explicitly shut down the USubscription service
    pub fn run(
        config: Arc<USubscriptionConfiguration>,
        up_transport: Arc<dyn UTransport>,
        up_client: Arc<dyn RpcClient>,
    ) -> Result<(Arc<dyn USubscriptionServiceAbstract>, USubscriptionStopper), UStatus> {
        helpers::init_once();

        let shutdown_notification = Arc::new(Notify::new());

        // Set up subscription manager actor
        let up_client_cloned = up_client.clone();
        let own_uri_cloned = config.get_source_uri().clone();
        let shutdown_notification_cloned = shutdown_notification.clone();
        let (subscription_sender, subscription_receiver) =
            mpsc::channel::<SubscriptionEvent>(config.subscription_command_buffer);
        let subscription_joiner = helpers::spawn_and_log_error(async move {
            subscription_manager::handle_message(
                own_uri_cloned,
                up_client_cloned,
                subscription_receiver,
                shutdown_notification_cloned,
            )
            .await;
            Ok(())
        });

        // Set up notification service actor
        let up_transport_cloned = up_transport.clone();
        let shutdown_notification_cloned = shutdown_notification.clone();
        let (notification_sender, notification_receiver) =
            mpsc::channel::<notification_manager::NotificationEvent>(
                config.notification_command_buffer,
            );
        let notification_joiner = helpers::spawn_and_log_error(async move {
            notification_manager::notification_engine(
                up_transport_cloned,
                notification_receiver,
                shutdown_notification_cloned,
            )
            .await;
            Ok(())
        });

        Ok((
            Arc::new(USubscriptionService {
                config,
                up_transport,
                subscription_sender,
                notification_sender,
            }),
            USubscriptionStopper {
                subscription_joiner: Some(subscription_joiner),
                notification_joiner: Some(notification_joiner),
                shutdown_notification,
            },
        ))
    }

    /// This sets up all applicable listeners to connect the USubscription service with it's transport implementation.
    /// The following rules apply:
    ///
    /// * complete usubscription functionality is only available for local uEntities (TODO verify how this works with empty authority_names)
    /// * subscribe() and unsubscribe() are also available for remote callers, but only those with UEntity ID type USUSBSCRIPTION (other USubscription services)
    ///
    /// # Arguments:
    ///
    /// * the `USubscriptionServiceAbstract` object to set up listeners for
    pub async fn now_listen(
        usubscription_service: Arc<dyn USubscriptionServiceAbstract>,
    ) -> Result<(), UStatus> {
        let any_request_uri = UUri::from_str("//*/FFFF/FF/0").unwrap();

        let mut any_local_uri = any_request_uri.clone();
        any_local_uri.authority_name = usubscription_service.get_authority();

        let mut any_usubscription_uri = any_request_uri.clone();
        any_usubscription_uri.ue_id = USUBSCRIPTION_TYPE_ID;

        // The following listeners are for serving any/all *local* uEntity clients
        let listener = Arc::new(listeners::SubscribeListener::new(
            usubscription_service.clone(),
        ));
        usubscription_service
            .get_transport()
            .register_listener(
                &any_local_uri,
                Some(&usubscription_service.get_resource_uri(RESOURCE_ID_SUBSCRIBE)),
                listener,
            )
            .await?;

        let listener = Arc::new(listeners::UnsubscribeListener::new(
            usubscription_service.clone(),
        ));
        usubscription_service
            .get_transport()
            .register_listener(
                &any_local_uri,
                Some(&usubscription_service.get_resource_uri(RESOURCE_ID_UNSUBSCRIBE)),
                listener,
            )
            .await?;

        let listener = Arc::new(listeners::RegisterForNotificationsListener::new(
            usubscription_service.clone(),
        ));
        usubscription_service
            .get_transport()
            .register_listener(
                &any_local_uri,
                Some(
                    &usubscription_service.get_resource_uri(RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS),
                ),
                listener,
            )
            .await?;

        let listener = Arc::new(listeners::UnregisterForNotificationsListener::new(
            usubscription_service.clone(),
        ));
        usubscription_service
            .get_transport()
            .register_listener(
                &any_local_uri,
                Some(
                    &usubscription_service
                        .get_resource_uri(RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS),
                ),
                listener,
            )
            .await?;

        let listener = Arc::new(listeners::FetchSubscribersListener::new(
            usubscription_service.clone(),
        ));
        usubscription_service
            .get_transport()
            .register_listener(
                &any_local_uri,
                Some(&usubscription_service.get_resource_uri(RESOURCE_ID_FETCH_SUBSCRIBERS)),
                listener,
            )
            .await?;

        let listener = Arc::new(listeners::FetchSubscriptionsListener::new(
            usubscription_service.clone(),
        ));
        usubscription_service
            .get_transport()
            .register_listener(
                &any_local_uri,
                Some(&usubscription_service.get_resource_uri(RESOURCE_ID_FETCH_SUBSCRIPTIONS)),
                listener,
            )
            .await?;

        // The following listeners are for serving remote usubscription services only (for remote subscribe and unsubscribe calls)
        let listener = Arc::new(listeners::SubscribeListener::new(
            usubscription_service.clone(),
        ));
        usubscription_service
            .get_transport()
            .register_listener(
                &any_usubscription_uri,
                Some(&usubscription_service.get_resource_uri(RESOURCE_ID_SUBSCRIBE)),
                listener,
            )
            .await?;

        let listener = Arc::new(listeners::UnsubscribeListener::new(
            usubscription_service.clone(),
        ));
        usubscription_service
            .get_transport()
            .register_listener(
                &any_usubscription_uri,
                Some(&usubscription_service.get_resource_uri(RESOURCE_ID_UNSUBSCRIBE)),
                listener,
            )
            .await?;

        Ok(())
    }
}

/// Implementation of <https://github.com/eclipse-uprotocol/up-spec/blob/main/up-l3/usubscription/v3/README.adoc#usubscription>
#[async_trait]
impl USubscription for USubscriptionService {
    /// Implementation of <https://github.com/eclipse-uprotocol/up-spec/tree/main/up-l3/usubscription/v3#51-subscription>
    async fn subscribe(
        &self,
        subscription_request: SubscriptionRequest,
    ) -> Result<SubscriptionResponse, UStatus> {
        let SubscriptionRequest {
            subscriber, topic, ..
        } = subscription_request;

        // Basic input validation
        let Some(topic) = topic.into_option() else {
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "No topic"));
        };
        if topic.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty topic UUri",
            ));
        }

        let Some(subscriber) = subscriber.into_option() else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "No SubscriberInfo",
            ));
        };
        if subscriber.is_empty() || subscriber.uri.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty SubscriberInfo or subscriber UUri",
            ));
        }

        debug!(
            "Got SubscriptionRequest for topic {}, from subscriber {}",
            topic.to_uri(INCLUDE_SCHEMA),
            subscriber.uri.to_uri(INCLUDE_SCHEMA)
        );

        // Communicate with subscription manager
        let (respond_to, receive_from) = oneshot::channel::<SubscriptionStatus>();
        let se = SubscriptionEvent::AddSubscription {
            subscriber: subscriber.clone(),
            topic: topic.clone(),
            respond_to,
        };
        if let Err(e) = self.subscription_sender.send(se).await {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Error communicating with subscription management: {e}"),
            ));
        }
        let Ok(status) = receive_from.await else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Error communicating with subscription management",
            ));
        };

        // Notify update channel
        let (respond_to, receive_from) = oneshot::channel::<()>();
        if let Err(e) = self
            .notification_sender
            .send(NotificationEvent::StateChange {
                subscriber,
                topic: topic.clone(),
                status: status.clone(),
                respond_to,
            })
            .await
        {
            error!("Error initiating subscription-change update notification: {e}");
        }
        if let Err(e) = receive_from.await {
            // Not returning an error here, as update notification is not a core concern wrt the actual subscription management
            error!("Error sending subscription-change update notification: {e}");
        };

        // Build and return result
        Ok(SubscriptionResponse {
            topic: Some(topic).into(),
            status: Some(status).into(),
            ..Default::default()
        })
    }

    /// Implementation of <https://github.com/eclipse-uprotocol/up-spec/tree/main/up-l3/usubscription/v3#52-unsubscribe>
    async fn unsubscribe(&self, unsubscribe_request: UnsubscribeRequest) -> Result<(), UStatus> {
        let UnsubscribeRequest {
            subscriber, topic, ..
        } = unsubscribe_request;

        // Basic input validation
        let Some(topic) = topic.into_option() else {
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "No topic"));
        };
        if topic.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty topic UUri",
            ));
        }

        let Some(subscriber) = subscriber.into_option() else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "No SubscriberInfo",
            ));
        };
        if subscriber.is_empty() || subscriber.uri.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty SubscriberInfo or subscriber UUri",
            ));
        }

        debug!(
            "Got UnsubscribeRequest for topic {}, from subscriber {}",
            topic.to_uri(INCLUDE_SCHEMA),
            subscriber.uri.to_uri(INCLUDE_SCHEMA)
        );

        // Communicate with subscription manager
        let (respond_to, receive_from) = oneshot::channel::<SubscriptionStatus>();
        let se = SubscriptionEvent::RemoveSubscription {
            subscriber: subscriber.clone(),
            topic: topic.clone(),
            respond_to,
        };
        if let Err(e) = self.subscription_sender.send(se).await {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Error communicating with subscription management: {e}"),
            ));
        }
        let Ok(status) = receive_from.await else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Error communicating with subscription management",
            ));
        };

        // Notify update channel
        let (respond_to, receive_from) = oneshot::channel::<()>();
        if let Err(e) = self
            .notification_sender
            .send(NotificationEvent::StateChange {
                subscriber,
                topic: topic.clone(),
                status: status.clone(),
                respond_to,
            })
            .await
        {
            // Not returning an error here, as update notification is not a core concern wrt the actual subscription management
            error!("Error initiating subscription-change update notification: {e}");
        }
        if let Err(e) = receive_from.await {
            // Not returning an error here, as update notification is not a core concern wrt the actual subscription management
            error!("Error sending subscription-change update notification: {e}");
        };

        // Return result
        Ok(())
    }

    async fn register_for_notifications(
        &self,
        notifications_register_request: NotificationsRequest,
    ) -> Result<(), UStatus> {
        let NotificationsRequest {
            subscriber, topic, ..
        } = notifications_register_request;

        // Basic input validation
        let Some(topic) = topic.into_option() else {
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "No topic"));
        };
        if topic.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty notification UUri",
            ));
        }
        if !topic.is_event() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "UUri not a valid event destination",
            ));
        }
        if self.get_source_uri().is_remote_authority(&topic) {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Cannot use remote topic for notifications",
            ));
        }

        let Some(subscriber) = subscriber.into_option() else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "No SubscriberInfo",
            ));
        };
        if subscriber.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty SubscriberInfo",
            ));
        }
        let Some(subscriber_uri) = subscriber.uri.into_option() else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "No subscriber UUri",
            ));
        };
        if subscriber_uri.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty subscriber UUri",
            ));
        }

        debug!(
            "Got RegisterForNotifications for notification topic {}, from subscriber {}",
            topic.to_uri(INCLUDE_SCHEMA),
            subscriber_uri.to_uri(INCLUDE_SCHEMA)
        );

        // Perform notification management
        if let Err(e) = self
            .notification_sender
            .send(NotificationEvent::AddNotifyee {
                subscriber: subscriber_uri,
                topic,
            })
            .await
        {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Failed to update notification settings: {e}"),
            ));
        }

        // Return result
        Ok(())
    }

    async fn unregister_for_notifications(
        &self,
        notifications_unregister_request: NotificationsRequest,
    ) -> Result<(), UStatus> {
        // Current implementation, we only track one notification channel/topic per subscriber, so ignore topic here
        let NotificationsRequest { subscriber, .. } = notifications_unregister_request;

        // Basic input validation
        let Some(subscriber) = subscriber.into_option() else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "No SubscriberInfo",
            ));
        };
        if subscriber.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty SubscriberInfo",
            ));
        }
        let Some(subscriber_uri) = subscriber.uri.into_option() else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "No subscriber UUri",
            ));
        };
        if subscriber_uri.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty subscriber UUri",
            ));
        }

        debug!(
            "Got UnregisterForNotifications for notification from subscriber {}",
            subscriber_uri.to_uri(INCLUDE_SCHEMA)
        );

        // Perform notification management
        if let Err(e) = self
            .notification_sender
            .send(NotificationEvent::RemoveNotifyee {
                subscriber: subscriber_uri,
            })
            .await
        {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Failed to update notification settings: {e}"),
            ));
        }

        // Return result
        Ok(())
    }

    async fn fetch_subscribers(
        &self,
        fetch_subscribers_request: FetchSubscribersRequest,
    ) -> Result<FetchSubscribersResponse, UStatus> {
        // Basic input validation
        let Some(topic) = fetch_subscribers_request.topic.as_ref() else {
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "No topic"));
        };
        if topic.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty topic UUri",
            ));
        }

        debug!(
            "Got FetchSubscribersRequest for topic {}",
            topic.to_uri(INCLUDE_SCHEMA)
        );

        // Communicate with subscription manager
        let (respond_to, receive_from) = oneshot::channel::<FetchSubscribersResponse>();
        let se = SubscriptionEvent::FetchSubscribers {
            request: fetch_subscribers_request,
            respond_to,
        };
        if let Err(e) = self.subscription_sender.send(se).await {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Error communicating with subscription management: {e}"),
            ));
        }
        let Ok(response) = receive_from.await else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Error receiving response from subscription management",
            ));
        };

        // Return result
        debug!(
            "Returning {} subscriber entries",
            response.subscribers.len()
        );
        Ok(response)
    }

    async fn fetch_subscriptions(
        &self,
        fetch_subscriptions_request: FetchSubscriptionsRequest,
    ) -> Result<FetchSubscriptionsResponse, UStatus> {
        // Basic input validation
        let Some(request) = fetch_subscriptions_request.request.as_ref() else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Missing Request property",
            ));
        };
        match request {
            Request::Topic(topic) => {
                if topic.is_empty() {
                    return Err(UStatus::fail_with_code(
                        UCode::INVALID_ARGUMENT,
                        "Empty request topic UUri",
                    ));
                }
            }
            Request::Subscriber(subscriber) => {
                if subscriber.is_empty() {
                    return Err(UStatus::fail_with_code(
                        UCode::INVALID_ARGUMENT,
                        "Empty request SubscriberInfo",
                    ));
                }
                let Some(subscriber_uri) = subscriber.uri.as_ref() else {
                    return Err(UStatus::fail_with_code(
                        UCode::INVALID_ARGUMENT,
                        "No request subscriber UUri",
                    ));
                };
                if subscriber_uri.is_empty() {
                    return Err(UStatus::fail_with_code(
                        UCode::INVALID_ARGUMENT,
                        "Empty request subscriber UUri",
                    ));
                }
            }
            _ => {
                return Err(UStatus::fail_with_code(
                    UCode::INVALID_ARGUMENT,
                    "Invalid/unknown Request variant",
                ));
            }
        }

        debug!("Got FetchSubscriptionsRequest");

        // Communicate with subscription manager
        let (respond_to, receive_from) = oneshot::channel::<FetchSubscriptionsResponse>();
        let se = SubscriptionEvent::FetchSubscriptions {
            request: fetch_subscriptions_request,
            respond_to,
        };
        if let Err(e) = self.subscription_sender.send(se).await {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Error communicating with subscription management: {e}"),
            ));
        }
        let Ok(response) = receive_from.await else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Error receiving response from subscription management",
            ));
        };

        // Return result
        debug!(
            "Returning {} Subscription entries",
            response.subscriptions.len()
        );
        Ok(response)
    }
}
