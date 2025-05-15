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
        mpsc::{self},
        Notify,
    },
    task::JoinHandle,
};

use up_rust::{
    communication::{InMemoryRpcServer, RpcServer},
    core::usubscription::{
        RESOURCE_ID_FETCH_SUBSCRIBERS, RESOURCE_ID_FETCH_SUBSCRIPTIONS,
        RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS, RESOURCE_ID_SUBSCRIBE,
        RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS, RESOURCE_ID_UNSUBSCRIBE,
    },
    UCode, UStatus, UTransport, UUri,
};

use crate::{
    handlers::{
        fetch_subscribers::FetchSubscribersRequestHandler,
        fetch_subscriptions::FetchSubscriptionsRequestHandler,
        register_for_notifications::RegisterNotificationsRequestHandler,
        subscribe::SubscriptionRequestHandler,
        unregister_for_notifications::UnregisterNotificationsRequestHandler,
        unsubscribe::UnubscribeRequestHandler,
    },
    helpers,
    notification_manager::{self, NotificationEvent},
    subscription_manager::{self, SubscriptionEvent},
    USubscriptionConfiguration,
};

/// Whether to include 'up:' uProtocol schema prefix in URIs in log and error messages
pub const INCLUDE_SCHEMA: bool = false;

// Remote-subscribe operation ttl; 5 minutes in milliseconds, as per https://github.com/eclipse-uprotocol/up-spec/tree/main/up-l3/usubscription/v3#6-timeout--retry-logic
pub(crate) const UP_REMOTE_TTL: u32 = 300000;

// Centralized definition, make it easier to accomodate potential changes to expiry type in up-spec
pub(crate) type ExpiryTimestamp = u128;

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
    transport: Arc<dyn UTransport>,
}

impl USubscriptionService {
    pub async fn run(
        config: Arc<USubscriptionConfiguration>,
        transport: Arc<dyn UTransport>,
    ) -> Result<USubscriptionStopper, UStatus> {
        helpers::init_once();

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

        // Set up notification manager actor
        let config_cloned = config.clone();
        let transport_cloned = transport.clone();
        let shutdown_notification_cloned = shutdown_notification.clone();
        let (notification_sender, notification_receiver) =
            mpsc::channel::<NotificationEvent>(config.notification_command_buffer);
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

        register_handlers(server, subscription_sender, notification_sender).await?;

        Ok(USubscriptionStopper {
            subscription_joiner: Some(subscription_joiner),
            notification_joiner: Some(notification_joiner),
            shutdown_notification,
        })
    }
}

async fn register_handlers(
    server: Arc<dyn RpcServer>,
    subscription_sender: tokio::sync::mpsc::Sender<SubscriptionEvent>,
    notification_sender: tokio::sync::mpsc::Sender<NotificationEvent>,
) -> Result<(), UStatus> {
    let origin_filter = UUri::any_with_resource_id(0);

    // Link up request handlers
    let subscription_request_handler = Arc::new(SubscriptionRequestHandler::new(
        subscription_sender.clone(),
        notification_sender.clone(),
    ));
    server
        .register_endpoint(
            Some(&origin_filter),
            RESOURCE_ID_SUBSCRIBE,
            subscription_request_handler,
        )
        .await
        .map_err(|e| UStatus::fail_with_code(UCode::INTERNAL, e.to_string()))?;

    let unsubscribe_request_handler = Arc::new(UnubscribeRequestHandler::new(
        subscription_sender.clone(),
        notification_sender.clone(),
    ));
    server
        .register_endpoint(
            Some(&origin_filter),
            RESOURCE_ID_UNSUBSCRIBE,
            unsubscribe_request_handler,
        )
        .await
        .map_err(|e| UStatus::fail_with_code(UCode::INTERNAL, e.to_string()))?;

    let register_notification_handler = Arc::new(RegisterNotificationsRequestHandler::new(
        notification_sender.clone(),
    ));
    server
        .register_endpoint(
            Some(&origin_filter),
            RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS,
            register_notification_handler,
        )
        .await
        .map_err(|e| UStatus::fail_with_code(UCode::INTERNAL, e.to_string()))?;

    let unregister_notification_handler = Arc::new(UnregisterNotificationsRequestHandler::new(
        notification_sender.clone(),
    ));
    server
        .register_endpoint(
            Some(&origin_filter),
            RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS,
            unregister_notification_handler,
        )
        .await
        .map_err(|e| UStatus::fail_with_code(UCode::INTERNAL, e.to_string()))?;

    let fetch_subscribers_handler = Arc::new(FetchSubscribersRequestHandler::new(
        subscription_sender.clone(),
    ));
    server
        .register_endpoint(
            Some(&origin_filter),
            RESOURCE_ID_FETCH_SUBSCRIBERS,
            fetch_subscribers_handler,
        )
        .await
        .map_err(|e| UStatus::fail_with_code(UCode::INTERNAL, e.to_string()))?;

    let fetch_subscriptions_handler = Arc::new(FetchSubscriptionsRequestHandler::new(
        subscription_sender.clone(),
    ));
    server
        .register_endpoint(
            Some(&origin_filter),
            RESOURCE_ID_FETCH_SUBSCRIPTIONS,
            fetch_subscriptions_handler,
        )
        .await
        .map_err(|e| UStatus::fail_with_code(UCode::INTERNAL, e.to_string()))?;

    Ok(())
}
