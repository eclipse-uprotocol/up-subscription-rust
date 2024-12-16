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

use up_rust::core::usubscription::SubscriptionRequest;
use up_rust::{UCode, UListener, UMessage, UMessageBuilder, UUID};

use crate::USubscriptionService;

pub(crate) async fn subscribe(
    &self,
    subscriber_uri: &UUri,
    subscription_request: SubscriptionRequest,
) -> Result<SubscriptionResponse, UStatus> {
    let SubscriptionRequest { topic, .. } = subscription_request;

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

    debug!(
        "Got SubscriptionRequest for topic {}, from subscriber {}",
        topic.to_uri(INCLUDE_SCHEMA),
        subscriber_uri.to_uri(INCLUDE_SCHEMA)
    );

    // Communicate with subscription manager
    let (respond_to, receive_from) = oneshot::channel::<SubscriptionStatus>();
    let se = SubscriptionEvent::AddSubscription {
        subscriber: subscriber_uri.clone(),
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
            subscriber: subscriber_uri.clone(),
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
