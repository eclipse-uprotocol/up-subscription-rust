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

use log::*;
use std::future::Future;
use std::sync::Once;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::{task, time::Duration};

use up_rust::{
    communication::{ServiceInvocationError, UPayload},
    UAttributes, UUri,
};

static INIT: Once = Once::new();

pub fn init_once() {
    INIT.call_once(env_logger::init);
}

type SpawnResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(crate) fn spawn_and_log_error<F>(fut: F) -> task::JoinHandle<()>
where
    F: Future<Output = SpawnResult<()>> + Send + 'static,
{
    task::spawn(async move {
        if let Err(e) = fut.await {
            error!("{e}")
        }
    })
}

// Internal helper for basic input validation, as used by every Rpc request handler
pub(crate) fn extract_inputs<T>(
    expected_resource_id: u16,
    resource_id: u16,
    payload: &Option<UPayload>,
    message_attributes: &UAttributes,
) -> Result<(T, UUri), ServiceInvocationError>
where
    T: protobuf::MessageFull,
{
    if resource_id != expected_resource_id {
        return Err(ServiceInvocationError::InvalidArgument(format!(
            "Wrong resource ID (expected {expected_resource_id}, got {resource_id})"
        )));
    }
    let Some(payload) = payload else {
        return Err(ServiceInvocationError::InvalidArgument(
            "No request payload".to_string(),
        ));
    };
    let request: T = payload.extract_protobuf().map_err(|e| {
        ServiceInvocationError::InvalidArgument(
            format!("Expected NotificationsRequest payload, error when unpacking {e}").to_string(),
        )
    })?;
    let Some(source) = message_attributes.source.as_ref() else {
        return Err(ServiceInvocationError::InvalidArgument(
            "No request source uri".to_string(),
        ));
    };

    Ok((request, source.clone()))
}

pub(crate) fn duration_until_timestamp(future_timestamp_millis: u128) -> Option<Duration> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis();

    if future_timestamp_millis > now {
        Some(Duration::from_millis(
            (future_timestamp_millis - now) as u64,
        ))
    } else {
        None // Timestamp is in the past
    }
}

// Validate that a topic URI
//  * is a valid uProtocol URI and
//  * is not empty
//  * does not contain a _wildcard_ authority and
//  * does not contain a _wildcard_ uEntity instance (`ue_id`) and
//  * does not contain a _wildcard_ resource ID
pub(crate) fn validate_uri(uri: &UUri) -> Result<(), ServiceInvocationError> {
    uri.check_validity()
        .map_err(|e| ServiceInvocationError::InvalidArgument(format!("Invalid URI {e}")))?;
    if uri.is_empty() {
        return Err(ServiceInvocationError::InvalidArgument(
            "Empty/default URI".to_string(),
        ));
    };
    if uri.has_wildcard_authority() {
        return Err(ServiceInvocationError::InvalidArgument(
            "URI with wildcard authority".to_string(),
        ));
    };
    if uri.has_wildcard_entity_instance() {
        return Err(ServiceInvocationError::InvalidArgument(
            "URI with wildcard entity instance".to_string(),
        ));
    };
    if uri.has_wildcard_resource_id() {
        return Err(ServiceInvocationError::InvalidArgument(
            "URI with wildcard resource id".to_string(),
        ));
    };

    Ok(())
}
