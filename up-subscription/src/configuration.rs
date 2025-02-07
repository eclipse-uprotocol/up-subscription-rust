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

use uriparse::Authority;

use up_rust::{
    core::usubscription::{USUBSCRIPTION_TYPE_ID, USUBSCRIPTION_VERSION_MAJOR},
    LocalUriProvider, UUri,
};

/// Default subscription and notification command channel buffer size
pub(crate) const DEFAULT_COMMAND_BUFFER_SIZE: usize = 1024;

#[derive(Debug)]
pub struct ConfigurationError(String);

impl ConfigurationError {
    pub fn new<T>(message: T) -> ConfigurationError
    where
        T: Into<String>,
    {
        ConfigurationError(message.into())
    }
}

impl std::fmt::Display for ConfigurationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Configuration error: {}", self.0))
    }
}

impl std::error::Error for ConfigurationError {}

#[derive(Clone, Debug)]
pub struct USubscriptionConfiguration {
    pub authority_name: String,
    pub subscription_command_buffer: usize,
    pub notification_command_buffer: usize,
}

/// Holder object for USubscription configuration options; this performs validation of configuration parameters at construction time,
/// and also acts as a `LocalUriProvider` for an USubscription service based on the given authority name.
impl USubscriptionConfiguration {
    /// Create an "immutable" (Arc) `USubscriptionConfiguration` object from a set of configuration parameters.
    ///
    /// Note: the default internal command channel buffer size is DEFAULT_COMMAND_BUFFER_SIZE = 1024
    ///
    /// # Arguments
    ///
    /// * `authority_name` - Authority part of UUri that this USubscription instance is reachable on
    /// * `subscription_command_buffer` - buffer size for subscription manager commands, defaults to DEFAULT_COMMAND_BUFFER_SIZE when `None` or 0 is passed  
    /// * `notification_command_buffer` - buffer size for notification manager commands, defaults to DEFAULT_COMMAND_BUFFER_SIZE when `None` or 0 is passed  
    ///
    /// # Errors
    ///
    /// Returns a ConfigurationError in case an invalid Authority string is provided; this is determined via the uriparse crate Authority::try_from() method.
    pub fn create(
        authority_name: String,
        subscription_command_buffer: Option<usize>,
        notification_command_buffer: Option<usize>,
    ) -> Result<USubscriptionConfiguration, ConfigurationError> {
        if let Err(e) = Authority::try_from(authority_name.as_bytes()) {
            return Err(ConfigurationError::new(format!(
                "Invalid authority name: {e}"
            )));
        }

        Ok(USubscriptionConfiguration {
            authority_name,
            subscription_command_buffer: subscription_command_buffer
                .unwrap_or(DEFAULT_COMMAND_BUFFER_SIZE)
                .clamp(1, DEFAULT_COMMAND_BUFFER_SIZE),
            notification_command_buffer: notification_command_buffer
                .unwrap_or(DEFAULT_COMMAND_BUFFER_SIZE)
                .clamp(1, DEFAULT_COMMAND_BUFFER_SIZE),
        })
    }
}

impl LocalUriProvider for USubscriptionConfiguration {
    fn get_authority(&self) -> String {
        self.authority_name.clone()
    }
    fn get_resource_uri(&self, resource_id: u16) -> up_rust::UUri {
        UUri::try_from_parts(
            &self.authority_name,
            USUBSCRIPTION_TYPE_ID,
            USUBSCRIPTION_VERSION_MAJOR,
            resource_id,
        )
        .expect("Error constructing usubscription service UUri")
    }
    fn get_source_uri(&self) -> up_rust::UUri {
        UUri::try_from_parts(
            &self.authority_name,
            USUBSCRIPTION_TYPE_ID,
            USUBSCRIPTION_VERSION_MAJOR,
            0x0,
        )
        .expect("Error constructing usubscription UUri")
    }
}
