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

use clap::Parser;
use clap_num::number_range;
use log::*;
use std::sync::Arc;
use tokio::signal;

use up_rust::{LocalUriProvider, UTransport};
use up_subscription::{ConfigurationError, USubscriptionConfiguration, USubscriptionService};

#[cfg(unix)]
use daemonize::Daemonize;

#[cfg(feature = "mqtt5")]
use up_transport_mqtt5::Mqtt5TransportOptions;

#[cfg(feature = "zenoh")]
use crate::transport::zenoh::ZenohArgs;

mod transport;
#[cfg(feature = "mqtt5")]
use transport::get_mqtt5_transport;
#[cfg(feature = "socket")]
use transport::get_socket_transport;
#[cfg(feature = "zenoh")]
use transport::get_zenoh_transport;

fn between_1_and_1024(s: &str) -> Result<usize, String> {
    number_range(s, 1, 1024)
}

#[derive(Debug)]
pub enum StartupError {
    ConfigurationError(String),
}

impl StartupError {
    pub fn configuration_error<T>(message: T) -> StartupError
    where
        T: Into<String>,
    {
        Self::ConfigurationError(message.into())
    }
}

impl std::fmt::Display for StartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConfigurationError(e) => f.write_fmt(format_args!("Configuration error: {}", e)),
        }
    }
}

impl std::error::Error for StartupError {}

#[derive(clap::ValueEnum, Clone, Default, Debug)]
enum Transport {
    #[default]
    None,
    #[cfg(feature = "mqtt5")]
    Mqtt5,
    #[cfg(feature = "socket")]
    Socket,
    #[cfg(feature = "zenoh")]
    Zenoh,
}

// All our args
#[derive(Parser)]
#[command(version, about = "Rust implementation of Eclipse uProtocol USubscription service.", long_about = None)]
pub(crate) struct Args {
    /// Authority name for usubscription service
    #[arg(short, long, env)]
    authority: String,

    /// Run as a daemon (in the background)
    #[cfg(unix)]
    #[arg(short, long, default_value_t = false)]
    daemon: bool,

    /// The transport implementation to use
    #[arg(short, long, env)]
    transport: Transport,

    /// Buffer size of subscription command channel - minimum 1, maximum 1024, defaults to 1024
    #[arg(short, long, env, value_parser=between_1_and_1024)]
    subscription_buffer: Option<usize>,

    /// Buffer size of notification command channel - minimum 1, maximum 1024, defaults to 1024
    #[arg(short, long, env, value_parser=between_1_and_1024)]
    notification_buffer: Option<usize>,

    /// Enable or disable persistency, default is true (enable)
    #[arg(short, long, env, default_value_t = true)]
    persistency: bool,

    /// Filesystem location for storing persistent data, default is current working directory
    #[arg(long, env)]
    storage_path: Option<String>,

    /// Increase verbosity of output, default is false (reduced verbosity)
    #[arg(short, long, env, default_value_t = false)]
    verbose: bool,

    #[cfg(feature = "mqtt5")]
    #[command(flatten)]
    mqtt_args: Mqtt5TransportOptions,

    #[cfg(feature = "zenoh")]
    #[command(flatten)]
    zenoh_args: ZenohArgs,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Setup logging, get configuration
    std::env::set_var("RUST_LOG", "info");
    #[cfg(feature = "zenoh")]
    std::env::set_var("RUST_LOG", "info,zenoh=warn");
    if args.verbose {
        std::env::set_var("RUST_LOG", "trace");
        #[cfg(feature = "zenoh")]
        std::env::set_var("RUST_LOG", "trace,zenoh=info");
    }
    up_subscription::init_once();

    let _config = match config_from_args(&args) {
        Err(e) => {
            panic!("Configuration error: {e}")
        }
        Ok(config) => Arc::new(config),
    };

    // Deal with transport module that we're to use
    let transport: Option<Arc<dyn UTransport>> = match args.transport {
        #[cfg(feature = "mqtt5")]
        Transport::Mqtt5 => Some(
            get_mqtt5_transport(_config.clone(), args.mqtt_args)
                .await
                .inspect_err(|e| panic!("Error setting up MQTT5 transport: {}", e.get_message()))
                .unwrap(),
        ),
        #[cfg(feature = "socket")]
        Transport::Socket => Some(
            get_socket_transport(_config.clone())
                .await
                .inspect_err(|e| panic!("Error setting up socket transport: {}", e.get_message()))
                .unwrap(),
        ),
        #[cfg(feature = "zenoh")]
        Transport::Zenoh => Some(
            get_zenoh_transport(_config.clone(), args.zenoh_args)
                .await
                .inspect_err(|e| panic!("Error setting up Zenoh transport: {}", e.get_message()))
                .unwrap(),
        ),
        _ => None,
    };

    if transport.is_none() {
        panic!("No valid transport protocol");
    }

    // Set up and run USubscription service
    let mut ustop = USubscriptionService::run(_config.clone(), transport.as_ref().unwrap().clone())
        .await
        .expect("Error starting usubscription service");

    info!(
        "Usubscription service running and listeners up on {}",
        _config.get_source_uri()
    );

    // Daemonize or wait for shutdown signal
    #[cfg(unix)]
    if args.daemon {
        let daemonize = Daemonize::new();
        match daemonize.start() {
            Ok(_) => {
                debug!("Success, running daemonized");
            }
            Err(e) => error!("Error, {}", e),
        }
    }

    signal::ctrl_c().await.expect("failed to listen for event");
    info!("Stopping usubscription service");
    ustop.stop().await;
}

fn config_from_args(args: &Args) -> Result<USubscriptionConfiguration, ConfigurationError> {
    let authority: &str = args.authority.trim();
    if authority.is_empty() {
        return Err(ConfigurationError::new("Authority name empty or missing"));
    }

    USubscriptionConfiguration::create(
        authority.to_string(),
        args.notification_buffer,
        args.subscription_buffer,
        args.persistency,
        args.storage_path.clone(),
    )
}
