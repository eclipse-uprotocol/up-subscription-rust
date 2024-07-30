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
use daemonize::Daemonize;
use log::*;
use std::sync::Arc;
use tokio::signal;

use up_rust::{communication::RpcClient, UTransport};
use up_subscription::{ConfigurationError, USubscriptionConfiguration, USubscriptionService};

mod modules;
#[cfg(feature = "socket")]
use modules::get_socket_handlers;
#[cfg(feature = "zenoh")]
use modules::get_zenoh_handlers;

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
enum Transports {
    #[default]
    None,
    #[cfg(feature = "socket")]
    Socket,
    #[cfg(feature = "zenoh")]
    Zenoh,
}

// All our args
#[derive(Parser, Debug)]
#[command(version, about = "Rust implementation of Eclipse uProtocol USubscription service.", long_about = None)]
pub(crate) struct Args {
    /// Authority name for usubscription service
    #[arg(short, long)]
    authority: String,

    /// Run as a daemon (in the background)
    #[arg(short, long, default_value_t = false)]
    daemon: bool,

    /// The transport implementation to use
    #[arg(short, long)]
    transport: Transports,

    /// Buffer size of subscription command channel - minimum 1, maximum 1024, defaults to 1024
    #[arg(short, long, value_parser=between_1_and_1024)]
    subscription_buffer: Option<usize>,

    /// Buffer size of notification command channel - minimum 1, maximum 1024, defaults to 1024
    #[arg(short, long, value_parser=between_1_and_1024)]
    notification_buffer: Option<usize>,

    /// Increase verbosity of output
    #[arg(short, long, default_value_t = false)]
    verbose: bool,
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

    let config = match config_from_args(&args) {
        Err(e) => {
            panic!("Configuration error: {e}")
        }
        Ok(config) => config,
    };

    // Deal with transport module that we're to use
    #[allow(unused_variables)]
    let transport: Option<Arc<dyn UTransport>> = None;
    #[allow(unused_variables)]
    let client: Option<Arc<dyn RpcClient>> = None;

    let (transport, client) = match args.transport {
        Transports::None => (None::<Arc<dyn UTransport>>, None::<Arc<dyn RpcClient>>),
        #[cfg(feature = "socket")]
        Transports::Socket => get_socket_handlers(config.clone()).await,
        #[cfg(feature = "zenoh")]
        Transports::Zenoh => get_zenoh_handlers(config.clone()).await,
    };

    if transport.is_none() || client.is_none() {
        panic!("No valid transport or client implementation available");
    }

    // Set up and run USubscription service
    let (urun, mut ustop) = USubscriptionService::run(
        config,
        transport.as_ref().unwrap().clone(),
        client.unwrap().clone(),
    )
    .expect("Error starting usubscription service");

    USubscriptionService::now_listen(urun.clone())
        .await
        .expect("Error setting up transport listeners");

    info!(
        "Usubscription service running and listeners up on {}",
        urun.get_source_uri().to_uri(true)
    );

    // Daemonize or wait for shutdown signal
    if args.daemon {
        let daemonize = Daemonize::new();
        match daemonize.start() {
            Ok(_) => debug!("Success, daemonized"),
            Err(e) => error!("Error, {}", e),
        }
    } else {
        signal::ctrl_c().await.expect("failed to listen for event");
        info!("Stopping usubscription service");
        ustop.stop().await;
    }
}

fn config_from_args(args: &Args) -> Result<Arc<USubscriptionConfiguration>, ConfigurationError> {
    let authority: &str = args.authority.trim();
    assert!(!authority.is_empty());

    USubscriptionConfiguration::create(
        authority.to_string(),
        args.notification_buffer,
        args.subscription_buffer,
    )
}
