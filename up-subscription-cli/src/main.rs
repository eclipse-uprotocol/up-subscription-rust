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

use clap::Parser;
use tokio::signal;
use tracing::error;
use up_rust::{
    communication::{InMemoryRpcServer, RpcServer},
    core::usubscription::{RESOURCE_ID_SUBSCRIBE, RESOURCE_ID_UNSUBSCRIBE},
};
use up_subscription::{
    adapters::{DynTransport, DynUriProvider},
    ConfigurationError, USubscriptionConfiguration, USubscriptionService,
};

#[cfg(feature = "mqtt5")]
use up_transport_mqtt5::Mqtt5TransportOptions;

#[cfg(feature = "zenoh")]
use crate::transport::zenoh::ZenohArgs;

mod transport;
use transport::get_local_transport;
#[cfg(feature = "mqtt5")]
use transport::get_mqtt5_transport;
#[cfg(feature = "zenoh")]
use transport::get_zenoh_transport;

#[derive(clap::Subcommand)]
enum Transport {
    /// Use in-memory local uProtocol transport
    Local,
    #[cfg(feature = "zenoh")]
    /// Use Zenoh as uProtocol transport
    Zenoh {
        #[command(flatten)]
        options: ZenohArgs,
    },
    #[cfg(feature = "mqtt5")]
    /// Use MQTT 5 as uProtocol transport
    Mqtt5 {
        #[command(flatten)]
        options: Mqtt5TransportOptions,
    },
}

// All our args
#[derive(Parser)]
#[command(version, about = "Rust implementation of Eclipse uProtocol Subscription service.", long_about = None)]
pub(crate) struct Args {
    /// Authority name for uSubscription service
    #[arg(short, long, env, value_name = "NAME", value_parser=clap::builder::NonEmptyStringValueParser::new())]
    authority: String,

    /// Number of subscription commands that can buffered - minimum 1, maximum 1024
    #[arg(short, long, env, value_name="SIZE", value_parser=clap::value_parser!(u16).range(1..=1024), default_value_t = up_subscription::DEFAULT_COMMAND_BUFFER_SIZE)]
    subscription_buffer: u16,

    /// Number of notifications that can buffered - minimum 1, maximum 1024
    #[arg(short, long, env, value_name="SIZE", value_parser=clap::value_parser!(u16).range(1..=1024), default_value_t = up_subscription::DEFAULT_COMMAND_BUFFER_SIZE)]
    notification_buffer: u16,

    /// Disable persistency of subscription and notification data
    #[arg(long="no-persistency", env="NO_PERSISTENCY", default_value_t = true, action=clap::ArgAction::SetFalse)]
    persistency: bool,

    /// Filesystem location for storing persistent data, default is current working directory
    #[arg(long, env, value_name = "PATH")]
    storage_path: Option<String>,

    /// Increase verbosity of output
    #[arg(short, long, env, default_value_t = false)]
    verbose: bool,

    #[command(subcommand)]
    transport: Transport,
}

impl TryFrom<&Args> for USubscriptionConfiguration {
    type Error = ConfigurationError;

    fn try_from(value: &Args) -> Result<Self, Self::Error> {
        USubscriptionConfiguration::create(
            value.authority.clone(),
            Some(value.notification_buffer),
            Some(value.subscription_buffer),
            value.persistency,
            value.storage_path.clone(),
        )
    }
}

fn init_logging(args: &Args) {
    if args.verbose {
        tracing_subscriber::fmt()
            .with_env_filter("trace,zenoh=info")
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter("info,zenoh=warn")
            .init();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup logging, get configuration
    let args = Args::parse();
    init_logging(&args);
    let config = USubscriptionConfiguration::try_from(&args).map(Arc::new)?;

    // Deal with transport module that we're to use
    let transport = match args.transport {
        Transport::Local => get_local_transport().await,
        #[cfg(feature = "mqtt5")]
        Transport::Mqtt5 { options } => get_mqtt5_transport(&args.authority, options).await,
        #[cfg(feature = "zenoh")]
        Transport::Zenoh { options } => get_zenoh_transport(&args.authority, options).await,
    }?;

    let rpc_server = InMemoryRpcServer::new(
        Arc::new(DynTransport::new(transport.clone())),
        Arc::new(DynUriProvider::new(config.clone())),
    );
    let service = Arc::new(
        USubscriptionService::new(config, transport)
            .run()
            .await
            .inspect_err(|e| error!("Error starting uSubscription service: {}", e))?,
    );

    rpc_server
        .register_endpoint(None, RESOURCE_ID_SUBSCRIBE, service.clone())
        .await?;
    rpc_server
        .register_endpoint(None, RESOURCE_ID_UNSUBSCRIBE, service.clone())
        .await?;

    // Wait for kill signal
    signal::ctrl_c().await.expect("failed to listen for event");

    // Drop the RPC server's references so the Arc can be unwrapped for stopping uSubscription service
    rpc_server
        .unregister_endpoint(None, RESOURCE_ID_SUBSCRIBE, service.clone())
        .await?;
    rpc_server
        .unregister_endpoint(None, RESOURCE_ID_UNSUBSCRIBE, service.clone())
        .await?;

    Arc::try_unwrap(service)
        .unwrap_or_else(|_| panic!("uSubscription service still has outstanding references"))
        .stop()
        .await;
    Ok(())
}
