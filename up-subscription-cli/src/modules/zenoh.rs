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

use up_rust::{communication::RpcClient, LocalUriProvider, UTransport};

use up_transport_zenoh::{zenoh_config, UPTransportZenoh, ZenohRpcClient};

pub(crate) async fn get_zenoh_handlers(
    uri_provider: Arc<dyn LocalUriProvider>,
) -> (Option<Arc<dyn UTransport>>, Option<Arc<dyn RpcClient>>) {
    // Load the config from file path
    // Config Examples: https://github.com/eclipse-zenoh/zenoh/blob/0.10.1-rc/DEFAULT_CONFIG.json5
    // let mut zenoh_cfg = Config::from_file("./DEFAULT_CONFIG.json5").unwrap();

    // Loat the default config struct

    let mut zenoh_cfg = zenoh_config::Config::default();

    // You can choose from Router, Peer, Client
    zenoh_cfg
        .set_mode(Some(zenoh_config::WhatAmI::Peer))
        .unwrap();

    let transport = Arc::new(
        UPTransportZenoh::new(zenoh_cfg, uri_provider.get_authority())
            .await
            .expect("Error setting up Zenoh transport"),
    );
    let client = Arc::new(ZenohRpcClient::new(transport.clone(), uri_provider));

    (Some(transport), Some(client))
}
