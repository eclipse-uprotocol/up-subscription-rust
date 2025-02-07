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

use up_rust::{LocalUriProvider, UStatus, UTransport};

pub(crate) async fn get_zenoh_transport(
    _uri_provider: Arc<dyn LocalUriProvider>,
) -> Result<Arc<dyn UTransport>, UStatus> {
    // UPTransportZenoh::try_init_log_from_env();

    // // Load the config from file path
    // // Config Examples: https://github.com/eclipse-zenoh/zenoh/blob/0.10.1-rc/DEFAULT_CONFIG.json5
    // // zenoh_config::Config::from_file(path).unwrap()

    // // Loat the default config struct
    // let mut zenoh_cfg = zenoh_config::Config::default();

    // // You can choose from Router, Peer, Client
    // zenoh_cfg.insert_json5("mode", "Peer").unwrap();

    // let transport = Arc::new(
    //     UPTransportZenoh::new(zenoh_cfg, uri_provider.get_source_uri().to_string())
    //         .await
    //         .unwrap(),
    // );

    // Some(transport)

    todo!()
}
