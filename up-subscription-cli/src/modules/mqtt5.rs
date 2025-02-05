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

use up_rust::{LocalUriProvider, UTransport};
use up_transport_mqtt5::{Mqtt5Transport, MqttClientOptions, TransportMode};

pub(crate) async fn get_mqtt5_handler(
    uri_provider: Arc<dyn LocalUriProvider>,
) -> Option<Arc<dyn UTransport>> {
    let client_options = MqttClientOptions {
        broker_uri: uri_provider.get_source_uri().to_string(),
        ..Default::default()
    };

    if let Ok(client) = Mqtt5Transport::new(
        TransportMode::InVehicle,
        client_options,
        uri_provider.get_authority(),
    )
    .await
    {
        return Some(Arc::new(client));
    }

    None
}
