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

use up_rust::{
    communication::{InMemoryRpcClient, RpcClient},
    LocalUriProvider, UTransport,
};

use up_transport_socket_rust::UTransportSocket;

pub(crate) async fn get_socket_handlers(
    uri_provider: Arc<dyn LocalUriProvider>,
) -> (Option<Arc<dyn UTransport>>, Option<Arc<dyn RpcClient>>) {
    let transport = Arc::new(UTransportSocket::new().expect("Error creating socket transport"));

    let client = Arc::new(
        InMemoryRpcClient::new(transport.clone(), uri_provider.clone())
            .await
            .expect("Error creating socket client"),
    );
    (Some(transport), Some(client))
}
