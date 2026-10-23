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

use up_rust::{LocalUriProvider, UListener, UMessage, UStatus, UTransport, UUri};

/// Adapt a type-erased transport to a concrete, `Sized` type so it can be used
/// with up-rust APIs generic over `T: UTransport` (e.g. `InMemoryRpcClient::new`).
pub struct DynTransport(Arc<dyn UTransport>);

impl DynTransport {
    pub fn new(transport: Arc<dyn UTransport>) -> Self {
        Self(transport)
    }
}

#[async_trait]
impl UTransport for DynTransport {
    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        self.0.send(message).await
    }

    async fn receive(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Result<UMessage, UStatus> {
        self.0.receive(source_filter, sink_filter).await
    }

    async fn register_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        self.0
            .register_listener(source_filter, sink_filter, listener)
            .await
    }

    async fn unregister_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        self.0
            .unregister_listener(source_filter, sink_filter, listener)
            .await
    }
}

pub struct DynUriProvider(Arc<dyn LocalUriProvider>);

impl DynUriProvider {
    pub fn new(uri_provider: Arc<dyn LocalUriProvider>) -> Self {
        Self(uri_provider)
    }
}

impl LocalUriProvider for DynUriProvider {
    fn get_authority(&self) -> String {
        self.0.get_authority()
    }

    fn get_resource_uri(&self, resource_id: u16) -> UUri {
        self.0.get_resource_uri(resource_id)
    }

    fn get_source_uri(&self) -> UUri {
        self.0.get_source_uri()
    }
}
