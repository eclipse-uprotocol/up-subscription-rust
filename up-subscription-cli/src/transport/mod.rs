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

#[cfg(feature = "mqtt5")]
pub(crate) mod mqtt5;
#[cfg(feature = "mqtt5")]
pub(crate) use mqtt5::get_mqtt5_transport;

#[cfg(feature = "socket")]
pub(crate) mod socket;
#[cfg(feature = "socket")]
pub(crate) use socket::get_socket_transport;

#[cfg(feature = "zenoh")]
pub(crate) mod zenoh;
#[cfg(feature = "zenoh")]
pub(crate) use zenoh::get_zenoh_transport;
