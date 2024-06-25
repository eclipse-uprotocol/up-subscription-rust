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

#[cfg(feature = "zenoh")]
mod zenoh;
#[cfg(feature = "zenoh")]
pub(crate) use zenoh::get_zenoh_handlers;

#[cfg(feature = "socket")]
mod socket;
#[cfg(feature = "socket")]
pub(crate) use socket::get_socket_handlers;
