#!/bin/sh

################################################################################
# Copyright (c) 2025 Contributors to the Eclipse Foundation
#
# See the NOTICE file(s) distributed with this work for additional
# information regarding copyright ownership.
#
# This program and the accompanying materials are made available under the
# terms of the Apache License Version 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0
#
# SPDX-License-Identifier: Apache-2.0
################################################################################

echo "Running cargo fmt --check"
cargo fmt --all --check

echo ""
echo "Running cargo clippy"
cargo clippy --all-targets --all-features --no-deps -- -W warnings -D warnings

echo ""
echo "Running cargo doc"
cargo doc --no-deps --all-features
