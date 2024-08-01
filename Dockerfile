################################################################################
# Copyright (c) 2024 Contributors to the Eclipse Foundation
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

FROM rust:slim as build

# create a new empty shell project
RUN USER=root mkdir /up-subscription-rust
WORKDIR /up-subscription-rust

# clone workspace
COPY ./ ./
# not yet available on stable syntax
#COPY --exclude=target ./ ./

# this build step will cache your dependencies, use same profile as cargo-dist
RUN cargo build --all-features --profile=dist

# our final base
FROM debian:stable-slim

# copy the build artifact from the build stage
COPY --from=build /up-subscription-rust/target/dist/up-subscription-cli .
RUN chmod +x /up-subscription-cli

ADD tools/startup.sh /
RUN chmod +x /startup.sh

# set the startup command to run your binary
ENTRYPOINT ["/startup.sh"]
