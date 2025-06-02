#!/bin/bash

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

BASELINE="current"
REPORT="failure_summaries"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --baseline|-b)
            BASELINE="$2"
            shift 2
            ;;
        --report|-r)
            REPORT="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [--baseline current|latest] [--report minimal|summary|failures|failure_summaries|failure_details|all]"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--baseline current|latest] [--report minimal|summary|failures|failure_summaries|failure_details|all]"
            exit 1
            ;;
    esac
done

echo "Using $BASELINE baseline (source file .env.oft-${BASELINE})"
source "$(dirname "$0")/../.env.oft-${BASELINE}" 

# Get rid of trailing (or other unwanted) whitespace
OFT_FILE_PATTERNS="$(echo $OFT_FILE_PATTERNS | xargs)"

echo "Running oft trace for files \"$OFT_FILE_PATTERNS\"${OFT_TAGS:+, \"with tags $OFT_TAGS\"}"

oft trace -v "$REPORT" -o plain $OFT_FILE_PATTERNS
