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

use up_rust::core::usubscription::UnsubscribeRequest;
use up_rust::{UCode, UListener, UMessage, UMessageBuilder, UStatus, UUID};

use crate::usubscription::USubscriptionServiceAbstract;

#[derive(Clone)]
pub struct UnsubscribeListener {
    up_subscription: Arc<dyn USubscriptionServiceAbstract>,
}

impl UnsubscribeListener {
    pub fn new(up_subscription: Arc<dyn USubscriptionServiceAbstract>) -> Self {
        UnsubscribeListener { up_subscription }
    }
}

#[async_trait]
impl UListener for UnsubscribeListener {
    // Perform any business logic related to a `UnsubscribeRequest`
    // Implements https://github.com/eclipse-uprotocol/up-spec/tree/main/up-l3/usubscription/v3#52-unsubscribe
    async fn on_receive(&self, msg: UMessage) {
        let unsubscribe_request: UnsubscribeRequest = msg
            .extract_protobuf()
            .expect("Expected UnsubscribeRequest payload");

        // 1. Check with backend
        let status = match self
            .up_subscription
            .unsubscribe(unsubscribe_request.clone())
            .await
        {
            Ok(()) => UStatus::ok(),
            Err(status) => status,
        };

        let message = UMessageBuilder::response_for_request(msg.attributes.get_or_default())
            .with_message_id(UUID::build())
            .with_comm_status(status.code.enum_value_or(UCode::INTERNAL))
            .build_with_protobuf_payload(&status)
            .expect("Error building response message");

        // 2. Respond to caller
        self.up_subscription
            .get_transport()
            .send(message)
            .await
            .expect("Error sending response message");
    }
}

#[cfg(test)]
mod tests {
    use mockall::predicate::eq;
    use std::str::FromStr;

    use up_rust::{
        core::usubscription::{
            UnsubscribeResponse, RESOURCE_ID_UNSUBSCRIBE, USUBSCRIPTION_TYPE_ID,
            USUBSCRIPTION_VERSION_MAJOR,
        },
        UStatus, UUri,
    };

    use super::*;
    use crate::usubscription;
    use crate::{helpers, tests::test_lib};

    // Test for two cases: 1) handling a usubscription Ok() Result, or 2) handling a usubscription Err() Result

    #[tokio::test]
    async fn test_unsubscribe_listener_success() {
        helpers::init_once();
        let unsubscribe_uri = UUri {
            authority_name: String::from("usubscription.mock"),
            ue_id: USUBSCRIPTION_TYPE_ID,
            ue_version_major: USUBSCRIPTION_VERSION_MAJOR as u32,
            resource_id: RESOURCE_ID_UNSUBSCRIBE as u32,
            ..Default::default()
        };

        // create request and matching expected response object(s)
        let unsubscribe_request = test_lib::helpers::unsubscribe_request(
            test_lib::helpers::local_topic1_uri(),
            test_lib::helpers::subscriber_info1(),
        );

        // detailed content of this are actually irrelevant - we're not checking business logic here, just whether the object is passed through ok
        let expected_response = UnsubscribeResponse::default();

        let listener_msg = UMessageBuilder::request(
            unsubscribe_uri,
            UUri::from_str(test_lib::helpers::UENTITY_OWN_URI).unwrap(),
            usubscription::UP_REMOTE_TTL,
        )
        .build_with_protobuf_payload(&unsubscribe_request)
        .unwrap();

        // setup mock for send()ing back response
        let expected_listener_reaction =
            UMessageBuilder::response_for_request(listener_msg.attributes.get_or_default())
                .with_comm_status(UCode::OK)
                .with_message_id(UUID::build())
                .build_with_protobuf_payload(&expected_response)
                .expect("Error building response message");

        // create and configure usubscription mock with above objects
        let mut usubscription_mock =
            test_lib::mocks::usubscription_mock_for_listener_tests(expected_listener_reaction);
        usubscription_mock
            .expect_unsubscribe()
            .with(eq(unsubscribe_request))
            .return_const(Ok(()));
        let usubscription_arc: Arc<dyn USubscriptionServiceAbstract> = Arc::new(usubscription_mock);

        // create listener and perform tested operation
        let listener = UnsubscribeListener::new(usubscription_arc);
        listener.on_receive(listener_msg).await;
    }

    #[tokio::test]
    async fn test_unsubscribe_listener_failure() {
        helpers::init_once();
        let unsubscribe_uri = UUri {
            authority_name: String::from("usubscription.mock"),
            ue_id: USUBSCRIPTION_TYPE_ID,
            ue_version_major: USUBSCRIPTION_VERSION_MAJOR as u32,
            resource_id: RESOURCE_ID_UNSUBSCRIBE as u32,
            ..Default::default()
        };

        // create request and matching expected response object(s)
        let unsubscribe_request = UnsubscribeRequest::default();

        // detailed content of this are actually irrelevant - we're not checking business logic here, just whether the object is passed through ok
        let expected_response = UStatus {
            code: UCode::INVALID_ARGUMENT.into(),
            ..Default::default()
        };

        let listener_msg = UMessageBuilder::request(
            unsubscribe_uri,
            UUri::from_str(test_lib::helpers::UENTITY_OWN_URI).unwrap(),
            usubscription::UP_REMOTE_TTL,
        )
        .build_with_protobuf_payload(&unsubscribe_request)
        .unwrap();

        // setup mock for send()ing back response
        let expected_listener_reaction =
            UMessageBuilder::response_for_request(listener_msg.attributes.get_or_default())
                .with_comm_status(UCode::INVALID_ARGUMENT)
                .with_message_id(UUID::build())
                .build_with_protobuf_payload(&expected_response)
                .expect("Error building response message");

        // create and configure usubscription mock with above objects
        let mut usubscription_mock =
            test_lib::mocks::usubscription_mock_for_listener_tests(expected_listener_reaction);
        usubscription_mock
            .expect_unsubscribe()
            .with(eq(unsubscribe_request))
            .return_const(Err(expected_response));
        let usubscription_arc: Arc<dyn USubscriptionServiceAbstract> = Arc::new(usubscription_mock);

        // create listener and perform tested operation
        let listener = UnsubscribeListener::new(usubscription_arc);
        listener.on_receive(listener_msg).await;
    }
}
