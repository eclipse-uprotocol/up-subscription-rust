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
use tokio::sync::{mpsc::Receiver, mpsc::Sender, oneshot, Notify};
use tracing::{debug, error, warn};

#[cfg(test)]
use up_rust::UUri;
use up_rust::{
    core::usubscription::{usubscription_uri, SubscriptionInfo, RESOURCE_ID_SUBSCRIPTION_CHANGE},
    LocalUriProvider, UMessageBuilder, UTransport, UUID,
};

use crate::{
    persistency::{self, PersistencyError},
    usubscription::SubscriberUUri,
    USubscriptionConfiguration,
};

/// Whether to include 'up:' uProtocol schema prefix in URIs in log and error messages
pub const INCLUDE_SCHEMA: bool = false;

// From usubscription.proto, the uprotocol.notification_topic resource ID
pub(crate) const SOURCE_URI_RESOURCE_ID: u16 = 0x8000;

// This is the core business logic for tracking and sending subscription update notifications. It is currently implemented as a single
// event-consuming function `notification_engine()`, which is supposed to be spawned into a task, and process the various notification
// `Events` that it can receive via tokio mpsc channel.

// This is the 'outside API' of notification handler
#[derive(Debug)]
pub(crate) enum NotificationEvent {
    AddNotifyee {
        subscriber: SubscriberUUri,
    },
    RemoveNotifyee {
        subscriber: SubscriberUUri,
    },
    StateChange {
        subscription_info: SubscriptionInfo,
        respond_to: oneshot::Sender<()>,
    },
    Reset {
        respond_to: oneshot::Sender<Result<(), PersistencyError>>,
    },
    #[cfg(test)]
    GetNotifyees {
        respond_to: oneshot::Sender<Vec<UUri>>,
    },
    // Purely for use during testing: force-set new notifyees ledger
    #[cfg(test)]
    SetNotifyees {
        notifyee_replacement: Vec<UUri>,
        respond_to: oneshot::Sender<()>,
    },
}

// Keeps track of and sends subscription update notification to all registered update-notification channels.
// Interfacing with this purely works via channels.
pub(crate) async fn notification_engine(
    configuration: Arc<USubscriptionConfiguration>,
    up_transport: Arc<dyn UTransport>,
    mut events: Receiver<NotificationEvent>,
    shutdown: Arc<Notify>,
) {
    // keep track of which clients want to receive direct notifications
    let mut notifications = persistency::NotificationStore::new(&configuration)
        .inspect_err(|e| {
            panic!("Cannot set up persistency for notification tracking {e}");
        })
        .unwrap();

    loop {
        let event = tokio::select! {
            event = events.recv() => match event {
                None => {
                    error!("Problem with notification command channel, received None-event");
                    break
                },
                Some(event) => event,
            },
            _ = shutdown.notified() => {
                break
            },
        };
        match event {
            // [impl->req~usubscription-register-notifications~1]
            NotificationEvent::AddNotifyee { subscriber } => {
                match notifications.add_notifyee(&subscriber) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Persistency failure {e}")
                    }
                };
            }
            // [impl->req~usubscription-unregister-notifications~1]
            NotificationEvent::RemoveNotifyee { subscriber } => {
                match notifications.remove_notifyee(&subscriber) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Persistency failure {e}")
                    }
                };
            }
            NotificationEvent::StateChange {
                // [impl->dsn~usubscription-change-message-type~0]
                subscription_info,
                respond_to,
            } => {
                // Send Update message to general notification channel
                // as per usubscription.proto RegisterForNotifications(NotificationsRequest)
                // [impl->req~usubscription-change-notification~1]
                match UMessageBuilder::publish(usubscription_uri(
                    None,
                    RESOURCE_ID_SUBSCRIPTION_CHANGE,
                ))
                .with_message_id(UUID::build())
                .build_with_protobuf_payload(&subscription_info)
                {
                    Err(e) => {
                        error!("Error building global update notification message: {e}");
                    }
                    Ok(update_msg) => {
                        if let Err(e) = up_transport.send(update_msg).await {
                            error!(
                                "Error sending global subscription-change update notification: {e}"
                            );
                        }
                    }
                }

                // Send Update notification message to all registered direct notifyees
                // [impl->req~usubscription-register-notifications~1]
                if let Ok(notifyees) = notifications.get_registered_notifyees() {
                    for notifyee in notifyees {
                        debug!(
                            "Sending notification to ({}), about topic {} changing state to {}",
                            notifyee.to_uri(INCLUDE_SCHEMA),
                            subscription_info.topic(),
                            subscription_info.status()
                        );

                        match UMessageBuilder::notification(
                            configuration.get_resource_uri(SOURCE_URI_RESOURCE_ID),
                            notifyee.clone(),
                        )
                        .build_with_protobuf_payload(&subscription_info)
                        {
                            Ok(update_msg) => {
                                let _r = up_transport.send(update_msg).await.inspect_err(|e|
                                    warn!("Error sending susbcriber-specific subscription-change update notification: {e}")
                                );
                            }
                            Err(e) => {
                                error!("Error building susbcriber-specific update notification message: {e}");
                            }
                        }
                    }
                }

                let _ = respond_to.send(());
            }

            #[cfg(test)]
            NotificationEvent::GetNotifyees { respond_to } => {
                let _r = respond_to.send(notifications.get_registered_notifyees().unwrap_or_else(
                    |e| {
                        error!("Problem retrieving notifyees from Persistency {e}");
                        Default::default()
                    },
                ));
            }

            NotificationEvent::Reset { respond_to } => {
                if respond_to.send(notifications.reset()).is_err() {
                    error!("Problem with internal communication");
                }
            }

            #[cfg(test)]
            NotificationEvent::SetNotifyees {
                notifyee_replacement,
                respond_to,
            } => {
                notifications.set_data(notifyee_replacement);
                let _r = respond_to.send(());
            }
        }
    }
}

// Convenience wrapper for sending state change notification messages
// `subscriber` is an Option, because in the case ob remote subscription state changes, there is no subscriber (other than local usubscription service)
pub(crate) async fn notify_state_change(
    notification_sender: Sender<NotificationEvent>,
    subscription_info: SubscriptionInfo,
) {
    let (respond_to, receive_from) = oneshot::channel::<()>();
    if let Err(e) = notification_sender
        .send(NotificationEvent::StateChange {
            subscription_info,
            respond_to,
        })
        .await
    {
        error!("Error initiating subscription-change update notification: {e}");
    }
    if let Err(e) = receive_from.await {
        // Not returning an error here, as update notification is not a core concern wrt the actual subscription management
        warn!("Error sending subscription-change update notification: {e}");
    };
}

// Convenience wrapper for resetting notification manager
pub(crate) async fn reset(
    notification_sender: Sender<NotificationEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (respond_to, receive_from) = oneshot::channel::<Result<(), PersistencyError>>();
    notification_sender
        .send(NotificationEvent::Reset { respond_to })
        .await?;
    let _ = receive_from.await?;
    Ok(())
}
