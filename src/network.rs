/// The network module, encapsulating all network related logic.
use async_trait::async_trait;
use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use libp2p::core::either::EitherError;
use libp2p::core::{Multiaddr, PeerId};
use libp2p::core::upgrade::{read_length_prefixed, write_length_prefixed, ProtocolName};
use libp2p::gossipsub::error::GossipsubHandlerError;
use libp2p::gossipsub::{
    GossipsubEvent, GossipsubMessage, IdentTopic as Topic, MessageAuthenticity, MessageId,
    ValidationMode,
};
use libp2p::identity::ed25519;
use libp2p::kad::record::store::MemoryStore;
use libp2p::kad::{GetProvidersOk, Kademlia, KademliaEvent, QueryId, QueryResult};
use libp2p::mdns::{Mdns, MdnsConfig, MdnsEvent};
use libp2p::multiaddr::Protocol;
use libp2p::request_response::{
    ProtocolSupport, RequestId, RequestResponse, RequestResponseCodec, RequestResponseEvent,
    RequestResponseMessage, ResponseChannel,
};
use libp2p::swarm::{ConnectionHandlerUpgrErr, SwarmBuilder, SwarmEvent};
use libp2p::{gossipsub, identity};
use libp2p::{NetworkBehaviour, Swarm};

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::iter;
use std::error::Error;
use std::time::Duration;
use void;

use tokio::io;

use serde::{Deserialize, Serialize};

use crate::bptree::*;

/// Creates the network components, namely:
///
/// - The network client to interact with the network layer from anywhere
///   within your application.
///
/// - The network event stream, e.g. for incoming requests.
///
/// - The network task driving the network itself.
pub async fn new(
    secret_key_seed: Option<u8>,
) -> Result<(Client, impl Stream<Item = Event>, EventLoop, PeerId), Box<dyn Error>> {
    // Create a public/private key pair, either random or based on a seed.
    let id_keys = match secret_key_seed {
        Some(seed) => {
            let mut bytes = [0u8; 32];
            bytes[0] = seed;
            let secret_key = ed25519::SecretKey::from_bytes(&mut bytes).expect(
                "this returns `Err` only if the length is wrong; the length is correct; qed",
            );
            identity::Keypair::Ed25519(secret_key.into())
        }
        None => identity::Keypair::generate_ed25519(),
    };
    let peer_id = id_keys.public().to_peer_id();

    let message_id_fn = |message: &GossipsubMessage| {
        let mut s = DefaultHasher::new();
        message.data.hash(&mut s);
        MessageId::from(s.finish().to_string())
    };

    let gossipsub_config = gossipsub::GossipsubConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
        .validation_mode(ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
        .message_id_fn(message_id_fn)
        .build()
        .expect("Valid config");

    // Build the Swarm, connecting the lower layer transport logic with the
    // higher layer network behaviour logic.
    let swarm = SwarmBuilder::new(
        libp2p::development_transport(id_keys.clone()).await?,
        ComposedBehaviour {
            kademlia: Kademlia::new(peer_id, MemoryStore::new(peer_id)),
            request_response: RequestResponse::new(
                GenericExchangeCodec(),
                iter::once((GenericProtocol(), ProtocolSupport::Full)),
                Default::default(),
            ),
            mdns: Mdns::new(MdnsConfig::default()).await?,
            gossipsub: gossipsub::Gossipsub::new(
                MessageAuthenticity::Signed(id_keys),
                gossipsub_config,
            )?,
        },
        peer_id,
    )
    .build();

    let (command_sender, command_receiver) = mpsc::channel(0);
    let (event_sender, event_receiver) = mpsc::channel(0);
    // let mut lease_map = HashMap::new();

    Ok((
        Client {
            sender: command_sender,
        },
        event_receiver,
        EventLoop::new(swarm, command_receiver, event_sender),
        peer_id,
    ))
}

#[derive(Clone)]
pub struct Client {
    sender: mpsc::Sender<Command>,
}

impl Client {
    /// Listen for incoming connections on the given address.

    pub async fn start_listening(&mut self, addr: Multiaddr) -> Result<(), Box<dyn Error + Send>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::StartListening { addr, sender })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not to be dropped.")
    }

    /// Dial the given peer at the given address.
    pub async fn dial(
        &mut self,
        peer_id: PeerId,
        peer_addr: Multiaddr,
    ) -> Result<(), Box<dyn Error + Send>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Dial {
                peer_id,
                peer_addr,
                sender,
            })
            .await
            .expect("Command receiver not to be dropped.");
        println!("dialed {:?}", peer_id);
        receiver.await.expect("Sender not to be dropped.")
    }

    /// Find the providers for the given file on the DHT.
    pub async fn get_providers(&mut self, searched_alias: String) -> HashSet<PeerId> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::GetProviders {
                searched_alias,
                sender,
            })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not to be dropped.")
    }

    pub async fn get_root_providers(&mut self) -> HashSet<PeerId> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::GetProviders {
                searched_alias: "root".to_string(),
                sender,
            })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not to be dropped.")
    }

    pub async fn request_lease(
        &mut self,
        provider_id: PeerId,
        key: Key,
        requester_id: PeerId,
        entry: Entry,
    ) -> Result<String, Box<dyn Error + Send>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::RequestLease {
                provider_id,
                sender,
                key,
                requester_id,
                entry,
            })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not be dropped.")
    }

    pub async fn respond_lease(
        &mut self,
        lease_response: String,
        channel: ResponseChannel<GenericResponse>,
    ) {
        self.sender
            .send(Command::RespondLease {
                lease_response,
                channel,
            })
            .await
            .expect("Command receiver not to be dropped.");
    }

    pub async fn request_remote_lease_search(
        &mut self,
        provider_id: PeerId,
        key: Key,
        requester_id: PeerId,
        entry: Entry,
        block_id: BlockId,
    ) -> Result<String, Box<dyn Error + Send>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::RequestRemoteLeaseSearch {
                provider_id,
                sender,
                key,
                requester_id,
                entry,
                block_id,
            })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not be dropped.")
    }

    pub async fn request_update_parent(
        &mut self,
        provider_id: PeerId,
        divider_key: Key,
        new_block_id: BlockId,
        parent_id: BlockId,
    ) -> Result<String, Box<dyn Error + Send>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::RequestUpdateParent {
                provider_id,
                sender,
                divider_key,
                new_block_id,
                parent_id,
            })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not be dropped.")
    }

    pub async fn respond_update_parent(
        &mut self,
        update_parent_respond: String,
        channel: ResponseChannel<GenericResponse>,
    ) {
        self.sender
            .send(Command::RespondUpdateParent {
                update_parent_respond,
                channel,
            })
            .await
            .expect("Command receiver not to be dropped.");
    }

    // pub async fn respond_split_block(&mut self, block_data: String,  channel: ResponseChannel<GenericResponse>) {
    //     self.sender
    //         .send(Command::RespondSplitBlock { block_data, channel })
    //         .await
    //         .expect("Command receiver not to be dropped.");
    // }

    // pub async fn respond_lease_change(&mut self, lease_change: String, channel: ResponseChannel<GenericResponse>) {
    //     self.sender
    //         .send(Command::RespondLeaseChange { lease_change,  channel })
    //         .await
    //         .expect("Command receiver not to be dropped.");
    // }

    pub async fn publish_topic(&mut self, topic: Topic, data: String) {
        self.sender
            .send(Command::PublishToTopic { topic, data })
            .await
            .expect("Command receiver not to be dropped.");
    }

    pub async fn boot_root(&mut self) {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::StartProviding {
                network_alias: "root".to_string(),
                sender,
            })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not to be dropped.");
    }

    pub async fn start_providing(&mut self, name: String) {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::StartProviding {
                network_alias: name,
                sender,
            })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not to be dropped.");
    }

    // pub async fn request_blockmap_size(
    //     &mut self,
    //     provider_id: PeerId,
    // ) -> Result<String, Box<dyn Error + Send>> {
    //     let (sender, receiver) = oneshot::channel();
    //     self.sender
    //         .send(Command::RequestBlockmapSize {
    //             provider_id,
    //             sender,
    //         })
    //         .await
    //         .expect("Command receiver not to be dropped.");
    //     receiver.await.expect("Sender not be dropped.")
    // }
    pub async fn respond_blockmap_size(
        &mut self,
        blockmap_size: usize,
        sender_id: PeerId,
        channel: ResponseChannel<GenericResponse>,
    ) {
        self.sender
            .send(Command::RespondBlockmapSize {
                blockmap_size,
                sender_id,
                channel,
            })
            .await
            .expect("Command receiver not to be dropped.");
    }

    pub async fn request_block_migration(&mut self, block: Block, target_id: PeerId) {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::RequestBlockMigration {
                block,
                target_id,
                sender,
            })
            .await
            .expect("Command receiver not to be dropped.");
        // receiver.await.expect("Sender not be dropped.")
    }

    pub async fn respond_block_migration(&mut self, block: Block, target_id: PeerId) {
        // Will be implemented after gossipsub
    }

    pub async fn subscribe_topic(&mut self, topic: Topic) {
        self.sender
            .send(Command::Subscribe {
                topic,
            })
            .await
            .expect("Command receiver not to be dropped.");
    }
}

pub struct EventLoop {
    swarm: Swarm<ComposedBehaviour>,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<Event>,
    pending_dial: HashMap<PeerId, oneshot::Sender<Result<(), Box<dyn Error + Send>>>>,
    pending_start_providing: HashMap<QueryId, oneshot::Sender<()>>,
    pending_get_providers: HashMap<QueryId, oneshot::Sender<HashSet<PeerId>>>,
    pending_generic_request:
        HashMap<RequestId, oneshot::Sender<Result<String, Box<dyn Error + Send>>>>,
}

impl EventLoop {
    fn new(
        swarm: Swarm<ComposedBehaviour>,
        command_receiver: mpsc::Receiver<Command>,
        event_sender: mpsc::Sender<Event>,
    ) -> Self {
        Self {
            swarm,
            command_receiver,
            event_sender,
            pending_dial: Default::default(),
            pending_start_providing: Default::default(),
            pending_get_providers: Default::default(),
            pending_generic_request: Default::default(),
        }
    }

    pub async fn run(mut self) {
        loop {
            futures::select! {
                event = self.swarm.next() => self.handle_event(event.expect("Swarm stream to be infinite.")).await,
                command = self.command_receiver.next() => match command {
                    Some(c) => self.handle_command(c).await,
                    // Command channel closed, thus shutting down the network event loop.
                    None=>  return,
                },
            }
        }
    }

    async fn handle_event(
        &mut self,
        event: SwarmEvent<
            ComposedEvent,
            EitherError<
                EitherError<
                    EitherError<ConnectionHandlerUpgrErr<io::Error>, io::Error>,
                    void::Void,
                >,
                GossipsubHandlerError,
            >,
        >,
    ) {
        match event {
            SwarmEvent::Behaviour(ComposedEvent::Mdns(MdnsEvent::Discovered(discovered_list))) => {
                for (peer, addr) in discovered_list {
                    self.swarm.behaviour_mut().kademlia.add_address(&peer, addr);
                    self.swarm
                        .behaviour_mut()
                        .gossipsub
                        .add_explicit_peer(&peer);
                    println!("added: {:?}", peer);
                }
            }
            SwarmEvent::Behaviour(ComposedEvent::Mdns(MdnsEvent::Expired(expired_list))) => {
                for (peer, _addr) in expired_list {
                    if !self.swarm.behaviour_mut().mdns.has_node(&peer) {
                        self.swarm.behaviour_mut().kademlia.remove_peer(&peer);
                        self.swarm
                            .behaviour_mut()
                            .gossipsub
                            .remove_explicit_peer(&peer);
                    }
                }
            }
            SwarmEvent::Behaviour(ComposedEvent::Gossipsub(GossipsubEvent::Message {
                propagation_source: peer_id,
                message_id: id,
                message,
            })) => {
                self.event_sender
                        .send(Event::GossibsubRequest {
                            incoming_message: message,
                            incoming_peer_id: peer_id,
                            })
                        .await
                        .expect("Event receiver not to be dropped ");
            }
            // SwarmEvent::Behaviour(ComposedEvent::Gossipsub(_)) => {}
            SwarmEvent::Behaviour(ComposedEvent::Kademlia(
                KademliaEvent::OutboundQueryCompleted {
                    id,
                    result: QueryResult::StartProviding(_),
                    ..
                },
            )) => {
                println!("{:?}", self.pending_start_providing);
                let sender: oneshot::Sender<()> = self
                    .pending_start_providing
                    .remove(&id)
                    .expect("Completed query to be previously pending.");
                let _ = sender.send(());
            }
            SwarmEvent::Behaviour(ComposedEvent::Kademlia(
                KademliaEvent::OutboundQueryCompleted {
                    id,
                    result: QueryResult::GetProviders(Ok(GetProvidersOk { providers, .. })),
                    ..
                },
            )) => {
                let _ = self
                    .pending_get_providers
                    .remove(&id)
                    .expect("Completed query to be previously pending.")
                    .send(providers);
            }
            SwarmEvent::Behaviour(ComposedEvent::Kademlia(_)) => {}
            SwarmEvent::Behaviour(ComposedEvent::RequestResponse(
                RequestResponseEvent::Message { message, .. },
            )) => match message {
                RequestResponseMessage::Request {
                    request, channel, ..
                } => {
                    self.event_sender
                        .send(Event::InboundRequest {
                            incoming_request: request.0,
                            channel,
                        })
                        .await
                        .expect("Event receiver not to be dropped.");
                }
                RequestResponseMessage::Response {
                    request_id,
                    response,
                } => {
                    let _ = self
                        .pending_generic_request
                        .remove(&request_id)
                        .expect("Request to still be pending.")
                        .send(Ok(response.0));
                }
            },
            SwarmEvent::Behaviour(ComposedEvent::RequestResponse(
                RequestResponseEvent::OutboundFailure {
                    request_id, error, ..
                },
            )) => {
                let _ = self
                    .pending_generic_request
                    .remove(&request_id)
                    .expect("Request to still be pending.")
                    .send(Err(Box::new(error)));
            }
            SwarmEvent::Behaviour(ComposedEvent::RequestResponse(
                RequestResponseEvent::ResponseSent { .. },
            )) => {}
            SwarmEvent::NewListenAddr { address, .. } => {
                let local_peer_id = *self.swarm.local_peer_id();
                println!(
                    "Local node is listening on {:?}",
                    address.with(Protocol::P2p(local_peer_id.into()))
                );
            }
            SwarmEvent::IncomingConnection { .. } => {}
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                if endpoint.is_dialer() {
                    if let Some(sender) = self.pending_dial.remove(&peer_id) {
                        let _ = sender.send(Ok(()));
                    }
                }
            }
            SwarmEvent::ConnectionClosed { .. } => {}
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                if let Some(peer_id) = peer_id {
                    if let Some(sender) = self.pending_dial.remove(&peer_id) {
                        let _ = sender.send(Err(Box::new(error)));
                    }
                }
            }
            SwarmEvent::IncomingConnectionError { .. } => {}
            SwarmEvent::Dialing(peer_id) => println!("Dialing {}", peer_id),
            e => panic!("{:?}", e),
        }
    }

    // ------------------------------------------------------
    // network receiving from the application
    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::StartListening { addr, sender } => {
                let _ = match self.swarm.listen_on(addr) {
                    Ok(_) => sender.send(Ok(())),
                    Err(e) => sender.send(Err(Box::new(e))),
                };
            }
            Command::Dial {
                peer_id,
                peer_addr,
                sender,
            } => {
                if self.pending_dial.contains_key(&peer_id) {
                    todo!("Already dialing peer.");
                } else {
                    self.swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, peer_addr.clone());
                    match self
                        .swarm
                        .dial(peer_addr.with(Protocol::P2p(peer_id.into())))
                    {
                        Ok(()) => {
                            self.pending_dial.insert(peer_id, sender);
                        }
                        Err(e) => {
                            let _ = sender.send(Err(Box::new(e)));
                        }
                    }
                }
            }
            Command::GetProviders {
                searched_alias,
                sender,
            } => {
                let query_id = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .get_providers(searched_alias.into_bytes().into());
                self.pending_get_providers.insert(query_id, sender);
            }
            Command::RequestLease {
                provider_id,
                sender,
                key,
                requester_id,
                entry,
            } => {
                let lease_request: IncomingRequest =
                    IncomingRequest::RequestLease(requester_id, key, entry);
                let serialize_request = serde_json::to_string(&lease_request).unwrap();
                let request_id = self
                    .swarm
                    .behaviour_mut()
                    .request_response
                    .send_request(&provider_id, GenericRequest(serialize_request));
                self.pending_generic_request.insert(request_id, sender);
            }
            Command::RespondLease {
                lease_response,
                channel,
            } => {
                // Done
                self.swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(channel, GenericResponse(lease_response))
                    .expect("Connection to peer to be still open.");
            }
            // Command::RequestBlockmapSize {
            //     provider_id,
            //     sender,
            // } => {
            //     // Done
            //     let blockmap_size_request: IncomingRequest =
            //         IncomingRequest::RequestBlockmapSize(provider_id);
            //     let serialize_request = serde_json::to_string(&blockmap_size_request).unwrap();
            //     let request_id = self
            //         .swarm
            //         .behaviour_mut()
            //         .request_response
            //         .send_request(&provider_id, GenericRequest(serialize_request));
            //     self.pending_generic_request.insert(request_id, sender);
            // }
            Command::RespondBlockmapSize {
                blockmap_size,
                sender_id,
                channel,
            } => {
                let blockmap_size_respond = (blockmap_size, sender_id);
                let serialize_request = serde_json::to_string(&blockmap_size_respond).unwrap();
                self.swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(channel, GenericResponse(serialize_request))
                    .expect("Connection to peer to be still open.");
            }
            Command::RequestUpdateParent {
                provider_id,
                sender,
                divider_key,
                new_block_id,
                parent_id,
            } => {
                let update_parent_request: IncomingRequest =
                    IncomingRequest::RequestUpdateParent(divider_key, new_block_id, parent_id);
                let serialize_request = serde_json::to_string(&update_parent_request).unwrap();
                let request_id = self
                    .swarm
                    .behaviour_mut()
                    .request_response
                    .send_request(&provider_id, GenericRequest(serialize_request));
                self.pending_generic_request.insert(request_id, sender);
            }
            Command::RespondUpdateParent {
                update_parent_respond,
                channel,
            } => {
                // Done
                self.swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(channel, GenericResponse(update_parent_respond))
                    .expect("Connection to peer to be still open.");
            }

            Command::RequestRemoteLeaseSearch {
                provider_id,
                sender,
                key,
                requester_id,
                entry,
                block_id,
            } => {
                let lease_request: IncomingRequest =
                    IncomingRequest::RequestRemoteSearch(requester_id, key, block_id, entry);
                let serialize_request = serde_json::to_string(&lease_request).unwrap();
                let request_id = self
                    .swarm
                    .behaviour_mut()
                    .request_response
                    .send_request(&provider_id, GenericRequest(serialize_request));
                self.pending_generic_request.insert(request_id, sender);
            }

            // Command::RespondLeaseChange {lease_change, channel } => { // Done
            //     self.swarm
            //         .behaviour_mut()
            //         .request_response
            //         .send_response(channel, GenericResponse(lease_change))
            //         .expect("Connection to peer to be still open.");
            // }
            Command::StartProviding {
                network_alias,
                sender,
            } => {
                // Done
                let query_id = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .start_providing(network_alias.into_bytes().into())
                    .expect("No store error.");
                self.pending_start_providing.insert(query_id, sender);
            }
            Command::PublishToTopic { topic, data } => {
                let _query_id = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(topic, "blocks")
                    .expect("Publishing");
            }

            Command::RequestBlockMigration {
                block,
                target_id,
                sender,
            } => {
                let block_migration_request: IncomingRequest =
                    IncomingRequest::RequestBlockMigration(block);
                let serialize_request = serde_json::to_string(&block_migration_request).unwrap();
                let request_id = self
                    .swarm
                    .behaviour_mut()
                    .request_response
                    .send_request(&target_id, GenericRequest(serialize_request));
                self.pending_generic_request.insert(request_id, sender);
            }

            Command::RespondBlockMigration { channel } => {
                let block_migration_response = String::from("Success");
                let serialize_request = serde_json::to_string(&block_migration_response).unwrap();
                self.swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(channel, GenericResponse(serialize_request))
                    .expect("Connection to peer to be still open.");
            }

            Command::Subscribe { topic } => {
                self.swarm
                    .behaviour_mut()
                    .gossipsub
                    .subscribe(&topic);
            }   
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum IncomingRequest {
    RequestLease(PeerId, Key, Entry),
    RequestUpdateParent(Key, BlockId, BlockId),
    RequestRemoteSearch(PeerId, Key, BlockId, Entry),
    // RequestBlockmapSize(PeerId),
    RequestBlockMigration(Block),
}


#[derive(Serialize, Deserialize, Debug)]
pub enum LeaseResponse {
    LeaseSuccess,
    LeaseContinuation(BlockId),
    LeaseFail,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum InsertionResponse {
    InsertSuccess,
    InsertContinuation(BlockId),
    InsertFail,
}

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "ComposedEvent")]
struct ComposedBehaviour {
    request_response: RequestResponse<GenericExchangeCodec>,
    kademlia: Kademlia<MemoryStore>,
    mdns: Mdns,
    gossipsub: gossipsub::Gossipsub,
}

#[derive(Debug)]
enum ComposedEvent {
    RequestResponse(RequestResponseEvent<GenericRequest, GenericResponse>),
    Kademlia(KademliaEvent),
    Mdns(MdnsEvent),
    Gossipsub(GossipsubEvent),
}



impl From<RequestResponseEvent<GenericRequest, GenericResponse>> for ComposedEvent {
    fn from(event: RequestResponseEvent<GenericRequest, GenericResponse>) -> Self {
        ComposedEvent::RequestResponse(event)
    }
}

impl From<KademliaEvent> for ComposedEvent {
    fn from(event: KademliaEvent) -> Self {
        ComposedEvent::Kademlia(event)
    }
}

impl From<MdnsEvent> for ComposedEvent {
    fn from(event: MdnsEvent) -> Self {
        ComposedEvent::Mdns(event)
    }
}

impl From<GossipsubEvent> for ComposedEvent {
    fn from(event: GossipsubEvent) -> Self {
        ComposedEvent::Gossipsub(event)
    }
}

#[derive(Debug)]
enum Command {
    StartListening {
        addr: Multiaddr,
        sender: oneshot::Sender<Result<(), Box<dyn Error + Send>>>,
    },
    Dial {
        peer_id: PeerId,
        peer_addr: Multiaddr,
        sender: oneshot::Sender<Result<(), Box<dyn Error + Send>>>,
    },
    GetProviders {
        searched_alias: String,
        sender: oneshot::Sender<HashSet<PeerId>>,
    },
    RequestLease {
        provider_id: PeerId,
        sender: oneshot::Sender<Result<String, Box<dyn Error + Send>>>,
        key: Key,
        requester_id: PeerId,
        entry: Entry,
    },
    RespondLease {
        lease_response: String,
        channel: ResponseChannel<GenericResponse>,
    },
    // RespondSplitBlock {
    //     block_data: String,
    //     channel: ResponseChannel<GenericResponse>,
    // },
    // RespondLeaseChange {
    //     lease_change: String,
    //     channel: ResponseChannel<GenericResponse>,
    // },
    StartProviding {
        network_alias: String,
        sender: oneshot::Sender<()>,
    },
    PublishToTopic {
        topic: Topic,
        data: String,
    },
    // RequestBlockmapSize {
    //     provider_id: PeerId,
    //     sender: oneshot::Sender<Result<String, Box<dyn Error + Send>>>,
    // },
    RespondBlockmapSize {
        blockmap_size: usize,
        sender_id: PeerId,
        channel: ResponseChannel<GenericResponse>,
    },
    RequestUpdateParent {
        provider_id: PeerId,
        sender: oneshot::Sender<Result<String, Box<dyn Error + Send>>>,
        divider_key: Key,
        new_block_id: BlockId,
        parent_id: BlockId,
    },
    RespondUpdateParent {
        update_parent_respond: String,
        channel: ResponseChannel<GenericResponse>,
    },
    RequestRemoteLeaseSearch {
        provider_id: PeerId,
        sender: oneshot::Sender<Result<String, Box<dyn Error + Send>>>,
        key: Key,
        requester_id: PeerId,
        entry: Entry,
        block_id: BlockId,
    },
    RequestBlockMigration {
        block: Block,
        target_id: PeerId,
        sender: oneshot::Sender<Result<String, Box<dyn Error + Send>>>,
    },
    RespondBlockMigration {
        channel: ResponseChannel<GenericResponse>,
    },
    Subscribe {
        topic: Topic,
    }
}

#[derive(Debug)]
pub enum Event {
    InboundRequest {
        incoming_request: String,
        channel: ResponseChannel<GenericResponse>,
    },
    GossibsubRequest {
        incoming_message: GossipsubMessage,
        incoming_peer_id: PeerId,
    }
}

// Simple file exchange protocol

#[derive(Debug, Clone)]
struct GenericProtocol();
#[derive(Clone)]
struct GenericExchangeCodec();
#[derive(Debug, Clone, PartialEq, Eq)]
struct GenericRequest(String);
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenericResponse(String);
// change the type from String to MyType

impl ProtocolName for GenericProtocol {
    fn protocol_name(&self) -> &[u8] {
        "/lease-exchange/1".as_bytes()
    }
}

#[async_trait]
impl RequestResponseCodec for GenericExchangeCodec {
    type Protocol = GenericProtocol;
    type Request = GenericRequest;
    type Response = GenericResponse;

    async fn read_request<T>(
        &mut self,
        _: &GenericProtocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let vec = read_length_prefixed(io, 1_000_000).await?;

        if vec.is_empty() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        Ok(GenericRequest(String::from_utf8(vec).unwrap()))
    }

    async fn read_response<T>(
        &mut self,
        _: &GenericProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let vec = read_length_prefixed(io, 1_000_000).await?;

        if vec.is_empty() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        Ok(GenericResponse(String::from_utf8(vec).unwrap()))
    }

    async fn write_request<T>(
        &mut self,
        _: &GenericProtocol,
        io: &mut T,
        GenericRequest(data): GenericRequest,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_length_prefixed(io, data).await?;
        io.close().await?;

        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &GenericProtocol,
        io: &mut T,
        GenericResponse(data): GenericResponse,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_length_prefixed(io, data).await?;
        io.close().await?;

        Ok(())
    }
}
