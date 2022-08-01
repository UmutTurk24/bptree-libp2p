use clap::Parser;
use rand::Rng;
use serde::{Serialize, Deserialize};
use tokio::spawn;
use tokio::io::AsyncBufReadExt;
use futures::prelude::*;
use libp2p::core::{Multiaddr, PeerId};
use libp2p::multiaddr::Protocol;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use void;
use tokio::time::{sleep, Duration};
mod BPNode;
use BPNode::{Block, Key, Entry, BlockId};
use crate::BPNode::BlockMap;

// run with cargo run -- --secret-key-seed #


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // env_logger::init();

    let opt = Opt::parse();
    // manual args
    // let secret_key_seed: Option<u8> = Some(1);
    let secret_key_seed = opt.secret_key_seed;
    let listen_address: Option<Multiaddr> = None;
    let peer: Option<Multiaddr> = None;

    let (mut network_client, mut network_events, network_event_loop, network_client_id) =
    // network::new(opt.secret_key_seed).await?;
    network::new(secret_key_seed).await?;

    // Spawn the network task for it to run in the background.
    spawn(network_event_loop.run());

    // In case a listen address was provided use it, otherwise listen on any address.
    match listen_address{
        Some(addr) => network_client
            .start_listening(addr)
            .await
            .expect("Listening not to fail."),
        None => network_client
            .start_listening("/ip4/0.0.0.0/tcp/0".parse()?)
            .await
            .expect("Listening not to fail."),
    };
    
    // In case the user provided an address of a peer on the CLI, dial it.
    // if let Some(addr) = opt.peer {
    if let Some(addr) = peer {
        let peer_id = match addr.iter().last() {
            Some(Protocol::P2p(hash)) => PeerId::from_multihash(hash).expect("Valid hash."),
            _ => return Err("Expect peer multiaddr to contain peer ID.".into()),
        };
        network_client
            .dial(peer_id, addr)
            .await
            .expect("Dial to succeed");
    }


    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();

    let mut number_generator = rand::thread_rng();
    let mut block_map = BlockMap::boot_new(&network_client_id);
    let mut lease_map: HashMap<Key,Entry> = HashMap::new();    
    
    let mut network_block_count = 10;
    let mut block_size_buffer = 5;

    // THREAD SPAWN FOR FINDING THE CLIENT WITH THE LEAST AMOUNT OF BLOCKS
    /*

    let mut thread_client = network_client.clone();

    let block_counter_handle = tokio::spawn(async move {
        thread_client.start_providing("block_map_counter".to_string()).await;
        loop {
            sleep(Duration::from_millis(20000)).await;
            let providers = thread_client.get_providers("block_map_counter".to_string()).await;

            let requests = providers.into_iter().map(|provider_id| {
                let mut thread_client = thread_client.clone();
                tokio::spawn(async move { thread_client.request_blockmap_size(provider_id).await }.boxed())
            });

            // let responses = requests.await.map_err(|_| "None of the providers returned.")?;
            // join!(requests);
            // let data = futures::future::select_all(requests).await.0;

            // let mut map_of_sizes: HashMap<String, usize> = HashMap::new();
            // requests.map(|raw_responses| {
            //     let responses = raw_responses.await;
            //     let deserealized_response: (usize, PeerId) = serde_json::from_str(&responses).unwrap();
            // });

            // println!("Item_resolved: {:?}", item_resolved);
                
            // TODOs:
            // Update the block_count 
        }
    });

     */

    
    loop {
        tokio::select! { 
            line_option = stdin.next_line() => match line_option {
                Ok(None) => {break;},
                Ok(Some(line)) => {
                    match line.as_str() {
                        // "getlease" => list_peers().await,
                        "getlease" => {
                            let random_key: u64 = number_generator.gen();
                            let providers = network_client.get_root_providers().await;

                            if providers.is_empty() {
                                return Err(format!("Could not find provider for leases.").into());
                            }
                            let requests = providers.into_iter().map(|provider_id| {
                                let mut network_client = network_client.clone();
                                let placeholder_entry = Entry::shallow_new(network_client_id);
                                tokio::spawn(async move { network_client.request_lease(provider_id, random_key, network_client_id, placeholder_entry).await }.boxed())
                            });
                        
                        println!("{:?}", requests);
                        
                        let root_response = futures::future::select_ok(requests)
                            .await
                            .map_err(|_| "None of the providers returned.")?
                            .0
                            .unwrap();

                        let lease_response: LeaseResponse = serde_json::from_str(&root_response).unwrap();
                        
                        match lease_response{
                                LeaseResponse::LeaseSuccess => {
                                    let placeholder_entry = Entry::shallow_new(network_client_id);
                                    lease_map.insert(random_key,placeholder_entry);
                                    println!("Lease was inserted successfully!");
                                },
                                LeaseResponse::LeaseContinuation(mut next_block_id) => {
                                    // CODE WON'T REACH HERE YET
                                    // RIGHT NOW THIS WON'T WORK BECAUSE NEW BLOCKS ARE NOT BEING ADDED TO KADEMLIA
                                    // NO SET PROVIDER

                                    loop {
                                        let providers = network_client.get_providers(next_block_id.to_string()).await;

                                        let requests = providers.into_iter().map(|provider_id| {
                                            let mut network_client = network_client.clone();
                                            let placeholder_entry = Entry::shallow_new(network_client_id);
                                            tokio::spawn(async move { network_client.request_remote_lease_search(provider_id, random_key, network_client_id, placeholder_entry, next_block_id).await }.boxed())
                                        });

                                        let responses = futures::future::select_ok(requests)
                                            .await
                                            .map_err(|_| "None of the providers returned.")?
                                            .0
                                            .unwrap();
                                        
                                        let remote_search_response: LeaseResponse = serde_json::from_str(&responses).unwrap();

                                        match remote_search_response{
                                            LeaseResponse::LeaseSuccess => {
                                                let placeholder_entry = Entry::shallow_new(network_client_id);
                                                lease_map.insert(random_key,placeholder_entry);
                                                println!("Lease was inserted successfully!");
                                                break
                                            },
                                            LeaseResponse::LeaseContinuation(new_next_block_id) => { next_block_id = new_next_block_id},
                                            LeaseResponse::LeaseFail => {println!("Something went wrong, try again"); break},
                                        }
                                    }
                                },
                                LeaseResponse::LeaseFail => {println!("Something went wrong, try again")},
                            }
                        },
                        "root" => {
                            network_client.boot_root().await; // to be found in the network with the name "root"


                            // SPAWN A THREAD FOR CHECKING WHO HAS THE LOWEST NUMBER OF BLOCKS
                            // AND MIGRATE BLOCKS TO THAT NODE

                            // tokio::spawn(async {
                            // });


                        }
                        _ => print!("unknown command\n"),
                    }
                },
                Err(_) => print!("Error handing input line: "),
            },
            event = network_events.next() => match event {
                    None => {
                        
                    },
                    Some(network::Event::InboundRequest {incoming_request, channel }) => {

                        // deserealize the incoming request and match with possible requests
                        let deserealized_request: IncomingRequest = serde_json::from_str(&incoming_request).unwrap();

                        match deserealized_request {
                            IncomingRequest::RequestLease(requester_id, requested_key, entry) => {
                                network_client.handle_request_lease(requester_id, requested_key, entry, &mut block_map, channel).await?;
                            },
                            IncomingRequest::RequestUpdateParent(divider_key, new_block_id, parent_id) => {
                                network_client.handle_request_update_parent(divider_key, new_block_id, parent_id, &mut block_map, channel).await?;
                            },
                            IncomingRequest::RequestRemoteSearch(requester_id, requested_key, block_id, entry) => {
                                network_client.handle_request_remote_lease_search(requester_id, requested_key, block_id, entry, &mut block_map, channel).await?;
                            },
                            IncomingRequest::RequestBlockmapSize(_requester_id) => {
                                network_client.handle_request_blockmap_size(&mut block_map, network_client_id, channel).await?;
                            }
                            IncomingRequest::RequestBlockMigration(block) => {
                                // Will be implemented after gossipsub
                            }
                        }                        
                    }
                }
            }
        }
    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
enum IncomingRequest {
    RequestLease(PeerId, Key, Entry),
    RequestUpdateParent(Key, BlockId, BlockId),
    RequestRemoteSearch(PeerId, Key, BlockId, Entry),
    RequestBlockmapSize(PeerId),
    RequestBlockMigration(Block),
}

#[derive(Serialize, Deserialize, Debug)]
enum LeaseResponse {
    LeaseSuccess,
    LeaseContinuation(BlockId),
    LeaseFail,
}

#[derive(Serialize, Deserialize, Debug)]
enum InsertionResponse {
    InsertSuccess,
    InsertContinuation(BlockId),
    InsertFail,
}

#[derive(Parser, Debug)]
#[clap(name = "libp2p file sharing example")]
struct Opt {
    /// Fixed value to generate deterministic peer ID.
    #[clap(long)]
    secret_key_seed: Option<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
struct FileResponse{
    data: String,
}

// To be implemented after gossipsub, since it can be only tested with loadbalancing

struct QueueMap {
    map: HashMap<BlockId,VecDeque<Box<dyn Queueable>>>
}

impl QueueMap{
    pub fn new() -> Self{
        return Self { map: Default::default() }
    }
    pub fn add(&mut self, key: BlockId, queue: VecDeque<Box<dyn Queueable>>) {
        self.map.insert(key, queue);
    }

    pub fn remove(&mut self, key: BlockId) -> Option<VecDeque<Box<dyn Queueable>>>{
        self.map.remove(&key)
    }
    pub fn update(&mut self, key: BlockId, action: Box<dyn Queueable>) {
        match self.map.entry(key) 
        {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                e.get_mut().push_back(action);
            },
            std::collections::hash_map::Entry::Vacant(_) => {//Err message
            },
        }
    }
}

struct QueuedActions {
    pub queued_actions: VecDeque<Box<dyn Queueable>>,
}

impl QueuedActions {
    pub fn run (&self) {
        for action in self.queued_actions.iter() {
            action.execute();
        }
    }
    pub fn queue(&mut self, action: Box<dyn Queueable>) {
        self.queued_actions.push_back(action);
    }

    pub fn dequeue (&mut self) -> Option<Box<dyn Queueable>> {
        self.queued_actions.pop_back()
    }
}

pub trait Queueable {
    fn execute (&self);
}

struct SearchBlock{
    block_id: BlockId,
}

/// The network module, encapsulating all network related logic.
mod network {
    use crate::BPNode::BlockId;

    use super::*;
    use async_trait::async_trait;
    use futures::channel::{mpsc, oneshot};
    use libp2p::core::either::EitherError;
    use libp2p::core::upgrade::{read_length_prefixed, write_length_prefixed, ProtocolName};
    use libp2p::gossipsub::error::GossipsubHandlerError;
    use libp2p::gossipsub::{GossipsubEvent, MessageAuthenticity, ValidationMode, IdentTopic as Topic, GossipsubMessage, MessageId};
    use libp2p::{identity, gossipsub};
    use libp2p::identity::ed25519;
    use libp2p::kad::record::store::MemoryStore;
    use libp2p::kad::{GetProvidersOk, Kademlia, KademliaEvent, QueryId, QueryResult};
    use libp2p::mdns::{Mdns, MdnsEvent, MdnsConfig};
    use libp2p::multiaddr::Protocol;
    use libp2p::request_response::{
        ProtocolSupport, RequestId, RequestResponse, RequestResponseCodec, RequestResponseEvent,
        RequestResponseMessage, ResponseChannel,
    };
    use libp2p::swarm::{ConnectionHandlerUpgrErr, SwarmBuilder, SwarmEvent};
    use libp2p::{NetworkBehaviour, Swarm};
    use tokio::io;
    use std::collections::hash_map::DefaultHasher;
    use std::collections::{HashMap, HashSet};
    use std::hash::{Hash, Hasher};
    use std::iter;


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
                gossipsub: gossipsub::Gossipsub::new(MessageAuthenticity::Signed(id_keys), gossipsub_config)?,
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

        pub async fn start_listening(
            &mut self,
            addr: Multiaddr,
        ) -> Result<(), Box<dyn Error + Send>> {
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
                .send(Command::GetProviders { searched_alias, sender })
                .await
                .expect("Command receiver not to be dropped.");
            receiver.await.expect("Sender not to be dropped.")
        }

        pub async fn get_root_providers(&mut self) -> HashSet<PeerId> {
            let (sender, receiver) = oneshot::channel();
            self.sender
                .send(Command::GetProviders { searched_alias: "root".to_string() , sender, })
                .await
                .expect("Command receiver not to be dropped.");
                receiver.await.expect("Sender not to be dropped.")
            }
            
        pub async fn request_lease(&mut self, provider_id: PeerId, key: Key, requester_id: PeerId, entry: Entry) -> Result<String, Box<dyn Error + Send>> {
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

        pub async fn respond_lease(&mut self, lease_response: String, channel: ResponseChannel<GenericResponse>) {
            self.sender
                .send(Command::RespondLease { lease_response, channel })
                .await
                .expect("Command receiver not to be dropped.");
        }

        pub async fn request_remote_lease_search(&mut self, provider_id: PeerId, key: Key, requester_id: PeerId, entry: Entry, block_id: BlockId) -> Result<String, Box<dyn Error + Send>> {
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

        pub async fn request_update_parent(&mut self, provider_id: PeerId, divider_key: Key, new_block_id: BlockId, parent_id: BlockId) -> Result<String, Box<dyn Error + Send>> {
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

        pub async fn respond_update_parent(&mut self, update_parent_respond: String, channel: ResponseChannel<GenericResponse>) {
            self.sender
                .send(Command::RespondUpdateParent {
                    update_parent_respond, channel
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

        pub async fn publish_topic(&mut self, topic: String) {
            let (sender, receiver) = oneshot::channel();
            let topic = Topic::new(topic);
            self.sender
                .send(Command::PublishTopic { topic, sender })
                .await
                .expect("Command receiver not to be dropped.");
            receiver.await.expect("Sender not to be dropped.");
        }

        pub async fn boot_root(&mut self) {
            let (sender, receiver) = oneshot::channel();
            self.sender
                .send(Command::StartProviding { network_alias: "root".to_string(), sender })
                .await
                .expect("Command receiver not to be dropped.");
            receiver.await.expect("Sender not to be dropped.");
        }

        pub async fn start_providing(&mut self, name: String) {
            let (sender, receiver) = oneshot::channel();
            self.sender
                .send(Command::StartProviding { network_alias: name, sender })
                .await
                .expect("Command receiver not to be dropped.");
            receiver.await.expect("Sender not to be dropped.");
        }

        pub async fn handle_request_lease(&mut self, _requester_id: PeerId, requested_key: Key, entry: Entry, block_map: &mut BlockMap, channel: ResponseChannel<GenericResponse>) -> Result<(), Box<dyn Error>> {
            
            // Each root takes the requests for initializing a search in the map. Therefore the search needs to 
            // start from the root node of the block_map
            let search_result = block_map.local_search(&requested_key, block_map.get_root_id());

                match search_result {
                    // Found a local leaf block
                    BPNode::LocalSearchResult::LeafBlock(leaf_block_id) => {
                        let leaf_block = block_map.get_mut_block(leaf_block_id);                                        
                        let insert_result = leaf_block.insert_entry(requested_key, entry);

                        match insert_result {
                            BPNode::LeafInsertResult::Completed() => {
                                let lease_response = LeaseResponse::LeaseSuccess;
                                let serialized_response = serde_json::to_string(&lease_response).unwrap();
                                self.respond_lease(serialized_response, channel).await; // there is no need to await. Find a way to just respond w the lease
                                Ok(())
                            },
                            BPNode::LeafInsertResult::SplitRequest(entry) => {
                                let (mut new_block, divider_key) = leaf_block.split_leaf_block();
                                new_block.set_right_block(leaf_block.get_right_block());
                                leaf_block.set_right_block(new_block.assign_random_id());
                                leaf_block.insert_entry(requested_key, entry);
                                
                                if leaf_block.get_parent_id() == 0 {
                                    // Create a new parent and assign the keys from splitted blocks
                                    let mut parent_block = Block::new();
                                    parent_block.create_new_parent(leaf_block, &divider_key, &mut new_block);
                                    
                                    block_map.update_root(parent_block.get_block_id());

                                    ////////////////////////////////////////
                                    // IMPLEMENT LOAD BALANCING HERE
                                    // IF TOO MANY BLOKCS ARE IN THE MAP
                                    // SEND THE BLOCK TO THE CLOSEST NODE
                                    /////////////////////////////////////////
                                    
                                    // Replace with block_size_buffer and network_block_count
                                    if (block_map.get_size() + 5) > 10 {
                                        // since block_size_buffer and peer_id will be in a tuple together...
                                        // send a request to the peer and migrate the block. Set the block unavailable
                                    }
                                    
                                    block_map.insert(new_block);
                                    block_map.insert(parent_block);

                                    let lease_response = LeaseResponse::LeaseSuccess;
                                    let serialized_response = serde_json::to_string(&lease_response).unwrap();
                                    self.respond_lease(serialized_response, channel).await;
                                    Ok(())
                                }
                                else {
                                    // Send a request to the parent block for inserting key/child pair

                                    let remote_parent_id = leaf_block.get_parent_id();
                                    let providers = self.get_providers(remote_parent_id.to_string()).await;

                                    if providers.is_empty() {
                                        let lease_response = LeaseResponse::LeaseFail;
                                        let serialized_response = serde_json::to_string(&lease_response).unwrap();
                                        self.respond_lease(serialized_response, channel).await;
                                        Ok(())
                                    } else {
                                        // Update the parent's block
                                    let update_parent_requests = providers.into_iter().map(|peer_id| {
                                        let mut network_client = self.clone();
                                        let new_block_id = new_block.get_block_id();
                                        tokio::spawn(async move { network_client.request_update_parent(peer_id, divider_key, new_block_id, remote_parent_id).await }.boxed())
                                    });

                                    // As long as there is a response, continue
                                    let _response = futures::future::select_ok(update_parent_requests)
                                        .await
                                        .map_err(|_| "None of the providers returned.")?
                                        .0
                                        .unwrap();
                                    
                                    let lease_response = LeaseResponse::LeaseSuccess;
                                    let serialized_response = serde_json::to_string(&lease_response).unwrap();
                                    self.respond_lease(serialized_response, channel).await;
                                    Ok(())
                                    }
                                }
                            },
                        }
                    },
                    BPNode::LocalSearchResult::RemoteBlock(remote_block_id) => {
                        let lease_response = LeaseResponse::LeaseContinuation(remote_block_id);
                        let serialized_response = serde_json::to_string(&lease_response).unwrap();
                        self.respond_lease(serialized_response, channel).await;
                        Ok(())
                    },
                    BPNode::LocalSearchResult::UnavailableBlock(unavailable_block_id) => {
                        // Will be completed after gossipsub is implemented
                        Ok(())
                    }
                }
        }

        pub async fn handle_request_remote_lease_search(&mut self, _requester_id: PeerId, requested_key: Key, current_block_id: BlockId, entry: Entry, block_map: &mut BlockMap, channel: ResponseChannel<GenericResponse>) -> Result<(), Box<dyn Error>> {
            let search_result = block_map.local_search(&requested_key, current_block_id);
            match search_result {
                // Found a local leaf block
                BPNode::LocalSearchResult::LeafBlock(leaf_block_id) => {
                    let leaf_block = block_map.get_mut_block(leaf_block_id);                                        
                    let insert_result = leaf_block.insert_entry(requested_key, entry);

                    match insert_result {
                        BPNode::LeafInsertResult::Completed() => {
                            let lease_response = LeaseResponse::LeaseSuccess;
                            let serialized_response = serde_json::to_string(&lease_response).unwrap();
                            self.respond_lease(serialized_response, channel).await; // there is no need to await. Find a way to just respond w the lease
                            Ok(())
                        },
                        BPNode::LeafInsertResult::SplitRequest(entry) => {
                            let (mut new_block, divider_key) = leaf_block.split_leaf_block();
                            new_block.set_right_block(leaf_block.get_right_block());
                            leaf_block.set_right_block(new_block.assign_random_id());
                            leaf_block.insert_entry(requested_key, entry);

                            if leaf_block.get_parent_id() == 0 {
                                // Create a new parent and assign the keys from splitted blocks
                                let mut parent_block = Block::new();
                                parent_block.create_new_parent(leaf_block, &divider_key, &mut new_block);
                                
                                block_map.update_root(parent_block.get_block_id());
                                ////////////////////////////////////////
                                // IMPLEMENT LOAD BALANCING HERE
                                // IF TOO MANY BLOKCS ARE IN THE MAP
                                // SEND THE BLOCK TO THE CLOSEST NODE
                                /////////////////////////////////////////
                                
                                block_map.insert(new_block);
                                block_map.insert(parent_block);

                                let lease_response = LeaseResponse::LeaseSuccess;
                                let serialized_response = serde_json::to_string(&lease_response).unwrap();
                                self.respond_lease(serialized_response, channel).await;
                                Ok(())
                            }
                            else {
                                // Send a request to the parent block for inserting key/child pair

                                let remote_parent_id = leaf_block.get_parent_id();
                                let providers = self.get_providers(remote_parent_id.to_string()).await;

                                if providers.is_empty() {
                                    let lease_response = LeaseResponse::LeaseFail;
                                    let serialized_response = serde_json::to_string(&lease_response).unwrap();
                                    self.respond_lease(serialized_response, channel).await;
                                    Ok(())
                                } else {
                                    let update_parent_requests = providers.into_iter().map(|peer_id| {
                                        let mut network_client = self.clone();
                                        let new_block_id = new_block.get_block_id();
                                        tokio::spawn(async move { network_client.request_update_parent(peer_id, divider_key, new_block_id, remote_parent_id).await }.boxed())
                                    });
    
                                    // As long as there is a response, continue
                                    let _response = futures::future::select_ok(update_parent_requests)
                                        .await
                                        .map_err(|_| "None of the providers returned.")?
                                        .0;
                                    
                                    let lease_response = LeaseResponse::LeaseSuccess;
                                    let serialized_response = serde_json::to_string(&lease_response).unwrap();
                                    self.respond_lease(serialized_response, channel).await;
                                    Ok(())
                                }                                
                            }
                        },
                    }
                },
                BPNode::LocalSearchResult::RemoteBlock(remote_block_id) => {
                    let lease_response = LeaseResponse::LeaseContinuation(remote_block_id);
                    let serialized_response = serde_json::to_string(&lease_response).unwrap();
                    self.respond_lease(serialized_response, channel).await;
                    Ok(())
                },
                BPNode::LocalSearchResult::UnavailableBlock(unavailable_block_id) => {
                    // Will be implemented after gossipsub
                    Ok(())
                }
            }
        }
    
        pub async fn handle_request_update_parent(&mut self, divider_key: Key, new_block_id: BlockId, parent_id: BlockId, block_map: &mut BlockMap, channel: ResponseChannel<GenericResponse>)-> Result<(), Box<dyn Error>> {
            let parent_block = block_map.get_mut_block(parent_id);
            let insert_result = parent_block.insert_child(divider_key, new_block_id);
            
            match insert_result {
                BPNode::InternalInsertResult::Completed() => {
                    let insertion_response = InsertionResponse::InsertSuccess;
                    let serialized_response = serde_json::to_string(&insertion_response).unwrap();
                    self.respond_update_parent(serialized_response, channel).await; // there is no need to await. Find a way to just respond w the lease
                    Ok(())
                },
                BPNode::InternalInsertResult::SplitRequest() => {
                    let (mut new_block, new_divider_key) = parent_block.split_internal_block();
                    new_block.set_right_block(parent_block.get_right_block());
                    parent_block.set_right_block(new_block.assign_random_id());
                    parent_block.insert_child(divider_key, new_block_id);

                    if parent_block.get_parent_id() == 0 {
                        // Create a new parent and assign the keys from splitted blocks
                        let mut new_parent_block = Block::new();
                        new_parent_block.create_new_parent(parent_block, &new_divider_key, &mut new_block);
                                
                        block_map.update_root(new_parent_block.get_block_id());
                        ////////////////////////////////////////
                        // IMPLEMENT LOAD BALANCING HERE
                        // IF TOO MANY BLOKCS ARE IN THE MAP
                        // SEND THE BLOCK TO THE CLOSEST NODE
                        /////////////////////////////////////////
                        

                        
                        block_map.insert(new_block);
                        block_map.insert(new_parent_block);

                        let insertion_response = InsertionResponse::InsertSuccess;
                        let serialized_response = serde_json::to_string(&insertion_response).unwrap();
                        self.respond_update_parent(serialized_response, channel).await; // there is no need to await. Find a way to just respond w the lease
                        Ok(())
                    }
                    else {
                        // Send a request to the parent block for inserting key/child pair

                        let remote_parent_id = parent_block.get_parent_id();
                        let providers = self.get_providers(remote_parent_id.to_string()).await;

                        if providers.is_empty() {
                            let insertion_response = InsertionResponse::InsertFail;
                            let serialized_response = serde_json::to_string(&insertion_response).unwrap();
                            self.respond_update_parent(serialized_response, channel).await;
                            Ok(())
                        } else {
                            // Update the parent's block
                        let update_parent_requests = providers.into_iter().map(|peer_id| {
                            let mut network_client = self.clone();
                            let new_block_id = new_block.get_block_id();
                            tokio::spawn(async move { network_client.request_update_parent(peer_id, divider_key, new_block_id, remote_parent_id).await }.boxed())
                        });

                        // As long as there is a response, continue
                        let _response = futures::future::select_ok(update_parent_requests)
                            .await
                            .map_err(|_| "None of the providers returned.")?
                            .0;
                        
                        let insertion_response = InsertionResponse::InsertSuccess;
                        let serialized_response = serde_json::to_string(&insertion_response).unwrap();
                        self.respond_update_parent(serialized_response, channel).await; // there is no need to await. Find a way to just respond w the lease
                        Ok(())
                        }
                    }
                },
            }
        }

        pub async fn handle_request_blockmap_size(&mut self, block_map: &mut BlockMap, sender_id: PeerId, channel: ResponseChannel<GenericResponse>) -> Result<(), Box<dyn Error>> {
            let blockmap_size = block_map.get_size();
            self.respond_blockmap_size(blockmap_size, sender_id, channel).await;
            Ok(())
        }

        pub async fn request_blockmap_size(&mut self, provider_id: PeerId) -> Result<String, Box<dyn Error + Send>> {
            let (sender, receiver) = oneshot::channel();
            self.sender
                .send(Command::RequestBlockmapSize {
                    provider_id,
                    sender,
                })
                .await
                .expect("Command receiver not to be dropped.");
            receiver.await.expect("Sender not be dropped.")
        }
        pub async fn respond_blockmap_size(&mut self, blockmap_size: usize, sender_id: PeerId, channel: ResponseChannel<GenericResponse>) {
            self.sender
                .send(Command::RespondBlockmapSize {  blockmap_size, sender_id, channel,   })
                .await
                .expect("Command receiver not to be dropped.");
        }

        pub async fn request_block_migration(&mut self,  block: Block, target_id: PeerId) {
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
                EitherError<EitherError<EitherError<ConnectionHandlerUpgrErr<io::Error>, io::Error>,void::Void>, GossipsubHandlerError>,
            >,
        ) {
            match event {
                SwarmEvent::Behaviour(ComposedEvent::Mdns(
                    MdnsEvent::Discovered(discovered_list))) => {
                    for (peer, addr) in discovered_list {
                        self
                            .swarm.behaviour_mut()
                            .kademlia
                            .add_address(&peer, addr);
                        self
                            .swarm
                            .behaviour_mut()
                            .gossipsub
                            .add_explicit_peer(&peer);
                        println!("added: {:?}", peer);
                    }
                }
                SwarmEvent::Behaviour(ComposedEvent::Mdns(
                    MdnsEvent::Expired(expired_list))) => {
                    for (peer, _addr) in expired_list {
                        if !self.swarm.behaviour_mut().mdns.has_node(&peer) {
                            self
                                .swarm
                                .behaviour_mut()
                                .kademlia
                                .remove_peer(&peer);
                            self
                                .swarm
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
                    println!("Got message: {} with id: {} from peer: {:?}", String::from_utf8_lossy(&message.data), id, peer_id);
                    
                },
                // SwarmEvent::Behaviour(ComposedEvent::Gossipsub(_)) => {}
                SwarmEvent::Behaviour(ComposedEvent::Kademlia(
                    KademliaEvent::OutboundQueryCompleted {
                        id,
                        result: QueryResult::StartProviding(_),
                        ..
                    },
                )) => {
                    println!("{:?}",self.pending_start_providing);
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
                Command::GetProviders { searched_alias, sender } => {
                    let query_id = self
                        .swarm
                        .behaviour_mut()
                        .kademlia
                        .get_providers(searched_alias.into_bytes().into());
                    self.pending_get_providers.insert(query_id, sender);
                }
                Command::RequestLease { provider_id, sender, key, requester_id, entry } => {
                    let lease_request: IncomingRequest = IncomingRequest::RequestLease(requester_id, key, entry);
                    let serialize_request =  serde_json::to_string(&lease_request).unwrap();
                    let request_id = self
                        .swarm
                        .behaviour_mut()
                        .request_response
                        .send_request(&provider_id, GenericRequest(serialize_request));
                    self.pending_generic_request.insert(request_id, sender);
                }
                Command::RespondLease {lease_response, channel } => { // Done
                    self.swarm
                        .behaviour_mut()
                        .request_response
                        .send_response(channel, GenericResponse(lease_response))
                        .expect("Connection to peer to be still open.");
                }
                Command::RequestBlockmapSize { provider_id, sender } => { // Done
                    let blockmap_size_request: IncomingRequest = IncomingRequest::RequestBlockmapSize(provider_id);
                    let serialize_request =  serde_json::to_string(&blockmap_size_request).unwrap();
                    let request_id = self
                        .swarm
                        .behaviour_mut()
                        .request_response
                        .send_request(&provider_id, GenericRequest(serialize_request));
                    self.pending_generic_request.insert(request_id, sender);
                }
                Command::RespondBlockmapSize { blockmap_size, sender_id, channel} => {
                    let blockmap_size_respond = (blockmap_size, sender_id);
                    let serialize_request =  serde_json::to_string(&blockmap_size_respond).unwrap();
                    self.swarm
                        .behaviour_mut()
                        .request_response
                        .send_response(channel, GenericResponse(serialize_request))
                        .expect("Connection to peer to be still open.");
                }
                Command::RequestUpdateParent { provider_id, sender, divider_key, new_block_id, parent_id} => {
                    let update_parent_request: IncomingRequest = IncomingRequest::RequestUpdateParent(divider_key, new_block_id, parent_id);
                    let serialize_request =  serde_json::to_string(&update_parent_request).unwrap();
                    let request_id = self
                        .swarm
                        .behaviour_mut()
                        .request_response
                        .send_request(&provider_id, GenericRequest(serialize_request));
                    self.pending_generic_request.insert(request_id, sender);
                },
                Command::RespondUpdateParent {update_parent_respond, channel } => { // Done
                    self.swarm
                        .behaviour_mut()
                        .request_response
                        .send_response(channel, GenericResponse(update_parent_respond))
                        .expect("Connection to peer to be still open.");
                }

                Command::RequestRemoteLeaseSearch {provider_id, sender, key, requester_id, entry,block_id} => {
                    let lease_request: IncomingRequest = IncomingRequest::RequestRemoteSearch(requester_id, key, block_id, entry);
                    let serialize_request =  serde_json::to_string(&lease_request).unwrap();
                    let request_id = self
                        .swarm
                        .behaviour_mut()
                        .request_response
                        .send_request(&provider_id, GenericRequest(serialize_request));
                    self.pending_generic_request.insert(request_id, sender);
                },


                // Command::RespondLeaseChange {lease_change, channel } => { // Done
                //     self.swarm
                //         .behaviour_mut()
                //         .request_response
                //         .send_response(channel, GenericResponse(lease_change))
                //         .expect("Connection to peer to be still open.");
                // }

                Command::StartProviding {network_alias, sender} => { // Done
                    let query_id = self
                        .swarm
                        .behaviour_mut()
                        .kademlia
                        .start_providing(network_alias.into_bytes().into())
                        .expect("No store error.");
                    self.pending_start_providing.insert(query_id,sender);
                },
                Command::PublishTopic {topic, sender} => { 
                    let _query_id = self
                        .swarm
                        .behaviour_mut()
                        .gossipsub
                        .publish(topic, "blocks")
                        .expect("Publishing");
                },
                
                Command::RequestBlockMigration { block, target_id, sender } => {
                    let block_migration_request: IncomingRequest = IncomingRequest::RequestBlockMigration(block);
                    let serialize_request =  serde_json::to_string(&block_migration_request).unwrap();
                    let request_id = self
                        .swarm
                        .behaviour_mut()
                        .request_response
                        .send_request(&target_id, GenericRequest(serialize_request));
                    self.pending_generic_request.insert(request_id, sender);
                }

                Command::RespondBlockMigration {channel} => {
                    let block_migration_response = String::from("Success");
                    let serialize_request =  serde_json::to_string(&block_migration_response).unwrap();
                    self.swarm
                        .behaviour_mut()
                        .request_response
                        .send_response(channel, GenericResponse(serialize_request))
                        .expect("Connection to peer to be still open.");
                }
            }
        }
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
        PublishTopic {
            topic: Topic,
            sender: oneshot::Sender<()>,
        },
        RequestBlockmapSize {
            provider_id: PeerId,
            sender: oneshot::Sender<Result<String, Box<dyn Error + Send>>>,
        },
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
        }

    }

    #[derive(Debug)]
    pub enum Event {
        InboundRequest {
            incoming_request: String,
            channel: ResponseChannel<GenericResponse>,
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
}
