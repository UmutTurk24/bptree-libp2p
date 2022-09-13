use clap::Parser;
use futures::prelude::*;
use libp2p::core::{Multiaddr, PeerId};
use libp2p::gossipsub::Topic;
use libp2p::multiaddr::Protocol;
use rand::Rng;
use std::collections::HashMap;
use tokio::io::AsyncBufReadExt;
use tokio::spawn;
// use std::default::default;
use std::error::Error;

mod bptree;
mod events;
mod gossippoll;
mod migration;
mod network;
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

    let (mut network_client, mut network_events, network_event_loop, network_client_id) = network::new(secret_key_seed).await?;

    // Spawn the network task for it to run in the background.
    spawn(network_event_loop.run());

    // In case a listen address was provided use it, otherwise listen on any address.
    match listen_address {
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
    let mut block_map = bptree::BlockMap::boot_new(&network_client_id);
    let mut lease_map: HashMap<bptree::Key, bptree::Entry> = HashMap::new();

    let mut queued_actions = migration::QueuedActions::new();

    let mut publish_topic = gossippoll::topic_publisher(10000).await;
    let migration = Topic::new("blockmap");

    /////////////////////////////////////////
    //  IMPLEMENT A ROUND-ROBIN SYSTEM    /// 
    //  FOR FINDING TARGET PEERS          ///
    //  FROM THE GOSSIP-SUB NETWORK       ///
    /////////////////////////////////////////
    
    let mut target_peer = migration::TargetPeer {
        peer_id: network_client_id,
        block_number: 0,
    };

    network_client.subscribe_topic(migration.clone()).await;

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
                            let placeholder_entry = bptree::Entry::shallow_new(network_client_id);
                            tokio::spawn(async move { network_client.request_lease(provider_id, random_key, network_client_id, placeholder_entry).await }.boxed())
                        });

                    println!("{:?}", requests);

                    let root_response = futures::future::select_ok(requests)
                        .await
                        .map_err(|_| "None of the providers returned.")?
                        .0
                        .unwrap();

                    let lease_response: network::LeaseResponse = serde_json::from_str(&root_response).unwrap();

                    match lease_response{
                            network::LeaseResponse::LeaseSuccess => {
                                let placeholder_entry = bptree::Entry::shallow_new(network_client_id);
                                lease_map.insert(random_key,placeholder_entry);
                                println!("Lease was inserted successfully!");
                            },
                            network::LeaseResponse::LeaseContinuation(mut next_block_id) => {
                                // CODE WON'T REACH HERE YET
                                // RIGHT NOW THIS WON'T WORK BECAUSE NEW BLOCKS ARE NOT BEING ADDED TO KADEMLIA
                                // NO SET PROVIDER

                                loop {
                                    let providers = network_client.get_providers(next_block_id.to_string()).await;

                                    let requests = providers.into_iter().map(|provider_id| {
                                        let mut network_client = network_client.clone();
                                        let placeholder_entry = bptree::Entry::shallow_new(network_client_id);
                                        tokio::spawn(async move { network_client.request_remote_lease_search(provider_id, random_key, network_client_id, placeholder_entry, next_block_id).await }.boxed())
                                    });

                                    let responses = futures::future::select_ok(requests)
                                        .await
                                        .map_err(|_| "None of the providers returned.")?
                                        .0
                                        .unwrap();

                                    let remote_search_response: network::LeaseResponse = serde_json::from_str(&responses).unwrap();

                                    match remote_search_response{
                                        network::LeaseResponse::LeaseSuccess => {
                                            let placeholder_entry = bptree::Entry::shallow_new(network_client_id);
                                            lease_map.insert(random_key,placeholder_entry);
                                            println!("Lease was inserted successfully!");
                                            break
                                        },
                                        network::LeaseResponse::LeaseContinuation(new_next_block_id) => { next_block_id = new_next_block_id},
                                        network::LeaseResponse::LeaseFail => {println!("Something went wrong, try again"); break},
                                    }
                                }
                            },
                            network::LeaseResponse::LeaseFail => {println!("Something went wrong, try again")},
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
                let deserealized_request: network::IncomingRequest = serde_json::from_str(&incoming_request).unwrap();

                match deserealized_request {
                    network::IncomingRequest::RequestLease(requester_id, requested_key, entry) => {
                        events::handle_request_lease(&mut network_client, requester_id, requested_key, entry, target_peer.clone(), &mut queued_actions, &mut block_map, channel).await?;
                    },
                    network::IncomingRequest::RequestUpdateParent(divider_key, new_block_id, new_block_availability, parent_id) => {
                        events::handle_request_update_parent(&mut network_client, divider_key, new_block_id, new_block_availability, parent_id, target_peer.clone(), &mut block_map, channel).await?;
                    },
                    network::IncomingRequest::RequestRemoteSearch(requester_id, requested_key, block_id, entry) => {
                        events::handle_request_remote_lease_search(&mut network_client, requester_id, requested_key, block_id, entry, target_peer.clone(), &mut queued_actions, &mut block_map, channel).await?;
                    },
                    network::IncomingRequest::RequestBlockMigration(block) => {
                        events::handle_request_block_migration(&mut network_client, block, &mut block_map, channel).await?;
                    },
                }
            }
            Some(network::Event::GossibsubRequest {incoming_message, incoming_peer_id}) => {
                let size: u64 = std::str::from_utf8(&incoming_message.data).unwrap().parse().unwrap();
                if target_peer.block_number < size {
                    target_peer.block_number = size;
                    target_peer.peer_id = incoming_peer_id;
                }
            }
        },
        publish_event = publish_topic.next() => match publish_event {
                Some(message_code) => {
                    if message_code == 0 {
                        let current_size = block_map.get_size();
                        network_client.publish_topic(migration.clone(), current_size.to_string()).await;
                    }
                },
                None => {},
            }
        }
    }
    Ok(())
}

#[derive(Parser, Debug)]
#[clap(name = "libp2p file sharing example")]
struct Opt {
    /// Fixed value to generate deterministic peer ID.
    #[clap(long)]
    secret_key_seed: Option<u8>,
}
