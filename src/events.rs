
use std::error::Error;
use futures::prelude::*;
use libp2p::PeerId;
use libp2p::request_response::ResponseChannel;

use crate::network::*;
use crate::bptree::*;
use crate::migration::*;


pub async fn handle_request_lease(client: &mut Client, _requester_id: PeerId, requested_key: Key, entry: Entry, target_peer: TargetPeer, block_map: &mut BlockMap, channel: ResponseChannel<GenericResponse>) -> Result<(), Box<dyn Error>> {
    
    // Each root takes the requests for initializing a search in the map. Therefore the search needs to 
    // start from the root node of the block_map
    let search_result = block_map.local_search(&requested_key, block_map.get_root_id());

        match search_result {
            // Found a local leaf block
            LocalSearchResult::LeafBlock(leaf_block_id) => {
                let leaf_block = block_map.get_mut_block(leaf_block_id);                                        
                let insert_result = leaf_block.insert_entry(requested_key, entry);

                match insert_result {
                    LeafInsertResult::Completed() => {
                        let lease_response = LeaseResponse::LeaseSuccess;
                        let serialized_response = serde_json::to_string(&lease_response).unwrap();
                        client.respond_lease(serialized_response, channel).await; // there is no need to await. Find a way to just respond w the lease
                        Ok(())
                    },
                    LeafInsertResult::SplitRequest(entry) => {
                        // Perform the split for insertion
                        let (mut new_block, divider_key) = leaf_block.split_leaf_block();
                        new_block.set_right_block(leaf_block.get_right_block());
                        leaf_block.set_right_block(new_block.assign_random_id());
                        leaf_block.insert_entry(requested_key, entry);
                        
                        if leaf_block.get_parent_id() == 0 {
                            // Create a new parent and assign the keys from splitted blocks
                            // Add the parent block to the block map
                            let mut parent_block = Block::new();
                            parent_block.create_new_parent(leaf_block, &divider_key, &mut new_block);
                            block_map.update_root(parent_block.get_block_id());
                            block_map.insert(parent_block);

                            // Move the newly created block to another client
                            let mut network_client = client.clone(); 
                            tokio::spawn(async move { network_client.request_block_migration(new_block, target_peer.peer_id).await }.boxed());
                            
                            // Return a response to the client
                            let lease_response = LeaseResponse::LeaseSuccess;
                            let serialized_response = serde_json::to_string(&lease_response).unwrap();
                            client.respond_lease(serialized_response, channel).await;
                            Ok(())
                        } else {
                            // Send a request to the parent block for inserting key/child pair
                            let remote_parent_id = leaf_block.get_parent_id();
                            
                            if !block_map.has_block(remote_parent_id) {

                                let providers = client.get_providers(remote_parent_id.to_string()).await;

                                if providers.is_empty() {
                                    let lease_response = LeaseResponse::LeaseFail;
                                    let serialized_response = serde_json::to_string(&lease_response).unwrap();
                                    client.respond_lease(serialized_response, channel).await;
                                    return Ok(())
                                } else {
                                // Update the parent's block
                                let update_parent_requests = providers.into_iter().map(|peer_id| {
                                    let mut network_client = client.clone();
                                    let new_block_id = new_block.get_block_id();
                                    let new_block_availability = new_block.get_availability();
                                    tokio::spawn(async move { network_client.request_update_parent(peer_id, divider_key, new_block_id, new_block_availability, remote_parent_id).await }.boxed())
                                });

                                // As long as there is a response, continue
                                let _response = futures::future::select_ok(update_parent_requests)
                                    .await
                                    .map_err(|_| "None of the providers returned.")?
                                    .0
                                    .unwrap();
                            }
                            } else {
                                let parent_block = block_map.get_mut_block(remote_parent_id);
                                parent_block.insert_child(divider_key, new_block.get_block_id(), new_block.get_availability());
                                
                            }
                    
                            let lease_response = LeaseResponse::LeaseSuccess;
                            let serialized_response = serde_json::to_string(&lease_response).unwrap();
                            client.respond_lease(serialized_response, channel).await;
                            Ok(())
                            
                        }
                    },
                }
            },
            LocalSearchResult::RemoteBlock(remote_block_id) => {
                let lease_response = LeaseResponse::LeaseContinuation(remote_block_id);
                let serialized_response = serde_json::to_string(&lease_response).unwrap();
                client.respond_lease(serialized_response, channel).await;
                Ok(())
            },
            LocalSearchResult::UnavailableBlock(unavailable_block_id) => {
                // Will be completed after gossipsub is implemented
                
                Ok(())
            }
        }
}

pub async fn handle_request_remote_lease_search(client: &mut Client, _requester_id: PeerId, requested_key: Key, current_block_id: BlockId, entry: Entry, target_peer: TargetPeer, block_map: &mut BlockMap, channel: ResponseChannel<GenericResponse>) -> Result<(), Box<dyn Error>> {
    let search_result = block_map.local_search(&requested_key, current_block_id);
    match search_result {
        // Found a local leaf block
        LocalSearchResult::LeafBlock(leaf_block_id) => {
            let leaf_block = block_map.get_mut_block(leaf_block_id);                                        
            let insert_result = leaf_block.insert_entry(requested_key, entry);

            match insert_result {
                LeafInsertResult::Completed() => {
                    let lease_response = LeaseResponse::LeaseSuccess;
                    let serialized_response = serde_json::to_string(&lease_response).unwrap();
                    client.respond_lease(serialized_response, channel).await; // there is no need to await. Find a way to just respond w the lease
                    Ok(())
                },
                LeafInsertResult::SplitRequest(entry) => {
                    let (mut new_block, divider_key) = leaf_block.split_leaf_block();
                    new_block.set_right_block(leaf_block.get_right_block());
                    leaf_block.set_right_block(new_block.assign_random_id());
                    leaf_block.insert_entry(requested_key, entry);

                    if leaf_block.get_parent_id() == 0 {
                        // Create a new parent and assign the keys from splitted blocks
                        let mut parent_block = Block::new();
                        parent_block.create_new_parent(leaf_block, &divider_key, &mut new_block);
                        block_map.update_root(parent_block.get_block_id());
                        block_map.insert(parent_block);

                        // Move the newly created block to another client
                        let mut network_client = client.clone(); 
                        tokio::spawn(async move { network_client.request_block_migration(new_block, target_peer.peer_id).await }.boxed());

                        let lease_response = LeaseResponse::LeaseSuccess;
                        let serialized_response = serde_json::to_string(&lease_response).unwrap();
                        client.respond_lease(serialized_response, channel).await;
                        Ok(())
                    }
                    else {
                        // Send a request to the parent block for inserting key/child pair
                        let remote_parent_id = leaf_block.get_parent_id();
                            
                        if !block_map.has_block(remote_parent_id) {
                            let providers = client.get_providers(remote_parent_id.to_string()).await;

                            if providers.is_empty() {
                                let lease_response = LeaseResponse::LeaseFail;
                                let serialized_response = serde_json::to_string(&lease_response).unwrap();
                                client.respond_lease(serialized_response, channel).await;
                                return Ok(())
                            } else {
                            // Update the parent's block
                            let update_parent_requests = providers.into_iter().map(|peer_id| {
                                let mut network_client = client.clone();
                                let new_block_id = new_block.get_block_id();
                                let new_block_availability = new_block.get_availability();
                                tokio::spawn(async move { network_client.request_update_parent(peer_id, divider_key, new_block_id, new_block_availability, remote_parent_id).await }.boxed())
                            });

                            // As long as there is a response, continue
                            let _response = futures::future::select_ok(update_parent_requests)
                                .await
                                .map_err(|_| "None of the providers returned.")?
                                .0
                                .unwrap();
                            }
                        } else {
                            // Searched parent is in the block map, do a local insertion
                            let parent_block = block_map.get_mut_block(remote_parent_id);
                            parent_block.insert_child(divider_key, new_block.get_block_id(), new_block.get_availability());
                        }
                        
                        let lease_response = LeaseResponse::LeaseSuccess;
                        let serialized_response = serde_json::to_string(&lease_response).unwrap();
                        client.respond_lease(serialized_response, channel).await;
                        Ok(())                                  
                    }
                },
            }
        },
        LocalSearchResult::RemoteBlock(remote_block_id) => {
            let lease_response = LeaseResponse::LeaseContinuation(remote_block_id);
            let serialized_response = serde_json::to_string(&lease_response).unwrap();
            client.respond_lease(serialized_response, channel).await;
            Ok(())
        },
        LocalSearchResult::UnavailableBlock(unavailable_block_id) => {
            // Will be implemented after gossipsub
            Ok(())
        }
    }
}

pub async fn handle_request_update_parent(client: &mut Client, divider_key: Key, new_block_id: BlockId, new_block_availability: bool, parent_id: BlockId, target_peer: TargetPeer, block_map: &mut BlockMap, channel: ResponseChannel<GenericResponse>)-> Result<(), Box<dyn Error>> {
    let parent_block = block_map.get_mut_block(parent_id);
    let insert_result = parent_block.insert_child(divider_key, new_block_id, new_block_availability);
    
    match insert_result {
        InternalInsertResult::Completed() => {
            let insertion_response = InsertionResponse::InsertSuccess;
            let serialized_response = serde_json::to_string(&insertion_response).unwrap();
            client.respond_update_parent(serialized_response, channel).await; // there is no need to await. Find a way to just respond w the lease
            Ok(())
        },
        InternalInsertResult::SplitRequest() => {
            let (mut new_block, new_divider_key) = parent_block.split_internal_block();
            new_block.set_right_block(parent_block.get_right_block());
            parent_block.set_right_block(new_block.assign_random_id());
            parent_block.insert_child(divider_key, new_block_id, new_block_availability);

            if parent_block.get_parent_id() == 0 {
                // Create a new parent and assign the keys from splitted blocks
                let mut new_parent_block = Block::new();
                new_parent_block.create_new_parent(parent_block, &new_divider_key, &mut new_block);
                block_map.update_root(new_parent_block.get_block_id());
                block_map.insert(new_parent_block);

                // Move the newly created block to another client
                let mut network_client = client.clone(); 
                tokio::spawn(async move { network_client.request_block_migration(new_block, target_peer.peer_id).await }.boxed());

                let insertion_response = InsertionResponse::InsertSuccess;
                let serialized_response = serde_json::to_string(&insertion_response).unwrap();
                client.respond_update_parent(serialized_response, channel).await; // there is no need to await. Find a way to just respond w the lease
                Ok(())
            }
            else {
                // Send a request to the parent block for inserting key/child pair
                let remote_parent_id = parent_block.get_parent_id();
                            
                if !block_map.has_block(remote_parent_id) {
                    let providers = client.get_providers(remote_parent_id.to_string()).await;

                    if providers.is_empty() {
                        let insertion_response = InsertionResponse::InsertFail;
                        let serialized_response = serde_json::to_string(&insertion_response).unwrap();
                        client.respond_update_parent(serialized_response, channel).await;
                        return Ok(())
                    } else {
                    // Update the parent's block
                    let update_parent_requests = providers.into_iter().map(|peer_id| {
                        let mut network_client = client.clone();
                        let new_block_id = new_block.get_block_id();
                        let block_availability = new_block.get_availability();
                        tokio::spawn(async move { network_client.request_update_parent(peer_id, new_divider_key, new_block_id, new_block_availability, remote_parent_id).await }.boxed())
                    });

                    // As long as there is a response, continue
                    let _response = futures::future::select_ok(update_parent_requests)
                        .await
                        .map_err(|_| "None of the providers returned.")?
                        .0
                        .unwrap();
                    }
                } else {
                    // Searched parent is in the block map, do a local insertion
                    let parent_block = block_map.get_mut_block(remote_parent_id);
                    parent_block.insert_child(divider_key, new_block.get_block_id(), new_block.get_availability());
                }
                
                let insertion_response = InsertionResponse::InsertSuccess;
                let serialized_response = serde_json::to_string(&insertion_response).unwrap();
                client.respond_update_parent(serialized_response, channel).await; // there is no need to await. Find a way to just respond w the lease
                Ok(()) 
            }
        },
    }
}

pub async fn handle_request_block_migration(client: &mut Client, block: Block, block_map: &mut BlockMap, channel: ResponseChannel<GenericResponse>) -> Result<(), Box<dyn Error>> {
    // Set provider the new block's id
    client.start_providing(block.get_block_id().to_string()).await;
    
    // Add the incoming block to the block map
    block_map.insert(block);

    // Respond back to the client
    client.respond_block_migration(channel).await;
    Ok(())
}
