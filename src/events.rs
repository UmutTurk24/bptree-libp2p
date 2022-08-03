
use std::error::Error;
use futures::prelude::*;
use libp2p::PeerId;
use libp2p::request_response::ResponseChannel;

use crate::network::*;
use crate::bptree::*;


pub async fn handle_request_lease(client: &mut Client, _requester_id: PeerId, requested_key: Key, entry: Entry, block_map: &mut BlockMap, channel: ResponseChannel<GenericResponse>) -> Result<(), Box<dyn Error>> {
    
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
                            client.respond_lease(serialized_response, channel).await;
                            Ok(())
                        }
                        else {
                            // Send a request to the parent block for inserting key/child pair

                            let remote_parent_id = leaf_block.get_parent_id();
                            let providers = client.get_providers(remote_parent_id.to_string()).await;

                            if providers.is_empty() {
                                let lease_response = LeaseResponse::LeaseFail;
                                let serialized_response = serde_json::to_string(&lease_response).unwrap();
                                client.respond_lease(serialized_response, channel).await;
                                Ok(())
                            } else {
                                // Update the parent's block
                            let update_parent_requests = providers.into_iter().map(|peer_id| {
                                let mut network_client = client.clone();
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
                            client.respond_lease(serialized_response, channel).await;
                            Ok(())
                            }
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

pub async fn handle_request_remote_lease_search(client: &mut Client, _requester_id: PeerId, requested_key: Key, current_block_id: BlockId, entry: Entry, block_map: &mut BlockMap, channel: ResponseChannel<GenericResponse>) -> Result<(), Box<dyn Error>> {
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
                        ////////////////////////////////////////
                        // IMPLEMENT LOAD BALANCING HERE
                        // IF TOO MANY BLOKCS ARE IN THE MAP
                        // SEND THE BLOCK TO THE CLOSEST NODE
                        /////////////////////////////////////////
                        
                        block_map.insert(new_block);
                        block_map.insert(parent_block);

                        let lease_response = LeaseResponse::LeaseSuccess;
                        let serialized_response = serde_json::to_string(&lease_response).unwrap();
                        client.respond_lease(serialized_response, channel).await;
                        Ok(())
                    }
                    else {
                        // Send a request to the parent block for inserting key/child pair

                        let remote_parent_id = leaf_block.get_parent_id();
                        let providers = client.get_providers(remote_parent_id.to_string()).await;

                        if providers.is_empty() {
                            let lease_response = LeaseResponse::LeaseFail;
                            let serialized_response = serde_json::to_string(&lease_response).unwrap();
                            client.respond_lease(serialized_response, channel).await;
                            Ok(())
                        } else {
                            let update_parent_requests = providers.into_iter().map(|peer_id| {
                                let mut network_client = client.clone();
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
                            client.respond_lease(serialized_response, channel).await;
                            Ok(())
                        }                                
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

pub async fn handle_request_update_parent(client: &mut Client, divider_key: Key, new_block_id: BlockId, parent_id: BlockId, block_map: &mut BlockMap, channel: ResponseChannel<GenericResponse>)-> Result<(), Box<dyn Error>> {
    let parent_block = block_map.get_mut_block(parent_id);
    let insert_result = parent_block.insert_child(divider_key, new_block_id);
    
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
                client.respond_update_parent(serialized_response, channel).await; // there is no need to await. Find a way to just respond w the lease
                Ok(())
            }
            else {
                // Send a request to the parent block for inserting key/child pair

                let remote_parent_id = parent_block.get_parent_id();
                let providers = client.get_providers(remote_parent_id.to_string()).await;

                if providers.is_empty() {
                    let insertion_response = InsertionResponse::InsertFail;
                    let serialized_response = serde_json::to_string(&insertion_response).unwrap();
                    client.respond_update_parent(serialized_response, channel).await;
                    Ok(())
                } else {
                    // Update the parent's block
                let update_parent_requests = providers.into_iter().map(|peer_id| {
                    let mut network_client = client.clone();
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
                client.respond_update_parent(serialized_response, channel).await; // there is no need to await. Find a way to just respond w the lease
                Ok(())
                }
            }
        },
    }
}

pub async fn handle_request_blockmap_size(client: &mut Client, block_map: &mut BlockMap, sender_id: PeerId, channel: ResponseChannel<GenericResponse>) -> Result<(), Box<dyn Error>> {
    let blockmap_size = block_map.get_size();
    client.respond_blockmap_size(blockmap_size, sender_id, channel).await;
    Ok(())
}
