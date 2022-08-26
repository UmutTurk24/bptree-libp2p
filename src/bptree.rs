// Author: Umut Turk
// B+Tree implementation for Rust
// Designed to fit the needs of a distributed Libp2p Networking Lease Sharing
// Date: Summer 2022
// Project: Concurrent and Distributed B+Tree for Libp2p



use serde::{Serialize, Deserialize};
use libp2p::core::PeerId;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::ops::{DerefMut, Deref};
use rand::Rng;
use math::round;

// Sets the block-size for the Block
static BLOCK_SIZE: u8 = 11;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Block{ 
    block_id: BlockId,                // Block's assigned block id
    block_size: u8,                   // Size of the block
    keys: Vec<Key>,                   // Vector of keys (u64) for indexing
    children: Vec<BlockId>,           // (only internal nodes) Vector of children (u64) for pointing to next block
    children_availability: Vec<bool>, // (only internal nodes) Vector of boolean for storing the availability of the link
    values: Vec<Entry>,               // (only leaf nodes) Vector of entries (peer_id, String) for given leases as entries
    right_block: BlockId,             // (only leaf nodes) Block id of the right block  
    parent: BlockId,                  // Block id of the parent
    availability: bool,                  // Block availability
}

/// Defines the Block traits for the application level
/// Allows creating, searching, and splitting a Block
impl Block{
    pub fn new () -> Self {
        return Self { 
            block_id: Default::default(),
            block_size: BLOCK_SIZE,
            keys: Default::default(),
            children: Default::default(),
            children_availability: Default::default(),
            values: Default::default(),
            right_block: Default::default(),
            parent: Default::default(),
            availability: true,
        }
    }
    /// Gets the peer_id of the client, serializes it with json, hashes the serialization, and then
    /// assigns the newly created id as the block_id
    /// 
    /// Returns an u64 that is used as the block_id
    pub fn set_block_id(&mut self, peer_id: &PeerId) -> BlockId {
        let mut hasher = DefaultHasher::new();
        let serialized_id = serde_json::to_string(&peer_id).unwrap();
        hasher.write(serialized_id.as_bytes());
        let fresh_id = hasher.finish();
        self.block_id = fresh_id;
        fresh_id
    }

    /// Returns the block_id of the block
    pub fn get_block_id(&self) -> BlockId {
        self.block_id
    }

    /// Searches for the block the new_key is in. Returns SearchResult: 
    /// - LeafBlock: block_id of the current block if the current block is a leaf block
    /// - NextBlock: block_id of the next_block that needs to be searched
    pub fn search_key(&self, new_key: &Key) -> SearchResult {

        // If a leaf block is reached, return the block
        if self.children.is_empty() {
            return SearchResult::LeafBlock(self.block_id);
        }

        // Internal node search operation
        else {
            let mut key_index: usize = 0;

            let index = self.keys.iter().position(|&index| index >= *new_key);
            if let Some(index) = index {
                key_index = index;
            } else {
                key_index = self.keys.len();
            }

            let next_block_id = &self.children[key_index];
            return SearchResult::NextBlock(*next_block_id);
        }
    }

    /// Insert an Key/Entry pair to the block. Returns LeafInsertResult
    /// - SplitRequest(entry): Block reached its limit and needs to split 
    /// - Completed(): Key/value pair is inserted successfully
    pub fn insert_entry(&mut self, new_key: Key, entry: Entry) -> LeafInsertResult {

            let mut key_index: usize = 0; 

            if self.keys.is_empty() {
                self.keys.push(new_key);
                self.values.push(entry);
                return LeafInsertResult::Completed();
            }

            if self.keys.len() > BLOCK_SIZE.into() {
                return LeafInsertResult::SplitRequest(entry);
            }

            for index in 0..self.keys.len() {
                let cur_key = self.keys[index];
                if new_key >= cur_key {
                    key_index = index;
                    break;
                }
            }

            self.values.insert(key_index, entry);
            self.keys.insert(key_index, new_key);
            return LeafInsertResult::Completed();
    }

    /// Splits a leaf block. 
    /// 
    /// Returns the new block and the divider key
    pub fn split_leaf_block(&mut self) -> (Block, Key) {
        let mut new_block = Block::new();
        new_block.assign_random_id();
        // self.right_block = new_block.block_id;
        let divider = round::floor((BLOCK_SIZE/2) as f64, 0);
        
        for index in (divider as u8)..BLOCK_SIZE {
            new_block.keys.push(self.keys[index as usize]);
            new_block.children.push(self.children[index as usize]);
            new_block.children_availability.push(self.children_availability[index as usize]);
        }

        for _ in (divider as u8)..BLOCK_SIZE {
            self.keys.pop();
            self.children.pop();
            self.children_availability.pop();
        }
        let divider_key = new_block.keys[0];
        (new_block, divider_key)
    }

    /// Assigns a random u64 to the block_id
    /// 
    /// Returns assigned u64
    pub fn assign_random_id(&mut self) -> u64 {
        let mut rng = rand::thread_rng();
        let num: u64 = rng.gen();
        self.block_id = num;
        num
    }

    /// Returns the block_id of the block that points to the right
    /// 
    /// Warning: Should only be used on leaf nodes.
    pub fn get_right_block(&self) -> BlockId{
        self.right_block
    }

    /// Sets the right block to the given right_block_id
    pub fn set_right_block(&mut self, right_block_id: BlockId) {
        self.right_block = right_block_id
    }


    /// Returns the block_id of the block that points to its parent
    pub fn get_parent_id(&self) -> BlockId {
        self.parent
    }

    /// Sets the parent block to the given parent_block_id
    pub fn set_parent(&mut self, parent_block_id: BlockId) {
        self.parent = parent_block_id;
    }
    /// Takes in the newly splitted blocks and their divider key as an input 
    /// 
    /// Updates the newly created parent block's internal nodes and assigns a block_id
    /// 
    /// Also updates the parent pointers of the child nodes 
    pub fn create_new_parent(&mut self, left_block: &mut Block, divider_key: &Key, right_block: &mut Block) {
        let parent_id = self.assign_random_id();
        left_block.set_parent(parent_id);
        right_block.set_parent(parent_id);
        self.insert_on_new_parent(&left_block.get_block_id(), divider_key, &right_block.get_block_id(), &left_block.get_availability(), &right_block.get_availability());
    }


    /// Inserts key/id pairs to the newly created parent
    pub fn insert_on_new_parent(&mut self, old_block_id: &BlockId, divider_key: &Key, new_block_id: &BlockId, left_block_availability: &bool, right_block_availability: &bool) {
        self.keys.push(*divider_key);

        self.children.push(*old_block_id);
        self.children.push(*new_block_id);

        self.children_availability.push(*left_block_availability);
        self.children_availability.push(*right_block_availability);
    }


    /// Inserts a key/block_id pair to the internal node
    /// 
    /// Returns InternalInsertResult,
    /// - SplitRequest(): Block reached its limit and needs to split 
    /// - Completed(): Key/value pair is inserted successfully
    pub fn insert_child(&mut self, new_key: Key, child_id: BlockId, child_availability: bool) -> InternalInsertResult {
        let mut key_index: usize = 0;

        // There will never be a case where this method is called on an empty inside block.
        // Only insert_on_new_parent is called on newly created parent blocks.

        // Check if the block is full
        if self.keys.len() > BLOCK_SIZE.into() {
            return InternalInsertResult::SplitRequest();
        }
        
        // 
        for key in 0..self.keys.len() {
            let cur_key = self.keys[key];
            if key >= cur_key.try_into().unwrap() {
                key_index = key;
                break;
            }
        }

        self.children.insert(key_index + 1, child_id); 
        self.children_availability.insert(key_index + 1, child_availability);
        self.keys.insert(key_index, new_key);
        return InternalInsertResult::Completed(); 
    }


    /// Splits an internal block. 
    /// 
    /// Returns the new block and the divider key
    pub fn split_internal_block(&mut self) -> (Block, u64) {
        let mut new_block = Block::new();
        // self.right_block = new_block.block_id;
        let divider = round::floor((BLOCK_SIZE/2) as f64, 0);
        
        for index in (divider as u8 + 1)..BLOCK_SIZE {
            new_block.keys.push(self.keys[index as usize]);
        }

        let divider_key = self.keys[divider as usize];
        for _ in (divider as u8)..BLOCK_SIZE {
            self.keys.pop();
        }
        for index in (divider as u8 + 1)..(BLOCK_SIZE+1) {
            new_block.children.push(self.children[index as usize]);
        }
        for _ in (divider as u8 + 1)..(BLOCK_SIZE) {
            self.children.pop();
        }
        (new_block, divider_key)
    }

    /// Returns a block's availability
    pub fn get_availability(&self) -> bool {
        self.availability
    }
}

/// Defines BlockMap traits for the application level
/// Allows storing, searching, and retrieving blocks within its hashmap
pub struct BlockMap {
    map: HashMap<BlockId, Block>,
    root_id: BlockId,
}

impl BlockMap {

    /// Create a new BlockMap with a block
    pub fn boot_new (peer_id: &PeerId) -> Self {
        let mut new_block = Block::new();
        let fresh_id = new_block.set_block_id(peer_id);
        let mut new_map: HashMap<BlockId, Block> = HashMap::new();
        new_map.insert(new_block.get_block_id(), new_block);
        
        return Self { 
            map: new_map,
            root_id: fresh_id,
        }
    }


    /// Initiates a local block search within the blockmap. Returns LocalSearchResult:
    /// - LeafBlock: block_id of the leaf node that is being found
    /// - RemoteBlock: block_id of the next block that key trails to
    pub fn local_search(&self, key: &Key, current_block_id: BlockId) -> LocalSearchResult {
        // Start the local search from the root
        let mut current_block = self.map.get(&current_block_id).unwrap();

        // Continue searching until the block is in another client
        // or a leaf block is reached

        loop {
            let search_result = current_block.search_key(key);

            match search_result {
                SearchResult::LeafBlock(leaf_block_id) => {
                    return LocalSearchResult::LeafBlock(leaf_block_id);
                },
                SearchResult::NextBlock(next_block_id) => {
                    let is_local = self.map.get(&next_block_id);

                    if let Some(local_block) = is_local {
                        current_block = local_block;
                        if !current_block.get_availability() {
                            return LocalSearchResult::UnavailableBlock(current_block.get_block_id());
                        }
                    } else {
                        return LocalSearchResult::RemoteBlock(next_block_id);
                    }
                },
            }
        }
    }


    /// Returns the root's block_id 
    pub fn get_root_id(&self) -> BlockId {
        self.root_id
    }

    /// Returns a mutable block from the map with the given block_id
    pub fn get_mut_block(&mut self, block_id: BlockId) -> &mut Block {
        self.map.get_mut(&block_id).unwrap()
    }


    /// Assigns a new block_id to the root
    pub fn update_root(&mut self, parent_id: BlockId) {
        self.root_id = parent_id;
    }

    /// Inserts new block_id/block to the map
    pub fn insert(&mut self, block: Block) {
        self.map.insert(block.get_block_id(), block);
    }

    /// Returns the size of the map
    pub fn get_size(&self) -> usize {
        self.map.len()
    }

    pub fn has_block(&self, block_id: BlockId) -> bool {
        self.map.contains_key(&block_id)
        
    }
    
}

pub enum LocalSearchResult {
    LeafBlock(BlockId),
    RemoteBlock(BlockId),
    UnavailableBlock(BlockId),
}
pub enum LeafInsertResult {
    Completed(),
    SplitRequest(Entry),
}

pub enum InternalInsertResult {
    Completed(),
    SplitRequest(),
}


#[derive(Debug,Serialize,Deserialize,Clone)]
pub enum SearchResult{
    LeafBlock(BlockId),
    NextBlock(BlockId),
}

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct Entry{ // 
    owner: PeerId, 
    data: Data,
}

impl Entry {
    pub fn new (peer_id: PeerId, data: Data) -> Self {
        return Self { 
            owner: peer_id, 
            data,
        }
    }
    pub fn shallow_new (peer_id: PeerId) -> Self {
        let empty_data: Data = Data{ data: "".to_string(), };
        return Self { 
            owner: peer_id, 
            data: empty_data,
        }
    }
}

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct Data {
    pub data: String,
}

pub type BlockId = u64;
pub type Key = u64; 

pub struct Child {
    block_id: BlockId,
    availability: bool,
}
