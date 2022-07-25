use serde::{Serialize, Deserialize};
use libp2p::core::PeerId;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use rand::Rng;
use math::round;

// Sets the block-size for the Block
static BLOCK_SIZE: u8 = 11;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Block{ 
    block_id: BlockId,          // Block's assigned block id
    block_size: u8,             // Size of the block
    keys: Vec<Key>,             // Vector of keys (u64) for indexing
    children: Vec<BlockId>,     // (only internal nodes) Vector of children (u64) for pointing to next block
    values: Vec<Entry>,         // (only leaf nodes) Vector of entries (peer_id, String) for given leases as entries
    right_block: BlockId,       // (only leaf nodes) Block id of the right block  
    parent: BlockId,            // Block id of the parent
}

/// Defines the Block traits for the application level
/// 
/// Allows creation and splitting a Block

impl Block{
    pub fn new () -> Self {
        return Self { 
            block_id: Default::default(),
            block_size: BLOCK_SIZE,
            keys: Default::default(),
            children: Default::default(),
            values: Default::default(),
            right_block: Default::default(),
            parent: Default::default(),
        }
    }
    /// Gets the peer_id of the client, serializes it with json, hashes the serialization, and then
    /// returns an u64 that is used as the block_id
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

    /// Searches for the block the key is in. Returns: 
    /// block_id of the current block if the current block is a leaf block
    /// Or, block_id of the next_block that needs to be searched
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
    /// Insert an Key/Entry pair to the block 
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

    pub fn split_leaf_block(&mut self) -> (Block, Key) {
        let mut new_block = Block::new();
        new_block.assign_random_id();
        // self.right_block = new_block.block_id;
        let divider = round::floor((BLOCK_SIZE/2) as f64, 0);
        
        for index in (divider as u8)..BLOCK_SIZE {
            new_block.keys.push(self.keys[index as usize]);
            new_block.children.push(self.children[index as usize]);
        }

        for _ in (divider as u8)..BLOCK_SIZE {
            self.keys.pop();
            self.children.pop();
        }
        let divider_key = new_block.keys[0];
        (new_block, divider_key)
    }

    pub fn assign_random_id(&mut self) -> u64 {
        let mut rng = rand::thread_rng();
        let num: u64 = rng.gen();
        self.block_id = num;
        num
    }

    pub fn get_right_block(&self) -> BlockId{
        self.right_block
    }

    pub fn set_right_block(&mut self, right_block_id: BlockId) {
        self.right_block = right_block_id
    }

    pub fn get_parent_id(&self) -> BlockId {
        self.parent
    }

    pub fn set_parent(&mut self, parent_block_id: BlockId) {
        self.parent = parent_block_id;
    }

    pub fn create_new_parent(&mut self, left_block: &mut Block, divider_key: &Key, right_block: &mut Block) {
        let parent_id = self.assign_random_id();
        left_block.set_parent(parent_id);
        right_block.set_parent(parent_id);
        self.insert_on_new_parent(left_block.get_block_id(), *divider_key, right_block.get_block_id());
    }

    pub fn insert_on_new_parent(&mut self, old_block_id: BlockId, divider_key: Key, new_block_id: BlockId) {
        self.keys.push(divider_key);
        self.children.push(old_block_id);
        self.children.push(new_block_id);
    }

    pub fn insert_child(&mut self, new_key: Key, child_id: BlockId) -> InternalInsertResult {
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

        self.children.insert(key_index + 1, child_id); // +1 bc by default there is a key in the first index already
        self.keys.insert(key_index, new_key);
        return InternalInsertResult::Completed(); 
    }

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
    

}

pub struct BlockMap {
    map: HashMap<BlockId, Block>,
    root_id: BlockId,
}

impl BlockMap {
    // pub fn new () -> Self {
    //     return Self { 
    //         map: Default::default(), 
    //         root_id: Default::default(),
    //     }
    // }

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
                    } else {
                        return LocalSearchResult::RemoteBlock(next_block_id);
                    }
                },
            }
        }
    }

    pub fn get_root_id(&self) -> BlockId {
        self.root_id
    }

    pub fn get_mut_block(&mut self, block_id: BlockId) -> &mut Block {
        self.map.get_mut(&block_id).unwrap()
    }

    pub fn update_root(&mut self, parent_id: BlockId) {
        self.root_id = parent_id;
    }

    pub fn insert(&mut self, block: Block) {
        self.map.insert(block.get_block_id(), block);
    }
    
}


pub enum LocalSearchResult {
    LeafBlock(BlockId),
    RemoteBlock(BlockId),
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
    // pub fn new (peer_id: PeerId, data: Data) -> Self {
    //     return Self { 
    //         owner: peer_id, 
    //         data: data,
    //     }
    // }
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


