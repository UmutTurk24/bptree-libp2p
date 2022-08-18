use std::collections::HashMap;
use std::collections::VecDeque;

use libp2p::PeerId;
use crate::bptree;

struct QueueMap {
    map: HashMap<bptree::BlockId,VecDeque<Box<dyn Queueable>>>
}

impl QueueMap{
    pub fn new() -> Self{
        return Self { map: Default::default() }
    }
    pub fn add(&mut self, key: bptree::BlockId, queue: VecDeque<Box<dyn Queueable>>) {
        self.map.insert(key, queue);
    }

    pub fn remove(&mut self, key: bptree::BlockId) -> Option<VecDeque<Box<dyn Queueable>>>{
        self.map.remove(&key)
    }
    pub fn update(&mut self, key: bptree::BlockId, action: Box<dyn Queueable>) {
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

pub struct QueuedActions {
    pub queued_actions: VecDeque<Box<dyn Queueable>>,
}

impl QueuedActions {
    pub fn new () -> Self {
        return Self { queued_actions: Default::default() }
    }

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
    block_id: bptree::BlockId,
}
#[derive(Clone)]
pub struct TargetPeer {
    pub peer_id: PeerId,
    pub block_number: u64,
}