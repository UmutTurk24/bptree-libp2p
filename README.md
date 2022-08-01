# Lease Sharing Application

## Getting started

- There is only two files, main.rs and mod.rs under BPNode file. 

## mod.rs Structure

- It has the Block and BlockMap implementations

- Block struct is built based on the template given by Dr. Mendes during the research done Summer of 2022. 

- Block struct allows basic operations needed like splitting a block, inserting, and such other operations

- BlockMap struct on the other hand is a hash_map for Block struct but allows local_search within the hash_map, and keeps track of the root block.

- Each function has its own designated error type, defined under their enums. For instance LocalSearchResult, which is the return type of local_search(...) function has three defined types under:

```
 LocalSearchResult {
    LeafBlock(BlockId),
    RemoteBlock(BlockId),
    UnavailableBlock(BlockId),
}
```
I preferred this method instead of using Result<(), Err> structure because in the long run it easier to read the code and follow up with errors. 

## main.rs Structure

- Consists of 3 parts:

#### Main Function

- Parses the command arguments, initializes the network, and passively connects to known nodes with mdns. (lines 24-71)

- TO BE IMPLEMENTED: Spawns a thread and keeps track of the client with the least number of blocks (lines 73-104)

- Main loop: Gets command line input AND handles network events. 

- Currently, "root" and "getlease" are the only inputs available. "root" command boots the root and makes the client reachable for requests for lease. "getlease" command finds a root and requests a lease for their given key and value. (lines 111-198)

- Note that because gossipsub is not fully implemented, all the blocks are local. The code will not reach certain parts as commented in the code. But those parts of the code that can not be reached are still tested, and they work.

- Network requests are handled in the second half of the loop. In order to distinguish between requests, it is deserialized and then handled accordingly.

#### Network Mod

- Creates a new client, eventloop and the swarm (lines 362-423)

- Client implements all the methods that are being used in the main-loop and requests being made to the client.

- Handling requests are done in this part - under Client's implementation.

- Other than the addition of gossipsub, mdns, and other minor changes to the RequestResponse Codec, the code is very similar to the filesharing example (lines 900 - the end). 
## Repository Structure

#### Queued Actions 

- Since gossipsub is still not implemented (I should stop using it as an excuse), unavailable blocks should not accept the requests. These actions should be contained in a map (key: block_id, value: queued_actions). I decided to use trait objects for this. For every action queued, it should implement "Queueable" trait, which will allow each function to be easily executed after migration.

- For instance, SearchBlock struct will be received when there is an unavailable block while using local_search. SearchBlock struct will implement the Queueable method, which will save every data that is needed to execute the a request in another client. (lines 268-322)

## Maintainers


- Umut Turk (umturk@davidson.edu / umut1656@gmail.com)
