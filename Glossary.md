# Glossary #

**_Epoch Number_** global publish counter in the Update Store, similiar to a subversion revision number. Starts at -1, after the first publish we are at Epoch 0. Every epoch contains exactly one publish.

**_Labeled Null_** different form SQL null - we can join on labeled nulls. They can be used when inferring new values though mappings. _1_ indicates there is no labeled null. All other values than _1_ are Skolem values.

**_Migration_** a one time setup operation where a peer's existing database schema is modified to add in columns and [tables](OrchestraTables.md) for Orchestra's use.

**_Peer_** a participant in an Orchestra system.

**_Publish_** when a peer publishes it notifies the _Update Store_ of local deltas to its instance since the peer's last publish.

Note that only net changes are included. If for example a tuple is inserted then deleted, nothing would be sent to the update store. If a tuple was inserted, deleted, and inserted, only one insert would be sent to the update store. A fuller explanation is in progress [here](Publish.md).

A publish is currently always followed an update exchange.

**_Reconcile_** a process where a list of updates is applied to a _Peer_ with any conflicts being resolved using that Peer's trust conditions. This is not supported in the current release.

In some cases Reconcile is used as a synonym for _Update Exchange_.

**_Reconciliation Number_** it's an _update exchange_ counter, kept track of by the _Update Store_. Each _peer_ has its own sequence of reconciliation numbers.

**_State Store_** a peer's local store of some of the same information contained in the _update store_. It is redundant in the sense that it can be recreated from the update store. It lessens the peer's need to communicate with the update store.

**_Schema_** a definition, written in XML, of an Orchestra system. Sometimes called an "Orchestra schema" to differentiate from a database schema.

**_Update Exchange_** when a peer asks the update store for all updates (deltas) that have occurred in all peers since since its last update exchange and runs those deltas through its mappings, applying the results to its tables. After a peer publishes that peer then does an update exchange. A fuller explanation is in progress [here](UpdateExchange.md).

**_Update Store_** the hub of an Orchestra system. Primarily, the repository for each peer's epoch deltas. It is also used to store each peer's decisions about which non-local transactions it has accepted or rejected. Composed of several Berkley DB instances.