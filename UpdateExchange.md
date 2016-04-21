As explained [here](OrchestraTables.md), when Orchestra is first set up the peers' base schemas are expanded to include a number of auxiliary tables which are used during update exchange.

Update exchange at a peer _p_ starts with the the retrieval from the update store of all insertions and deletions which have been published by other peers in the time since _p_'s last publish/update exchange.

