Orchestra uses a number of tables. These tables are created during migration and are automatically populated by Orchestra. The user only interacts with his or her original tables. For each base relation _Rel_ in the Orchestra system the following tables are created.

# Tables which constitute the state of an Orchestra peer #
These tables, along with _Rel_, constitute the peer's Orchestra instance.

**_Rel_`_`L**
> For <em>L</em>ocally added data. At the beginning of a publish/update exchange operation Orchestra determines which tuples have been added to _Rel_ since the last publish/update exchange. These new tuples will be added to _Rel_`_`L by update exchange.

**_Rel_`_`R**
> For local <em>R</em>ejections. These are tuples the peer has decided that it does not want to receive via update exchange. Their presence here prevents update exchange from adding them to _Rel_.

> If a tuple is deleted from _Rel_, that tuple will appear in _Rel_`_`R only if there is a mapping which could put it back into _Rel_.

**_Rel_`_`PREV**
> After a peer performs an update exchange, the contents of _Rel_ are copied into this table, so that the next time the peer publishes a diff can be done between _Rel_ and _Rel_`_`PREV so that any any local modifications captured.

# Tables used by the publish/update exchange operation #
These tables are used by Orchestra during a publish/update exchange operation. They are otherwise kept empty. Tuples placed in the table _RelX_`_`INS will be <em>Ins</em>erted into the table _RelX_ during publish/update exchange, while tuples placed in the table _RelX_`_`DEL will be <em>Del</em>eted from the table _RelX_.

**_Rel_`_`INS, _Rel_`_`L`_`INS, _Rel_`_`R`_`INS**
> Contain tuples that should be inserted from the corresponding non-"`_`INS" table. So, for example, _Rel_`_`INS contains the tuples that will be inserted into _Rel_. Putting data into these tables is one of the first steps of update exchange.

**_Rel_`_`DEL, _Rel_`_`L`_`DEL, _Rel_`_`R`_`DEL**
> Like `_`INS, but for deletions rather than insertions.

**_Rel_`_`NEW, _Rel_`_`L`_`NEW, _Rel_`_`R`_`NEW**
> The goal of update exchange is to essentially compute new versions of all relations (_Rel_, _Rel_`_`L, _Rel_`_`R), by propagating the data found in the `_`INS and `_`DEL relations. `_`NEW relations represent these new versions. We added these in order to be able to compute the new versions without necessarily replacing the old ones. At the end of the propagation, we replace all relations with their `_`NEW version.

## Tables specific to deletion propagation ##
Tuples that are only implied by data that is removed are deleted: if a tuple A is removed, then any tuple B which derived from A must also be removed, assuming there is no alternate derivation of B which does not depend on A.

For deletion propagation, the update exchange algorithm involves a step where we perform a derivation test, to determine if some tuples need to be deleted because there is no way to derive them from base tuples. The `_`INV (for 'inverse' or 'inverted') and `_`RCH (for 'reachable') relations are used by that test.

**_Rel_`_`INV, _Rel_`_`L`_`INV, _Rel_`_`R`_`INV**
> Initially all tuples to be tested (called "affected" tuples) are put in _Rel_`_`INV. Orchestra generates a datalog program that inserts all tuples involved in derivations of the affected tuples _Rel_`_`INV with the goal of finding the ones that are base tuples (which end up in _Rel_`_`L`_`INV relations).

**_Rel_`_`RCH**
> Then, another datalog program starts from _Rel_`_`L`_`INV, and computes a subset of the universal solution based on them. This subset is stored in _Rel_`_`RCH. Finally, we compare _Rel_`_`INV with _Rel_`_`RCH relation, and their difference is the set of tuples that need to be deleted from _Rel_. We place those tuples in _Rel_`_`DEL, and iterate the algorithm again, to propagate these "new" deletions further.