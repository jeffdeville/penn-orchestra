/*
 * Copyright (C) 2010 Trustees of the University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS of ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.cis.orchestra.reconciliation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Subtuple;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.iterators.ListIteratorResultIterator;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;
import edu.upenn.cis.orchestra.reconciliation.Db.InvalidUpdate;

public class HashTableStore extends DiffStore {
	public static class FlattenError extends SSException  {
		private static final long serialVersionUID = 1L;

		FlattenError(Exception e) {
			super(e);
		}
	}

	public static final Factory FACTORY = new Factory();

	public static class Factory implements StateStore.Factory {
		public StateStore getStateStore(AbstractPeerID pid, ISchemaIDBinding s, int lastTid) {
			return new HashTableStore(pid, s, lastTid);
		}

		public void serialize(Document doc, Element store) {
			store.setAttribute("type", "hash");
		}

		static public Factory deserialize(Element store) {
			return new Factory();
		}
	}
	// The current state of the database, one element for each relation
	// Mapping from key to StoreEntry
//	ArrayList<HashMap<Subtuple,StoreEntry>> state;
	private Map<Integer,HashMap<Subtuple,StoreEntry>> state;

	// The updates for each key, for each reconciliation; modifications
	// get stored twice (once for old value, once for the new) unless they have
	// the same key. There is one HashMap for each relation, and each HashMap contains
	// mapping from key to reconciliation to a list of relevant updates
	// TODO: fix this
//	ArrayList<HashMap<Subtuple,HashMap<Integer,List<Update>>>> updates;
	private Map<Integer,HashMap<Subtuple,HashMap<Integer,List<Update>>>> updates;

	// These will be flattened together when advanceRecno is called
	private ArrayList<Update> currRecnoUpdates;

	public HashTableStore(AbstractPeerID pid, ISchemaIDBinding schema, int lastTid) {
		super(pid, schema, lastTid);
//		final int numRel = schema.getNumRelations();
//		state = new ArrayList<HashMap<Subtuple,StoreEntry>>(numRel);
//		updates = new ArrayList<HashMap<Subtuple,HashMap<Integer,List<Update>>>>(numRel);
		state = new HashMap<Integer,HashMap<Subtuple,StoreEntry>>();
		updates = new HashMap<Integer,HashMap<Subtuple,HashMap<Integer,List<Update>>>>();
		
		
//		for (int i = 0; i < numRel; ++i) {
//			state.add(new HashMap<Subtuple,StoreEntry>());
//			updates.add(new HashMap<Subtuple,HashMap<Integer,List<Update>>>());
//		}
		currRecnoUpdates = new ArrayList<Update>();
	}
	
	private HashMap<Subtuple,HashMap<Integer,List<Update>>> getUpdatesFor(int relID) {
		HashMap<Subtuple,HashMap<Integer,List<Update>>> ret = updates.get(relID);
		
		if (ret == null) {
			ret = new HashMap<Subtuple,HashMap<Integer,List<Update>>>();
			updates.put(relID, ret);
		}
		
		return ret;
	}
	
	private HashMap<Subtuple,StoreEntry> getStateFor(int relID) {
		HashMap<Subtuple,StoreEntry> ret = state.get(relID);
		
		if (ret == null) {
			ret = new HashMap<Subtuple,StoreEntry>();
			state.put(relID, ret);
		}
		
		return ret;
	}

	void clearStateBeforeImpl(int recno) {
		for (HashMap<Subtuple,HashMap<Integer,List<Update>>> updatesForRelation : updates.values()) {
			for (HashMap<Integer,List<Update>> updatesForKey : updatesForRelation.values()) {
				for (int i : updatesForKey.keySet()) {
					if (i < recno) {
						updatesForKey.remove(i);
					}
				}
			}
		}
		updates.clear();
	}

	void addToUpdateList(int recno, Update u) throws UnknownTable {
		final int currRecno = getRecno();
		if (recno == currRecno) {
			currRecnoUpdates.add(u);
		} else {			
			HashMap<Subtuple,HashMap<Integer,List<Update>>> updatesForTable = getUpdatesFor(u.getRelationID());//updates.get(u.getRelationID());
			if (updatesForTable == null) {
				throw new UnknownTable(u.getRelationName());
			}
			ArrayList<Tuple> keys = new ArrayList<Tuple>(2);
			if (u.getOldVal() != null) {
				keys.add(u.getOldVal());
			}
			if (u.getNewVal() != null) {
				if (u.getOldVal() == null || (! u.getNewVal().sameKey(u.getOldVal()))) {
					keys.add(u.getNewVal());
				}
			}

			// Flatten update into existing updates

			for (Tuple key : keys) {
				HashMap<Integer,List<Update>> updatesForKey = updatesForTable.get(key);
				if (updatesForKey == null) {
					updatesForKey = new HashMap<Integer,List<Update>>();
					updatesForTable.put(key.getKeySubtuple(),updatesForKey);
				}
				List<Update> updateList = updatesForKey.get(recno);
				if (updateList == null) {
					updateList = new ArrayList<Update>(1);
					updatesForKey.put(recno,updateList);
					updateList.add(u.duplicate());
				} else {
					Update lastUpdate = updateList.get(updateList.size() - 1);
					if (u.isDeletion()) {
						updateList.remove(updateList.size() - 1);
						if (! lastUpdate.isInsertion()) {
							updateList.add(new Update(lastUpdate.getOldVal(), null));
						}
					} else if (u.isUpdate()) {
						updateList.remove(updateList.size() - 1);
						updateList.add(new Update(lastUpdate.getOldVal(), u.getNewVal()));
					} else if (u.isInsertion()) {
						updateList.add(u.duplicate());
					}
				}
			}
		}
	}

	List<Update> getUpdateList(Tuple key, int recno) throws UnknownTable, FlattenError {
		HashMap<Subtuple,HashMap<Integer,List<Update>>> updatesForRelation = getUpdatesFor(key.getRelationID());//updates.get(key.getRelationID());
		HashMap<Integer, List<Update>> updatesForKey = updatesForRelation.get(key.getKeySubtuple());

		ArrayList<Update> retval = new ArrayList<Update>();

		if (updatesForKey != null) {

			final int currRecno = getRecno();

			for (int i = recno; i < currRecno; ++i) {
				List<Update> updatesForRecno = updatesForKey.get(i);
				if (updatesForRecno != null) {
					retval.addAll(updatesForRecno);
				}
			}
		}

		try {
			for (Update u : Db.flatten(currRecnoUpdates)) {
				if ((u.getOldVal() != null && u.getOldVal().sameKey(key)) ||
						(u.getNewVal() != null && u.getNewVal().sameKey(key))) {
					// Update has a tuple with desired key
					retval.add(u.duplicate());
				}
			}
		} catch (Db.InconsistentUpdates iu) {
			throw new FlattenError(iu);
		} catch (InvalidUpdate e) {
			throw new FlattenError(e);
		}

		return retval;
	}

	public ResultIterator<Tuple> getStateIterator(String relname) {
//		Integer relnum = schema.getIDForName(relname);
		Integer relnum = schMap.getRelationNamed(relname).getRelationID();

		if (relnum == null) {
			throw new IllegalArgumentException("Request for unknown relation " + relname);
		}

		HashMap<Subtuple,StoreEntry> stateMap = getStateFor(relnum); 
		ArrayList<Tuple> tuples = new ArrayList<Tuple>(stateMap.size());//state.get(relnum).size());

		for (StoreEntry se : stateMap.values()) {
			tuples.add(se.value);
		}

		return new ListIteratorResultIterator<Tuple>(tuples.listIterator());
	}

	void recnoHasAdvanced() throws UnknownTable, FlattenError {
		try {
			ArrayList<Update> flattenedUpdates;
			try {
				flattenedUpdates = Db.flatten(currRecnoUpdates);
			} catch (InvalidUpdate e) {
				throw new FlattenError(e);
			}
			final int recno = getRecno() - 1;
			for (Update u : flattenedUpdates) {
				addToUpdateList(recno, u);
			}
			currRecnoUpdates.clear();
		} catch (Db.InconsistentUpdates iu) {
			throw new FlattenError(iu);
		}
	}

	StoreEntry getStoreEntry(Tuple t) throws UnknownTable {
		try {
			HashMap<Subtuple,StoreEntry> stateForRelation = getStateFor(t.getRelationID());//state.get(t.getRelationID());
			return stateForRelation.get(t.getKeySubtuple());
		} catch (IndexOutOfBoundsException ioob) {
			throw new UnknownTable(t.getRelationName());
		}
	}

	void setStoreEntry(Tuple t, StoreEntry se) throws UnknownTable {
		try {
			HashMap<Subtuple,StoreEntry> stateForRelation = getStateFor(t.getRelationID());//state.get(t.getRelationID());
			if (se == null) {
				stateForRelation.remove(t.getKeySubtuple());
			} else {
				stateForRelation.put(t.getKeySubtuple(), se);
			}
		} catch (IndexOutOfBoundsException ioob) {
			throw new UnknownTable(t.getRelationName());
		}
	}

	@Override
	public void close() {
		// Doesn't need to do anything
	}

	@Override
	public void reopen() {
	}

	@Override
	protected void resetDiffStore() {
		currRecnoUpdates.clear();

//		final int numRel = schema.getNumRelations();
//		state = new ArrayList<HashMap<Subtuple,StoreEntry>>(numRel);
//		updates = new ArrayList<HashMap<Subtuple,HashMap<Integer,List<Update>>>>(numRel);
		state.clear();
		updates.clear();
//		for (int i = 0; i < numRel; ++i) {
//			state.add(new HashMap<Subtuple,StoreEntry>());
//			updates.add(new HashMap<Subtuple,HashMap<Integer,List<Update>>>());
//		}
	}
}
