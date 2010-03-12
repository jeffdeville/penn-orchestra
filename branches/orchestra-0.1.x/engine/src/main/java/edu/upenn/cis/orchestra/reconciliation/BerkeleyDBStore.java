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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.iterators.IteratorException;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;

public class BerkeleyDBStore extends DiffStore {
	public static class BDBStateStoreException extends SSException {
		private static final long serialVersionUID = 1L;

		BDBStateStoreException(DatabaseException dbe) {
			super(dbe);
		}
		
		BDBStateStoreException(String msg) {
			super(msg);
		}
	}

	public static class Factory implements StateStore.Factory {
		Environment e;
		String stateName;
		String updatesName;
		public Factory(Environment e, String stateName, String updatesName) {
			this.e = e;
			this.stateName = stateName;
			this.updatesName = updatesName;
		}
		public StateStore getStateStore(AbstractPeerID pid, ISchemaIDBinding s, int lastTid) throws BDBStateStoreException {
			try {
				return new BerkeleyDBStore(e, stateName, updatesName, pid, s, lastTid);
			} catch (DatabaseException dbe) {
				throw new BDBStateStoreException(dbe);
			}
		}
		
		public void serialize(Document doc, Element store) {
			store.setAttribute("type", "bdb");
			store.setAttribute("state", stateName);
			store.setAttribute("updates", updatesName);
			try {
				store.setAttribute("workdir", e.getHome().getPath());
			} catch (DatabaseException e) {
				assert(false);	// shouldn't happen
			}
		}
		
		static public Factory deserialize(Element store) throws DatabaseException {
			String stateName = store.getAttribute("state");
			String updatesName = store.getAttribute("updates");
			String workdir = store.getAttribute("workdir");
			File envHome = new File(workdir);
			if (!envHome.exists()) {
				envHome.mkdir();
			}
			EnvironmentConfig config = new EnvironmentConfig();
			config.setAllowCreate(true);
			Environment e = new Environment(envHome, config);
			return new Factory(e, stateName, updatesName);
		}
	}


	private Environment e;
	
	private String stateName;
	private String updatesName;
	// statesDb is a mapping from tuple key column bytes to byteified StoreEntry 
	// (relationId, primary key subtuple) --> StoreEntry
	private Database stateDb;
	// updatesDb is a mapping from tuple key column bytes, recno to two updates
	// (relationId, primary key subtuple, recno) --> (Update, Update)
	private Database updatesDb;
	public BerkeleyDBStore(Environment e, String stateName, String updatesName, AbstractPeerID pid, ISchemaIDBinding schema, int lastTid) throws DatabaseException {
		super(pid, schema, lastTid);

		this.e = e;
		this.stateName = stateName;
		this.updatesName = updatesName;
		DatabaseConfig dbc = new DatabaseConfig();
		dbc.setAllowCreate(true);
		stateDb = e.openDatabase(null, stateName, dbc);
		updatesDb = e.openDatabase(null, updatesName, dbc);
	}

	public void close() throws BDBStateStoreException {
		try {
			stateDb.close();
			updatesDb.close();
		} catch (DatabaseException de) {
			throw new BDBStateStoreException(de);
		}
		stateDb = null;
		updatesDb = null;
	}

	public void reopen() throws BDBStateStoreException {
		try {
			stateDb = e.openDatabase(null, updatesName, null);
			updatesDb = e.openDatabase(null, updatesName, null);
		} catch (DatabaseException de) {
			throw new BDBStateStoreException(de);
		}
	}

	@Override
	void clearStateBeforeImpl(int recno) throws BDBStateStoreException {
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry val = new DatabaseEntry();

		try {
			Cursor c = updatesDb.openCursor(null, null);
			OperationStatus os = c.getFirst(key, val, LockMode.DEFAULT);
			while (os != OperationStatus.NOTFOUND) {
				byte[] bytes = key.getData();
				final int bytesPerInt = IntType.bytesPerInt;
				int keyLength = IntType.getValFromBytes(bytes, 0);
				int recordRecno = IntType.getValFromBytes(bytes, bytesPerInt + keyLength);
				if (recordRecno < recno) {
					os = c.delete();
				}
				os = c.getNext(key, val, LockMode.DEFAULT);
			}
			c.close();
		} catch (DatabaseException de) {
			throw new BDBStateStoreException(de);
		}

	}

	@Override
	StoreEntry getStoreEntry(Tuple t) throws SSException {
		DatabaseEntry key = new DatabaseEntry(t.getKeyColumnBytes());
		DatabaseEntry val = new DatabaseEntry();
		try {
			if (stateDb.get(null, key, val, LockMode.DEFAULT) != OperationStatus.SUCCESS) {
				return null;
			}
		} catch (DatabaseException de) {
			throw new BDBStateStoreException(de);
		}

		byte[] data = val.getData();

		return StoreEntry.fromBytes(schMap, data, 0, data.length);
	}

	@Override
	void setStoreEntry(Tuple t, StoreEntry se) throws BDBStateStoreException {
		DatabaseEntry key = new DatabaseEntry(t.getKeyColumnBytes());
		try {
			if (se == null) {
				stateDb.delete(null, key);
			} else {
				DatabaseEntry val = new DatabaseEntry(se.getBytes());
				stateDb.put(null, key, val);
			}
		} catch (DatabaseException de) {
			throw new BDBStateStoreException(de);
		}
	}

	@Override
	void addToUpdateList(int recno, Update u) throws BDBStateStoreException {
		ByteBufferWriter bbw = new ByteBufferWriter();
		ArrayList<byte[]> keys = new ArrayList<byte[]>();
		if (u.isInsertion()) {
			bbw.addToBuffer(u.getNewVal().getKeyColumnBytes());
			bbw.addToBuffer(recno);
			keys.add(bbw.getByteArray());
		} else if (u.isDeletion()) {
			bbw.addToBuffer(u.getOldVal().getKeyColumnBytes());
			bbw.addToBuffer(recno);
			keys.add(bbw.getByteArray());
		} else if (u.isUpdate()) {
			bbw.addToBuffer(u.getOldVal().getKeyColumnBytes());
			bbw.addToBuffer(recno);
			keys.add(bbw.getByteArray());
			if (! u.getOldVal().sameKey(u.getNewVal())) {
				bbw.clear();
				bbw.addToBuffer(u.getNewVal().getKeyColumnBytes());
				bbw.addToBuffer(recno);
				keys.add(bbw.getByteArray());
			}
		}

		for (byte[] key : keys) {
			ArrayList<Update> updates  = new ArrayList<Update>(2);

			ByteBufferReader bbr = new ByteBufferReader(schMap);
			try {
				DatabaseEntry keyEntry = new DatabaseEntry(key);
				DatabaseEntry value = new DatabaseEntry();
				OperationStatus os = updatesDb.get(null, keyEntry, value, LockMode.DEFAULT);
				if (os == OperationStatus.NOTFOUND) {
					updates.add(u);
				} else {
					// os == OperationStatus.SUCCESS
					byte[] valBytes = value.getData();
					bbr.reset(valBytes, 0, valBytes.length);
					Update u1 = bbr.readUpdate();
					Update u2 = bbr.readUpdate();
					if (! bbr.hasFinished()) {
						throw new BDBStateStoreException("Value from database is too long");
					}
					if (u1 != null) {
						updates.add(u1);
					}
					if (u2 != null) {
						updates.add(u2);
					}
					// Flatten into existing updates as needed
					Update lastUpdate = updates.get(updates.size() - 1);
					if (u.isDeletion()) {
						updates.remove(updates.size() - 1);
						if (! lastUpdate.isInsertion()) {
							updates.add(new Update(lastUpdate.getOldVal(), null));
						}
					} else if (u.isUpdate()) {
						updates.remove(updates.size() - 1);
						updates.add(new Update(lastUpdate.getOldVal(), u.getNewVal()));
					} else if (u.isInsertion()) {
						updates.add(u);
					}
				}

				if (updates.size() > 2) {
					throw new BDBStateStoreException("Should never have more than two updates for a given key during a given reconciliation: " + updates);
				}

				if (updates.isEmpty()) {
					updatesDb.delete(null, keyEntry);
				} else {
					bbw.clear();
					bbw.addToBuffer(updates.get(0), Update.SerializationLevel.VALUES_ONLY);
					bbw.addToBuffer(updates.size() == 2 ? updates.get(1) : null, Update.SerializationLevel.VALUES_ONLY);
					value.setData(bbw.getByteArray());
					updatesDb.put(null, keyEntry, value);
				}
			} catch (DatabaseException de) {
				throw new BDBStateStoreException(de);
			}
		}
	}

	@Override
	List<Update> getUpdateList(Tuple t, int startRecno) throws BDBStateStoreException {
		ByteBufferWriter bbw = new ByteBufferWriter();
		ByteBufferReader bbr = new ByteBufferReader(schMap);
		bbw.addToBuffer(t.getKeyColumnBytes());
		bbw.addToBuffer(startRecno);
		byte[] keyBytes = bbw.getByteArray();

		List<Update> retval = new ArrayList<Update>();

		int recno = startRecno;

		int currRecno = getRecno();

		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry value = new DatabaseEntry();

		for ( ; ; ) {
			key.setData(keyBytes);
			try {
				OperationStatus os = updatesDb.get(null, key, value, LockMode.DEFAULT);
				if (os == OperationStatus.SUCCESS) {
					byte[] valBytes = value.getData();
					bbr.reset(valBytes, 0, valBytes.length);
					Update u1 = bbr.readUpdate();
					Update u2 = bbr.readUpdate();
					if (u1 == null) {
						throw new BDBStateStoreException("Entry in updates database should always contain at least one update");
					}
					retval.add(u1);
					if (u2 != null) {
						retval.add(u2);
					}
				}
			} catch (DatabaseException de) {
				throw new BDBStateStoreException(de);
			}


			if (recno == currRecno) {
				break;
			}
			++recno;
			// Update keyBytes by changing recno
			byte[] recnoBytes = IntType.getBytes(recno);
			for (int i = 0; i < recnoBytes.length; ++i) {
				keyBytes[keyBytes.length - recnoBytes.length + i] = recnoBytes[i];
			}
		}

		return retval;
	}

	@Override
	void recnoHasAdvanced() {
		// Doesn't need to do anything, since we flatten even the most
		// recent reconciliation as we go
	}

	public ResultIterator<Tuple> getStateIterator(String relname) throws BDBStateStoreException {
//		final int id = schMap.getIDForName(relname);
		final int id = schMap.getRelationNamed(relname).getRelationID();
		final DatabaseEntry key = new DatabaseEntry(IntType.getBytes(id));
		final DatabaseEntry val = new DatabaseEntry();


		try {

			return new ResultIterator<Tuple>() {
				Cursor c = stateDb.openCursor(null, null);
				boolean atEnd = false;
				{
					OperationStatus os = c.getSearchKeyRange(key, val, LockMode.DEFAULT);
					atEnd = (os == OperationStatus.NOTFOUND);
				}
				// Number of tuples known to exist for this relation
				int readCount = 0;
				// Tuple to be returned by call to next, indexed from 0
				int pos = 0;

				public void close() throws IteratorException {
					try {
						c.close();
					} catch (DatabaseException e) {
						throw new IteratorException("Could not close BerkeleyDB State Store iterator", e);
					}
					c = null;
				}

				public boolean hasNext() throws IteratorException {
					if (atEnd) {
						return false;
					}
					if (IntType.getValFromBytes(key.getData(), 0) != id) {
						return false;
					}
					return true;
				}

				public boolean hasPrev() throws IteratorException {
					return pos > 0;
				}

				public Tuple next() throws IteratorException, NoSuchElementException {
					if (! hasNext()) {
						throw new NoSuchElementException();
					}
					byte[] data = val.getData();
					Tuple retval;
					try {
						retval = StoreEntry.fromBytes(schMap, data, 0, data.length).value;
					} catch (SSException e) {
						throw new IteratorException(e);
					}
					OperationStatus os;
					try {
						os = c.getNext(key, val, LockMode.DEFAULT);
					} catch (DatabaseException e) {
						throw new IteratorException("Could not read from BerkeleyDB State Store iterator", e);
					}
					atEnd = (os == OperationStatus.NOTFOUND);
					++pos;
					if (pos > readCount) {
						readCount = pos;
					}
					return retval;
				}

				public Tuple prev() throws IteratorException, NoSuchElementException {
					if (! hasPrev()) {
						throw new NoSuchElementException();
					}
					if (atEnd) {
						// Cursor didn't advance even though we logically did,
						// and key and value are unchanged
						atEnd = false;
					} else {
						try {
							OperationStatus os = c.getPrev(key, val, LockMode.DEFAULT);
							if (os != OperationStatus.SUCCESS) {
								throw new IteratorException("Inconsistent state in BerkeleyDB State Store iterator");
							}
						} catch (DatabaseException e) {
							throw new IteratorException("Could not read from BerkeleyDB State Store iterator", e);
						}						
					}
					byte[] data = val.getData();
					Tuple retval;
					try {
						retval = StoreEntry.fromBytes(schMap, data, 0, data.length).value;
					} catch (SSException e) {
						throw new IteratorException(e);
					}
					--pos;
					return retval;
				}
			};
		} catch (DatabaseException de) {
			throw new BDBStateStoreException(de);
		}
	}

	@Override
	protected void resetDiffStore() throws SSException {
		close();
		try {
			e.truncateDatabase(null, stateName, false);
			e.truncateDatabase(null, updatesName, false);
		} catch (DatabaseException dbe) {
			throw new BDBStateStoreException(dbe);
		}
		reopen();
	}

}
