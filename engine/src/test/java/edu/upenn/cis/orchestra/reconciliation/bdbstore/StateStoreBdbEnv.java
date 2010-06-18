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

package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;

import edu.upenn.cis.orchestra.OrchestraUtil;
import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.StateStore.SSException;

/**
 * Represents a Berkeley database state store environment and format.
 * 
 * @author John Frommeyer
 * 
 */
public class StateStoreBdbEnv implements IBdbStoreEnvironment {

	/** The {@code Environment} for the databases. */
	private Environment env;

	/** The {@code Database}s contained in {@code env}. */
	private List<Database> myDbs = new ArrayList<Database>();

	/** The format of the database. */
	private final BdbEnvironment stateStoreFormat;
	private final static Method readInt;
	private final static Method readTuple;
	private final static Method readStoreEntry;
	private final static Method readUpdate;
	static {
		try {
			readInt = BdbEntryInfo.getByteBufferReaderMethod("readInt");
			readTuple = StateStoreBdbEnv.class.getMethod("keyTupleFromBytes",
					ByteBufferReader.class);
			readStoreEntry = StateStoreBdbEnv.class.getMethod(
					"storeEntryFromBytes", ByteBufferReader.class);

			readUpdate = BdbEntryInfo.getByteBufferReaderMethod("readUpdate");

		} catch (Throwable e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	/**
	 * DOCUMENT ME
	 * 
	 */
	private static BdbEnvironment initializeFormat(String envPrefix,
			String peerName) {
		Map<String, BdbDatabase> databases = OrchestraUtil.newHashMap();
		List<BdbEntryInfo> keyInfo = OrchestraUtil.newArrayList();
		List<BdbEntryInfo> dataInfo = OrchestraUtil.newArrayList();

		// updates:(relationId, primary key subtuple, recno) --> (Update,
		// Update)
		// keyInfo.add(new BdbEntryInfo("relationID", readInt, false));
		keyInfo.add(new BdbEntryInfo("key", readTuple, false));
		keyInfo.add(new BdbEntryInfo("recno", readInt, false));
		dataInfo.add(new BdbEntryInfo("update1", readUpdate, false));
		dataInfo.add(new BdbEntryInfo("update2", readUpdate, false));

		databases.put("updatesDb", new BdbDatabase("updatesDb", keyInfo,
				dataInfo));
		keyInfo.clear();
		dataInfo.clear();

		// state: (relationId, primary key subtuple) --> StoreEntry
		// keyInfo.add(new BdbEntryInfo("relationID", readInt, false));
		keyInfo.add(new BdbEntryInfo("key", readTuple, false));
		dataInfo.add(new BdbEntryInfo("storeEntry", readStoreEntry, false));
		databases.put("stateDb", new BdbDatabase("stateDb", keyInfo, dataInfo));

		keyInfo.clear();
		dataInfo.clear();

		BdbEnvironment format = new BdbEnvironment(envPrefix, peerName,
				databases);
		return format;
	}

	/**
	 * Clients may need a {@code SchemaIDBinding} for creating {@code
	 * ByteBufferReader}s.
	 */
	private final ISchemaIDBinding binding;

	StateStoreBdbEnv(String envPrefix, String peerName,
			ISchemaIDBinding schemaIDBinding)
			throws EnvironmentLockedException, DatabaseException {
		File envHome = new File(envPrefix + "_" + peerName);
		stateStoreFormat = initializeFormat(envPrefix, peerName);
		binding = schemaIDBinding;

		EnvironmentConfig myEnvConfig = new EnvironmentConfig();
		// myEnvConfig.setTransactional(true);

		if (envHome.exists()) {
			// Open the environment
			env = new Environment(envHome, myEnvConfig);
			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setAllowCreate(false);
			dbConfig.setReadOnly(true);

			List<String> dbNames = env.getDatabaseNames();
			for (String dbName : dbNames) {
				myDbs.add(env.openDatabase(null, dbName, dbConfig));

			}
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.bdbstore.IBdbStoreEnvironment#close()
	 */
	@Override
	public void close() {
		if (env != null) {
			try {
				for (Database db : myDbs) {
					db.close();
				}
				env.close();
			} catch (Exception e) {
				throw new IllegalStateException(
						"Error closing StateStoreBdbEnv.", e);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.bdbstore.IBdbStoreEnvironment#getDbs()
	 */
	@Override
	public List<Database> getDbs() {
		return myDbs;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.bdbstore.IBdbStoreEnvironment#getFormat()
	 */
	@Override
	public BdbEnvironment getFormat() {
		return stateStoreFormat;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.bdbstore.IBdbStoreEnvironment#getSchemaIDBinding()
	 */
	@Override
	public ISchemaIDBinding getSchemaIDBinding() {
		return binding;
	}

	/**
	 * Returns a string representation of the {@code StoreEntry} encoded in
	 * {@code bbr}.
	 * 
	 * @param bbr
	 * @return a string representation of the {@code StoreEntry} encoded in
	 *         {@code bbr}
	 * @throws SSException
	 */
	public static String storeEntryFromBytes(ByteBufferReader bbr)
			throws SSException {
		StringBuffer sb = new StringBuffer();
		Tuple value = bbr.readTuple();
		sb.append(value);
		sb.append(" {");
		int numTids = bbr.readInt();

		for (int i = 0; i < numTids; ++i) {
			sb.append(bbr.readTxnPeerID());
			sb.append(", ");
		}

		sb.append("}");
		if (!bbr.hasFinished()) {
			throw new SSException(
					"Data remaining in buffer after reading StoreEntry");
		}
		return sb.toString();
	}

	/**
	 * Returns a string representation of the key tuple encoded in {@code bbr}.
	 * 
	 * @param bbr
	 * @return a string representation of the key tuple encoded in {@code bbr}
	 */
	public static String keyTupleFromBytes(ByteBufferReader bbr) {

		StringBuffer sb = new StringBuffer();
		Relation r = bbr.readRelationFromId();
		if (r == null) {
			// updatesDb puts a record length in front of the relation ID.
			r = bbr.readRelationFromId();
		}
		sb.append(r.getName());
		sb.append("(");
		int numCols = r.getNumCols();
		SortedSet<Integer> keyCols = r.getKeyCols();
		for (int i = 0; i < numCols; i++) {
			if (keyCols.contains(Integer.valueOf(i))) {
				boolean labeledNull = bbr.readBoolean();
				if (labeledNull) {
					sb.append("LN(");
					sb.append(bbr.readInt());
					sb.append("), ");
				} else {
					Type t = r.getColType(i);
					int length = bbr.readInt();
					byte[] bytes = bbr.readByteArrayNoLength(length);
					sb.append(t.fromBytes(bytes));
					sb.append(", ");
				}
			}
		}
		sb.append(")");
		return sb.toString();
	}
}
