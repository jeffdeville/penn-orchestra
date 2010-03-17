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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dom4j.Document;
import org.dom4j.Element;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import edu.upenn.cis.orchestra.OrchestraUtil;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.LocalSchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.bdbstore.SchemaIDBinding.SchemaMap;

/**
 * Encapsulates some of the work needed to open a Berkeley database holding an
 * update store.
 * 
 * @author John Frommeyer
 * 
 */
public class UpdateStoreBdbEnv implements IBdbStoreEnvironment {

	/** The {@code Environment} for the databases. */
	private final Environment env;

	/** The {@code Database}s contained in {@code env}. */
	private final List<Database> myDbs = new ArrayList<Database>();

	/** The format of the database. */
	private final static BdbEnvironment updateStoreFormat;

	static {
		try {
			Method readPeerID = BdbEntryInfo
					.getByteBufferReaderMethod("readPeerID");
			Method readTxnPeerID = BdbEntryInfo
					.getByteBufferReaderMethod("readTxnPeerID");
			Method readBoolean = BdbEntryInfo
					.getByteBufferReaderMethod("readBoolean");
			Method readInt = BdbEntryInfo.getByteBufferReaderMethod("readInt");
			Method readUpdate = BdbEntryInfo
					.getByteBufferReaderMethod("readUpdate");

			Method txnPeerIdFromBytes = TxnPeerID.class.getMethod("fromBytes",
					byte[].class);
			Method abstractPeerIdFromBytes = AbstractPeerID.class.getMethod(
					"fromBytes", byte[].class);

			Map<String, BdbDatabase> databases = OrchestraUtil.newHashMap();
			List<BdbEntryInfo> keyInfo = OrchestraUtil.newArrayList();
			List<BdbEntryInfo> dataInfo = OrchestraUtil.newArrayList();

			// decisions: (peer, tid) -> (accepted[boolean], recno[int])
			keyInfo.add(new BdbEntryInfo("peerID", readPeerID, false));
			keyInfo.add(new BdbEntryInfo("txnPeerID", readTxnPeerID, false));
			dataInfo.add(new BdbEntryInfo("accepted", readBoolean, false));
			dataInfo.add(new BdbEntryInfo("recno", readInt, false));

			databases.put("decisions", new BdbDatabase("decisions", keyInfo,
					dataInfo));
			keyInfo.clear();
			dataInfo.clear();

			// "epochContents": epoch -> list of tids
			keyInfo.add(new BdbEntryInfo("epoch", readInt, false));
			dataInfo.add(new BdbEntryInfo("txnPeerID", readTxnPeerID, true));
			databases.put("epochContents", new BdbDatabase("epochContents",
					keyInfo, dataInfo));

			keyInfo.clear();
			dataInfo.clear();

			// "lastRecno": peer -> last reconciliation number
			keyInfo.add(new BdbEntryInfo("peerID", abstractPeerIdFromBytes,
					false));
			dataInfo.add(new BdbEntryInfo("recno", readInt, false));
			databases.put("lastRecno", new BdbDatabase("lastRecno", keyInfo,
					dataInfo));
			keyInfo.clear();
			dataInfo.clear();

			// "txns": tid -> transaction contents
			keyInfo
					.add(new BdbEntryInfo("txnPeerID", txnPeerIdFromBytes,
							false));
			dataInfo.add(new BdbEntryInfo("update", readUpdate, true));
			databases.put("txns", new BdbDatabase("txns", keyInfo, dataInfo));
			keyInfo.clear();
			dataInfo.clear();

			// "recnoEpochs": (peer, recno) -> epoch
			keyInfo.add(new BdbEntryInfo("peerID", readPeerID, false));
			keyInfo.add(new BdbEntryInfo("recno", readInt, false));
			dataInfo.add(new BdbEntryInfo("epoch", readInt, false));
			databases.put("recnoEpochs", new BdbDatabase("recnoEpochs",
					keyInfo, dataInfo));
			keyInfo.clear();
			dataInfo.clear();

			// "reconAcceptedTxns": (peer, recno) -> tid
			keyInfo.add(new BdbEntryInfo("peerID", readPeerID, false));
			keyInfo.add(new BdbEntryInfo("recno", readInt, false));
			dataInfo.add(new BdbEntryInfo("txnPeerID", txnPeerIdFromBytes,
					false));
			databases.put("reconAcceptedTxns", new BdbDatabase(
					"reconAcceptedTxns", keyInfo, dataInfo));
			keyInfo.clear();
			dataInfo.clear();

			// "reconRejectedTxns": (peer, recno) -> tid
			keyInfo.add(new BdbEntryInfo("peerID", readPeerID, false));
			keyInfo.add(new BdbEntryInfo("recno", readInt, false));
			dataInfo.add(new BdbEntryInfo("txnPeerID", txnPeerIdFromBytes,
					false));
			databases.put("reconRejectedTxns", new BdbDatabase(
					"reconRejectedTxns", keyInfo, dataInfo));
			keyInfo.clear();
			dataInfo.clear();

			updateStoreFormat = new BdbEnvironment("updateStore_env", databases);

		} catch (Throwable e) {
			throw new ExceptionInInitializerError(e);
		}
	}
	/**
	 * Clients may need a {@code SchemaIDBinding} for creating {@code
	 * ByteBufferReader}s.
	 */
	private final ISchemaIDBinding binding;

	/**
	 * Creates an {@code UpdateStoreBdbEnv}. Sets up {@code env} and opens all
	 * databases.
	 * 
	 * @param peerIDToSchema
	 * @throws Exception
	 */
	UpdateStoreBdbEnv(File envHome, String cdssName) throws Exception {
		EnvironmentConfig myEnvConfig = new EnvironmentConfig();
		myEnvConfig.setTransactional(true);

		// Open the environment
		env = new Environment(envHome, myEnvConfig);
		binding = initializeBinding(env, cdssName);

		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setAllowCreate(false);
		dbConfig.setReadOnly(true);

		List<String> dbNames = env.getDatabaseNames();
		for (String dbName : dbNames) {
			if (dbName.startsWith("recon")) {
				dbConfig.setSortedDuplicates(true);
			} else {
				dbConfig.setSortedDuplicates(false);
			}
			if (dbName.equals("schemaInfo")) {
				dbConfig.setTransactional(false);
			} else {
				dbConfig.setTransactional(true);
			}
			myDbs.add(env.openDatabase(null, dbName, dbConfig));

		}
	}

	private static ISchemaIDBinding initializeBinding(Environment e, String cdss)
			throws Exception {
		Database peerSchemaInfo = null;
		try {
			ByteBufferWriter bbw = new ByteBufferWriter();
			bbw.addToBuffer(cdss);

			DatabaseEntry key = new DatabaseEntry(bbw.getByteArray());

			DatabaseEntry data = new DatabaseEntry();
			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setAllowCreate(false);
			dbConfig.setReadOnly(true);
			dbConfig.setTransactional(false);
			peerSchemaInfo = e.openDatabase(null, "schemaInfo", dbConfig);

			OperationStatus os2 = peerSchemaInfo.get(null, key, data,
					LockMode.DEFAULT);

			if (os2 == OperationStatus.SUCCESS) {
				ByteArrayInputStream schMap = new ByteArrayInputStream(data
						.getData());

				ObjectInputStream os = new ObjectInputStream(schMap);

				SchemaMap map = (SchemaMap) os.readObject();
				return new LocalSchemaIDBinding(map._peerSchemaMap);
			}
			throw new IllegalStateException(
					"Could not find peer to schema map for CDSS: " + cdss);
		} finally {
			if (peerSchemaInfo != null) {
				peerSchemaInfo.close();
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.IBdbStoreEnvironment#getEnv()
	 */
	public Environment getEnv() {
		return env;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.IBdbStoreEnvironment#close()
	 */
	public void close() {
		if (env != null) {
			try {
				for (Database db : myDbs) {
					db.close();
				}
				env.close();
			} catch (Exception e) {
				throw new IllegalStateException(
						"Error closing UpdateStoreBdbEnv.", e);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.IBdbStoreEnvironment#getDbs()
	 */
	public List<Database> getDbs() {
		return myDbs;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.IBdbStoreEnvironment#getSchemaIDBinding()
	 */
	public ISchemaIDBinding getSchemaIDBinding() {
		return binding;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.IBdbStoreEnvironment#getFormat()
	 */
	public BdbEnvironment getFormat() {
		return updateStoreFormat;
	}
}

/**
 * A simple representation of a Berkeley database Environment.
 * 
 * @author John Frommeyer
 * 
 */
class BdbEnvironment {

	/** The name of the BDb environment. The relative directory name. */
	private String name;

	/** The directory which is the database environment. */
	private File environmentFile;

	/** Maps a database name to its {@code BdbDatabase} representation. */
	private Map<String, BdbDatabase> databases;

	/**
	 * Create a {@code BdbEnvironment}.
	 * 
	 * @param environmentName
	 * @param databaseMap
	 */
	BdbEnvironment(String environmentName, Map<String, BdbDatabase> databaseMap) {
		name = environmentName;
		environmentFile = new File(environmentName);
		databases = databaseMap;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(getEnvLocation().getAbsolutePath() + "\n");
		for (BdbDatabase db : databases.values()) {
			sb.append(db);
			sb.append("\n");
		}
		return sb.toString();
	}

	/**
	 * Returns the {@code BdbDatabase} corresponding to {@code dbName}.
	 * 
	 * @param dbName
	 * @return a {@code BdbDatabase}.
	 */
	BdbDatabase getDatabase(String dbName) {
		return databases.get(dbName);
	}

	/**
	 * Returns the names of all contained {@code BdbDatabase}s.
	 * 
	 * @return the names of all contained {@code BdbDatabase}s.
	 */
	Set<String> getDatabaseNames() {
		return databases.keySet();
	}

	/**
	 * Returns the {@code BdbEnvironment} represented by the XML {@code doc}.
	 * 
	 * @param doc
	 * @return the {@code BdbEnvironment} represented by the XML {@code doc}.
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 * @throws ClassNotFoundException
	 */

	static BdbEnvironment deserialize(Document doc) throws SecurityException,
			NoSuchMethodException, ClassNotFoundException {
		Map<String, BdbDatabase> dbs = OrchestraUtil.newHashMap();
		Element env = doc.getRootElement();
		@SuppressWarnings("unchecked")
		List<Element> dbElements = env.elements("database");
		for (Element db : dbElements) {
			BdbDatabase bdb = BdbDatabase.deserialize(db);
			dbs.put(bdb.getName(), bdb);
		}
		String envName = env.attributeValue("name");
		return new BdbEnvironment(envName, dbs);
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return the environmentFile
	 */
	public File getEnvLocation() {
		return environmentFile;
	}
}

/**
 * A simple representation of a Berkeley database.
 * 
 * @author John Frommeyer
 * 
 */
class BdbDatabase {

	/** How to decode the key. */
	private List<BdbEntryInfo> keyInfo;

	/** How to decode the data. */
	private List<BdbEntryInfo> dataInfo;

	/** The database name. */
	private String name;

	/**
	 * Create a {@code BdbDatabase}.
	 * 
	 * @param dbName
	 * 
	 * @param keyInfo1
	 * @param dataInfo1
	 */
	BdbDatabase(String dbName, List<BdbEntryInfo> keyInfoList,
			List<BdbEntryInfo> dataInfoList) {
		name = dbName;

		this.keyInfo = OrchestraUtil.newArrayList(keyInfoList);
		this.dataInfo = OrchestraUtil.newArrayList(dataInfoList);
	}

	/**
	 * Returns the name of the database.
	 * 
	 * @return the name of the database.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the list containing information about how the key should be
	 * decoded.
	 * 
	 * @return the list containing information about how the key should be
	 *         decoded.
	 */
	public List<BdbEntryInfo> getKeyInfo() {
		return Collections.unmodifiableList(keyInfo);
	}

	/**
	 * Returns the list containing information about how the data should be
	 * decoded.
	 * 
	 * @return the list containing information about how the data should be
	 *         decoded.
	 */
	public List<BdbEntryInfo> getDataInfo() {
		return Collections.unmodifiableList(dataInfo);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return name + "\n\tKey: " + keyInfo + "\n\tData: " + dataInfo;
	}

	/**
	 * Returns the {@code BdbDatabase} represented by the XML element {@code db}
	 * .
	 * 
	 * @param db
	 * @return the {@code BdbDatabase} represented by the XML element {@code db}
	 *         .
	 * 
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 * @throws ClassNotFoundException
	 */
	public static BdbDatabase deserialize(Element db) throws SecurityException,
			NoSuchMethodException, ClassNotFoundException {
		String dbName = db.attributeValue("name");
		List<BdbEntryInfo> key = OrchestraUtil.newArrayList();
		List<BdbEntryInfo> data = OrchestraUtil.newArrayList();
		@SuppressWarnings("unchecked")
		List<Element> keyMethods = db.element("key").elements("method");
		for (Element method : keyMethods) {
			key.add(BdbEntryInfo.deserialize(method));
		}
		@SuppressWarnings("unchecked")
		List<Element> dataMethods = db.element("data").elements("method");
		for (Element method : dataMethods) {
			data.add(BdbEntryInfo.deserialize(method));
		}
		return new BdbDatabase(dbName, key, data);
	}

}

/**
 * Indicates how a (piece of) a Berkeley database entry should be decoded from
 * its {@code byte[]} form.
 * 
 * @author John Frommeyer
 * 
 */
class BdbEntryInfo {

	/** The method to use to decode (a piece of) the key. */
	private Method entryReader;

	/**
	 * Indicates that {@code keyReader} should be repeatedly applied until the
	 * entry has been fully processed.
	 */
	private boolean repeater;

	/** A tag for this entry. */
	private String entryTag;

	/** A count for {@code repeater}s. */
	private int count = 0;

	/**
	 * Creates an {@code EntryInfo}.
	 * 
	 * @param entryTag
	 * @param entryReader either one of the no-argument {@code read*} methods
	 *            from {@code ByteBufferReader} or a static method from any
	 *            other class with a single {@code byte[]} argument and non-
	 *            {@code void} return type.
	 * @param repeater {@code true} if {@code keyReader} should be applied until
	 *            the entry has been fully processed.
	 */
	BdbEntryInfo(@SuppressWarnings("hiding") final String entryTag,
			@SuppressWarnings("hiding") final Method entryReader,
			@SuppressWarnings("hiding") final boolean repeater) {
		this.entryReader = entryReader;
		this.repeater = repeater;
		this.entryTag = entryTag;
	}

	/**
	 * Resets the count for this entry's tag.
	 * 
	 */
	public void reset() {
		count = 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "(" + entryReader.getName() + ", repeats = " + repeater + ")";
	}

	/**
	 * Returns the entry reading method's name.
	 * 
	 * @return the entry reading method's name.
	 */
	public String getMethodName() {
		return entryReader.getName();
	}

	/**
	 * Returns the result of invoking the enclosed method. If the method is
	 * static, then it is invoked using the remaining, unprocessed {@code byte}
	 * contents of {@code bbr}. Otherwise it is assumed to be a {@code
	 * ByteBufferReader} method and is invoked on {@code bbr}.
	 * 
	 * @param bbr
	 * @return the result of invoking the enclosed method.
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 */
	public Object invokeMethod(ByteBufferReader bbr)
			throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		Object returnVal = null;
		if (Modifier.isStatic(entryReader.getModifiers())) {
			Class<?>[] paramters = entryReader.getParameterTypes();
			if (Arrays.asList(paramters).contains(ByteBufferReader.class)) {
				returnVal = entryReader.invoke(null, bbr);
			} else {
				returnVal = entryReader.invoke(null, bbr
						.readByteArrayNoLength(bbr.getLengthRemaining()));
			}
		} else {
			returnVal = entryReader.invoke(bbr, (Object[]) null);
		}

		return returnVal;
	}

	/**
	 * Returns {@code true} if the method should be applied repeatedly.
	 * 
	 * @return {@code true} if the method should be applied repeatedly.
	 */
	public boolean isRepeater() {
		return repeater;
	}

	/**
	 * Returns the {@code EntryInfo} represented by the XML element {@code
	 * method}.
	 * 
	 * @param method
	 * @return the {@code EntryInfo} represented by the XML element {@code
	 *         method}.
	 * 
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 * @throws ClassNotFoundException
	 */
	public static BdbEntryInfo deserialize(Element method)
			throws SecurityException, NoSuchMethodException,
			ClassNotFoundException {
		String methodName = method.attributeValue("name");
		boolean methodRepeats = false;
		if (methodName.endsWith("+")) {
			methodName = methodName.substring(0, methodName.length() - 1);
			methodRepeats = true;
		}
		String className = method.attributeValue("class");
		Class<?> methodClass = ByteBufferReader.class;
		Method m = null;
		if (className != null) {
			methodClass = Class.forName(className);
			m = methodClass.getMethod(methodName, byte[].class);
		} else {
			m = methodClass.getMethod(methodName, (Class<?>[]) null);
		}

		String tag = method.attributeValue("tag");
		if (tag == null) {
			tag = m.getName();
		}
		return new BdbEntryInfo(tag, m, methodRepeats);
	}

	/**
	 * Returns the method from {@code ByteBufferReader} with name {@code
	 * methodName}. Assumes there are no arguments.
	 * 
	 * @param methodName
	 * @return the method from {@code ByteBufferReader} with name {@code
	 *         methodName}.
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 */
	public static Method getByteBufferReaderMethod(String methodName)
			throws SecurityException, NoSuchMethodException {
		return ByteBufferReader.class.getMethod(methodName, (Class<?>[]) null);
	}

	/**
	 * @return the entryTag
	 */
	public String getEntryTag() {
		String tag = entryTag;
		if (repeater) {
			tag += "_" + count++;
		}
		return tag;
	}

}
