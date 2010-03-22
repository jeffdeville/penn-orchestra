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

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;

/**
 * This class can be used to create a DbUnit dataset from a Berkeley database
 * version of an Orchestra update store.
 * 
 * @author John Frommeyer
 * 
 */
public class BdbDataSetFactory {

	/** Takes care of the Berkeley database environment. */
	private IBdbStoreEnvironment env;
	private ISchemaIDBinding schemaIDBinding;
	private final List<IBdbStoreEnvironment> stateStoreEnvs = newArrayList();
	private final File usHome;
	private final String cdss;
	private final Collection<String> peerNames;
	private static final Logger logger = LoggerFactory
			.getLogger(BdbDataSetFactory.class);

	/**
	 * Creates a {@code BdbDataSetFactory}. Assumes that the schema for {@code
	 * cdssName} has already been loaded in the Berkeley database.
	 * 
	 * @param bdbDirectory
	 * @param cdssName
	 * @param peers
	 * @throws Exception
	 */
	public BdbDataSetFactory(File bdbDirectory, String cdssName,
			Collection<String> peers) throws Exception {
		usHome = bdbDirectory;
		cdss = cdssName;
		peerNames = peers;
		initializeEnvironments(usHome, cdss, peerNames);
	}

	private void initializeEnvironments(File bdbDirectory, String cdssName,
			Collection<String> peers) throws Exception,
			EnvironmentLockedException, DatabaseException {
		if (env == null) {
			env = new UpdateStoreBdbEnv(bdbDirectory, cdssName);
			schemaIDBinding = env.getSchemaIDBinding();
			for (String peer : peers) {
				stateStoreEnvs.add(new StateStoreBdbEnv("stateStore_env", peer,
						schemaIDBinding));
			}
		}
	}

	/**
	 * Returns a {@code FlatXmlDataSet} representation of the underlying
	 * Berkeley database.
	 * 
	 * @return a {@code FlatXmlDataSet} representation of the Berkeley database.
	 * @throws Exception
	 */
	public FlatXmlDataSet getDataSet() throws Exception {

		Element root = DocumentHelper.createElement("dataset");
		Document dsDoc = DocumentHelper.createDocument(root);
		DefaultDataSet dataset = new DefaultDataSet();
		initializeEnvironments(usHome, cdss, peerNames);
		BdbEnvironment envFormat = env.getFormat();

		List<Database> dbs = env.getDbs();

		processDatabases(root, dataset, envFormat, dbs);
		for (IBdbStoreEnvironment ss : stateStoreEnvs) {
			BdbEnvironment stateStoreFormat = ss.getFormat();
			List<Database> ssDbs = ss.getDbs();
			processDatabases(root, dataset, stateStoreFormat, ssDbs);
		}
		close();
		return new FlatXmlDataSet(new StringReader(dsDoc.asXML()), false, true,
				false);
	}

	private void processDatabases(Element root, DefaultDataSet dataset,
			BdbEnvironment envFormat, List<Database> dbs) throws Exception {

		for (Database db : dbs) {
			String dbName = db.getDatabaseName();
			BdbDatabase dbFormat = envFormat.getDatabase(dbName);
			String peerName = envFormat.getPeerName();
			String tagName = (peerName == null) ? dbName : envFormat
					.getPeerName()
					+ "." + dbName;

			DefaultTable table = new DefaultTable(tagName);
			dataset.addTable(table);
			DatabaseEntry key = new DatabaseEntry();
			DatabaseEntry data = new DatabaseEntry();

			if (dbFormat != null) {
				Cursor c = db.openCursor(null, null);
				int rows = 0;
				try {
					while (c.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
						rows++;
						byteBufferRead(root, tagName, key, data, dbFormat,
								schemaIDBinding);
					}
					if (rows == 0) {
						root.addElement(tagName);
					}
				} finally {
					c.close();
				}
			}
		}
	}

	/**
	 * Closes up the underlying Berkeley database.
	 * 
	 */
	private void close() {
		env.close();
		for (IBdbStoreEnvironment ss : stateStoreEnvs) {
			ss.close();
		}
		env = null;
		stateStoreEnvs.clear();
	}

	/**
	 * 
	 * 
	 * @param args not used.
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		/*
		 * IDataSet actual = new SortedDataSet(new FlatXmlDataSet(new File(
		 * "bdb_actual.xml"), false, true)); IDataSet expected = new
		 * SortedDataSet(new FlatXmlDataSet(new File( "bdb_expected.xml"),
		 * false, true)); Assertion.assertEquals(expected, actual);
		 */

		BdbDataSetFactory factory = new BdbDataSetFactory(new File(
				"updateStore_env"), "ppodLN", newArrayList("pPODPeer1",
				"pPODPeer2"));

		FlatXmlDataSet ds = factory.getDataSet();
		FlatXmlDataSet.write(ds, new FileWriter("updateStore.xml", false));

	}

	/**
	 * Returns the {@code Object} encoded in {@code bytes}. A helper for those
	 * cases where there is not a better way to read the object.
	 * 
	 * @param bytes
	 * @return the {@code Object} represented by {@code bytes}.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Object readObject(byte[] bytes) throws IOException,
			ClassNotFoundException {
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

		ObjectInputStream os = new ObjectInputStream(bais);

		Object thing = os.readObject();
		return thing;
	}

	/**
	 * Translates the {@code key} and {@code entry} into an {@code Element}.
	 * 
	 * @param root
	 * @param tagName
	 * @param key
	 * @param data
	 * @param dbFormat
	 * @param schemaIDBinding
	 */
	private static void byteBufferRead(Element root, String tagName,
			DatabaseEntry key, DatabaseEntry data, BdbDatabase dbFormat,
			ISchemaIDBinding schemaIDBinding) {

		Element row = root.addElement(tagName);
		if (logger.isTraceEnabled()) {
			byte[] bytes = key.getData();
			logger.trace("{} key: {}", tagName, Arrays.toString(bytes));
		}
		List<BdbEntryInfo> keyFormat = dbFormat.getKeyInfo();
		entryToAttributes(row, key, keyFormat, schemaIDBinding);
		if (logger.isTraceEnabled()) {
			byte[] bytes = data.getData();
			logger.trace("{} data: {}", tagName, Arrays.toString(bytes));
		}
		List<BdbEntryInfo> dataFormat = dbFormat.getDataInfo();
		entryToAttributes(row, data, dataFormat, schemaIDBinding);

	}

	/**
	 * Translates {@code entry} into a set of attributes for {@code row}.
	 * 
	 * @param row
	 * @param entry
	 * @param entryFormat
	 * @param schemaIDBinding
	 */
	private static void entryToAttributes(Element row, DatabaseEntry entry,
			List<BdbEntryInfo> entryFormat, ISchemaIDBinding schemaIDBinding) {

		ByteBufferReader keyReader = new ByteBufferReader(schemaIDBinding,
				entry.getData());
		for (BdbEntryInfo info : entryFormat) {
			boolean repeater = info.isRepeater();
			Exception ex = null;
			try {
				if (keyReader.hasFinished()) {
					row.addAttribute(info.getEntryTag(), "");
				} else if (repeater) {
					while (!keyReader.hasFinished()) {
						Object keyPiece = info.invokeMethod(keyReader);
						row.addAttribute(info.getEntryTag(), keyPiece
								.toString());
					}
				} else {
					Object keyPiece = info.invokeMethod(keyReader);
					String value = keyPiece == null ? "null" : keyPiece
							.toString();
					row.addAttribute(info.getEntryTag(), value);
				}
			} catch (Exception e) {
				ex = e;
			}
			if (ex != null) {
				row.addAttribute(info.getEntryTag(), "ERROR ("
						+ info.getMethodName() + ")");
				ex.printStackTrace();
			}
			info.reset();
		}

	}
}
