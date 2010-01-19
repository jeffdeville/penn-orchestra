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
package edu.upenn.cis.orchestra;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding;

/**
 * This class can be used to create a DbUnit dataset from a Berkeley database
 * version of an Orchestra update store.
 * 
 * @author John Frommeyer
 * 
 */
public class BdbDataSetFactory {

	/** Takes care of the Berkeley database environment. */
	private final UpdateStoreBdbEnv env;

	/**
	 * Creates a factory which can be used to get a DbUnit XML dataset
	 * representation of the update store.
	 * 
	 * @param bdbDirectory the directory containing the update store.
	 * @throws Exception
	 */
	@Deprecated
	public BdbDataSetFactory(File bdbDirectory)
			throws Exception {

		env = new UpdateStoreBdbEnv(bdbDirectory);
	}
	
	/**
	 * Creates a {@code BdbDataSetFactory}.

	 * @param bdbDirectory 
	 * @param peerIDToSchema
	 * @throws Exception 
	 */
	public BdbDataSetFactory(File bdbDirectory, Map<AbstractPeerID, Schema> peerIDToSchema) throws Exception {
		env = new UpdateStoreBdbEnv(bdbDirectory, peerIDToSchema);
	}

	/**
	 * Returns a {@code FlatXmlDataSet} representation of the underlying
	 * Berkeley database.
	 * 
	 * @return a {@code FlatXmlDataSet} representation of the Berkeley database.
	 * @throws DataSetException
	 * @throws IOException
	 * @throws DatabaseException
	 */
	public FlatXmlDataSet getDataSet() throws DataSetException, IOException,
			DatabaseException {

		Element root = DocumentHelper.createElement("dataset");
		Document dsDoc = DocumentHelper.createDocument(root);
		DefaultDataSet dataset = new DefaultDataSet();
		BdbEnvironment envFormat = env.getFormat();

		ISchemaIDBinding schemaIDBinding = env.getSchemaIDBinding();
		List<Database> dbs = env.getDbs();

		for (Database db : dbs) {
			String dbName = db.getDatabaseName();
			DefaultTable table = new DefaultTable(dbName);
			dataset.addTable(table);
			DatabaseEntry key = new DatabaseEntry();
			DatabaseEntry data = new DatabaseEntry();
			BdbDatabase dbFormat = envFormat.getDatabase(dbName);
			if (dbFormat != null) {
				Cursor c = db.openCursor(null, null);
				int rows = 0;
				try {
					while (c.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
						rows++;
						byteBufferRead(root, dbName, key, data, dbFormat,
								schemaIDBinding);
					}
					if (rows == 0) {
						root.addElement(dbName);
					}
				} finally {
					c.close();
				}
			}
		}

		return new FlatXmlDataSet(new StringReader(dsDoc.asXML()), false, true,
				false);
	}

	/**
	 * Closes up the underlying Berkeley database.
	 * 
	 */
	public void close() {
		env.close();
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
				"updateStore_env"));
		// factory.printBdb();
		try {
			FlatXmlDataSet ds = factory.getDataSet();
			FlatXmlDataSet.write(ds, new FileWriter("updateStore.xml", false));
		} finally {
			factory.close();
		}
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
	 * @param dbName
	 * @param key
	 * @param data
	 * @param dbFormat
	 * @param schemaIDBinding
	 */
	private static void byteBufferRead(Element root, String dbName,
			DatabaseEntry key, DatabaseEntry data, BdbDatabase dbFormat,
			ISchemaIDBinding schemaIDBinding) {

		Element row = root.addElement(dbName);

		List<BdbEntryInfo> keyFormat = dbFormat.getKeyInfo();
		entryToAttributes(row, key, keyFormat, schemaIDBinding);

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
				do {
					Object keyPiece = info.invokeMethod(keyReader);
					row.addAttribute(info.getEntryTag(), keyPiece.toString());
				} while (repeater && !keyReader.hasFinished());
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
