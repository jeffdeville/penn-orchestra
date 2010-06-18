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

import static edu.upenn.cis.orchestra.util.DomUtils.getChildElementByName;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import edu.upenn.cis.orchestra.sql.ISqlDrop;
import edu.upenn.cis.orchestra.sql.ISqlDropSchema;
import edu.upenn.cis.orchestra.sql.ISqlFactory;
import edu.upenn.cis.orchestra.sql.SqlFactories;
import edu.upenn.cis.orchestra.util.DomUtils;

/**
 * Utilities for testing.
 * 
 * @author John Frommeyer
 * 
 */
public class TestUtil {

	/**
	 * A testng group name for junit4 tests which have been converted to also
	 * run as testng tests.
	 */
	public static final String JUNIT4_TESTNG_GROUP = "junit4";

	/**
	 * A testng group name for junit3 tests which have been converted.
	 */
	public static final String JUNIT3_TESTNG_GROUP = "junit3";

	/**
	 * A testng group name for broken tests.
	 */
	public static final String BROKEN_TESTNG_GROUP = "broken";

	/**
	 * A testng group name for fast running testng tests.
	 */
	public static final String FAST_TESTNG_GROUP = "fast";

	/** A testng group name for slow running testng tests. */
	public static final String SLOW_TESTNG_GROUP = "slow";

	/** A testng group name for testng tests still in development. */
	public static final String DEV_TESTNG_GROUP = "development";

	/** A testng group name for tests requiring access to a database */
	public static final String REQUIRES_DATABASE_TESTNG_GROUP = "requires-database";

	/**
	 * A testng group name for testng tests which require parameters to set up
	 * an environment.
	 */
	public static final String PARAMETERIZED_TESTNG_GROUP = "parameterized-tests";

	/**
	 * The logger.
	 */
	private static final Logger logger = LoggerFactory
			.getLogger(TestUtil.class);

	/**
	 * Prevent instantiation.
	 * 
	 */
	private TestUtil() {}

	/**
	 * Attempts to drop the tables listed in {@code tablesToDrop} and then the
	 * schemas in {@code schemasToDrop} using {@code jdbcConnection}. Any
	 * {@code SQLException}s are logged and ignored.
	 * 
	 * @param jdbcConnection
	 * @param tablesToDrop
	 * @param schemasToDrop
	 * @throws Exception
	 */
	public static void clearDb(Connection jdbcConnection,
			Set<String> tablesToDrop, Set<String> schemasToDrop)
			throws Exception {
		logger
				.debug(
						"Will attempt to drop tables from the following schemas: {}. Will also attempt to drop schemas",
						schemasToDrop);
		ISqlFactory sqlFactory = SqlFactories.getSqlFactory();
		if (tablesToDrop.size() != 0) {
			logger.debug("Attempting to drop tables: {}", tablesToDrop);
			int failedDrops = 0;
			for (String tableName : tablesToDrop) {
				try {
					ISqlDrop drop = sqlFactory.newDrop(tableName);
					jdbcConnection.createStatement().execute(drop.toString());
					logger.debug("Dropped table: {}", tableName);
				} catch (SQLException e) {
					failedDrops++;
					logger
							.debug(
									"Failed to drop table: {}. SQLState: {}. Error Code: {}",
									new Object[] { tableName, e.getSQLState(),
											Integer.valueOf(e.getErrorCode()) });
				}
			}
			logger.debug("Dropped {} out of {} tables.", Integer
					.valueOf(tablesToDrop.size() - failedDrops), Integer
					.valueOf(tablesToDrop.size()));
		}

		int failedSchemaDrops = 0;
		for (String schemaName : schemasToDrop) {
			try {
				ISqlDropSchema dropSchema = sqlFactory
						.newDropSchema(schemaName);
				jdbcConnection.createStatement().execute(dropSchema.toString());
				logger.debug("Dropped schema: {}", schemaName);
			} catch (SQLException e) {
				failedSchemaDrops++;
				logger.debug("Failed to drop schema: {}. SQLState: {}.",
						schemaName, e.getSQLState());
			}
		}
		logger.debug("Dropped {} out of {} schemas.", Integer
				.valueOf(schemasToDrop.size() - failedSchemaDrops), Integer
				.valueOf(schemasToDrop.size()));
	}

	/**
	 * Assumes that there is a file named {@code sqlFileName} in {@code
	 * testDataDirectory} containing simple semicolon terminated SQL statements.
	 * These statements are executed using {@code jdbcConnection}.
	 * 
	 * @param jdbcConnection
	 * @param testDataDirectory
	 * @param sqlFileName
	 * @throws IOException
	 * @throws SQLException
	 */
	public static void createBaseTables(Connection jdbcConnection,
			File testDataDirectory, String sqlFileName) throws IOException,
			SQLException {
		File sqlFile = new File(testDataDirectory, sqlFileName);
		executeSqlScript(jdbcConnection, sqlFile);

	}

	/**
	 * Assumes that {@code sqlScript} contains simple semicolon terminated SQL
	 * statements. These statements are executed using {@code jdbcConnection}.
	 * 
	 * @param jdbcConnection
	 * @param sqlScript
	 * @throws IOException
	 * @throws SQLException
	 */
	public static void executeSqlScript(Connection jdbcConnection,
			File sqlScript) throws IOException, SQLException {
		BufferedReader sqlFile = new BufferedReader(new FileReader(sqlScript));
		List<String> sqlStatements = OrchestraUtil.newArrayList();
		StringBuffer sqlBuffer = new StringBuffer();
		String line;
		while ((line = sqlFile.readLine()) != null) {
			line = line.trim();
			if (!line.startsWith("--")) {
				sqlBuffer.append(line);
				if (sqlBuffer.toString().endsWith(";")) {
					sqlStatements.add(sqlBuffer.toString().substring(0,
							sqlBuffer.toString().length() - 1));
					sqlBuffer = new StringBuffer();
				}
			}
		}

		for (String sqlStatement : sqlStatements) {
			jdbcConnection.createStatement().execute(sqlStatement);
			logger.debug("Executed: [{}]", sqlStatement);
		}

	}

	/**
	 * Returns a {@code Document} representing an Orchestra schema file. This
	 * will be the same as the file found at {@code schemaFileTemplate} except
	 * that the value of {@code /catalog/engine/mappings} has been created on
	 * the fly using the parameters passed in.
	 * 
	 * @param schemaFileTemplate
	 * @param dbURL the resulting value of {@code
	 *            /catalog/engine/mappings/@server}.
	 * @param dbUser the resulting value of {@code
	 *            /catalog/engine/mappings/@username}.
	 * @param dbPassword the resulting value of {@code
	 *            /catalog/engine/mappings/@password}.
	 * @param type the resulting value of {@code /catalog/engine/mappings/@type}
	 *            .
	 * 
	 * @return a {@code Document} representing an Orchestra schema file.
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 */
	public static Document addMappingElement(File schemaFileTemplate,
			String dbURL, String dbUser, String dbPassword, String type)
			throws ParserConfigurationException, SAXException, IOException {

		FileInputStream in = new FileInputStream(schemaFileTemplate);
		Document document = DomUtils.createDocument(in);
		Element root = document.getDocumentElement();

		// Set mappings element.
		Element engine = DomUtils.getChildElementByName(root, "engine");
		List<Element> oldMappings = DomUtils.getChildElementsByName(engine,
				"mappings");
		for (Element oldMapping : oldMappings) {
			engine.removeChild(oldMapping);
		}
		Element mappings = DomUtils.addChild(document, engine, "mappings");
		mappings.setAttribute("server", dbURL);
		mappings.setAttribute("username", dbUser);
		mappings.setAttribute("password", dbPassword);
		mappings.setAttribute("type", type);
		return document;
	}

	/**
	 * Returns a {@code Document} representing an Orchestra schema file. This
	 * will be the same as {@code schemaDocTemplate} except any existing {@code
	 * /catalog/engine/mappings} will be replaced by one which has been created
	 * on the fly using the parameters passed in.
	 * 
	 * @param schemaDocTemplate
	 * @param dbURL the resulting value of {@code
	 *            /catalog/engine/mappings/@server}.
	 * @param dbUser the resulting value of {@code
	 *            /catalog/engine/mappings/@username}.
	 * @param dbPassword the resulting value of {@code
	 *            /catalog/engine/mappings/@password}.
	 * @param type the resulting value of {@code /catalog/engine/mappings/@type}
	 *            .
	 * 
	 * @return a {@code Document} representing an Orchestra schema file.
	 */
	public static Document replaceMappingElement(Document schemaDocTemplate,
			String dbURL, String dbUser, String dbPassword, String type) {
		Document document = (Document) schemaDocTemplate.cloneNode(true);
		Element root = document.getDocumentElement();

		// Set mappings element.
		Element engine = DomUtils.getChildElementByName(root, "engine");
		List<Element> oldMappings = DomUtils.getChildElementsByName(engine,
				"mappings");
		for (Element oldMapping : oldMappings) {
			engine.removeChild(oldMapping);
		}
		Element mappings = DomUtils.addChild(document, engine, "mappings");
		mappings.setAttribute("server", dbURL);
		mappings.setAttribute("username", dbUser);
		mappings.setAttribute("password", dbPassword);
		mappings.setAttribute("type", type);
		return document;
	}

	/**
	 * Returns the result of setting {@code peer/@localPeer = 'true' for
	 * peer/[@name = localPeerName]} in {@code schemaDocTemplate}. No changes
	 * are made to {@code schemaDocTemplate}.
	 * 
	 * @param schemaDocTemplate
	 * @param localPeerName
	 * @return a new copy of {@code schemaDocTemplate} with {@code
	 *         localPeerName} set as the local peer.
	 */
	public static Document setLocalPeer(Document schemaDocTemplate,
			String localPeerName) {
		Document document = (Document) schemaDocTemplate.cloneNode(true);
		Element root = document.getDocumentElement();
		List<Element> peers = DomUtils.getChildElementsByName(root, "peer");
		boolean localPeerSet = false;
		for (Iterator<Element> iter = peers.iterator(); iter.hasNext()
				&& !localPeerSet;) {
			Element peer = iter.next();
			if (localPeerName.equals(peer.getAttribute("name"))) {
				peer.setAttribute("localPeer", "true");
				localPeerSet = true;
			} else {
				// Not really necessary, just to make sure we don't end up with
				// more than one local peer.
				peer.setAttribute("localPeer", "false");
			}
		}
		Element store = getChildElementByName(root, "store");
		Element state = getChildElementByName(store, "state");
		String type = state.getAttribute("type");
		if ("bdb".equals(type)){
			String workdirPrefix = state.getAttribute("workdir");
			state.setAttribute("workdir", workdirPrefix + "_" + localPeerName);
		}
		return document;
	}
}
