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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.dbunit.JdbcDatabaseTester;
import org.dbunit.dataset.filter.IncludeTableFilter;
import org.dbunit.operation.DatabaseOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.util.DomUtils;

/**
 * These are the test framework items which we need on a per {@code Peer} basis.
 * 
 * @author John Frommeyer
 * 
 */
public class OrchestraTestFrame {

	/**
	 * The logger.
	 */
	private final Logger logger = LoggerFactory.getLogger(getClass());

	/**
	 * The name of the peer this frame is for.
	 */
	private final String peerName;

	/**
	 * The DB connection URL. This will be the value of {@code
	 * /catalog/engine/mappings/@server} in the Orchestra schema file which will
	 * be used.
	 */
	private final String dbURL;

	/**
	 * The DB user. This will be the value of {@code
	 * /catalog/engine/mappings/@username} in the Orchestra schema file which
	 * will be used.
	 */
	private final String dbUser;

	/**
	 * The password for (@code dbUser}. This will be the value of {@code
	 * /catalog/engine/mappings/@password} in the Orchestra schema file which
	 * will be used.
	 */
	private final String dbPassword;

	/** The DbUnit tester. */
	private final JdbcDatabaseTester dbTester;

	/** A JDBC Connection. */
	private final Connection jdbcConnection;

	/**
	 * If {@code true}, then we assume that there are pre-existing base tables
	 * containing data.
	 */
	private final boolean preExistingTables;

	OrchestraTestFrame(String jdbcDriver, Element peerElement)
			throws ClassNotFoundException, SQLException {
		peerName = peerElement.getAttribute("name");
		String dbState = peerElement.getAttribute("dbState");
		preExistingTables = ("no-pre-existing".equalsIgnoreCase(dbState)) ? false
				: true;
		Element mappings = DomUtils.getChildElementByName(peerElement,
				"mappings");
		dbURL = mappings.getAttribute("server");
		dbUser = mappings.getAttribute("username");
		dbPassword = mappings.getAttribute("password");

		dbTester = new JdbcDatabaseTester(jdbcDriver, this.dbURL, this.dbUser,
				this.dbPassword);
		jdbcConnection = DriverManager.getConnection(this.dbURL, this.dbUser,
				this.dbPassword);

	}

	/**
	 * Attempts to drop any tables associated with {@code orchestraSchemaName}
	 * so that the test can run in a clean environment. Will then create schemas
	 * and tables if {@code preExistingTables == true}.
	 * 
	 * @throws Exception
	 */
	void prepare(String orchestraSchemaName, File testDataDirectory,
			List<String> dbSchemaNames, String[] dbSchemaNameRegexps)
			throws Exception {
		// Drop all tables and schemas.
		List<String> tablesToDrop = DbUnitUtil.getFilteredTableNames(
				new IncludeTableFilter(dbSchemaNameRegexps), dbTester);
		TestUtil.clearDb(jdbcConnection, tablesToDrop, dbSchemaNames);

		// If necessary, create schemas and tables.
		try {
			if (preExistingTables) {
				TestUtil.createBaseTables(jdbcConnection, testDataDirectory,
						orchestraSchemaName + "-" + peerName
								+ "-preOrchestra.sql");
				File datasetFile = new File(testDataDirectory,
						orchestraSchemaName + "-" + peerName
								+ "-preOrchestra.xml");
				if (datasetFile.exists()) {
					DbUnitUtil.executeDbUnitOperation(
							DatabaseOperation.CLEAN_INSERT, datasetFile,
							dbTester);
					DbUnitUtil.checkDatabase(datasetFile,
							new IncludeTableFilter(dbSchemaNameRegexps),
							dbTester, null);
				}
			} else {
				logger.debug("Assuming no pre-existing tables.");
			}
		} finally {
			if (jdbcConnection != null) {
				jdbcConnection.close();
			}
		}
	}

	String getDbURL() {
		return dbURL;
	}

	String getDbUser() {
		return dbUser;
	}

	String getDbPassword() {
		return dbPassword;
	}

	/**
	 * Returns the name of the peer for this test frame.
	 * 
	 * @return the name of the peer for this test frame
	 */
	String getPeerName() {
		return peerName;
	}

	/**
	 * Returns the {@code JdbcDatabaseTester} for this {@code
	 * OrchestraTestFrame}.
	 * 
	 * @return the {@code JdbcDatabaseTester} for this {@code
	 *         OrchestraTestFrame}
	 */
	JdbcDatabaseTester getDbTester() {
		return dbTester;
	}

}
