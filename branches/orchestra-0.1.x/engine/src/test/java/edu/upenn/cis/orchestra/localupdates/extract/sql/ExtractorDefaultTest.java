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
package edu.upenn.cis.orchestra.localupdates.extract.sql;

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.TestUtil.REQUIRES_DATABASE_TESTNG_GROUP;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.dbunit.JdbcDatabaseTester;
import org.dbunit.operation.DatabaseOperation;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.DbUnitUtil;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.NameNotFound;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateRelationIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateSchemaIdException;
import edu.upenn.cis.orchestra.localupdates.ILocalUpdates;
import edu.upenn.cis.orchestra.localupdates.extract.IExtractor;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.DBConnectionError;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.NoExtractorClassException;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.RDBMSExtractError;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.SchemaIncoherentWithDBError;

/**
 * Testing update extraction.
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = { FAST_TESTNG_GROUP, REQUIRES_DATABASE_TESTNG_GROUP })
public class ExtractorDefaultTest {

	private Connection testConnection;
	private Connection extractorConnection;

	private String testSchema = "EXTRACTSCHEMA";
	private String baseTable = "BASE";
	private JdbcDatabaseTester tester;

	/**
	 * Setting up the test. Right now we only have the DB2 version.
	 * 
	 * @param jdbcDriver
	 * @param dbURL
	 * @param dbUser
	 * @param dbPassword
	 * @throws SQLException
	 */
	@BeforeClass
	@Parameters(value = { "jdbc-driver", "db-url", "db-user", "db-password" })
	public final void init(String jdbcDriver, String dbURL, String dbUser,
			String dbPassword) throws SQLException {
		Config.setJDBCDriver(jdbcDriver);
		Config.setSQLServer(dbURL);
		Config.setUser(dbUser);
		Config.setPassword(dbPassword);

		Properties connectionProperties = new Properties();
		connectionProperties.setProperty("user", dbUser);
		connectionProperties.setProperty("password", dbPassword);
		System.setProperty("jdbc.drivers", jdbcDriver);

		testConnection = DriverManager.getConnection(dbURL,
				connectionProperties);
		extractorConnection = DriverManager.getConnection(dbURL,
				connectionProperties);
		extractorConnection.setAutoCommit(false);

	}

	/**
	 * Sets up dbunit with parameters passed in by testng.
	 * 
	 * @param jdbcDriver
	 * @param dbURL
	 * @param dbUser
	 * @param dbPassword
	 * @throws Exception
	 */
	@BeforeClass
	@Parameters(value = { "jdbc-driver", "db-url", "db-user", "db-password" })
	public final void initDBUnit(String jdbcDriver, String dbURL,
			String dbUser, String dbPassword) throws Exception {
		tester = new JdbcDatabaseTester(jdbcDriver, dbURL, dbUser, dbPassword);
		URL initalStateURL = getClass().getResource("initialState.xml");
		File initalStateFile = new File(initalStateURL.getPath());
		DbUnitUtil.executeDbUnitOperation(DatabaseOperation.CLEAN_INSERT,
				initalStateFile, tester);
	}

	/**
	 * This is the actual test. We delete the previous contents of R and insert
	 * some new rows. We then run the extraction process and check that all of
	 * the deletions and insertions were captured.
	 * 
	 * @throws SQLException
	 * @throws DuplicateRelationIdException
	 * @throws NoExtractorClassException
	 * @throws RDBMSExtractError
	 * @throws DBConnectionError
	 * @throws SchemaIncoherentWithDBError
	 * @throws InterruptedException
	 * @throws NameNotFound
	 * @throws DuplicateSchemaIdException
	 * 
	 */

	public final void testUpateExtraction() throws SQLException,
			DuplicateRelationIdException, NoExtractorClassException,
			SchemaIncoherentWithDBError, DBConnectionError, RDBMSExtractError,
			InterruptedException, NameNotFound, DuplicateSchemaIdException {

		Schema schema = new Schema("ExtractUpdateTestSchema");
		List<RelationField> fields = newArrayList();
		fields.add(new RelationField("RID", "The R ID", new IntType(false,
				false)));
		fields.add(new RelationField("RSTR", "The R STR", new StringType(false,
				false, true, 10)));
		Relation relation = new Relation(null, testSchema, baseTable,
				baseTable, "The source table for test", true, true, fields);
		relation.markFinished();
		schema.addRelation(relation);
		Peer peer = new Peer("ExtractUpdateTestPeer", "",
				"Extract Update Test Peer");
		peer.addSchema(schema);
		IExtractor<Connection> extractor = new ExtractorDefault();
		ILocalUpdates localUpdates = extractor.extractTransactions(peer,
				extractorConnection);
		// In real life the ILocalUpdater would take care of this.
		extractorConnection.commit();
		List<Update> updates = localUpdates.getLocalUpdates(schema, relation);

		assertEquals(updates.size(), 2);
		int insertCount = 0;
		int deleteCount = 0;
		for (Update update : updates) {
			if (update.isDeletion()) {
				deleteCount++;
				assertEquals(update.getOldVal().get("RID"), Integer.valueOf(3));
				assertEquals(update.getOldVal().get("RSTR"), "Deletion");
			} else if (update.isInsertion()) {
				insertCount++;
				assertEquals(update.getNewVal().get("RID"), Integer.valueOf(2));
				assertEquals(update.getNewVal().get("RSTR"), "Insertion");
			} else {
				throw new IllegalStateException(
						"Every Update should be either an insertion or deletion.");
			}
		}
		assertEquals(deleteCount, 1);
		assertEquals(insertCount, 1);
		
	}

	/**
	 * Make sure the JDBC connection is closed.
	 * 
	 * @throws SQLException
	 */
	@AfterClass(alwaysRun = true)
	public final void close() throws SQLException {
		if (testConnection != null) {
			testConnection.close();
		}
		// In real life the ILocalUpdater would take care of this.
		if (extractorConnection != null) {
			extractorConnection.close();
		}
	}
}
