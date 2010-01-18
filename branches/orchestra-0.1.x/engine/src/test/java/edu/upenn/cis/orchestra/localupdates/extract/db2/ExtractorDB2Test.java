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
package edu.upenn.cis.orchestra.localupdates.extract.db2;

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.OrchestraUtil.newTreeMap;
import static edu.upenn.cis.orchestra.TestUtil.DEV_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.TestUtil.REQUIRES_DATABASE_TESTNG_GROUP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.SortedMap;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import edu.upenn.cis.orchestra.Config;
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
import edu.upenn.cis.orchestra.sql.ISqlDelete;
import edu.upenn.cis.orchestra.sql.ISqlExpression;
import edu.upenn.cis.orchestra.sql.ISqlFactory;
import edu.upenn.cis.orchestra.sql.ISqlFromItem;
import edu.upenn.cis.orchestra.sql.ISqlInsert;
import edu.upenn.cis.orchestra.sql.ISqlSelect;
import edu.upenn.cis.orchestra.sql.ISqlSelectItem;
import edu.upenn.cis.orchestra.sql.SqlFactories;
import edu.upenn.cis.orchestra.sql.ISqlConstant.Type;
import edu.upenn.cis.orchestra.sql.ISqlExpression.Code;

/**
 * Testing update extraction. Assumes that DB2 the DBMS and that SQL replication
 * has been set up and is running for a source table {@code CCDTEST.R} with
 * target table (@code CCDTEST.R_CCD} of type CCD.
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = {REQUIRES_DATABASE_TESTNG_GROUP, DEV_TESTNG_GROUP})
public class ExtractorDB2Test {

	private Connection testConnection;
	private Connection extractorConnection;

	private String testSchema = "CCDTEST";
	private String sourceTable = "R";
	private String sourceTableFQN = testSchema + "." + sourceTable;
	private String ccdTable = sourceTable + "_CCD";
	private String ccdTableFQN = testSchema + "." + ccdTable;
	private final ISqlFactory sqlFactory = SqlFactories.getSqlFactory();
	private final SortedMap<Integer, String> deletions = newTreeMap();
	private final SortedMap<Integer, String> inserts = newTreeMap();

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
		clearCCDTable();
		setupSourceTable();
		Schema schema = new Schema("ExtractUpdateTestSchema");
		List<RelationField> fields = newArrayList();
		fields.add(new RelationField("RID", "The R ID", new IntType(false,
				false)));
		fields.add(new RelationField("RSTR", "The R STR", new StringType(false,
				false, true, 10)));
		Relation relation = new Relation(null, testSchema, sourceTable,
				sourceTable, "The source table for test", true, true, fields);
		relation.markFinished();
		schema.addRelation(relation);
		Peer peer = new Peer("ExtractUpdateTestPeer", "",
				"Extract Update Test Peer");
		peer.addSchema(schema);
		IExtractor<Connection> extractor = new ExtractorDB2();
		Thread.sleep(60000);
		ILocalUpdates localUpdates = extractor
				.extractTransactions(peer, extractorConnection);
		//In real life the ILocalUpdater would take care of this.
		extractorConnection.commit();
		List<Update> updates = localUpdates.getLocalUpdates(schema, relation);

		assertEquals(updates.size(), deletions.size() + inserts.size());
		// Test always does deletions first
		for (int i = 0, id = deletions.firstKey().intValue(); i < deletions
				.size(); i++, id++) {
			Update update = updates.get(i);
			assertTrue(update.isDeletion());
			String expectedStr = deletions.get(Integer.valueOf(id));
			assertEquals(update.getOldVal().get("RSTR"), expectedStr);
		}
		for (int i = 0, id = inserts.firstKey().intValue(); i < inserts.size(); i++, id++) {
			Update update = updates.get(i + deletions.size());
			assertTrue(update.isInsertion());
			String expectedStr = inserts.get(Integer.valueOf(id));
			assertEquals(update.getNewVal().get("RSTR"), expectedStr);
		}
	}

	/**
	 * Clears out the CCD table for the test.
	 * 
	 * @throws SQLException
	 * 
	 */
	private void clearCCDTable() throws SQLException {
		ISqlDelete clearTable = sqlFactory.newSqlDelete(ccdTableFQN);
		Statement delete = testConnection.createStatement();
		try {
			delete.executeUpdate(clearTable.toString());
		} finally {
			delete.close();
		}
	}

	/**
	 * Does the deletions and insertions for the test.
	 * 
	 * @throws SQLException
	 */
	private void setupSourceTable() throws SQLException {
		ISqlSelectItem selectItem = sqlFactory.newSelectItem("*");
		ISqlFromItem selectFrom = sqlFactory.newFromItem(sourceTableFQN);
		ISqlSelect select = sqlFactory.newSelect(selectItem, selectFrom)
				.addOrderBy(
						Collections.singletonList(sqlFactory
								.newOrderByItem(sqlFactory.newConstant(
										"RID", Type.COLUMNNAME))));
		Statement statement = testConnection.createStatement(
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

		try {
			ResultSet preexisting = statement.executeQuery(select.toString()
					+ " for update");
			while (preexisting.next()) {
				int id = preexisting.getInt(1);
				String str = preexisting.getString(2);
				deletions.put(Integer.valueOf(id), str);
				preexisting.deleteRow();
			}
		} finally {
			statement.close();
		}
		ISqlInsert insertSql = sqlFactory.newSqlInsert(sourceTableFQN);
		ISqlExpression intoClause = sqlFactory.newExpression(Code.COMMA);
		intoClause.addOperand(sqlFactory.newConstant("?",
				Type.PREPARED_STATEMENT_PARAMETER));
		intoClause.addOperand(sqlFactory.newConstant("?",
				Type.PREPARED_STATEMENT_PARAMETER));
		insertSql.addValueSpec(intoClause);
		PreparedStatement insertStatement = testConnection
				.prepareStatement(insertSql.toString());

		try {
			Random rand = new Random();
			int idBase = rand.nextInt(100);

			for (int i = 0; i < 3; i++) {
				int newId = idBase + i;
				String str = Integer.toHexString(rand.nextInt(1000));
				inserts.put(Integer.valueOf(newId), str);
				insertStatement.setInt(1, newId);
				insertStatement.setString(2, str);
				insertStatement.executeUpdate();
			}
		} finally {
			insertStatement.close();
		}
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
		//In real life the ILocalUpdater would take care of this.
		if (extractorConnection != null) {
			extractorConnection.close();
		}
	}
}
