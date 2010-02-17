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
package edu.upenn.cis.orchestra.localupdates.sql;

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.TestUtil.REQUIRES_DATABASE_TESTNG_GROUP;

import java.io.File;
import java.net.URL;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.dbunit.JdbcDatabaseTester;
import org.dbunit.dataset.filter.IncludeTableFilter;
import org.dbunit.operation.DatabaseOperation;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.DbUnitUtil;
import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateRelationIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateSchemaIdException;
import edu.upenn.cis.orchestra.localupdates.ILocalUpdater;
import edu.upenn.cis.orchestra.localupdates.extract.sql.ExtractorDefault;

/**
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = { FAST_TESTNG_GROUP, REQUIRES_DATABASE_TESTNG_GROUP })
public class LocalUpdaterJdbcTest {

	private String testSchema = "EXTRACTSCHEMA";
	private String baseTable = "BASE";
	private String baseTableFqn = testSchema + "." + baseTable;
	private JdbcDatabaseTester tester;
	private Peer localPeer;

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
		// Properties connectionProperties = new Properties();
		// connectionProperties.setProperty("user", dbUser);
		// connectionProperties.setProperty("password", dbPassword);
		System.setProperty("jdbc.drivers", jdbcDriver);

		tester = new JdbcDatabaseTester(jdbcDriver, dbURL, dbUser, dbPassword);
		TestUtil.clearDb(tester.getConnection().getConnection(), newArrayList(
				baseTableFqn, baseTableFqn + ExtractorDefault.TABLE_SUFFIX,
				baseTableFqn + Relation.LOCAL, baseTableFqn + Relation.REJECT,
				baseTableFqn + Relation.LOCAL + "_" + AtomType.INS,
				baseTableFqn + Relation.LOCAL + "_" + AtomType.DEL,
				baseTableFqn + Relation.REJECT + "_" + AtomType.INS,
				baseTableFqn + Relation.REJECT + "_" + AtomType.DEL),
				Collections.singletonList(testSchema));
		File sqlScript = new File(getClass().getResource("extractschema.sql")
				.getPath());
		TestUtil.executeSqlScript(tester.getConnection().getConnection(),
				sqlScript);
		URL initalStateURL = getClass().getResource("initialState.xml");
		File initalStateFile = new File(initalStateURL.getPath());
		DbUnitUtil.executeDbUnitOperation(DatabaseOperation.CLEAN_INSERT,
				initalStateFile, tester);
	}

	/**
	 * @throws DuplicateRelationIdException
	 * @throws DuplicateSchemaIdException
	 * 
	 */
	@BeforeClass
	public void setUpLocalPeer() throws DuplicateRelationIdException,
			DuplicateSchemaIdException {
		Schema schema = new Schema("LocalUpdaterTestSchema");
		Relation relation = createRelation("");
		schema.addRelation(relation);
		Relation relationLocal = createRelation("_L");
		schema.addRelation(relationLocal);
		Relation relationReject = createRelation("_R");
		schema.addRelation(relationReject);
		localPeer = new Peer("LocalUpdaterTestPeer", "",
				"Extract Update Test Peer");
		localPeer.addSchema(schema);

	}

	private Relation createRelation(String suffix) {
		List<RelationField> fields = newArrayList();
		fields.add(new RelationField("RID", "The R ID", new IntType(false,
				false)));
		fields.add(new RelationField("RSTR", "The R STR", new StringType(false,
				false, true, 10)));
		String relationName = baseTable + suffix;
		Relation newRelation = new Relation(null, testSchema, relationName,
				relationName, "The description", true, true, fields);
		newRelation.markFinished();
		return newRelation;
	}

	/**
	 * Test that the extractor and applier work together correctly.
	 * 
	 * @throws Exception
	 */
	public final void updateExtractionAndApplicationTest() throws Exception {
		ILocalUpdater updater = new LocalUpdaterJdbc(Config.getUser(), Config
				.getPassword(), Config.getSQLServer());
		updater.extractAndApplyLocalUpdates(localPeer);
		File expected = new File(getClass().getResource("finalState.xml")
				.toURI());
		DbUnitUtil.checkDatabase(expected, new IncludeTableFilter(
				new String[] { testSchema + ".*" }), tester, null);
	}
}
