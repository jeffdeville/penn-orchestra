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
package edu.upenn.cis.orchestra.localupdates.apply.sql;

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.TestUtil.REQUIRES_DATABASE_TESTNG_GROUP;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.dbunit.DatabaseUnitException;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.filter.IncludeTableFilter;
import org.dbunit.operation.DatabaseOperation;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import edu.upenn.cis.orchestra.DbUnitUtil;
import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.NameNotFound;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateRelationIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateSchemaIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.localupdates.ILocalUpdates;
import edu.upenn.cis.orchestra.localupdates.LocalUpdates;
import edu.upenn.cis.orchestra.localupdates.apply.IApplier;

/**
 * Test the SQL implementation of {@code IApplyUpdates}.
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = { REQUIRES_DATABASE_TESTNG_GROUP, FAST_TESTNG_GROUP })
public class ApplierSqlTest {
	private Peer localPeer;
	private Schema schema;
	private Relation relation;
	private Relation relationLocal;
	private Relation relationReject;
	private Connection connection;
	private ILocalUpdates localUpdates;
	private JdbcDatabaseTester tester;
	private final String dbschema = "applyschema";
	private final String dbtable = "applyrelation";
	private final String dbtablefqn = dbschema + "." + dbtable;

	/**
	 * @throws DuplicateRelationIdException
	 * @throws DuplicateSchemaIdException
	 * 
	 */
	@BeforeClass
	public void setUpLocalPeer() throws DuplicateRelationIdException,
			DuplicateSchemaIdException {
		schema = new Schema("ApplyUpdateTestSchema");
		relation = createRelation("");
		schema.addRelation(relation);
		relationLocal = createRelation(Relation.LOCAL);
		schema.addRelation(relationLocal);
		relationReject = createRelation(Relation.REJECT);
		schema.addRelation(relationReject);
		localPeer = new Peer("ApplyUpdateTestPeer", "",
				"Apply Update Test Peer");
		localPeer.addSchema(schema);

	}

	private Relation createRelation(String suffix) {
		List<RelationField> fields = newArrayList();
		fields.add(new RelationField("RID", "The R ID", new IntType(false,
				false)));
		fields.add(new RelationField("RSTR", "The labeled nullable R STR",
				new StringType(false, true, true, 10)));
		String relationName = dbtable + suffix;
		Relation newRelation = new Relation(null, dbschema, relationName,
				relationName, "The description", true, true, fields);
		newRelation.markFinished();
		return newRelation;
	}

	/**
	 * Sets up the connection with parameters passed in by testng.
	 * 
	 * @param jdbcDriver
	 * @param dbURL
	 * @param dbUser
	 * @param dbPassword
	 * @throws SQLException
	 */
	@BeforeClass
	@Parameters(value = { "jdbc-driver", "db-url", "db-user", "db-password" })
	public final void initConnection(String jdbcDriver, String dbURL,
			String dbUser, String dbPassword) throws SQLException {

		Properties connectionProperties = new Properties();
		connectionProperties.setProperty("user", dbUser);
		connectionProperties.setProperty("password", dbPassword);
		System.setProperty("jdbc.drivers", jdbcDriver);

		connection = DriverManager.getConnection(dbURL, connectionProperties);
		connection.setAutoCommit(false);

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
		TestUtil.clearDb(tester.getConnection().getConnection(), newArrayList(
				dbtablefqn, dbtablefqn + Relation.LOCAL,
				dbtablefqn + Relation.REJECT, dbtablefqn
						+ Relation.LOCAL + "_" + AtomType.INS, dbtablefqn
						+ Relation.LOCAL + "_" + AtomType.DEL, dbtablefqn
						+ Relation.REJECT + "_" + AtomType.INS, dbtablefqn
						+ Relation.REJECT + "_" + AtomType.DEL), Collections
				.singletonList(dbschema));
		File sqlScript = new File(getClass().getResource("applyschema.sql").getPath());
		TestUtil.executeSqlScript(tester.getConnection().getConnection(), sqlScript);
		URL initalStateURL = getClass().getResource("initialState.xml");
		File initalStateFile = new File(initalStateURL.getPath());
		DbUnitUtil.executeDbUnitOperation(DatabaseOperation.CLEAN_INSERT,
				initalStateFile, tester);
	}

	/**
	 * Creates a {@code LocalUpdates} for the test.
	 * 
	 * @throws ValueMismatchException
	 * @throws NameNotFound
	 * 
	 */
	@BeforeClass(dependsOnMethods = { "setUpLocalPeer", "initConnection" })
	public void setUpLocalUpdates() throws NameNotFound, ValueMismatchException {
		LocalUpdates.Builder builder = new LocalUpdates.Builder(localPeer);

		Tuple inserted = new Tuple(relation);
		inserted.set("RID", Integer.valueOf(1));
		inserted.set("RSTR", "Mitchell");
		builder.addUpdate(schema, relation, new Update(null, inserted));

		inserted = new Tuple(relation);
		inserted.set("RID", Integer.valueOf(3));
		inserted.set("RSTR", "Mark");
		builder.addUpdate(schema, relation, new Update(null, inserted));

		Tuple deleted = new Tuple(relation);
		deleted.set("RID", Integer.valueOf(2));
		deleted.set("RSTR", "Webb");
		builder.addUpdate(schema, relation, new Update(deleted, null));

		deleted = new Tuple(relation);
		deleted.set("RID", Integer.valueOf(4));
		deleted.set("RSTR", "Jeremy");
		builder.addUpdate(schema, relation, new Update(deleted, null));

		localUpdates = builder.buildLocalUpdates();

	}

	/**
	 * The actual test. Verifies that the updates were applied as expected.
	 * 
	 * @throws Exception
	 * @throws DatabaseUnitException
	 * @throws IOException
	 * @throws DataSetException
	 * 
	 */
	public void testApplyUpdatesSql() throws DataSetException, IOException,
			DatabaseUnitException, Exception {
		IApplier<Connection> apply = new ApplierSql();

		apply.applyUpdates(localUpdates, connection);
		// In real life the connection is committed and closed by the applier's
		// ILocalUpdater parent. But we have to do it here.
		connection.commit();
		connection.close();
		assertTrue(connection.isClosed());
		File expectedDataSetFile = new File(getClass().getResource(
				"finalState.xml").getPath());
		DbUnitUtil.checkDatabase(expectedDataSetFile, new IncludeTableFilter(
				new String[] { dbschema + ".*" }), tester, null);

	}
}
