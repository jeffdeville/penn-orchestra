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
import static edu.upenn.cis.orchestra.util.DomUtils.createDocument;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.reconciliation.StubSchemaIDBindingClient;

/**
 * Test that we can correctly determine when a given tuple is derivable from
 * some mapping.
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = { FAST_TESTNG_GROUP, REQUIRES_DATABASE_TESTNG_GROUP })
public class DerivabilityCheckTest {
	private OrchestraSystem system;
	private Connection connection;

	/**
	 * Create the {@code OrchestraSystem} we will use.
	 * 
	 * @throws Exception
	 */
	@BeforeClass
	public void setup() throws Exception {
		InputStream in = getClass().getResourceAsStream(
				"derivabilityCheckTest.schema");
		Document schema = TestUtil
				.setLocalPeer(createDocument(in), "pPODPeer1");
		system = new OrchestraSystem(schema,
				new StubSchemaIDBindingClient.StubFactory(schema));
		in.close();
	}

	/**
	 * Sets up the connection with parameters passed in by testng. Also sets
	 * the initial state of database.
	 * 
	 * @param jdbcDriver
	 * @param dbURL
	 * @param dbUser
	 * @param dbPassword
	 * @throws Exception
	 */
	@BeforeClass
	@Parameters(value = { "jdbc-driver", "db-url", "db-user", "db-password" })
	public final void initDatabase(String jdbcDriver, String dbURL,
			String dbUser, String dbPassword) throws Exception {

		Properties connectionProperties = new Properties();
		connectionProperties.setProperty("user", dbUser);
		connectionProperties.setProperty("password", dbPassword);
		System.setProperty("jdbc.drivers", jdbcDriver);

		connection = DriverManager.getConnection(dbURL, connectionProperties);
		TestUtil.clearDb(connection, newArrayList("DERIVABILITY2.PM0"),
				Collections.singletonList("DERIVABILITY2"));
		File sqlScript = new File(getClass().getResource(
				"derivabilityschema.sql").getPath());
		TestUtil.executeSqlScript(connection, sqlScript);

	}

	/**
	 * Makes sure the connection is closed.
	 * 
	 * @throws SQLException
	 */
	@AfterClass(alwaysRun = true)
	public final void closeConnection() throws SQLException {
		if (connection != null) {
			connection.close();
		}
	}

	/**
	 * The test.
	 * 
	 * @throws Exception
	 */
	public void derivabilityTest() throws Exception {
		IDerivabilityCheck checker = system.getDerivabilityCheck();
		List<Schema> allSchemas = system.getAllSchemas();
		for (Schema s : allSchemas) {
			Collection<Relation> relations = s.getRelations();
			for (Relation r : relations) {
				Tuple tuple = new Tuple(r);
				Tuple tupleLN = new Tuple(r);
				Tuple notDerivable = new Tuple(r);
				tuple.set("OTU_ID", Integer.valueOf(1));
				tupleLN.set("OTU_ID", Integer.valueOf(10));
				notDerivable.set("OTU_ID", Integer.valueOf(100));
				tuple.set("LABEL", "Homo Sapiens");
				tupleLN.set("LABEL", "Primates");
				notDerivable.set("LABEL", "Carnivora");
				tuple.set("OBJ_VERSION", Integer.valueOf(2));
				tupleLN.setLabeledNull("OBJ_VERSION", 20);
				notDerivable.set("OBJ_VERSION", Integer.valueOf(200));
				tuple.set("PPOD_VERSION", Integer.valueOf(3));
				tupleLN.set("PPOD_VERSION", Integer.valueOf(30));
				notDerivable.set("PPOD_VERSION", Integer.valueOf(300));
				if ("derivability2.OTU".equals(r.getFullQualifiedDbId())) {
					assertTrue(checker.isDerivable(tuple, connection),
							"Should be derivable: " + tuple);
					assertTrue(checker.isDerivable(tupleLN, connection),
							"Should be derivable: " + tupleLN);
				} else {
					assertFalse(checker.isDerivable(tuple, connection),
							"Should not be derivable: " + tuple);
					assertFalse(checker.isDerivable(tupleLN, connection),
							"Should not be derivable: " + tupleLN);
				}
				assertFalse(checker.isDerivable(notDerivable, connection),
						"Should not be derivable: " + notDerivable);
			}
		}
	}

}
