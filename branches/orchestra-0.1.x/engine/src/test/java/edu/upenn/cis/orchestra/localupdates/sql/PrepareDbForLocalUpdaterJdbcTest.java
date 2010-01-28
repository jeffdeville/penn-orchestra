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

import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.TestUtil.REQUIRES_DATABASE_TESTNG_GROUP;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import org.dbunit.Assertion;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatDtdDataSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.DbUnitUtil;
import edu.upenn.cis.orchestra.OrchestraSchema;
import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;

/**
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = { FAST_TESTNG_GROUP, REQUIRES_DATABASE_TESTNG_GROUP })
public class PrepareDbForLocalUpdaterJdbcTest {
	private OrchestraSystem system;
	private JdbcDatabaseTester tester;
	private FlatDtdDataSet expected;

	/**
	 * Initializes the XML document.
	 * 
	 * @param jdbcDriver
	 * @param dbURL
	 * @param dbUser
	 * @param dbPassword
	 * 
	 * @throws Exception
	 * 
	 */
	@BeforeClass
	@Parameters(value = { "jdbc-driver", "db-url", "db-user", "db-password" })
	public void initializeSystem(String jdbcDriver, String dbURL,
			String dbUser, String dbPassword) throws Exception {
		URL url = Config.class.getResource("ppodLN/ppodLN.schema");
		OrchestraSchema orchestraSchema = new OrchestraSchema(new File(url
				.toURI()));
		system = OrchestraSystem.deserialize2(orchestraSchema.toDocument(dbURL,
				dbUser, dbPassword, "pPODPeer2"));
		tester = new JdbcDatabaseTester(jdbcDriver, dbURL, dbUser, dbPassword);
		Config.setJDBCDriver(jdbcDriver);
		//Class.forName(jdbcDriver);
		IDatabaseConnection connection = DbUnitUtil
				.getConfiguredDbUnitConnection(tester);
		List<String> empty = Collections.emptyList();
		TestUtil.clearDb(connection.getConnection(), Collections
				.singletonList("PPOD2.OTU_PREV"), empty);
		expected = new FlatDtdDataSet(getClass().getResourceAsStream(
				"expectedPrevTableMetaData.dtd"));
		connection.close();
	}

	/**
	 * Tests the default {@code ILocalUpdater} properly prepares DB (via its
	 * {@code OrchestraSystem}.
	 * 
	 * @throws Exception
	 * 
	 */
	public void prepareDbTest() throws Exception {
		IDatabaseConnection testConnection = null;
		system.getMappingDb().connect();
		try {
			system.prepareSystemForLocalUpdater();
			testConnection = DbUnitUtil.getConfiguredDbUnitConnection(tester);
			IDataSet actual = testConnection.createDataSet(new String[] { "PPOD2.OTU_PREV" });
			Assertion.assertEquals(expected, actual);
		} finally {
			system.clearStoreServer();
			system.stopStoreServer();
			system.getMappingDb().finalize();
			if (testConnection != null) {
				testConnection.close();
			}
		}
	}
}
