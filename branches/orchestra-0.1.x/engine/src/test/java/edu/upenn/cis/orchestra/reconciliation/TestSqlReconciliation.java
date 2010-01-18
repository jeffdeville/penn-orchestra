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
package edu.upenn.cis.orchestra.reconciliation;

import static edu.upenn.cis.orchestra.TestUtil.REQUIRES_DATABASE_TESTNG_GROUP;

import java.sql.SQLException;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory;

@Test(groups = {REQUIRES_DATABASE_TESTNG_GROUP})
public class TestSqlReconciliation extends TestReconciliation {
	private String jdbcUrl = "jdbc:db2://localhost:50000/orchestr";
	private String username = "";
	private String password = "";
	private String jdbcDriver = "com.ibm.db2.jcc.DB2Driver";

	SqlUpdateStore.Factory factory;

	@Parameters( { "jdbc-url", "username", "password", "jdbc-driver" })
	public TestSqlReconciliation(String jdbcUrlParam, String usernameParam,
			String passwordParam, String jdbcDriverParam)
			throws ClassNotFoundException, SQLException,
			UpdateStore.USException {
		jdbcUrl = jdbcUrlParam;
		username = usernameParam;
		password = passwordParam;
		jdbcDriver = jdbcDriverParam;
		Class.forName(jdbcDriver);
		factory = new SqlUpdateStore.Factory(jdbcUrl, username, password);
	}

	public TestSqlReconciliation() throws ClassNotFoundException, SQLException,
			UpdateStore.USException {
		Class.forName(jdbcDriver);
		factory = new SqlUpdateStore.Factory(jdbcUrl, username, password);
	}

	@Override
	protected Factory getStoreFactory() {
		return factory;
	}

	protected void clearState(Schema s) throws Exception {
		factory.resetStore(s);
	}

}
