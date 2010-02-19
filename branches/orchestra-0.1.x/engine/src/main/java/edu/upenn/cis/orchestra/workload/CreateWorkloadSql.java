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
package edu.upenn.cis.orchestra.workload;

import com.sleepycat.je.Environment;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.IntPeerID;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.reconciliation.BerkeleyDBStore;
import edu.upenn.cis.orchestra.reconciliation.ClientCentricDb;
import edu.upenn.cis.orchestra.reconciliation.Db;
import edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.SqlUpdateStore;
import edu.upenn.cis.orchestra.reconciliation.StateStore;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore;

public class CreateWorkloadSql {
	static String jdbcUrl;
	static String username;
	static String password;
	public static void main(String[] args) throws Exception {
		Class.forName(Config.getJDBCDriver());
		CreateWorkload cw = new CreateWorkload();
		cw.createWorkload(new SqlFactory(), args);
	}
	static class SqlFactory implements CreateWorkload.DatabaseFactory {
		UpdateStore.Factory usf;
		SqlFactory() {
			usf = new SqlUpdateStore.Factory(jdbcUrl, username, password);
		}
		public Db createDb(int id, Schema s, Environment env) throws Exception {
			StateStore.Factory ssf = new BerkeleyDBStore.Factory(env, "statestore_state" + id, "statestore_updates" + id);
			SchemaIDBinding scm = new SchemaIDBinding(env); 
			return new ClientCentricDb(scm, s, new IntPeerID(id), usf, ssf);
		}

		public void initDb(Schema s) throws Exception {
			usf.resetStore(s);
		}
	}
}
