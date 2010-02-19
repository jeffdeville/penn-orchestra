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

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.datamodel.IntPeerID;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.reconciliation.BerkeleyDBStore;
import edu.upenn.cis.orchestra.reconciliation.ClientCentricDb;
import edu.upenn.cis.orchestra.reconciliation.Db;
import edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.StateStore;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore;
import edu.upenn.cis.orchestra.reconciliation.bdbstore.BerkeleyDBStoreClient;
import edu.upenn.cis.orchestra.reconciliation.bdbstore.BerkeleyDBStoreServer;

public class CreateWorkloadBDB {
	static final File envDir = new File("bdbstoredir");
	static final File configFile = new File("bdbstoreconf");
	

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		BerkeleyDBStoreServer bdss;
		// TODO Auto-generated method stub
		if (envDir.isDirectory()) {
			for (File f : envDir.listFiles()) {
				f.delete();
			}
		}
		envDir.delete();
		envDir.mkdir();
		configFile.delete();
		
		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setAllowCreate(true);
		ec.setTransactional(true);

		Environment e = new Environment(envDir,ec);
		bdss = new BerkeleyDBStoreServer(e);
		
		CreateWorkload cw = new CreateWorkload();
		try {
			cw.createWorkload(new BDBFactory(), args);
		} catch (Exception exc) {
			System.err.println("Caught exception: " + exc);
			exc.printStackTrace(System.err);
		} finally {
			bdss.quit();
			e.close();
		}
	}


	static class BDBFactory implements CreateWorkload.DatabaseFactory {
		UpdateStore.Factory usf;
		BDBFactory() throws Exception {
			usf = new BerkeleyDBStoreClient.Factory(new InetSocketAddress(InetAddress.getLocalHost(), BerkeleyDBStoreServer.DEFAULT_PORT));
		}

		public Db createDb(int id, Schema s, Environment env) throws Exception {
			StateStore.Factory ssf = new BerkeleyDBStore.Factory(env, "statestore_state" + id, "statestore_updates" + id);
			SchemaIDBinding scm = new SchemaIDBinding(env); 
			return new ClientCentricDb(scm, s, new IntPeerID(id), usf, ssf);
		}

		public void initDb(Schema s) throws Exception {
		}
		
	}
}
