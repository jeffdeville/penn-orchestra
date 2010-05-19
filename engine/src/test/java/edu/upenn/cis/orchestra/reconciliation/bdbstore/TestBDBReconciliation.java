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
package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.reconciliation.TestReconciliation;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory;

@Test
public class TestBDBReconciliation extends TestReconciliation {
	BerkeleyDBStoreServer server;
	Environment e;
	File envDir = new File("bdbstoredir");
	
	@BeforeMethod
	@Override
	public void setUp() throws Exception {
		super.setUp();
		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setAllowCreate(true);
		ec.setTransactional(true);
		//ec.setCachePercent(1);
		if (envDir.isDirectory()) {
			for (File f : envDir.listFiles()) {
				f.delete();
			}
		}
		envDir.delete();
		envDir.mkdir();
		e = new Environment(envDir,ec);
		server = new BerkeleyDBStoreServer(e);
		server.registerAllSchemas(SCHEMA_NAME, schemas, peerMap);
	}
	
	@AfterMethod
	@Override
	public void tearDown() throws Exception {
		super.tearDown();
		server.quit();
	}
	

	@Override
	protected void clearState(Schema s) throws Exception {
		// This should be taken care of by tearDown

	}

	@Override
	protected Factory getStoreFactory() {
		try {
			return new BerkeleyDBStoreClient.Factory(new InetSocketAddress(InetAddress.getLocalHost(), BerkeleyDBStoreServer.DEFAULT_PORT), null);
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	
}
