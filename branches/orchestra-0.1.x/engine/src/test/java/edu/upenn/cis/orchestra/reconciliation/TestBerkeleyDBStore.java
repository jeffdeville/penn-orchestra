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

import java.io.File;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Schema;

@Test
public class TestBerkeleyDBStore extends TestStore {
	private Environment e;
	@Override
	StateStore getStore(AbstractPeerID pi, ISchemaIDBinding scm, Schema s) throws Exception {
		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setAllowCreate(true);
		ec.setTransactional(true);
		File f = new File("dbenv");
		if (f.exists()) {
			File[] files = f.listFiles();
			for (File file : files) {
				file.delete();
			}
		} else {
			f.mkdir();
		}
		e = new Environment(f, ec);
		return new BerkeleyDBStore(e, "state", "updates", pi, scm, -1);

	}
	
	@AfterMethod
	public void tearDown() throws Exception {
		super.tearDown();
		((BerkeleyDBStore) ss).close();
		//Environment is closed by store.
	}

}
