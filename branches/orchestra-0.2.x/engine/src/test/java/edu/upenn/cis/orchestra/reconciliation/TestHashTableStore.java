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

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Schema;

@Test
public class TestHashTableStore extends TestStore {

	StateStore getStore(AbstractPeerID ipi, ISchemaIDBinding sch, Schema s) throws Exception {
		return new HashTableStore(ipi, sch, -1);
	}

	@Override
	@AfterMethod
	public void tearDown() throws Exception {
		super.tearDown();
	}
	
	

}
