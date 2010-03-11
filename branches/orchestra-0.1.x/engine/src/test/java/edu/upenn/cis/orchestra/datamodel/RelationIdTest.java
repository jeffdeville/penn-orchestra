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

package edu.upenn.cis.orchestra.datamodel;

import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.util.DomUtils.createDocument;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;

/**
 * Verifies that each {@code OrchestraSystem} agrees on the integer ids of the
 * relations in the system.
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = { FAST_TESTNG_GROUP })
public class RelationIdTest {
	private OrchestraSystem firstSystem;
	private OrchestraSystem secondSystem;
	private static final String firstPeer = "pPODPeer1";
	private static final String secondPeer = "pPODPeer2";
	private static final String firstSchema = "pPODPeer1Schema1";
	private static final String secondSchema = "pPODPeer2Schema1";
	private static final String[] firstRelations = { "OTU", "OTU2" };
	private static final String[] secondRelations = { "OTU" };

	/**
	 * Instantiate the {@code OrchestraSystem}s.
	 * 
	 * @throws Exception
	 */
	@BeforeClass
	public void setupSystems() throws Exception {
		InputStream is = getClass()
				.getResourceAsStream("relationIDTest.schema");
		Document template = createDocument(is);
		Document firstSchemaDoc = TestUtil.setLocalPeer(template, firstPeer);
		Document secondSchemaDoc = TestUtil.setLocalPeer(template, secondPeer);
		firstSystem = OrchestraSystem.deserialize(firstSchemaDoc);
		secondSystem = OrchestraSystem.deserialize(secondSchemaDoc);

	}

	/**
	 * Verify that the systems agree on all relations.
	 * 
	 * @throws RelationNotFoundException
	 */
	public void relationIdTest() throws RelationNotFoundException {
		checkRelationId(firstPeer, firstSchema, firstRelations);
		checkRelationId(secondPeer, secondSchema, secondRelations);
	}

	/**
	 * Clear and stop the update store.
	 * 
	 * @throws Exception
	 */
	@AfterClass(alwaysRun = true)
	public void stopUpdateStore() throws Exception {
		firstSystem.disconnect();
		secondSystem.disconnect();
		firstSystem.clearStoreServer();
		firstSystem.stopStoreServer();
	}

	private void checkRelationId(String peer, String schema, String[] relations)
			throws RelationNotFoundException {
		for (String relation : relations) {
			RelationContext relationFromFirst = firstSystem.getRelationByName(
					peer, schema, relation);
			RelationContext relationFromSecond = secondSystem
					.getRelationByName(peer, schema, relation);
			assertTrue(relationFromFirst.getRelation().getRelationID() == relationFromSecond
					.getRelation().getRelationID());
		}
	}
}
