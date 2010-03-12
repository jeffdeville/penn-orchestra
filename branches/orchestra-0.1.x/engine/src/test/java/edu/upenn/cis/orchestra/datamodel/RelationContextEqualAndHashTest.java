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

import static edu.upenn.cis.orchestra.OrchestraUtil.newHashMap;
import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.util.DomUtils.createDocument;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import java.util.Map;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.TestUtil;

/**
 * Testing {@code Peer.equals()} and {@code hashCode()}.
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = { FAST_TESTNG_GROUP, "equals-tests" })
public class RelationContextEqualAndHashTest {

	private Element relationContextElement;
	private RelationContext rc1;
	private RelationContext rc2;
	private OrchestraSystem system;
	private Document orchestraSystemDoc;

	/**
	 * Parse the serialized relation context into an {@code Element}.
	 * 
	 * @throws Exception
	 * 
	 */
	@BeforeClass
	public void parseDocument() throws Exception {
		InputStream in = getClass().getResourceAsStream("relationContext.xml");
		relationContextElement = createDocument(in).getDocumentElement();
		in.close();
		in = Config.class.getResourceAsStream("ppodLN/ppodLNHash.schema");
		orchestraSystemDoc = createDocument(in);
		in.close();
	}

	/**
	 * Create the contexts.
	 * 
	 * @throws Exception
	 */
	@BeforeMethod
	public void deserialize() throws Exception {
		system = OrchestraSystem.deserialize(TestUtil.setLocalPeer(orchestraSystemDoc, "pPODPeer2"));
		rc1 = RelationContext.deserialize(relationContextElement, system);
		rc2 = RelationContext.deserialize(relationContextElement, system);

	}

	/**
	 * Clear and stop update store.
	 * 
	 * @throws Exception
	 */
	@AfterMethod
	public void cleanupUpdateStore() throws Exception {
		system.clearStoreServer();
		system.stopStoreServer();
	}
	
	/**
	 * Should have {@code x1.equals(x2)} => {@code x1.hashCode() ==
	 * x2.hashCode()}.
	 * 
	 */
	public void equalsAndHashTest() {
		assertTrue(rc1.equals(rc2), ".equals() returned false.");
		assertTrue(rc2.equals(rc1), ".equals() returned false.");
		assertTrue(rc1.hashCode() == rc2.hashCode(),
				"Objects were .equal() with non-equal hashCode().");
	}

	/**
	 * If two {@code RelationContext} represent the same relation, then the
	 * internal objects should also be {@code .equals()}.
	 * 
	 */
	public void stateTest() {
		Map<Relation, RelationContext> relationToContext = newHashMap();
		assertTrue(rc1.getRelation().equals(rc2.getRelation()));
		assertTrue(rc1.getSchema().equals(rc2.getSchema()));
		assertTrue(rc1.getPeer().equals(rc2.getPeer()));

		assertTrue(rc2.getRelation().equals(rc1.getRelation()));
		assertTrue(rc2.getSchema().equals(rc1.getSchema()));
		assertTrue(rc2.getPeer().equals(rc1.getPeer()));

		relationToContext.put(rc1.getRelation(), rc1);
		relationToContext.put(rc2.getRelation(), rc2);

		int mapSize = relationToContext.size();
		assertTrue(mapSize == 1, "Expected mapSize == 1, but it was " + mapSize);
	}
}
