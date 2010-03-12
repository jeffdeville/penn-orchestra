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

import static edu.upenn.cis.orchestra.util.DomUtils.createDocument;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.InputStream;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * A test of {@code RelationContext} deserialization.
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = TestUtil.FAST_TESTNG_GROUP)
public class RelationContextSerializationTest {
	private Document userRelations;
	private Document provenanceRelation;
	private OrchestraSystem system;

	/**
	 * Initializes the XML document.
	 * 
	 * @throws Exception
	 * 
	 */
	@BeforeClass
	public void initializeRelations() throws Exception {
		InputStream in = getClass().getResourceAsStream("userRelations.xml");
		userRelations = DomUtils.createDocument(in);
		in.close();
		in = getClass().getResourceAsStream("provenanceRelation.xml");
		provenanceRelation = createDocument(in);
		in.close();
		in = Config.class.getResourceAsStream("ppodLN/ppodLNHash.schema");
		system = OrchestraSystem.deserialize(TestUtil.setLocalPeer(createDocument(in), "pPODPeer2"));
		in.close();
	}

	/**
	 * Clear and stop update store.
	 * 
	 * @throws Exception
	 */
	@AfterClass
	public void cleanupUpdateStore() throws Exception {
		system.clearStoreServer();
		system.stopStoreServer();
	}
	
	/**
	 * Making sure that we can deserialize {@code RelationContext} instances
	 * which wrap {@code ProvenanceRelation}s.
	 * 
	 * @throws XMLParseException
	 */
	public void provenanceRelationDeserializationTest()
			throws XMLParseException {
		Element rel = DomUtils.getChildElementByName(provenanceRelation
				.getDocumentElement(), "relationContext");
		RelationContext relationContext = RelationContext.deserialize(rel, system);
		assertNotNull(relationContext);
		String xmlName = rel
				.getAttribute("relation");
		String objName = relationContext.getRelation().getName();
		assertEquals(objName, xmlName);
	}

	
	
	/**
	 * Making sure that we can deserialize {@code RelationContext} instances.
	 * 
	 * @throws XMLParseException
	 */
	public void deserializationTest() throws XMLParseException {
		Element rel = DomUtils.getChildElementByName((Element) userRelations
				.getFirstChild(), "relationContext");
		RelationContext relationContext = RelationContext.deserialize(rel,
				system);
		assertNotNull(relationContext);
		String xmlName = rel
				.getAttribute("relation");
		String objName = relationContext.getRelation().getName();
		assertEquals(objName, xmlName);
	}

	
}
