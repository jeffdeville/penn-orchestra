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
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;

import org.custommonkey.xmlunit.Diff;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.IgnoreWhitespaceTextNodesDiff;
import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.reconciliation.StubSchemaIDBindingClient;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * A test of {@code Mapping} deserialization.
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = TestUtil.FAST_TESTNG_GROUP)
public class MappingVerboseSerializationTest {
	private Document systemMappings;
	private Document fakeMappingDoc;
	private OrchestraSystem system;

	/**
	 * Initializes the XML document.
	 * 
	 * @throws Exception
	 * 
	 */
	@BeforeClass
	public void initializeMappings() throws Exception {
		InputStream in = getClass().getResourceAsStream("systemMappings.xml");
		systemMappings = DomUtils.createDocument(in);
		in.close();
		in = getClass().getResourceAsStream("fakeMapping.xml");
		fakeMappingDoc = DomUtils.createDocument(in);
		in.close();
		// in = Config.class.getResourceAsStream("ppodLN/ppodLN.schema");
		// system = OrchestraSystem.deserialize(createDocument(in));
		// in.close();
	}

	/**
	 * Creates a new {@code OrchestraSystem} instance for each test method.
	 * 
	 * @throws Exception
	 */
	@BeforeMethod
	public void initializeOrchestraSystem() throws Exception {
		InputStream in = Config.class
				.getResourceAsStream("ppodLN/ppodLNHash.schema");
		Document schema = TestUtil
				.setLocalPeer(createDocument(in), "pPODPeer2");
		system = new OrchestraSystem(schema,
				new StubSchemaIDBindingClient.StubFactory(schema));
		in.close();
	}

	/**
	 * Making sure that we can deserialize {@code Mapping} instances.
	 * 
	 * @throws XMLParseException
	 * @throws IOException
	 */
	public void deserializationTest() throws XMLParseException, IOException {
		Element m = DomUtils.getChildElementByName(systemMappings
				.getDocumentElement(), "mapping");

		Mapping mapping = Mapping.deserializeVerboseMapping(m, system);
		assertNotNull(mapping);
		String xmlName = m.getAttribute("name");
		String objName = mapping.getId();
		assertEquals(objName, xmlName);

		Document controlDoc = DomUtils.createDocument();

		Node controlNode = controlDoc.importNode(m, true);
		controlDoc.appendChild(controlNode);

		Document testDoc = DomUtils.createDocument();
		Element testNode = mapping.serializeVerbose(testDoc);
		testDoc.appendChild(testNode);

		Diff diff = new IgnoreWhitespaceTextNodesDiff(controlDoc, testDoc);
		// DomUtils.write(testDoc, System.out);
		assertTrue(diff.identical(), diff.toString());
	}

	/**
	 * Make sure that fake mappings are detected.
	 * 
	 * @throws XMLParseException
	 */
	public void fakeMappingDeserializationTest() throws XMLParseException {
		Mapping fakeMapping = Mapping.deserializeVerboseMapping(fakeMappingDoc
				.getDocumentElement(), system);
		assertTrue(fakeMapping.isFakeMapping());
		Document roundTripDoc = DomUtils.createDocument();
		Element roundTripElement = fakeMapping.serializeVerbose(roundTripDoc);
		roundTripDoc.appendChild(roundTripElement);
		Diff diff = new IgnoreWhitespaceTextNodesDiff(fakeMappingDoc,
				roundTripDoc);
		assertTrue(diff.identical(), diff.toString());
	}

}
