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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.reconciliation.StubSchemaIDBindingClient;

/**
 * Test that we correctly identify systems with bidirectional mappings.
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = { FAST_TESTNG_GROUP })
public class BidirDetectionTest {
	private Document bidirSystemDoc;
	private XPathExpression pathToMapping;
	private Element mappingElement;
	private OrchestraSystem bidirSystem;

	/**
	 * Set up a path to the mapping element.
	 * 
	 * @throws XPathExpressionException
	 */
	@BeforeClass
	public void setupXPath() throws XPathExpressionException {
		XPathFactory factory = XPathFactory.newInstance();
		XPath path = factory.newXPath();
		pathToMapping = path.compile("/catalog/mapping");
	}

	/**
	 * Read in the XML files.
	 * 
	 * @throws IOException
	 * @throws XPathExpressionException
	 * 
	 */
	@BeforeMethod()
	public void setupDocuments() throws IOException, XPathExpressionException {
		InputStream in = getClass().getResourceAsStream(
				"bidirDetectionTest.schema");
		bidirSystemDoc = createDocument(in);
		in.close();
		NodeList mappingNodes = (NodeList) pathToMapping.evaluate(
				bidirSystemDoc, XPathConstants.NODESET);
		assertTrue(mappingNodes.getLength() == 1,
				"There should only be one mapping.");
		mappingElement = (Element) mappingNodes.item(0);
	}

	/**
	 * An Orchestra schema file with a bidirectional mapping should result in a
	 * bidirectional {@code OrchestraSystem}.
	 * 
	 * @throws Exception
	 */
	public void bidirTest() throws Exception {
		mappingElement.setAttribute("bidirectional", "true");
		createOrchestraSystem();
		assertTrue(bidirSystem.isBidirectional());
	}

	/**
	 * An Orchestra schema file with a non-bidirectional mapping should result
	 * in a non-bidirectional {@code OrchestraSystem}.
	 * 
	 * @throws Exception
	 */
	public void notBidirTest() throws Exception {
		mappingElement.setAttribute("bidirectional", "false");
		createOrchestraSystem();
		assertFalse(bidirSystem.isBidirectional());
	}

	/**
	 * An Orchestra schema file with no mapping specifying bidirectionality one
	 * way or the other should result in a non-bidirectional {@code
	 * OrchestraSystem}.
	 * 
	 * @throws Exception
	 */
	public void defaultBidirTest() throws Exception {
		assertNull(mappingElement.getAttributeNode("bidirectional"));
		createOrchestraSystem();
		assertFalse(bidirSystem.isBidirectional());
	}

	private void createOrchestraSystem() throws Exception, InterruptedException {
		Document schema = TestUtil.setLocalPeer(bidirSystemDoc, "pPODPeer2");
		bidirSystem = new OrchestraSystem(schema,
				new StubSchemaIDBindingClient.StubFactory(schema));
	}

}
