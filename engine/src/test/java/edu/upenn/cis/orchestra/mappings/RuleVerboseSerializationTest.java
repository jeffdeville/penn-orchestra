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
package edu.upenn.cis.orchestra.mappings;

import static edu.upenn.cis.orchestra.util.DomUtils.createDocument;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;

import org.custommonkey.xmlunit.Diff;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.IgnoreWhitespaceTextNodesDiff;
import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.reconciliation.StubSchemaIDBindingClient;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * A test of {@code Rule} deserialization.
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = {TestUtil.FAST_TESTNG_GROUP})
public class RuleVerboseSerializationTest {
	private Document ruleDocument;
	private Document fakeMappingRuleDoc;
	private OrchestraSystem system;
	
	/**
	 * Initializes the XML document.
	 * @throws Exception 
	 * 
	 */
	@BeforeClass
	public void initializeMappings() throws Exception {
		InputStream in = getClass().getResourceAsStream("rule.xml");
		ruleDocument = DomUtils.createDocument(in);
		in.close();
		in = getClass().getResourceAsStream("fakeMappingRule.xml");
		fakeMappingRuleDoc = DomUtils.createDocument(in);
		in.close();
		in = Config.class.getResourceAsStream("ppodLN/ppodLNHash.schema");
		Document schema = TestUtil.setLocalPeer(createDocument(in), "pPODPeer2");
		system = new OrchestraSystem(schema, new StubSchemaIDBindingClient.StubFactory(schema));
		in.close();
	}

	
	
	/**
	 * Making sure that we can deserialize {@code Rule} instances.
	 * @throws XMLParseException 
	 * 
	 * @throws XMLParseException
	 * @throws IOException 
	 * @throws IOException
	 */
	public void deserializationTest() throws XMLParseException, IOException {
		Rule rule = Rule.deserializeVerboseRule(ruleDocument.getDocumentElement(), system);
		assertNotNull(rule, "Deserialized Rule was null.");
		
		Document roundTripDocument = DomUtils.createDocument();
		Element roundTripElement = rule.serializeVerbose(roundTripDocument);
		roundTripDocument.appendChild(roundTripElement);
		Diff diff = new IgnoreWhitespaceTextNodesDiff(ruleDocument, roundTripDocument);
		assertTrue(diff.identical(), diff.toString());
	}
	
	/**
	 * Testing that fakeMappings are detected.
	 * 
	 * @throws XMLParseException
	 */
	public void fakeMappingRuleTest() throws XMLParseException {
		Rule fakeMappingRule = Rule.deserializeVerboseRule(fakeMappingRuleDoc.getDocumentElement(), system);
		assertTrue(fakeMappingRule.isFakeMapping());
	}
}
