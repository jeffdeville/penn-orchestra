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
package edu.upenn.cis.orchestra.deltaRules;

import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.util.DomUtils.createDocument;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;

import org.custommonkey.xmlunit.Diff;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.IgnoreWhitespaceTextNodesDiff;
import edu.upenn.cis.orchestra.OrchestraDifferenceListener;
import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.reconciliation.StubSchemaIDBindingClient;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * Testing {@code DeltaRuleGen}.
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = { FAST_TESTNG_GROUP })
public class DeltaRuleGenTest {
	private Document builtInSchemas;
	private Document translationRulesDoc;
	private Document expectedDeletionRulesDoc;
	private Document expectedInsertionRulesDoc;
	private OrchestraSystem system;

	/**
	 * Parses the setup files.
	 * 
	 * @throws Exception
	 */
	@BeforeClass
	public void setup() throws Exception {
		InputStream in = Config.class.getResourceAsStream("functions.schema");
		builtInSchemas = createDocument(in);
		in.close();
		in = getClass().getResourceAsStream("translationRules.xml");
		translationRulesDoc = createDocument(in);
		in.close();
		in = getClass().getResourceAsStream("expectedDeletionRules.xml");
		expectedDeletionRulesDoc = createDocument(in);
		in.close();
		in = getClass().getResourceAsStream("expectedInsertionRules.xml");
		expectedInsertionRulesDoc = createDocument(in);
		in.close();
		in = Config.class.getResourceAsStream("ppodLN/ppodLNHash.schema");
		Document schema = TestUtil
				.setLocalPeer(createDocument(in), "pPODPeer2");
		system = new OrchestraSystem(schema,
				new StubSchemaIDBindingClient.StubFactory(schema));
		in.close();

	}

	/**
	 * Test deletion rule generation.
	 * 
	 * @throws XMLParseException
	 * @throws IOException
	 */
	public void deletionRulesTest() throws XMLParseException, IOException {
		IDeltaRuleGen delRuleGen = new DeletionDeltaRuleGen(
				translationRulesDoc, builtInSchemas, system);
		IDeltaRules delRules = delRuleGen.getDeltaRules();
		Document actualRules = delRules.serialize();

		// DomUtils.write(actualRules, new
		// FileWriter("actualDeletionRules.xml"));
		// Document doc = toSqlDoc(delRules.getCode());
		// write(doc, new FileWriter("deletionSql.xml"));

		Diff diff = new IgnoreWhitespaceTextNodesDiff(expectedDeletionRulesDoc,
				actualRules);
		diff.overrideDifferenceListener(new OrchestraDifferenceListener());
		assertTrue(diff.similar(), diff.toString());
	}

	/**
	 * Test insertion rule generation.
	 * 
	 * @throws XMLParseException
	 * @throws IOException
	 */
	public void insertionRulesTest() throws XMLParseException, IOException {
		IDeltaRuleGen insRuleGen = new InsertionDeltaRuleGen(
				translationRulesDoc, builtInSchemas, system);
		IDeltaRules insRules = insRuleGen.getDeltaRules();
		Document actualRules = insRules.serialize();

		// DomUtils.write(actualRules, new
		// FileWriter("actualInsertionRules.xml"));
		// Document doc = insRules.serializeAsCode();
		// DomUtils.write(doc, new FileWriter("insertionSql.xml"));

		Diff diff = new IgnoreWhitespaceTextNodesDiff(
				expectedInsertionRulesDoc, actualRules);
		diff.overrideDifferenceListener(new OrchestraDifferenceListener());
		assertTrue(diff.similar(), diff.toString());
	}
}
