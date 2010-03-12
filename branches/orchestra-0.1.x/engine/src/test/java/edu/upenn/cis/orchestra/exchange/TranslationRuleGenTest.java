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
package edu.upenn.cis.orchestra.exchange;

import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.util.DomUtils.createDocument;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import java.util.List;

import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.ElementNameAndAttributeQualifier;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.IgnoreWhitespaceTextNodesDiff;
import edu.upenn.cis.orchestra.OrchestraDifferenceListener;
import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.mappings.Rule;

/**
 * Testing {@code TranslationRuleGen}.
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = { FAST_TESTNG_GROUP })
public class TranslationRuleGenTest {
	private Document systemMappings;
	private Document userRelations;
	private Document builtInSchemas;
	private Document expectedTranslationRulesDoc;
	private OrchestraSystem system;

	/**
	 * Parses the setup files.
	 * 
	 * @throws Exception
	 */
	@BeforeClass
	public void setup() throws Exception {
		InputStream in = getClass().getResourceAsStream("systemMappings.xml");
		systemMappings = createDocument(in);
		in.close();
		in = getClass().getResourceAsStream("userRelations.xml");
		userRelations = createDocument(in);
		in.close();
		in = Config.class.getResourceAsStream("functions.schema");
		builtInSchemas = createDocument(in);
		in.close();
		in = getClass().getResourceAsStream("expectedTranslationRules.xml");
		expectedTranslationRulesDoc = createDocument(in);
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
		if (system != null) {
			system.clearStoreServer();
			system.stopStoreServer();
		}
	}
	
	/**
	 * Verifies the translation state produced by {@code TranslationRuleGen}.
	 * 
	 * @throws Exception
	 */
	public void translationRuleGenTest() throws Exception {
		ITranslationRuleGen ruleGen = TranslationRuleGen.newInstance(
				systemMappings, userRelations, builtInSchemas, system);

		List<Rule> sourceToTarget = ruleGen.computeTranslationRules();
		assertNotNull(sourceToTarget);
		assertFalse(sourceToTarget.isEmpty());
		// ruleGen.getState();
		Document actualTranslationRulesDoc = ruleGen.getState().serialize(
				OrchestraSystem.deserializeBuiltInFunctions(builtInSchemas));
		//DomUtils.write(actualTranslationRulesDoc, new FileWriter(
		//"actualTranslationRules.xml"));
		Diff diff = new IgnoreWhitespaceTextNodesDiff(
				expectedTranslationRulesDoc, actualTranslationRulesDoc);
		diff.overrideElementQualifier(new ElementNameAndAttributeQualifier());
		diff.overrideDifferenceListener(new OrchestraDifferenceListener());
		assertTrue(diff.similar(), diff.toString());
	}
}
