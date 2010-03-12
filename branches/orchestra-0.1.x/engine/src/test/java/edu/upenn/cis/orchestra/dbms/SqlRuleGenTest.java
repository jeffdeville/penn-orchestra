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
package edu.upenn.cis.orchestra.dbms;

import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.util.DomUtils.createDocument;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;

import org.custommonkey.xmlunit.Diff;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.IgnoreWhitespaceTextNodesDiff;
import edu.upenn.cis.orchestra.OrchestraDifferenceListener;
import edu.upenn.cis.orchestra.TestUtil;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.deltaRules.DeltaRules;
import edu.upenn.cis.orchestra.deltaRules.IDeltaRules;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * Testing {@code SqlRuleGen}.
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = { FAST_TESTNG_GROUP })
public class SqlRuleGenTest {
	private Document deletionRulesDoc;
	private Document insertionRulesDoc;
	private Document expectedDeletionCodeDoc;
	private Document expectedInsertionCodeDoc;
	private OrchestraSystem system;

	/**
	 * Parses the setup files.
	 * 
	 * @throws Exception
	 */
	@BeforeClass
	public void setup() throws Exception {
		InputStream in = getClass().getResourceAsStream("deletionRules.xml");
		deletionRulesDoc = createDocument(in);
		in.close();
		in = getClass().getResourceAsStream("insertionRules.xml");
		insertionRulesDoc = createDocument(in);
		in.close();
		in = getClass().getResourceAsStream("expectedDeletionCode.xml");
		expectedDeletionCodeDoc = createDocument(in);
		in.close();
		in = getClass().getResourceAsStream("expectedInsertionCode.xml");
		expectedInsertionCodeDoc = createDocument(in);
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
	 * Test deletion sql generation.
	 * 
	 * @throws XMLParseException
	 * @throws IOException
	 */
	public void deletionCodeTest() throws XMLParseException, IOException {
		IDeltaRules delRules = DeltaRules.deserialize(deletionRulesDoc, system);
		Document actualDeletionCodeDoc = delRules.serializeAsCode();
		//write(actualDeletionCodeDoc, new FileWriter("actualDelCode.xml"));
		Diff diff = new IgnoreWhitespaceTextNodesDiff(expectedDeletionCodeDoc,
				actualDeletionCodeDoc);
		diff.overrideDifferenceListener(new OrchestraDifferenceListener());
		assertTrue(diff.identical(), diff.toString());
		
		
	}

	/**
	 * Test insertion sql generation.
	 * 
	 * @throws XMLParseException
	 * @throws IOException
	 */
	public void insertionCodeTest() throws XMLParseException, IOException {
		IDeltaRules insRules = DeltaRules.deserialize(insertionRulesDoc, system);
		Document actualInsertionCodeDoc = insRules.serializeAsCode();
		//write(actualInsertionCodeDoc, new FileWriter("actualInsCode.xml"));
		Diff diff = new IgnoreWhitespaceTextNodesDiff(
				expectedInsertionCodeDoc, actualInsertionCodeDoc);
		diff.overrideDifferenceListener(new OrchestraDifferenceListener());
		assertTrue(diff.identical(), diff.toString());
		
	}
	
	/**
	 * Running {@code serializeAsCode()} twice should yield the same result as running it once.
	 * Bug 109
	 * @throws XMLParseException
	 * @throws IOException
	 */
	public void idempotentTest() throws XMLParseException, IOException {
		IDeltaRules insRules = DeltaRules.deserialize(insertionRulesDoc, system);
		insRules.serializeAsCode();
		
		Document regeneratedCodeDoc = insRules.serializeAsCode();
		//write(regeneratedCodeDoc, new FileWriter("regeneratedInsCode.xml"));
		Diff secondDiff = new IgnoreWhitespaceTextNodesDiff(expectedInsertionCodeDoc,
				regeneratedCodeDoc);
		secondDiff.overrideDifferenceListener(new OrchestraDifferenceListener());
		assertTrue(secondDiff.identical(), secondDiff.toString());
	}
}
