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
package edu.upenn.cis.orchestra.datalog;

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
 * A test of {@Datalog} deserialization.
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = { TestUtil.FAST_TESTNG_GROUP })
public class DatalogDeserializationTest {
	private OrchestraSystem system;
	private Document datalogDocument;

	/**
	 * Initializes the XML document.
	 * 
	 * @throws Exception
	 * 
	 */
	@BeforeClass
	public void initializeDatalog() throws Exception {
		InputStream in = getClass().getResourceAsStream("datalog.xml");
		datalogDocument = DomUtils.createDocument(in);
		in.close();
		in = Config.class.getResourceAsStream("ppodLN/ppodLNHash.schema");
		Document schema = TestUtil
				.setLocalPeer(createDocument(in), "pPODPeer2");
		system = new OrchestraSystem(schema,
				new StubSchemaIDBindingClient.StubFactory(schema));
		in.close();
	}

	
	/**
	 * Checks that a round trip from serialized to deserialized to serialized is
	 * OK.
	 * 
	 * @throws XMLParseException
	 * @throws IOException
	 */
	public void deserializationTest() throws XMLParseException, IOException {
		Datalog datalog = Datalog.deserialize(datalogDocument
				.getDocumentElement(), system);
		assertNotNull(datalog);

		Document roundTripDoc = DomUtils.createDocument();
		Element roundTripElement = datalog.serialize(roundTripDoc);
		roundTripDoc.appendChild(roundTripElement);
		Diff diff = new IgnoreWhitespaceTextNodesDiff(datalogDocument,
				roundTripDoc);
		assertTrue(diff.identical(), diff.toString());

	}

}
