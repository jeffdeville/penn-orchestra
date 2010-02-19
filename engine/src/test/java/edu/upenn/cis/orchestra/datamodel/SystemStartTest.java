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
import static org.testng.Assert.assertEquals;

import java.io.InputStream;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.TestUtil;

/**
 * Testing changes to how an {@code OrchestraSystem} starts.
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = { FAST_TESTNG_GROUP/*, DEV_TESTNG_GROUP*/})
public class SystemStartTest {

	private Document document;

	/**
	 * Initializes the XML document.
	 * 
	 * @throws Exception
	 * 
	 */
	@BeforeClass
	public void initializeDocument() throws Exception {
		InputStream in = Config.class
				.getResourceAsStream("ppodLN/ppodLN.schema");
		document = createDocument(in);
		in.close();
	}

	/**
	 * What happens when an {@code OrchestraSystem} starts.
	 * @throws Exception 
	 * 
	 */
	public void startupTest() throws Exception {
		OrchestraSystem system = null;
		try {
			system = OrchestraSystem.deserialize(TestUtil
				.setLocalPeer(document, "pPODPeer2"));
			assertEquals(system.getPeers().size(), 2);
			
		} finally {
			if (system != null) {
				system.stopStoreServer();
				system.disconnect();
			}
		}
	}
}
