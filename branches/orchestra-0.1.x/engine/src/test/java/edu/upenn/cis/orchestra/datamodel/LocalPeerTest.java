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

import static edu.upenn.cis.orchestra.TestUtil.BROKEN_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.TestUtil.setLocalPeer;
import static edu.upenn.cis.orchestra.util.DomUtils.createDocument;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.exceptions.NoLocalPeerException;

/**
 * DOCUMENT ME
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = { FAST_TESTNG_GROUP})
public class LocalPeerTest {

	private Document schemaTemplate;
	OrchestraSystem system;

	/**
	 * Read in Orchestra Schema template.
	 * 
	 */
	@BeforeClass
	public void setUpDocument() {
		schemaTemplate = createDocument(Config.class
				.getResourceAsStream("ppodLN/ppodLNHash.schema"));
		// write(doc, System.err);
	}

	/**
	 * Clear and stop update store.
	 * 
	 * @throws Exception
	 */
	@AfterMethod
	public void cleanupUpdateStore() throws Exception {
		if (system != null) {
			system.clearStoreServer();
			system.stopStoreServer();
		}
	}
	
	/**
	 * Make sure we set the correct peer as local
	 * 
	 * @throws Exception
	 */
	public void localPeerTest() throws Exception {

		system = OrchestraSystem.deserialize(setLocalPeer(
				schemaTemplate, "pPODPeer2"));
		assertTrue(system.isLocalPeer(system.getPeer("pPODPeer2")));
		assertFalse(system.isLocalPeer(system.getPeer("pPODPeer1")));
	}

	/**
	 * No local peer should throw an exception.
	 * 
	 * @throws Exception
	 */
	@Test(expectedExceptions = { NoLocalPeerException.class }, groups = { BROKEN_TESTNG_GROUP })
	public void noLocalPeerTest() throws Exception {

		system = OrchestraSystem.deserialize(schemaTemplate);

	}
}
