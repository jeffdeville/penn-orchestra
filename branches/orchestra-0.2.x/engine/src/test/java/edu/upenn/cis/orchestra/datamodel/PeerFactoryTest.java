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
import static edu.upenn.cis.orchestra.util.DomUtils.getChildElementsByName;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateMappingIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateRelationIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateSchemaIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient;
import edu.upenn.cis.orchestra.reconciliation.StubSchemaIDBindingClient;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * Testing {@code PeerFactory}.
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = { FAST_TESTNG_GROUP })
public class PeerFactoryTest {
	private Document orchestraSchema;
	private Element pPODPeer1Element;
	private final String peerName = "pPODPeer1";
	private UpdateStore.Factory usFactory;

	/**
	 * Setup common objects.
	 * 
	 * @throws IOException
	 * @throws XMLParseException
	 */
	@BeforeClass
	public void readSchema() throws IOException, XMLParseException {
		InputStream in = Config.class
				.getResourceAsStream("ppodLN/ppodLNHash.schema");
		orchestraSchema = createDocument(in);
		in.close();
		List<Element> peerElements = getChildElementsByName(orchestraSchema
				.getDocumentElement(), "peer");
		for (Element peer : peerElements) {
			if (peerName.equals(peer.getAttribute("name"))) {
				pPODPeer1Element = peer;
				break;
			}
		}
		assertNotNull(pPODPeer1Element);
		usFactory = new StubSchemaIDBindingClient.StubFactory(orchestraSchema);
	}

	/**
	 * Make sure we can get Peers from the factory.
	 * 
	 * @throws XMLParseException
	 * @throws RelationNotFoundException
	 * @throws UnsupportedTypeException
	 * @throws DuplicateMappingIdException
	 * @throws UnknownRefFieldException
	 * @throws DuplicateRelationIdException
	 * @throws DuplicateSchemaIdException
	 * @throws USException
	 */
	public void peerFactoryTest() throws XMLParseException,
			DuplicateSchemaIdException, DuplicateRelationIdException,
			UnknownRefFieldException, DuplicateMappingIdException,
			UnsupportedTypeException, RelationNotFoundException, USException {
		ISchemaIDBindingClient client = usFactory.getSchemaIDBindingClient();
		client.reconnect();
		List<Peer> peers = getPeersFromNewClient();
		client.disconnect();
		assertTrue(peers.size() == 2);
	}

	private List<Peer> getPeersFromNewClient() throws USException,
			DuplicateSchemaIdException {

		ISchemaIDBindingClient client = usFactory.getSchemaIDBindingClient();
		client.reconnect();
		Element catalog = orchestraSchema.getDocumentElement();
		PeerFactory factory1 = new PeerFactory(catalog.getAttribute("name"),
				getChildElementsByName(catalog, "peer"), client);
		List<Peer> peer1 = factory1.retrievePeers();
		assertNotNull(peer1);
		client.disconnect();
		return peer1;
	}

}
