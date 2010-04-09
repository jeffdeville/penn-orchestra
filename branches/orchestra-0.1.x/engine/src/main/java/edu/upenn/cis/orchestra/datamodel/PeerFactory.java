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

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.util.DomUtils.addChild;
import static edu.upenn.cis.orchestra.util.DomUtils.createDocument;
import static edu.upenn.cis.orchestra.util.DomUtils.getChildElementsByName;

import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateSchemaIdException;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient;
import edu.upenn.cis.orchestra.reconciliation.LocalSchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;

/**
 * Responsible for retrieving schema information from the update store and
 * creating the system's peers.
 * 
 * @author John Frommeyer
 * 
 */
public class PeerFactory {
	private final String cdss;
	private final ISchemaIDBindingClient schemaIDBindingClient;
	// private final Logger logger = LoggerFactory.getLogger(getClass());
	private final List<Element> peerElements;
	private ISchemaIDBinding localSchemaIDBinding;

	/**
	 * Construct the factory.
	 * 
	 * @param cdssName
	 * @param peerElements
	 * @param bindingClient
	 */
	public PeerFactory(String cdssName,
			@SuppressWarnings("hiding") final List<Element> peerElements,
			ISchemaIDBindingClient bindingClient) {
		cdss = cdssName;
		this.peerElements = peerElements;
		schemaIDBindingClient = bindingClient;
	}

	/**
	 * Returns the list of Peers specified by {@code orchestraSchema}. Schemas
	 * for these peers are retrieved from the update store.
	 * 
	 * @return the list of Peers specified by {@code orchestraSchema}
	 * @throws USException
	 * @throws DuplicateSchemaIdException
	 */
	public List<Peer> retrievePeers() throws USException,
			DuplicateSchemaIdException {
		List<Peer> peers = newArrayList();
		for (Element peerElement : peerElements) {
			peers.add(Peer.deserializePeerNoChildren(peerElement));
		}

		schemaIDBindingClient.reconnect();
		Map<AbstractPeerID, Schema> peerIDToSchema = schemaIDBindingClient
				.getAllSchemas(cdss);
		if (peerIDToSchema.isEmpty()) {
			peerIDToSchema = loadSchemas();
		}

		for (Peer peer : peers) {
			Schema s = peerIDToSchema.get(peer.getPeerId());
			peer.addSchema(s);
		}
		localSchemaIDBinding = new LocalSchemaIDBinding(peerIDToSchema);
		return peers;
	}

	private Map<AbstractPeerID, Schema> loadSchemas() throws USException {
		Map<AbstractPeerID, Schema> schemas;
		Document peerDoc = createDocument();
		Element root = addChild(peerDoc, peerDoc, cdss);
		for (Element peerElement : peerElements) {
			Node copy = peerDoc.importNode(peerElement, true);
			root.appendChild(copy);
		}
		schemas = schemaIDBindingClient.loadSchemas(peerDoc);
		return schemas;
	}

	/**
	 * @return the localSchemaIDBinding
	 */
	public ISchemaIDBinding getSchemaIDBinding() {
		return localSchemaIDBinding;
	}
}
