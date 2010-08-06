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

package edu.upenn.cis.orchestra.reconciliation;

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.OrchestraUtil.newHashMap;
import static edu.upenn.cis.orchestra.util.DomUtils.getChildElementsByName;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.TrustConditions;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateRelationIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;
import edu.upenn.cis.orchestra.reconciliation.bdbstore.SchemaIDBinding;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * A client to be used for tests which only requires an update store factory to
 * create datamodel objects and nothing else. No Berkeley database is used.
 * 
 * @author John Frommeyer
 * 
 */
public class StubSchemaIDBindingClient implements ISchemaIDBindingClient {

	/**
	 * A factory to be used for tests which only requires an update store
	 * factory to create datamodel objects and nothing else. No Berkeley
	 * database is used.
	 * <p>
	 * All methods except {@code getSchemaIDBindingClient()} either do nothing
	 * or return {@code null} or {@code false}.
	 * <p>
	 * {@code getSchemaIDBindingClient()} returns a {@code
	 * StubSchemaIDBindingClient}.
	 * 
	 * @author John Frommeyer
	 * 
	 */
	public static class StubFactory implements UpdateStore.Factory {

		private final Document schemaDoc;

		/**
		 * Given an Orchestra schema, create a {@code Factory} which will allow
		 * the Orchestra objects to be created in memory, without a real update
		 * store.
		 * 
		 * @param schema
		 */
		public StubFactory(Document schema) {
			this.schemaDoc = schema;
		}

		/**
		 * {@inheritDoc}
		 * 
		 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#dumpUpdateStore(edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding,
		 *      edu.upenn.cis.orchestra.datamodel.Schema)
		 */
		@Override
		public USDump dumpUpdateStore(ISchemaIDBinding binding, Schema schema)
				throws USException {
			throw new UnsupportedOperationException();//return null;
		}

		/**
		 * {@inheritDoc}
		 * 
		 * @throws RelationNotFoundException
		 * @throws UnsupportedTypeException
		 * @throws XMLParseException
		 * @throws UnknownRefFieldException
		 * @throws DuplicateRelationIdException
		 * 
		 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#getSchemaIDBindingClient()
		 */
		@Override
		public ISchemaIDBindingClient getSchemaIDBindingClient() {
			return new StubSchemaIDBindingClient(schemaDoc);
		}

		/**
		 * {@inheritDoc}
		 * 
		 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#getUpdateStore(edu.upenn.cis.orchestra.datamodel.AbstractPeerID,
		 *      edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding,
		 *      edu.upenn.cis.orchestra.datamodel.Schema,
		 *      edu.upenn.cis.orchestra.datamodel.TrustConditions)
		 */
		@Override
		public UpdateStore getUpdateStore(AbstractPeerID pid,
				ISchemaIDBinding sch, Schema s, TrustConditions tc)
				throws USException {
			throw new UnsupportedOperationException();//return null;
		}

		/**
		 * {@inheritDoc}
		 * 
		 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#isLocal()
		 */
		@Override
		public boolean isLocal() {
			throw new UnsupportedOperationException();//return false;
		}

		/**
		 * {@inheritDoc}
		 * 
		 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#resetStore(edu.upenn.cis.orchestra.datamodel.Schema)
		 */
		@Override
		public void resetStore(Schema s) throws USException {}

		/**
		 * {@inheritDoc}
		 * 
		 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#restoreUpdateStore(edu.upenn.cis.orchestra.reconciliation.USDump)
		 */
		@Override
		public void restoreUpdateStore(USDump d) throws USException {throw new UnsupportedOperationException();}

		/**
		 * {@inheritDoc}
		 * 
		 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#serialize(org.w3c.dom.Document,
		 *      org.w3c.dom.Element)
		 */
		@Override
		public void serialize(Document doc, Element update) {throw new UnsupportedOperationException();}

		/**
		 * {@inheritDoc}
		 * 
		 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#startUpdateStoreServer()
		 */
		@Override
		public Process startUpdateStoreServer() throws USException {
			throw new UnsupportedOperationException();//return null;
		}

		/**
		 * {@inheritDoc}
		 * 
		 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#stopUpdateStoreServer()
		 */
		@Override
		public void stopUpdateStoreServer() throws USException {}

		/**
		 * {@inheritDoc}
		 * 
		 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#updateStoreServerIsRunning()
		 */
		@Override
		public boolean updateStoreServerIsRunning() {
			throw new UnsupportedOperationException();//return false;
		}

	}

	private final Map<AbstractPeerID, Schema> peerIDToSchema = newHashMap();

	private StubSchemaIDBindingClient(Document schema) {
		Element catalog = schema.getDocumentElement();
		List<Element> peers = getChildElementsByName(catalog, "peer");
		List<Schema> schemas = newArrayList();
		Map<AbstractPeerID, Integer> peerIDToInteger = newHashMap();
		try {
			SchemaIDBinding.loadSchemas(peers, schemas, peerIDToSchema,
					peerIDToInteger);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient#disconnect()
	 */
	@Override
	public void disconnect() throws USException {}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient#getAllSchemas(java.lang.String)
	 */
	@Override
	public Map<AbstractPeerID, Schema> getAllSchemas(String cdss)
			throws USException {
		return peerIDToSchema;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient#getHostedSystems()
	 */
	@Override
	public Set<String> getHostedSystems() throws USException {
		throw new UnsupportedOperationException();//	return null;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient#getSchema(java.lang.String,
	 *      edu.upenn.cis.orchestra.datamodel.AbstractPeerID, java.lang.String)
	 */
	@Override
	public Schema getSchema(String cdss, AbstractPeerID pid, String schema)
			throws USException {
		throw new UnsupportedOperationException();//return null;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient#isConnected()
	 */
	@Override
	public boolean isConnected() {
		throw new UnsupportedOperationException();//		return false;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient#loadSchemas(org.w3c.dom.Document)
	 */
	@Override
	public Map<AbstractPeerID, Schema> loadSchemas(Document peerDoc)
			throws USException {
		return peerIDToSchema;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient#reconnect()
	 */
	@Override
	public boolean reconnect() {
		return true;
	}

}
