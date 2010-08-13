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

import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;

/**
 * Interact with a {@code SchemaIDBinding} being managed by an {@code
 * UpdateStore}.
 * 
 * @author John Frommeyer
 * 
 */
public interface ISchemaIDBindingClient {

	/**
	 * Disconnect from the server.
	 * 
	 * @throws USException
	 */
	void disconnect() throws USException;

	/**
	 * Returns {@code true} if this client is connected to the server, {@code
	 * false} otherwise.
	 * 
	 * @return {@code true} if this client is connected to the server, {@code
	 *         false} otherwise
	 */
	boolean isConnected();

	/**
	 * Returns the schema with name {@code schema} belonging to the {@code Peer}
	 * with id {@code pid} from the CDSS {@code cdss}. If the schema cannot be
	 * found should return null rather than throwing an exception.
	 * 
	 * @param cdss
	 * @param pid
	 * @param schema
	 * @return the schema with name {@code schema} belonging to the {@code Peer}
	 *         with id {@code pid} from the CDSS {@code cdss}
	 * @throws USException
	 */
	Schema getSchema(String cdss, AbstractPeerID pid, String schema)
			throws USException;

	/**
	 * Returns all the schemas from the CDSS {@code cdss}. If the schema cannot
	 * be found should return empty map rather than throwing an exception.
	 * 
	 * @param cdss
	 * @return the schemas from the CDSS {@code cdss}
	 * @throws USException
	 */
	Map<AbstractPeerID, Schema> getAllSchemas(String cdss) throws USException;

	/**
	 * If this client is disconnected, will attempt to reconnect to the server.
	 * @return {@code true} if connection succeeded, {@code false} otherwise
	 * 
	 */
	boolean reconnect();

	/**
	 * Returns the result of loading the {@code SchemaIDBinding} with the
	 * schemas specified by {@code peerDoc}.
	 * 
	 * @param peerDoc
	 * @return a map from peer id to its associated schema, as specified by {@code peerDoc}.
	 * @param peerDoc
	 * @return a map from peer id to its associated schema, as specified by {@code peerDoc}.
	 * @throws USException 
	 */
	Map<AbstractPeerID, Schema> loadSchemas(Document peerDoc)
			throws USException;

	/**
	 * Returns the CDSS names for which the {@code SchemaIDBinding} has information.
	 * 
	 * @return the CDSS names for which the {@code SchemaIDBinding} has information
	 * @throws USException
	 */
	Set<String> getHostedSystems() throws USException;
}
