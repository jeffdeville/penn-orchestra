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
package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import static edu.upenn.cis.orchestra.OrchestraUtil.newHashMap;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;

/**
 * Implementation of {@code ISchemaIDBindingClient} for when the update store is
 * a {@code BerkeleyDBStoreServer}.
 * 
 * @author John Frommeyer
 * 
 */
public class SchemaIDBindingBerkeleyDBStoreClient implements
		ISchemaIDBindingClient {
	private final InetSocketAddress host;
	private Socket socket;
	private ObjectOutputStream oos;
	private ObjectInputStream ois;
	private final Map<AbstractPeerID, Schema> schemas = newHashMap();

	// private static final Logger logger = LoggerFactory
	// .getLogger(SchemaIDBindingBerkeleyDBStoreClient.class);

	SchemaIDBindingBerkeleyDBStoreClient(
			@SuppressWarnings("hiding") final InetSocketAddress host) {
		this.host = host;
	}

	private synchronized Object sendRequest(Object o, Class<?>... classes)
			throws USException {
		Object reply;
		try {
			oos.writeObject(o);
			oos.flush();
			reply = ois.readObject();
		} catch (Exception e) {
			throw new USException(e);
		}

		if (reply instanceof Exception) {
			throw new USException((Exception) reply);
		}

		if (reply instanceof BadMsg) {
			throw new USException("Server received unexpected message: "
					+ ((BadMsg) reply).o);
		}

		for (Class<?> c : classes) {
			if (c.isInstance(reply)) {
				return reply;
			}
		}

		throw new USException("Received object is of unexpected type "
				+ reply.getClass().getName());
	}

	@Override
	public void disconnect() throws USException {
		if (socket == null) {
			// Already disconnected
			return;
		}
		sendRequest(new EndOfStreamMsg(), EndOfStreamMsg.class);
		try {
			oos.close();
			ois.close();
			socket.close();
		} catch (Exception e) {
			throw new USException(
					"Could not disconnect from update store server", e);
		}
		oos = null;
		ois = null;
		socket = null;
	}

	@Override
	public boolean isConnected() {
		return (socket != null);
	}

	@Override
	public Schema getSchema(String cdss, AbstractPeerID pid, String schemaName)
			throws USException {
		Schema s = schemas.get(pid);
		if (s != null) {
			return s;
		}
		GetSchemaByNameResponse response = null;
		response = (GetSchemaByNameResponse) sendRequest(new GetSchemaByName(
				cdss, pid, schemaName), GetSchemaByNameResponse.class);
		s = response.getSchema();
		if (s != null) {
			schemas.put(pid, s);
		}
		return s;
	}

	@Override
	public void reconnect() throws USException {
		if (socket != null) {
			// Already connected
			return;
		}
		try {
			socket = new Socket(host.getAddress(), host.getPort());
			oos = new ObjectOutputStream(socket.getOutputStream());
			ois = new ObjectInputStream(socket.getInputStream());
		} catch (Exception e) {
			throw new USException(
					"Could not connect to update store server at "
							+ host.getAddress() + ":" + host.getPort() + ".", e);
		}

		Object response = sendRequest(new Ping(), Ack.class);
		if (response == null || !(response instanceof Ack)) {
			throw new USException("Could not contact update store server at "
					+ host.getAddress() + ":" + host.getPort() + ".");
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient#getAllSchemas
	 * (java.lang.String)
	 */
	@Override
	public Map<AbstractPeerID, Schema> getAllSchemas(String cdss)
			throws USException {
		GetAllSchemas request = new GetAllSchemas(cdss);
		GetAllSchemasResponse response = (GetAllSchemasResponse) sendRequest(
				request, GetAllSchemasResponse.class);
		return response.getPeerToSchemas();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient#loadSchemas
	 * (org.w3c.dom.Document)
	 */
	@Override
	public Map<AbstractPeerID, Schema> loadSchemas(Document peerDoc)
			throws USException {
		LoadSchemas request = new LoadSchemas(peerDoc);
		LoadSchemasResponse response = (LoadSchemasResponse) sendRequest(
				request, LoadSchemasResponse.class);
		return response.getSchemaMap();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeedu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient#
	 * getHostedSystems()
	 */
	@Override
	public Set<String> getHostedSystems() throws USException {
		GetHostedSystemsResponse response = (GetHostedSystemsResponse) sendRequest(
				new GetHostedSystems(), GetHostedSystemsResponse.class);
		return response.getSystemNames();
	}
}
