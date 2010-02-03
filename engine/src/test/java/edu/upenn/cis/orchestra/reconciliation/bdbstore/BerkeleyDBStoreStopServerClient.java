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

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;

/**
 * A simple client for sending a {@code StopUpdateStore} message to a Berkeley
 * Update Store Server.
 * 
 * @author John Frommeyer
 * 
 */
public class BerkeleyDBStoreStopServerClient {
	private final InetSocketAddress host;
	private Socket socket;
	private ObjectOutputStream oos;
	private ObjectInputStream ois;

	/**
	 * A client for the given {@code hostname} and {@code port}.
	 * 
	 * @param hostname
	 * @param port
	 */
	public BerkeleyDBStoreStopServerClient(String hostname, int port) {
		this.host = new InetSocketAddress(hostname, port);
	}

	private synchronized Object sendRequest(Object o, Class<?>... classes)
			throws USException {

		try {
			oos.writeObject(o);
			oos.flush();
		} catch (Exception e) {
			throw new USException(e);
		}
		if (classes != null) {
			Object reply;
			try {
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
		return null;
	}

	/**
	 * Request that the update store be stopped.
	 * 
	 * @throws USException
	 */
	public void stopUpdateStore() throws USException {
		StopUpdateStore request = new StopUpdateStore();
		sendRequest(request, (Class<?>[]) null);
	}

	/**
	 * Connect to the server.
	 * 
	 * @throws USException
	 */
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
			throw new USException("Could not connect to update store server", e);
		}

		Object response = sendRequest(new Ping(), Ack.class);
		if (response == null || !(response instanceof Ack)) {
			throw new USException("Could not connect to update store server");
		}

	}
	
	/**
	 * Disconnect from the server.
	 * 
	 * @throws USException
	 */
	public void disconnect() throws USException {
		if (socket == null) {
			// Already disconnected
			return;
		}
		try {
			oos.close();
			ois.close();
			socket.close();
		} catch (Exception e) {
			throw new USException("Could not disconnect from update store server", e);
		}
		oos = null;
		ois = null;
		socket = null;
	}

}
