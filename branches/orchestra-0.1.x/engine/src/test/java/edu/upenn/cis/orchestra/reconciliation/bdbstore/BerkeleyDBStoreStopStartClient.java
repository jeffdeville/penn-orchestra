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

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;

import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;

/**
 * A simple client for controlling a Berkeley Update Store Server in tests.
 * 
 * @author John Frommeyer
 * 
 */
final public class BerkeleyDBStoreStopStartClient {
	private BerkeleyDBStoreServer server;
	private final String storeName;
	private final int port;

	/**
	 * A client for the given {@code storeName} and {@code port}. Server will be
	 * on the localhost.
	 * 
	 * @param storeName
	 * @param port
	 */
	public BerkeleyDBStoreStopStartClient(
			@SuppressWarnings("hiding") final String storeName,
			@SuppressWarnings("hiding") final int port) {
		this.storeName = storeName;
		this.port = port;
	}

	/**
	 * A client for the given {@code storeName} and default port. Server will be
	 * on the localhost.
	 * 
	 * @param storeName
	 */
	public BerkeleyDBStoreStopStartClient(
			@SuppressWarnings("hiding") final String storeName) {
		this.storeName = storeName;
		this.port = BerkeleyDBStoreServer.DEFAULT_PORT;
	}

	/**
	 * Request that the update store be stopped.
	 * 
	 * @throws USException
	 * @throws InterruptedException
	 * @throws DatabaseException
	 * @throws IOException
	 */
	public void clearAndStopUpdateStore() throws USException, IOException,
			DatabaseException, InterruptedException {
		if (server != null) {
			try {
				clearUpdateStore();
			} finally {
				server.quit();
			}
		}
	}

	public void startAndClearUpdateStore() throws USException,
			EnvironmentLockedException, DatabaseException, IOException,
			ClassNotFoundException {
		File f = new File(storeName + "_env");
		if (!f.exists()) {
			f.mkdir();
		} else if (f.isDirectory()) {
			File[] files = f.listFiles();
			for (File file : files) {
				file.delete();
			}
		}

		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setAllowCreate(true);
		ec.setTransactional(true);
		Environment env = new Environment(f, ec);
		server = new BerkeleyDBStoreServer(env, port);
		clearUpdateStore();
	}

	public void clearUpdateStore() throws USException {
		try {
			Socket clearSocket = new Socket("localhost", port);
			ObjectOutputStream clearOos = new ObjectOutputStream(clearSocket
					.getOutputStream());
			ObjectInputStream clearOis = new ObjectInputStream(clearSocket
					.getInputStream());
			clearOos.writeObject(new Reset());
			clearOos.writeObject(new EndOfStreamMsg());
			clearOos.flush();
			Object response = clearOis.readObject();

			clearOos.close();
			clearSocket.close();
			if (response instanceof Ack) {
				return;
			} else if (response instanceof Exception) {
				throw new USException("Error restoring BDB store from dump",
						(Exception) response);
			} else {
				throw new USException("Recevied unexpected reply of type "
						+ response.getClass().getName() + ": " + response);
			}

		} catch (IOException ioe) {
			throw new USException("Error resetting BDB store", ioe);
		} catch (ClassNotFoundException e) {
			throw new USException(
					"Could not deserialize response from BDB store", e);
		}
	}
}
