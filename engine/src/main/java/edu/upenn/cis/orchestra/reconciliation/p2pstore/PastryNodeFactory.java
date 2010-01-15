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
package edu.upenn.cis.orchestra.reconciliation.p2pstore;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import rice.p2p.commonapi.Node;
import rice.pastry.NodeHandle;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.IPNodeIdFactory;

public class PastryNodeFactory implements NodeFactory {
	rice.environment.Environment env;
	NodeIdFactory nidFactory;
	SocketPastryNodeFactory factory;
	NodeHandle bootHandle;
	int createdCount = 0;
	int port;

	public PastryNodeFactory(rice.environment.Environment env, int port) {
		this(env, port, null);
	}

	public PastryNodeFactory(rice.environment.Environment env, int port, NodeHandle bootHandle) {
		this.env = env;
		this.port = port;
		this.bootHandle = bootHandle;
		try {
			nidFactory = new IPNodeIdFactory(InetAddress.getLocalHost(), port, env);
		} catch (UnknownHostException e) {
			throw new RuntimeException("Couldn't determine local host", e);
		}
		try {
			factory = new SocketPastryNodeFactory(nidFactory, port, env);
		} catch (java.io.IOException ioe) {
			throw new RuntimeException(ioe.getMessage(), ioe);
		}

	}

	public Node getNode() {
		PastryNode node = null;
		try {
			// Make sure that only one node per factory tries to create
			// a new ring
			synchronized (this) {
				if (bootHandle == null) {
					if (createdCount == 0) {
						node = factory.newNode(null);
						while (! node.isReady()) {
							Thread.sleep(10);
						}
					} else {
						InetAddress localhost = InetAddress.getLocalHost();
						InetSocketAddress bootaddress = new InetSocketAddress(localhost, port);
						bootHandle = factory.getNodeHandle(bootaddress);
					}
					++createdCount;
				}
			}
			if (node == null) {
				node = factory.newNode(bootHandle);
				while (! node.isReady()) {
					Thread.sleep(10);
				}
			}
			return node;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	public void shutdownNode(Node n) {
		rice.environment.Environment e = n.getEnvironment();
		((PastryNode) n).destroy();	
		boolean busy = true;
		while (busy) {
			busy = false;
			if (e.getSelectorManager().getNumInvocations() > 0) {
				busy = true;
			}
			if (busy) {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e1) {
					return;
				}
			}
		}
	}

	public IdFactory getIdFactory() {
		return new PastryIdFactory();
	}
}