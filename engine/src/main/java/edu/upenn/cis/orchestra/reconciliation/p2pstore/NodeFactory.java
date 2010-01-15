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

import rice.p2p.commonapi.Node;

/**
 * @author netaylor
 *
 */
public interface NodeFactory {
	/**
	 * Allocate a node in the P2P network
	 * 
	 * @return The node itself
	 */
	public Node getNode();

	/**
	 * Deallocate a node in the P2P network
	 * 
	 * @param n	The node to deallocate. It becomes invalid after
	 * this function is called.
	 */
	public void shutdownNode(Node n);

	/**
	 * Get an IdFactory for the network type created by this NodeFactory. The
	 * IdFactory must have a public zero-argument constructor so that a copy
	 * can be created by the comparator used for the BerkeleyDB database.
	 * 
	 * @return
	 */
	public IdFactory getIdFactory();
}