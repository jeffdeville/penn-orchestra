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

import rice.p2p.commonapi.Id;

public interface IdFactory {
	/**
	 * Turn a byte representation of some content into
	 * an Id for the P2P network. This may perform hashing or
	 * some other unspecified scrambling before calling PastryId.build
	 * (or the equivalent)
	 * 
	 * @param bytes 	The input to turn into an Id
	 * @return			An Id derived from <code>bytes</code>
	 */
	public Id getIdFromContent(byte[] bytes);

	/**
	 * Turn a serialized form of an Id (ie. from Id.toByteArray())
	 * back into an Id. This performs no transformations on the input,
	 * and should be an exact inverse of toByteArray();
	 * 
	 * @param bytes		The byte array form of an Id created using this NodeFactory.
	 * @return			The Id that was stored as a byte array in <code>bytes</code>
	 */
	public Id getIdFromByteArray(byte[] bytes);

}
