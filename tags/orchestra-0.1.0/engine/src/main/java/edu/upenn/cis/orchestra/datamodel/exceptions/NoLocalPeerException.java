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
package edu.upenn.cis.orchestra.datamodel.exceptions;

import edu.upenn.cis.orchestra.datamodel.Peer;

/**
 * Exception thrown when the local peer has not been set.
 * 
 * @author John Frommeyer
 * 
 */
public class NoLocalPeerException extends ModelException {

	
	private static final long serialVersionUID = 1L;

	/**
	 * Construct exception for case when more than one peer has {@code peer/@localPeer = 'true'}.
	 * @param localPeer1
	 * @param localPeer2
	 */
	public NoLocalPeerException(Peer localPeer1,
			Peer localPeer2) {
		super("Both peer " + localPeer1.getId()
				+ " and peer " + localPeer2.getId()
				+ " have been designated as local. Exactly one peer can have 'localPeer = true'.");
	}

	/**
	 * Construct exception for case when no peer has {@code peer/@localPeer = 'true'}.
	 * 
	 */
	public NoLocalPeerException() {
		super("No local peer defined. Must set 'localPeer = true' for some peer.");
	}
}
