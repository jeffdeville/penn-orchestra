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

import static edu.upenn.cis.orchestra.OrchestraUtil.toSerializableSet;

import java.io.Serializable;
import java.util.Set;

/**
 * A response object for {@code BerkeleyDBStoreServer}. This is a response to
 * {@code GetHostedSystems}. Used to return the names of all hosted CDSSs.
 * 
 * @author John Frommeyer
 * 
 */
class GetHostedSystemsResponse implements Serializable {

	private static final long serialVersionUID = 1L;
	private final Set<String> systemNames;

	GetHostedSystemsResponse(
			@SuppressWarnings("hiding") final Set<String> systemNames) {
		this.systemNames = toSerializableSet(systemNames);
	}

	Set<String> getSystemNames() {
		return systemNames;
	}

}
