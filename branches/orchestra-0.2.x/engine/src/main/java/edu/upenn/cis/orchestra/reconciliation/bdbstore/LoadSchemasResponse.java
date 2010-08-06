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

import java.io.Serializable;
import java.util.Map;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Schema;

/**
 * A response object for {@code BerkeleyDBStoreServer}. This is the response to
 * a {@code LoadSchemas} request and contains the result of deserializing that
 * request's CDSS definition.
 * 
 * @author John Frommeyer
 * 
 */
class LoadSchemasResponse implements Serializable {

	private static final long serialVersionUID = 1L;

	private final Map<AbstractPeerID, Schema> schemaMap;

	LoadSchemasResponse(
			@SuppressWarnings("hiding") final Map<AbstractPeerID, Schema> schemaMap) {
		this.schemaMap = schemaMap;
	}

	Map<AbstractPeerID, Schema> getSchemaMap() {
		return schemaMap;
	}

}
