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

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;

/**
 * Request for a  schema.
 * 
 * @author John Frommeyer
 * 
 */
public class GetSchemaByName implements Serializable {
	private static final long serialVersionUID = 1L;

	private final byte[] pid;
	private final String cdss;
	private final String schemaName;

	GetSchemaByName(@SuppressWarnings("hiding") final String cdss,
			@SuppressWarnings("hiding") final AbstractPeerID pid,
			@SuppressWarnings("hiding") final String schemaName) {
		this.cdss = cdss;
		this.pid = pid.getBytes();
		this.schemaName = schemaName;
	}

	AbstractPeerID getPid() {
		return AbstractPeerID.fromBytes(pid);
	}
	
	String getCdss() {
		return cdss;
	}

	String getSchemaName() {
		return schemaName;
	}
	
}
