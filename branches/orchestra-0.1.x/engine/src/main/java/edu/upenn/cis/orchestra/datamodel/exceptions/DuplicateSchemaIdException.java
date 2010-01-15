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


/**
 * Exception generated when trying to add a schema to a peer while 
 * there is already a schema with the same id
 * @author Olivier Biton
 *
 */
public class DuplicateSchemaIdException extends ModelException {
	public static final long serialVersionUID = 1L;
	
	private String _peerId;
	private String _schemaId;
	
	public DuplicateSchemaIdException (String peerId, String schemaId)
	{
		super ("Schema id " + schemaId + " is already used in peer " + peerId);
		_peerId = peerId;
		_schemaId = schemaId;
	}

	public String getPeerId() {
		return _peerId;
	}

	public String getSchemaId() {
		return _schemaId;
	}
}
