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


//TODO: add peer id !
/**
 * Exception generated when trying to add a relation to a schema while 
 * there is already a relation with the same id
 * @author Olivier Biton
 *
 */
public class DuplicateRelationIdException extends ModelException {
	public static final long serialVersionUID = 1L; 
	
	String _relId;
	String _schemaId;
	
	public DuplicateRelationIdException (String relId, String schemaId)
	{
		super ("Relation id " + relId + " is already used in schema " + schemaId);
		_schemaId = schemaId;
		_relId = relId;
	}

	public String getRelId() {
		return _relId;
	}

	public String getSchemaId() {
		return _schemaId;
	}
	
}
