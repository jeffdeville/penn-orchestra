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
package edu.upenn.cis.orchestra.localupdates.extract.exceptions;

/**
 * Indicates that there was some unexpected difference in the database schema.
 * @author John Frommeyer
 *
 */
public class SchemaIncoherentWithDBError extends Exception {

	private static final long serialVersionUID = 1L;

	/**
	 * Thrown when {@code relation} does not exist in {@code schema}.
	 * 
	 * @param schema
	 * @param relation
	 */
	public SchemaIncoherentWithDBError(String schema, String relation) {
		super("Relation " + relation + " does not exist in schema " + schema);
	}

	/**
	 * Thrown when there is a problem with {@code column}.
	 * 
	 * @param schema
	 * @param relation
	 * @param column
	 * @param exists
	 */
	public SchemaIncoherentWithDBError(String schema, String relation, String column, boolean exists) {
		super("In schema " + schema + ", Column " + relation + "." + column + (exists?" type mismatch":" does not exist in schema "));
	}

}
