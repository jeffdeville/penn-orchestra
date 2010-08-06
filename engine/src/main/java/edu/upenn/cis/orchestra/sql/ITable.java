/*
 * Copyright (C) 2009 Trustees of the University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.cis.orchestra.sql;

/**
 * Holds schema name and table name strings.
 * 
 * @author Sam Donnelly
 */
public interface ITable {

	/**
	 * Get the name of the schema of this table, or {@code null} if there is no
	 * such schema.
	 * 
	 * @return the name of the schema of this table, or {@code null} if there is
	 *         no such schema
	 */
	String getSchemaName();

	/**
	 * Get the name of this table.
	 * 
	 * @return the name of this table
	 */
	String getName();

	/**
	 * Return an {@code "[schema.]table]"}.
	 * 
	 * @return {@code "[schema.]table]"}
	 */
	@Override
	String toString();

}
