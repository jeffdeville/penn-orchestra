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
 * A column in a {@code CREATE} statement.
 * 
 * @author Sam Donnelly
 */
public interface ISqlColumnDef {

	/**
	 * Get the column name.
	 * 
	 * @return the column name
	 */
	String getName();

	/**
	 * Get the default value of the column.
	 * 
	 * @return the default value of the column
	 */
	String getDefault();

	/**
	 * Get the type of the column.
	 * 
	 * @return the type of the column.
	 *         <p>
	 *         NOTE: this is free text.
	 */
	String getType();

	/**
	 * Get the SQL for this column definition.
	 * 
	 * @return the SQL for this column definition.
	 */
	String toString();

}