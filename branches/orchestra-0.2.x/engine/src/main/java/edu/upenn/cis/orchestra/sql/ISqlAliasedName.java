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
 * A name/alias association<br>
 * Names can have two forms:
 * <ul>
 * <li>[schema.]table</li>
 * <li>[[schema.]table.]column</li>
 * </ul>
 */
public interface ISqlAliasedName {

	/**
	 * If the name is of the form {@code [table.]column}, returns the table
	 * part, or {@code null} if there is no table part.
	 * 
	 * @return if the name is of the form [schema.]table.column, returns the
	 *         name part or {@code null} if there is no table part
	 */
	String getTable();

	/**
	 * If the name is of the form [table.]column: return the column part.
	 * 
	 * @return if the name is of the form [[schema.]table.]column: return the
	 *         column part
	 */
	String getColumn();

	/**
	 * Get the alias associated to the current name.
	 * 
	 * @return the alias associated to the current name or {@code null} if there
	 *         is no assigned alias
	 */
	String getAlias();

	/**
	 * Associate an alias with the current name.
	 * 
	 * @param alias the alias to associate with the current name
	 * 
	 * @return this {@code ISqlAliasedName}
	 */
	ISqlAliasedName setAlias(String alias);

	/**
	 * Get the SQL for this {@code ISqlAliasedName}.
	 * 
	 * @return the SQL for this {@code ISqlAliasedName}
	 */
	String toString();
}