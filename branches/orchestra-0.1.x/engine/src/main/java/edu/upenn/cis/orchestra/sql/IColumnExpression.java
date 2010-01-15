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
 * A column in an {@code ITable}.
 * 
 * @author Sam Donnelly
 */
public interface IColumnExpression {

	/**
	 * Get the column name.
	 * 
	 * @return the column name
	 */
	String getColumn();

	/**
	 * Get the table to which this {@code IColumnExpression} belongs, or {@code
	 * null} if there is no associated table.
	 * 
	 * @return the table to which this {@code IColumnExpression} belongs, or
	 *         {@code null} if there is no associated table
	 */
	ITable getTable();

	/**
	 * Convenience method equivalent to {@code getTable() != null ?
	 * getTable().getName() : null}.
	 * 
	 * @return {@code getTable() != null ? getTable().getName() : null}
	 */
	String getTableName();

	/**
	 * Convenience method equivalent to {@code getTable() != null ?
	 * getTable().getSchemaName() : null}.
	 * 
	 * @return {@code getTable() != null ? getTable().getName().getSchemaName()
	 *         : null}
	 */
	String getSchemaName();

}
