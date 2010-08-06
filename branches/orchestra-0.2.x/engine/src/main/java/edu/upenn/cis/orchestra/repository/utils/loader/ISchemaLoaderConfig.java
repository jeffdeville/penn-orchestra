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
package edu.upenn.cis.orchestra.repository.utils.loader;

/**
 * Configuration for <code>ISchemaLoader</code>s.
 * 
 * @author Sam Donnelly
 */
public interface ISchemaLoaderConfig {

	/**
	 * Return <code>true</code> if the table passes the filter,
	 * <code>false</code> otherwise.
	 * 
	 * @param databaseProductName the database we're using.
	 * @param schemaName the schema name.
	 * @return see description.
	 */
	public boolean filterOnSchemaName(String databaseProductName,
			String schemaName);

	/**
	 * Return <code>true</code> if the table passes the filter,
	 * <code>false</code> otherwise.
	 * 
	 * @param tableName the table name.
	 * @return see description.
	 */
	public boolean filterOnTableName(String tableName);

}
