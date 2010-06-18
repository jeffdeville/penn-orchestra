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

package edu.upenn.cis.orchestra.localupdates.apply.sql;

import java.sql.SQLException;

import edu.upenn.cis.orchestra.datamodel.Update;

/**
 * Insertion statements required by {@code ApplierSql}.
 * 
 * @author John Frommeyer
 * 
 */
interface IApplierStatements {

	/**
	 * 
	 * Add a statement for {@code update}.
	 * 
	 * @param update
	 * @throws Exception
	 */
	void add(Update update) throws Exception;

	/**
	 * Returns the number of updates resulting from executing the accumulated statements.
	 * 
	 * @return the number of tuples inserted
	 * @throws SQLException
	 */
	int apply() throws SQLException;
}
