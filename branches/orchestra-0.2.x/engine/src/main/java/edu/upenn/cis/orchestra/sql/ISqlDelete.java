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
 * Represents an SQL {@code DELETE} statement, as in {DELETE FROM table_name
 * WHERE some_column=some_value}.
 * 
 * @author Sam Donnelly
 */
public interface ISqlDelete extends ISqlStatement {

	/**
	 * Add a {@code WHERE} clause to the {@code DELETE} statement. Overwrites
	 * any {@code WHERE} clause that was already there.
	 * 
	 * @param w an SQL expression compatible with a {@code WHERE} clause.
	 * @return this {@code ISqlDelete}
	 */
	ISqlDelete addWhere(ISqlExpression w);
}