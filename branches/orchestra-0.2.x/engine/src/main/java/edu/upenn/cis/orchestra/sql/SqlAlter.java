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
 * A simple {@code ISqlAlter}.
 * <p>
 * Its {@code toString()} method only outputs {@code "ALTER TABLE " + _table},
 * so the client must append text to make a well-formed statement.
 * 
 * @author Sam Donnelly
 */
class SqlAlter implements ISqlAlter {

	/** Table being {@code ALTER}ed. */
	private final String _table;

	/**
	 * Create an {@code SqlAlter} with target table.
	 * 
	 * @param table the table to be altered
	 */
	SqlAlter(final String table) {
		_table = table;
	}

	/**
	 * {@inheritDoc} This method only outputs {@code "ALTER TABLE " + _table},
	 * so the client must append text to make a well-formed statement.
	 */
	@Override
	public String toString() {
		// if(_type == SET_DEF_VAL)
		return "ALTER TABLE " + _table + " ";
	}
}
