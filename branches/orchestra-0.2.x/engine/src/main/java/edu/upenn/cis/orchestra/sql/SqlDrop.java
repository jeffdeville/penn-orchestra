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
 * Simple container implementation of {@code ISqlDrop}.
 * <p>
 * Intentionally package-private.
 * 
 * @author Sam Donnelly
 */
class SqlDrop implements ISqlDrop {

	/** The name of the table we're dropping. */
	private final String _table;

	/**
	 * Create a {@code DROP} statement on a given table.
	 * 
	 * @param tab the table name
	 */
	SqlDrop(final String tab) {
		_table = tab;
	}

	/** {@inheritDoc} */
	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer("DROP TABLE ");
		buf.append(_table);
		return buf.toString();
	}
}
