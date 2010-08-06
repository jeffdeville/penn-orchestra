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
 * Simple {@code ISqlRename}.
 * <p>
 * Intentionally package-private.
 * 
 * @author Sam Donnelly
 */
class SqlRename implements ISqlRename {

	/** Source. */
	private final ITable _source;

	/** Destination. */
	private final ITable _dest;

	/**
	 * Create a {@code RENAME source TO dest} statement.
	 * 
	 * @param source source table
	 * @param dest destination table
	 */
	SqlRename(final ITable source, final ITable dest) {
		_source = source;
		_dest = dest;
	}

	/** {@inheritDoc} */
	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer("rename table ");
		buf.append(_source);
		buf.append(" to ");
		// Notice that we only output the table part (no schema) for the
		// destination.
		buf.append(_dest.getName());
		return buf.toString();
	}
}
