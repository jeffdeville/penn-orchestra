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

import edu.upenn.cis.orchestra.Config;

/**
 * Simple container implementation of {@code ISqlDropSchema}.
 * <p>
 * Intentionally package-private.
 * 
 * @author John Frommeyer
 */
class SqlDropSchema implements ISqlDropSchema {

	/** The name of the schema we're dropping. */
	private final String _schema;

	/**
	 * Create a {@code DROP SCHEMA} statement on a given schema.
	 * 
	 * @param schema the schema name
	 */
	SqlDropSchema(final String schema) {
		_schema = schema;
	}

	/** {@inheritDoc} */
	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer("DROP SCHEMA ");
		buf.append(_schema);
		if (Config.isDB2()) {
			buf.append(" RESTRICT");
		}
		return buf.toString();
	}
}
