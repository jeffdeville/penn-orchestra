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
package edu.upenn.cis.orchestra.sql.dtp;

import org.eclipse.datatools.modelbase.sql.query.SQLQueryObject;
import org.eclipse.datatools.modelbase.sql.query.util.SQLQuerySourceFormat;

import edu.upenn.cis.orchestra.sql.ISqlFactory;
import edu.upenn.cis.orchestra.sql.SqlFactories;

/**
 * A base class for {@link ISQLQueryObject}s.
 * 
 * @author Sam Donnelly
 * @param T see {@link ISQLQueryObject}
 */
abstract class AbstractSQLQueryObject<T extends SQLQueryObject> implements
		ISQLQueryObject<T> {
	/** For making SQL objects. */
	protected static final ISqlFactory _sqlFactory = SqlFactories
			.getSqlFactory();

	/** {@inheritDoc} */
	@Override
	public String toString() {
		// We save the old value of getQualifyIdentifiers(), change it so that
		// we get the qualification we want, and then set
		// it back again. We don't know if this is necessary, but we take the
		// cautious approach.
		int originalQualifyIdentifiers = getSQLQueryObject().getSourceInfo()
				.getSqlFormat().getQualifyIdentifiers();
		getSQLQueryObject()
				.getSourceInfo()
				.getSqlFormat()
				.setQualifyIdentifiers(
						SQLQuerySourceFormat.QUALIFY_IDENTIFIERS_WITH_SCHEMA_NAMES);
		String sql = getSQLQueryObject().getSQL();
		getSQLQueryObject().getSourceInfo().getSqlFormat()
				.setQualifyIdentifiers(originalQualifyIdentifiers);
		return sql;
	}
}
