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

import static edu.upenn.cis.orchestra.sql.dtp.SqlDtpUtil.getSQLQueryParserFactory;

import org.eclipse.datatools.modelbase.sql.query.QueryDeleteStatement;
import org.eclipse.datatools.modelbase.sql.query.QuerySearchCondition;
import org.eclipse.datatools.modelbase.sql.query.TableInDatabase;

import edu.upenn.cis.orchestra.sql.ISqlDelete;
import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlExpression;
import edu.upenn.cis.orchestra.sql.ITable;

/**
 * An {@code ISqlDelete} backed with DTP.
 * <p>
 * Intentionally package-private.
 * 
 * @author Sam Donnelly
 */
class DtpSqlDelete extends AbstractSQLQueryObject<QueryDeleteStatement>
		implements ISqlDelete {

	/**
	 * Wrapped DTP object.
	 */
	private final QueryDeleteStatement _delete;

	/**
	 * Construct a {@code DELETE} statement for the table represented by {@code
	 * table}.
	 * 
	 * @param table the target of the {@code DELETE} statement
	 */
	DtpSqlDelete(final ITable table) {
		@SuppressWarnings("unchecked")
		ISQLQueryObject<TableInDatabase> tableInDatabaseTable = (ISQLQueryObject<TableInDatabase>) table;
		_delete = getSQLQueryParserFactory().createDeleteStatement(
				tableInDatabaseTable.getSQLQueryObject(), null, null);
	}

	/**
	 * Construct an {@code SqlDelete} from the target table.
	 * 
	 * @param tableName the name of the table in the {@code FROM} part of the
	 *            {@code DELETE} statement.
	 */
	DtpSqlDelete(final String tableName) {
		this(tableName, null);
	}

	/**
	 * Construct an {@code SqlDelete} who's {@code FROM} part has a table and an
	 * alias.
	 * <p>
	 * For example: {@code DELETE LBI FROM lightboxes.dbo.lightboxItem AS LBI
	 * where LBI.UserName = "jdoe";}.
	 * 
	 * 
	 * @param tableName table name.
	 * @param alias alias for the table.
	 */
	DtpSqlDelete(final String tableName, final String alias) {
		this(_sqlFactory.newTable(tableName, alias));
	}

	/**
	 * From a {@code QueryDeleteStatement}, construct a {@code DtpSqlDelete}.
	 * 
	 * @param delete source statement.
	 */
	DtpSqlDelete(final QueryDeleteStatement delete) {
		_delete = delete;
	}

	/** {@inheritDoc} */
	@Override
	public ISqlDelete addWhere(final ISqlExp where) {
		@SuppressWarnings("unchecked")
		final ISQLQueryObject<QuerySearchCondition> querySearchConditionWhere = (ISQLQueryObject<QuerySearchCondition>) where;
		QuerySearchCondition whereClause = getSQLQueryParserFactory()
		// Nested for backward compatibility with ZQL.
				.createNestedCondition(
						querySearchConditionWhere.getSQLQueryObject());
		_delete.setWhereClause(whereClause);
		return this;
	}

	/** {@inheritDoc} */
	@Override
	public QueryDeleteStatement getSQLQueryObject() {
		return _delete;
	}
}
