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

import org.eclipse.datatools.modelbase.sql.query.QuerySearchCondition;
import org.eclipse.datatools.modelbase.sql.query.TableExpression;
import org.eclipse.datatools.modelbase.sql.query.TableReference;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParserFactory;

import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlFromItem;
import edu.upenn.cis.orchestra.sql.ITable;

/**
 * DTP backed {@code ISqlFromItem}.
 * <p>
 * Intentionally package-private.
 * 
 * @author Sam Donnelly
 */
class DtpSqlFromItem extends AbstractSQLQueryObject<TableReference> implements
		ISqlFromItem {

	/**
	 * The wrapped {@code TableReference}. This will be either a {@code
	 * TableExpression} or a {@code TableJoined}.
	 */
	private TableReference _tableReference;

	/**
	 * Construct a {@code DtpSqlFromItem}.
	 */
	DtpSqlFromItem() {}

	/**
	 * Create a new <code>FROM</code> clause on a given table.
	 * 
	 * @param fullname the table name. Must be in the form {@code
	 *            "[schema.]table]"}.
	 */
	DtpSqlFromItem(final String fullname) {
		ITable table = _sqlFactory.newTable(fullname);
		_tableReference = getSQLQueryParserFactory().createSimpleTable(
				table.getSchemaName(), table.getName());
	}

	/** {@inheritDoc} */
	@Override
	public ISqlFromItem setAlias(final String alias) {
		if (_tableReference instanceof TableExpression) {
			((TableExpression) _tableReference)
					.setTableCorrelation(getSQLQueryParserFactory()
							.createTableCorrelation(alias));
		} else {
			throw new IllegalStateException(
					"can't setAlias on a non-TableExpresion");
		}
		return this;
	}

	/**
	 * Construct a join {@code FROM} clause.
	 * 
	 * @param type the type of join.
	 * @param left the left side of the join.
	 * @param right the right side of the join.
	 * @param cond {@code WHERE} part of the {@code FROM} clause.
	 */
	DtpSqlFromItem(Join type, ISqlFromItem left, ISqlFromItem right,
			ISqlExp cond) {
		int joinType = -1;
		switch (type) {
		case FULLOUTERJOIN:
			joinType = SQLQueryParserFactory.JOIN_FULL_OUTER;
			break;
		case INNERJOIN:
			joinType = SQLQueryParserFactory.JOIN_DEFAULT_INNER;
			break;
		case LEFTOUTERJOIN:
			joinType = SQLQueryParserFactory.JOIN_LEFT_OUTER;
			break;
		case RIGHTOUTERJOIN:
			joinType = SQLQueryParserFactory.JOIN_RIGHT_OUTER;
			break;
		default:
			throw new IllegalArgumentException("Join " + type
					+ " not supported.");
		}
		@SuppressWarnings("unchecked")
		final ISQLQueryObject<TableReference> leftTableReference = (ISQLQueryObject<TableReference>) left;
		@SuppressWarnings("unchecked")
		final ISQLQueryObject<TableReference> rightTableReference = (ISQLQueryObject<TableReference>) right;
		@SuppressWarnings("unchecked")
		final ISQLQueryObject<QuerySearchCondition> condQuerySearchCondition = (ISQLQueryObject<QuerySearchCondition>) cond;
		_tableReference = getSQLQueryParserFactory().createJoinedTable(
				leftTableReference.getSQLQueryObject(), joinType,
				rightTableReference.getSQLQueryObject(),
				condQuerySearchCondition.getSQLQueryObject());
	}

	/**
	 * A DTP->{@code edu.upenn.cis.orchestra.sql} bridge: from a {@code
	 * TableReference} construct a {@code DtpSqlFromItem}.
	 * 
	 * @param tableReference source object
	 */
	DtpSqlFromItem(TableReference tableReference) {
		_tableReference = tableReference;
	}

	/** {@inheritDoc} */
	@Override
	public String getAlias() {
		if (_tableReference instanceof TableExpression) {
			return ((TableExpression) _tableReference).getTableCorrelation()
					.getName();
		} else {
			throw new IllegalStateException(
					"can't getAlias on non-TableExpression");
		}
	}

	/**
	 * Getter.
	 * <P>
	 * This is an undefined operation for {@code ISqlFromItem}s.
	 * 
	 * @see edu.upenn.cis.orchestra.sql.ISqlAliasedName#getColumn()
	 * @return nothing
	 * @throws UnsupportedOperationException whenever this method is called
	 */
	@Override
	public String getColumn() {
		throw new UnsupportedOperationException(
				"getColumn() is undefined for ISqlFromItem's");
	}

	/** {@inheritDoc} */
	@Override
	public String getTable() {
		return _tableReference.getName();
	}

	/** {@inheritDoc} */
	@Override
	public TableReference getSQLQueryObject() {
		return _tableReference;
	}
}