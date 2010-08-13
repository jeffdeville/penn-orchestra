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

import java.util.ArrayList;
import java.util.List;

import org.eclipse.datatools.modelbase.sql.query.QueryInsertStatement;
import org.eclipse.datatools.modelbase.sql.query.QuerySelectStatement;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionColumn;

import edu.upenn.cis.orchestra.sql.IColumnExpression;
import edu.upenn.cis.orchestra.sql.ISqlConstant;
import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlExpression;
import edu.upenn.cis.orchestra.sql.ISqlInsert;
import edu.upenn.cis.orchestra.sql.ISqlSelect;
import edu.upenn.cis.orchestra.sql.ITable;

/**
 * DTP-backed {@code ISqlInsert}.
 * <p>
 * Intentionally package-private.
 * 
 * @author Sam Donnelly
 */
class DtpSqlInsert extends AbstractSQLQueryObject<QueryInsertStatement>
		implements ISqlInsert {

	/** That which we are wrapping. */
	private final QueryInsertStatement _insertStatement;

	/**
	 * Create an {@code INSERT} statement on a given table.
	 * 
	 * @param qualifiedTable the optionally schema qualified name of the table.
	 */
	DtpSqlInsert(final String qualifiedTable) {
		this(_sqlFactory.newTable(qualifiedTable));
	}

	/**
	 * Create an {@code INSERT} statement who's target is {@code table}.
	 * 
	 * @param table the target of the {@code INSERT} statement
	 */
	DtpSqlInsert(final String qualifiedTable, final List<String> cols) {
		this(_sqlFactory.newTable(qualifiedTable), cols);
	}

	/**
	 * Create an {@code INSERT} statement who's target is {@code table}.
	 * 
	 * @param table the target of the {@code INSERT} statement
	 */
	DtpSqlInsert(final ITable table) {
		_insertStatement = getSQLQueryParserFactory().createInsertStatement(
				getSQLQueryParserFactory().createSimpleTable(
						table.getSchemaName(), table.getName()),
				(List<?>) null, (List<?>) null);
	}

	/**
	 * Create an {@code INSERT} statement who's target is {@code table}.
	 * 
	 * @param table the target of the {@code INSERT} statement
	 */
	DtpSqlInsert(final ITable table, final List<String> cols) {
		List<ValueExpressionColumn> vc = new ArrayList<ValueExpressionColumn>();
		for (String c : cols) {
			IColumnExpression ic = _sqlFactory.newColumnExpression(c);
			vc.add(getSQLQueryParserFactory()
					.createColumnExpression(ic.getColumn(),
							ic.getTableName()));
		}

		_insertStatement = getSQLQueryParserFactory().createInsertStatement(
				getSQLQueryParserFactory().createSimpleTable(
						table.getSchemaName(), table.getName()),
				vc, (List<?>) null);
	}

	/**
	 * From a {@code QueryInsertStatement}, construct the equivalent
	 * {@DtpSqlInsert}.
	 * 
	 * @param insertStatement the source.
	 */
	DtpSqlInsert(final QueryInsertStatement insertStatement) {
		_insertStatement = insertStatement;
	}

	/** {@inheritDoc} */
	@Override
	public ISqlInsert addValueSpec(final ISqlExp e) {
		if (e instanceof ISqlExpression) {
			_insertStatement.getSourceValuesRowList().clear();
			final ISqlExpression sqlExpression = (ISqlExpression) e;

			if (ISqlExpression.Code.COMMA != sqlExpression.getCode()) {
				throw new IllegalArgumentException(
						"e must have ISqlExpression.COMMA == getCode()");
			}

			@SuppressWarnings("unchecked")
			final List<Object> valuesRows = _insertStatement
					.getSourceValuesRowList();
			valuesRows
					.add(((ValuesRowExpression) sqlExpression).getValuesRow());
		} else if (e instanceof ISqlSelect) {
			@SuppressWarnings("unchecked")
			final ISQLQueryObject<QuerySelectStatement> querySelectStatementSqlSelect = (ISQLQueryObject<QuerySelectStatement>) e;
			_insertStatement.setSourceQuery(getSQLQueryParserFactory()
					.createQueryExpressionRoot(
					// Nested for backward compatibility with ZQL
							getSQLQueryParserFactory().createQueryNested(
									querySelectStatementSqlSelect
											.getSQLQueryObject().getQueryExpr()
											.getQuery()), null));
		}
		return this;
	}

	/** {@inheritDoc} */
	@Override
	public QueryInsertStatement getSQLQueryObject() {
		return _insertStatement;
	}

	/** {@inheritDoc} */
	@Override
	public ISqlInsert addTargetColumns(List<? extends ISqlConstant> columnNames) {
		for (ISqlConstant columnName : columnNames) {
			if (columnName.getType() != ISqlConstant.Type.COLUMNNAME) {
				throw new IllegalArgumentException(
						"columnNames contains element of illegal ISqlConstant.Type: "
								+ columnName.getType()
								+ ". columnNames can only contain elements with getType() == ISqlConstant.Type.COLUMNNAME.");

			}
			@SuppressWarnings("unchecked")
			ISQLQueryObject<ValueExpressionColumn> columnExpression = (ISQLQueryObject<ValueExpressionColumn>) columnName;
			getSQLQueryParserFactory().createColumnList(
					_insertStatement.getTargetColumnList(),
					columnExpression.getSQLQueryObject());
		}
		return this;
	}
}