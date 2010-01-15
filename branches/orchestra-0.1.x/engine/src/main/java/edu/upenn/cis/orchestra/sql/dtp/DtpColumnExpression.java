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

import org.eclipse.datatools.modelbase.sql.query.TableInDatabase;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionColumn;

import edu.upenn.cis.orchestra.sql.DotSeparatedValues;
import edu.upenn.cis.orchestra.sql.IColumnExpression;
import edu.upenn.cis.orchestra.sql.IIndexedStringValues;
import edu.upenn.cis.orchestra.sql.ITable;

/**
 * Dtp-backed {@code IColumnExpression}.
 * <p>
 * Intentionally package-private.
 * 
 * @author Sam Donnelly
 */
class DtpColumnExpression extends AbstractSQLQueryObject<ValueExpressionColumn>
		implements IColumnExpression {

	/** Backing DTP object. */
	private final ValueExpressionColumn _valueExpressionColumn;

	/**
	 * Optional reference to an {@code ITable} to which this {@code
	 * IColumnExpression} belongs.
	 */
	private final ITable _table;

	/**
	 * Create a {@code DtpColumnExpression} with an optionally-qualified column
	 * name.
	 * 
	 * @param column optionally-qualified column name in {@code
	 *            [[schema.]table.]column} form
	 */
	DtpColumnExpression(final String column) {
		final IIndexedStringValues indexedStringValues = new DotSeparatedValues(
				column, 3);
		_valueExpressionColumn = getSQLQueryParserFactory()
				.createColumnExpression(indexedStringValues.get(0),
						indexedStringValues.get(1), indexedStringValues.get(2));
		if (_valueExpressionColumn.getTableExpr() == null) {
			_table = null;
		} else {
			// This cast is safe because of the way
			// getSQLQueryParserFactory.createColumnExpression(indexedStringValues.get(0),
			// indexedStringValues.get(1), indexedStringValues.get(2)) is
			// implemented.
			_table = new DtpTable((TableInDatabase) _valueExpressionColumn
					.getTableExpr());
		}
	}

	/** {@inheritDoc} */
	@Override
	public String getColumn() {
		return _valueExpressionColumn.getName();
	}

	/** {@inheritDoc} */
	@Override
	public ITable getTable() {
		return _table;
	}

	/** {@inheritDoc} */
	@Override
	public ValueExpressionColumn getSQLQueryObject() {
		return _valueExpressionColumn;
	}

	/** {@inheritDoc} */
	@Override
	public String getTableName() {
		return getTable() != null ? getTable().getName() : null;
	}

	/** {@inheritDoc} */
	@Override
	public String getSchemaName() {
		return getTable() != null ? getTable().getSchemaName() : null;
	}
}
