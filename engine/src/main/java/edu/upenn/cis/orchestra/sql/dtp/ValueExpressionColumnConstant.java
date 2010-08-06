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

import org.eclipse.datatools.modelbase.sql.query.ValueExpressionColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.upenn.cis.orchestra.sql.IColumnExpression;
import edu.upenn.cis.orchestra.sql.ISqlConstant;

/**
 * A DTP-backed {@code ISqlConstant.getType() == OptimizerType.COLUMNNAME}.
 * <p>
 * Intentionally package-private.
 * 
 * @author Sam Donnelly
 */
class ValueExpressionColumnConstant extends
		AbstractSQLQueryObject<ValueExpressionColumn> implements ISqlConstant {

	/** Logger. */
	private static final Logger _logger = LoggerFactory
			.getLogger(ValueExpressionColumnConstant.class);

	/** The wrapped DTP object. */
	private final ValueExpressionColumn _valueExpressionColumn;

	/**
	 * Construct a {@code ConstantValueExpression} with the given column name.
	 * 
	 * @param column the column, in {@code [table.]column]} form.
	 */
	ValueExpressionColumnConstant(final String column) {
		_logger.debug("value: {}", column);
		IColumnExpression columnExp = _sqlFactory.newColumnExpression(column);
		_valueExpressionColumn = getSQLQueryParserFactory()
				.createColumnExpression(columnExp.getColumn(),
						columnExp.getTableName());
		_logger.debug("toString(): {}", toString());
	}

	/** {@inheritDoc} */
	@Override
	public Type getType() {
		return Type.COLUMNNAME;
	}

	/** {@inheritDoc} */
	@Override
	public String getValue() {
		return _valueExpressionColumn.getName();
	}

	/** {@inheritDoc} */
	@Override
	public ValueExpressionColumn getSQLQueryObject() {
		return _valueExpressionColumn;
	}
}
