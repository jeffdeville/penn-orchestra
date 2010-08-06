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

import org.eclipse.datatools.modelbase.sql.query.QueryValueExpression;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionCombined;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParserFactory;

import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlExpression;

class ValueExpressionCombinedExpression extends
		AbstractDtpSqlExpression<ValueExpressionCombined> {

	/** Wrapped DTP object. */
	private final ValueExpressionCombined _valueExpressionCombined;

	ValueExpressionCombinedExpression(final Code code) {
		super(code);
		switch (code) {
		case PLUSSIGN:
			_valueExpressionCombined = getSQLQueryParserFactory()
					.createCombinedExpression(null,
							SQLQueryParserFactory.COMBINED_OPERATOR_ADD, null);
			break;
		case MINUSSIGN:
			_valueExpressionCombined = getSQLQueryParserFactory()
					.createCombinedExpression(null,
							SQLQueryParserFactory.COMBINED_OPERATOR_SUBTRACT,
							null);
			break;
		case MULTSIGN:
			_valueExpressionCombined = getSQLQueryParserFactory()
					.createCombinedExpression(null,
							SQLQueryParserFactory.COMBINED_OPERATOR_MULTIPLY,
							null);
			break;
		case DIVSIGN:
			_valueExpressionCombined = getSQLQueryParserFactory()
					.createCombinedExpression(null,
							SQLQueryParserFactory.COMBINED_OPERATOR_DIVIDE,
							null);
			break;
		case PIPESSIGN:
			_valueExpressionCombined = getSQLQueryParserFactory()
					.createCombinedExpression(
							null,
							SQLQueryParserFactory.COMBINED_OPERATOR_CONCATENATE,
							null);
			break;
		default:
			throw new IllegalArgumentException(code + " not supported");
		}
	}

	/** {@inheritDoc} */
	@Override
	public ISqlExpression addOperand(final ISqlExp o) {

		// We downcast only as far as necessary and there may not even be any
		// "implements ISQLQueryObject<QueryValueExpression>" classes defined. So
		// for example grepping "implements ISQLQueryObject<QueryValueExpression> may come
		// w/ no results.
		@SuppressWarnings("unchecked")
		ISQLQueryObject<QueryValueExpression> dtpSQLQueryObject = (ISQLQueryObject<QueryValueExpression>) o;

		switch (getOperandCount()) {
		case 0:
			_valueExpressionCombined.setLeftValueExpr(dtpSQLQueryObject
					.getSQLQueryObject());
			break;
		case 1:
			_valueExpressionCombined.setRightValueExpr(dtpSQLQueryObject
					.getSQLQueryObject());
			break;
		default:
			throw new IllegalStateException("Too many operands: "
					+ getOperandCount() + 1);
		}
		incrementOperandCount();
		return this;
	}

	/** {@inheritDoc} */
	@Override
	public ValueExpressionCombined getSQLQueryObject() {
		return _valueExpressionCombined;
	}

}
