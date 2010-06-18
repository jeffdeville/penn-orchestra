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

import org.eclipse.datatools.modelbase.sql.query.PredicateIsNull;
import org.eclipse.datatools.modelbase.sql.query.QuerySearchCondition;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionColumn;

import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlExpression;

/**
 * DTP-backed {@code ISqlExpression} for type {@code
 * ISqlExpression.Code.IS_NULL} .
 * <p>
 * Intentionally package-private.
 * 
 * @author John Frommeyer
 */
class PredicateIsNullSqlExpression extends
		AbstractQuerySearchConditionExpression {

	/** DTP backing object. */
	private final PredicateIsNull _predicateIsNull;

	/**
	 * Construct a {@code PredicateIsNullSqlExpression}.
	 */
	PredicateIsNullSqlExpression() {
		super(Code.IS_NULL);
		_predicateIsNull = getSQLQueryParserFactory().createPredicateNull(null,
				false);
	}

	/**
	 * Build an {@code PredicateIsNullSqlExpression} from a {@code
	 * PredicateIsNull} .
	 * 
	 * @param predicateIsNull from which to build our {@code
	 *            PredicateIsNullSqlExpression}.
	 */
	PredicateIsNullSqlExpression(final PredicateIsNull predicateIsNull) {
		super(Code.IS_NULL);
		_predicateIsNull = predicateIsNull;
	}

	/** {@inheritDoc} */
	@Override
	public ISqlExpression addOperand(final ISqlExp o) {
		@SuppressWarnings("unchecked")
		AbstractSQLQueryObject<ValueExpressionColumn> querySelectStatementSqlExp = (AbstractSQLQueryObject<ValueExpressionColumn>) o;
		_predicateIsNull.setValueExpr(querySelectStatementSqlExp
				.getSQLQueryObject());
		incrementOperandCount(); // Just for consistency - it's not functional.
		return this;
	}

	/** {@inheritDoc} */
	@Override
	public QuerySearchCondition getSQLQueryObject() {
		return _predicateIsNull;
	}
}
