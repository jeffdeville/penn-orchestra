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

import org.eclipse.datatools.modelbase.sql.query.PredicateBasic;
import org.eclipse.datatools.modelbase.sql.query.QuerySearchCondition;
import org.eclipse.datatools.modelbase.sql.query.QueryValueExpression;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParserFactory;

import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlExpression;

/**
 * An {@code ISqlExpression} which wraps a {@code PredicateBasic} and implements
 * the comparison predicates.
 * <p>
 * Intentionally package-private.
 * 
 * @author Sam Donnelly
 */
class PredicateBasicSqlExpression extends
		AbstractQuerySearchConditionExpression {

	/** Wrapped DTP object. */
	private final PredicateBasic _predicateBasic;

	/**
	 * Construct an {@code PredicateBasicSqlExpression} give an operator code.
	 * 
	 * @param code
	 *            the code of the expression. Must be on of the {@code int}
	 *            constants from {@code ISqlExpression}.
	 */
	public PredicateBasicSqlExpression(final Code code) {
		super(code);

		switch (code) {
		case EQ:
			_predicateBasic = getSQLQueryParserFactory().createPredicateBasic(
					null, SQLQueryParserFactory.COMPARISON_OPERATOR_EQ, null);
			break;
		case NEQ:
			_predicateBasic = getSQLQueryParserFactory().createPredicateBasic(
					null, SQLQueryParserFactory.COMPARISON_OPERATOR_NE, null);
			break;
		case LT:
			_predicateBasic = getSQLQueryParserFactory().createPredicateBasic(
					null, SQLQueryParserFactory.COMPARISON_OPERATOR_LT, null);
			break;
		case LTE:
			_predicateBasic = getSQLQueryParserFactory().createPredicateBasic(
					null, SQLQueryParserFactory.COMPARISON_OPERATOR_LE, null);
			break;
		case GT:
			_predicateBasic = getSQLQueryParserFactory().createPredicateBasic(
					null, SQLQueryParserFactory.COMPARISON_OPERATOR_GT, null);
			break;
		case GTE:
			_predicateBasic = getSQLQueryParserFactory().createPredicateBasic(
					null, SQLQueryParserFactory.COMPARISON_OPERATOR_GE, null);
			break;
		default:
			throw new IllegalArgumentException("code " + code
					+ " not supported.");
		}
	}

	/**
	 * Construct an {@code PredicateBasicSqlExpression} from a {@code
	 * PredicateBasic}. So this is a DTP->{@code edu.upenn.cis.orchestra.sql}
	 * bridge.
	 * 
	 * @param predicateBasic
	 *            from which we construct this {@code
	 *            PredicateBasicSqlExpression}.
	 */
	PredicateBasicSqlExpression(final PredicateBasic predicateBasic) {
		super(Code.EQ);
		_predicateBasic = predicateBasic;
	}

	/** {@inheritDoc} */
	@Override
	public ISqlExpression addOperand(final ISqlExp o) {

		// We downcast only as far as necessary and there may not even be any
		// "implements ISQLQueryObject<QueryValueExpression>" classes defined.
		@SuppressWarnings("unchecked")
		final ISQLQueryObject<QueryValueExpression> constantQueryValueExpression = (ISQLQueryObject<QueryValueExpression>) o;
		switch (getOperandCount()) {
		case 0:
			_predicateBasic.setLeftValueExpr(constantQueryValueExpression
					.getSQLQueryObject());
			break;
		case 1:
			_predicateBasic.setRightValueExpr(constantQueryValueExpression
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
	public QuerySearchCondition getSQLQueryObject() {
		return _predicateBasic;
	}
}
