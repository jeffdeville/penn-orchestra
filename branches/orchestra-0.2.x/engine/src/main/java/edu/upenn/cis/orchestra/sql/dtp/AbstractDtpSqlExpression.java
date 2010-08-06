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

import java.util.List;

import org.eclipse.datatools.modelbase.sql.query.PredicateBasic;
import org.eclipse.datatools.modelbase.sql.query.PredicateExists;
import org.eclipse.datatools.modelbase.sql.query.QueryCombined;
import org.eclipse.datatools.modelbase.sql.query.QuerySearchCondition;
import org.eclipse.datatools.modelbase.sql.query.SQLQueryObject;
import org.eclipse.datatools.modelbase.sql.query.SearchConditionCombined;
import org.eclipse.datatools.modelbase.sql.query.SearchConditionCombinedOperator;
import org.eclipse.datatools.modelbase.sql.query.SearchConditionNested;

import edu.upenn.cis.orchestra.sql.AbstractSqlExpression;
import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlExpression;

/**
 * A base class for DTP-backed implementations of {@link ISqlExpression}.
 * <p>
 * It doesn't extend from {@link AbstractSQLQueryObject} because it extends from
 * {@link AbstractSqlExpression}.
 * 
 * Intentionally package-private.
 * 
 * @author Sam Donnelly
 * @param <T>
 *            {@inheritDoc}
 */
abstract class AbstractDtpSqlExpression<T extends SQLQueryObject> extends
		AbstractSqlExpression implements ISQLQueryObject<T> {

	/**
	 * The code of the operator that this object represents.
	 */
	private Code _code;

	/**
	 * Set the code and, unless {@code code} is {@code Code._NOT_SUPPORTED}, set
	 * the operator to {@code code.toString()}. If {@code code} is {@code Code._NOT_SUPPORTED}, set 
	 * the operator to {@code null}. 
	 * 
	 * @param code the code
	 * @return this
	 */
	protected AbstractDtpSqlExpression<T> setCode(final Code code) {
		_code = code;
		if (Code._NOT_SUPPORTED.equals(code)) {
			_op = null;
		} else {
			_op = _code.toString();
		}
		return this;
	}

	/** Keep track of the number of operands we've added. */
	private int _operandCount = 0;

	/** SQL string of this objects <code>operator</code>. */
	private String _op;

	public AbstractDtpSqlExpression() {

	}

	/**
	 * Create an SQL Expression given the operator.
	 * 
	 * @param code
	 *            the type of <code>ISqlExpression</code>.
	 */
	AbstractDtpSqlExpression(final Code code) {
		_code = code;
		_op = _code.toString();
	}

	/** {@inheritDoc} */
	@Override
	public ISqlExpression setOperator(final String operator) {
		if (getCode() == Code._NOT_SUPPORTED) {
			_op = operator;
		} else {
			throw new RuntimeException(
					"Can only set operator if code is not supported.");
		}
		return this;
	}

	/** {@inheritDoc} */
	@Override
	public String getOperator() {
		return _op;
	}

	/** {@inheritDoc} */
	@Override
	public Code getCode() {
		return _code;
	}

	/** {@inheritDoc} */
	@Override
	public List<ISqlExp> getOperands() {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	/**
	 * Increment the operand counter.
	 * 
	 * @return the new value of the operand counter.
	 */
	protected int incrementOperandCount() {
		return _operandCount++;
	}

	/**
	 * Get the current operand counter value.
	 * 
	 * @return the current operand counter value.
	 */
	protected int getOperandCount() {
		return _operandCount;
	}

	/** {@inheritDoc} */
	@Override
	public String toString() {
		return getSQLQueryObject().getSQL();
	}

	/**
	 * Given a {@code QuerySearchCondition}, construct the equivalent {@code
	 * ISqlExpression}.
	 * <p>
	 * Intentionally package-private.
	 * 
	 * @param querySearchCondition
	 *            see description
	 * @return see description
	 */
	static ISqlExpression newSqlExpression(
			final QuerySearchCondition querySearchCondition) {
		if (querySearchCondition instanceof PredicateExists) {
			return new PredicateExistsSqlExpression(
					(PredicateExists) querySearchCondition);
		} else if (querySearchCondition instanceof PredicateBasic) {
			return new PredicateBasicSqlExpression(
					(PredicateBasic) querySearchCondition);
		} else if (querySearchCondition instanceof QueryCombined) {
			return new QueryCombinedExpression(
					(QueryCombined) querySearchCondition);
		} else if (querySearchCondition instanceof SearchConditionCombined) {
			SearchConditionCombined searchConditionCombined = (SearchConditionCombined) querySearchCondition;
			ISqlExpression.Code code;
			if (searchConditionCombined.getCombinedOperator().equals(
					SearchConditionCombinedOperator.AND_LITERAL)) {
				code = ISqlExpression.Code.AND;
			} else if (searchConditionCombined.getCombinedOperator().equals(
					SearchConditionCombinedOperator.OR_LITERAL)) {
				code = ISqlExpression.Code.OR;
			} else {
				throw new IllegalArgumentException(
						"Unsupported SearchConditionCombined operator: "
								+ searchConditionCombined.getCombinedOperator());
			}

			return new SearchConditionCombinedExpression(code,
					(SearchConditionCombined) querySearchCondition);
		} else if (querySearchCondition instanceof SearchConditionNested) {
			if (querySearchCondition.isNegatedCondition()) {
				return new NegateQuerySearchConditionExpression(
						querySearchCondition);
			}
		} else if (querySearchCondition == null) {
			return null;
		}

		throw new IllegalArgumentException("Unsupported querySearchCondition: "
				+ querySearchCondition.getClass());
	}

}
