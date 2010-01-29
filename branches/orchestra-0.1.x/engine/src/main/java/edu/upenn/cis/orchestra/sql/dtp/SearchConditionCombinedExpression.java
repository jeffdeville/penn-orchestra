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
import org.eclipse.datatools.modelbase.sql.query.SearchConditionCombined;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParserFactory;

import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlExpression;

/**
 * An {@code AbstractQuerySearchConditionExpression} backed with a {@code
 * SearchConditionCombinedExpression}.
 * <p>
 * Intentionally package-private.
 * 
 * @author Sam Donnelly
 */
class SearchConditionCombinedExpression extends
		AbstractQuerySearchConditionExpression {

	/** Backing DTP object. */
	private final SearchConditionCombined _searchConditionCombined;

	/**
	 * Getter.
	 * 
	 * @return the wrapped <code>QuerySearchCondition</code>.
	 */
	public QuerySearchCondition getQuerySearchCondition() {
		return _searchConditionCombined;
	}

	/**
	 * Create a new {@code SearchConditionCombinedExpression} with a given
	 * {@code Code} and {@code SearchConditionCombined}.
	 * 
	 * @param code the {@Code} for this {@code ISqlExpression}
	 * @param searchConditionCombined the backing {@code
	 *            SearchConditionCombined}
	 */
	SearchConditionCombinedExpression(final Code code,
			final SearchConditionCombined searchConditionCombined) {
		super(code);
		_searchConditionCombined = searchConditionCombined;
	}

	/**
	 * From a {@code Code} construct a {@code SearchConditionCombined}. So, this
	 * is an {@code edu.upenn.cis.orchestra.sql ->
	 * edu.upenn.cis.orchestra.sql.dtp} mapping.
	 * 
	 * 
	 * @param code must be {@code AND} or {@code OR}.
	 * 
	 * @throws IllegalArgumentException if {@code Code} is note {@code AND} or
	 *             {@code OR}
	 */
	SearchConditionCombinedExpression(final Code code) {
		super(code);
		switch (code) {
		case AND:
			_searchConditionCombined = getSQLQueryParserFactory()
					.createCombinedCondition(null, null,
							SQLQueryParserFactory.COMBINED_OPERATOR_AND);
			break;
		case OR:
			_searchConditionCombined = getSQLQueryParserFactory()
					.createCombinedCondition(null, null,
							SQLQueryParserFactory.COMBINED_OPERATOR_OR);
			break;
		default:
			throw new IllegalArgumentException(code + " not supported");
		}
	}

	/** {@inheritDoc} */
	@Override
	public ISqlExpression addOperand(final ISqlExp o) {
		@SuppressWarnings("unchecked")
		final ISQLQueryObject<QuerySearchCondition> expressionQuerySearchCondition = (ISQLQueryObject<QuerySearchCondition>) o;
		final QuerySearchCondition condition = getSQLQueryParserFactory()
		// Nested for backward compatibility with ZQL.
				.createNestedCondition(
						expressionQuerySearchCondition.getSQLQueryObject());
		switch (getOperandCount()) {
		case 0:
			_searchConditionCombined.setLeftCondition(condition);
			break;
		case 1:
			_searchConditionCombined.setRightCondition(condition);
			break;
		default:
			throw new IllegalStateException("Too many operands: "
					+ (getOperandCount() + 1));
		}
		incrementOperandCount();
		return this;
	}

	/** {@inheritDoc} */
	@Override
	public QuerySearchCondition getSQLQueryObject() {
		return _searchConditionCombined;
	}
}
