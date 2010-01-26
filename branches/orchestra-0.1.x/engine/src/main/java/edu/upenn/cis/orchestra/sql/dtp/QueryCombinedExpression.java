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

import org.eclipse.datatools.modelbase.sql.query.QueryCombined;
import org.eclipse.datatools.modelbase.sql.query.QueryExpressionBody;
import org.eclipse.datatools.modelbase.sql.query.QuerySelectStatement;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParserFactory;

import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlExpression;

/**
 * A {@code Code.EXCEPT ISqlExpression} implemented with a {@code QueryCombined}
 * .
 * <p>
 * Intentionally package-private.
 * 
 * @author Sam Donnelly
 */
class QueryCombinedExpression extends AbstractDtpSqlExpression<QueryCombined> {

	/** The wrapped {@code QueryCombined}. */
	private final QueryCombined _queryCombined;

	/**
	 * Construct a {@code super(Code.EXCEPT)}.
	 */
	QueryCombinedExpression() {
		super(Code.EXCEPT);
		_queryCombined = getSQLQueryParserFactory().createQueryCombined(null,
				SQLQueryParserFactory.QUERY_COMBINED_EXCEPT, null);
	}

	/**
	 * From a {@code QueryCombined}, construct the equivalent {@code
	 * ISqlExpression}.
	 * 
	 * @param queryCombined source.
	 */
	QueryCombinedExpression(QueryCombined queryCombined) {
		super(Code.EXCEPT);
		_queryCombined = queryCombined;
	}

	/** {@inheritDoc} */
	@Override
	public ISqlExpression addOperand(ISqlExp o) {
		@SuppressWarnings("unchecked")
		final ISQLQueryObject<QuerySelectStatement> querySelectStatementSqlSelect = (ISQLQueryObject<QuerySelectStatement>) o;
		final QueryExpressionBody queryExpressionBody = querySelectStatementSqlSelect
				.getSQLQueryObject().getQueryExpr().getQuery();
		switch (getOperandCount()) {
		case 0:
			_queryCombined.setRightQuery(queryExpressionBody);
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
	public QueryCombined getSQLQueryObject() {
		return _queryCombined;
	}
}
