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

import org.eclipse.datatools.modelbase.sql.query.PredicateLike;
import org.eclipse.datatools.modelbase.sql.query.QuerySearchCondition;
import org.eclipse.datatools.modelbase.sql.query.QueryValueExpression;

import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlExpression;

/**
 * A {@code Code.LIKE ISqlExpression} implemented with a {@code PredicateLike}.
 * 
 * @author Sam Donnelly
 */
class PredicateLikeSqlExpression extends AbstractQuerySearchConditionExpression {

	/** The wrapped DTP object. */
	private final PredicateLike _predicateLike = getSQLQueryParserFactory()
			.createPredicateLike(null, false, null, null);

	/**
	 * Construct a {@code super(Code.LIKE}.
	 */
	PredicateLikeSqlExpression() {
		super(Code.LIKE);
	}

	/** {@inheritDoc} */
	@Override
	public ISqlExpression addOperand(final ISqlExp o) {
		
		// We downcast only as far as necessary and there may not even be any
		// "implements ISQLQueryObject<QueryValueExpression>" classes defined.
		@SuppressWarnings("unchecked")
		final ISQLQueryObject<QueryValueExpression> expressionQueryValueSqlExp = (ISQLQueryObject<QueryValueExpression>) o;
		switch (getOperandCount()) {
		case 0:
			_predicateLike.setMatchingValueExpr(expressionQueryValueSqlExp
					.getSQLQueryObject());
			break;
		case 1:
			_predicateLike.setPatternValueExpr(expressionQueryValueSqlExp
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
		return _predicateLike;
	}
}
