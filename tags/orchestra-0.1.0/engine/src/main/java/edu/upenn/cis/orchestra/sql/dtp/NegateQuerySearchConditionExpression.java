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

import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlExpression;

/**
 * A {@code Code.NOT ISqlExpression} implemented with a {@code
 * QuerySearchCondition}.
 * <p>
 * Intentionally package-private.
 * 
 * @author Sam Donnelly
 */
class NegateQuerySearchConditionExpression extends
		AbstractQuerySearchConditionExpression {

	/** Wrapped DTP object. */
	private QuerySearchCondition _querySearchCondition;

	/**
	 * Construct a {@code NegateQuerySearchConditionExpression}.
	 */
	NegateQuerySearchConditionExpression() {
		super(Code.NOT);
	}

	/**
	 * Construct a {@code NegateQuerySearchConditionExpression} with a {@code
	 * QuerySearchCondition}.
	 * 
	 * @param querySearchCondition the source expression
	 */
	NegateQuerySearchConditionExpression(
			QuerySearchCondition querySearchCondition) {
		super(Code.NOT);
		_querySearchCondition = querySearchCondition;
	}

	/** {@inheritDoc} */
	@Override
	public ISqlExpression addOperand(ISqlExp operand) {
		@SuppressWarnings("unchecked")
		ISQLQueryObject<QuerySearchCondition> querySearchConditionOperand = (ISQLQueryObject<QuerySearchCondition>) operand;
		_querySearchCondition = getSQLQueryParserFactory()
				.createNestedConditionNegated(
						querySearchConditionOperand.getSQLQueryObject());
		return this;
	}

	/** {@inheritDoc} */
	@Override
	public QuerySearchCondition getSQLQueryObject() {
		return _querySearchCondition;
	}
}
