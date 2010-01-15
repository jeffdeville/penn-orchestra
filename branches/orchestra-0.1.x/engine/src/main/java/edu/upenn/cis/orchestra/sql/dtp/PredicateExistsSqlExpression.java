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

import org.eclipse.datatools.modelbase.sql.query.PredicateExists;
import org.eclipse.datatools.modelbase.sql.query.QuerySearchCondition;
import org.eclipse.datatools.modelbase.sql.query.QuerySelectStatement;

import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlExpression;

/**
 * DTP-backed {@code ISqlExpression} for type {@code ISqlExpression.Code.EXISTS}
 * .
 * <p>
 * Intentionally package-private.
 * 
 * @author Sam Donnelly
 */
class PredicateExistsSqlExpression extends
		AbstractQuerySearchConditionExpression {

	/** DTP backing object. */
	private final PredicateExists _predicateExists;

	/**
	 * Construct a {@code PredicateExistsSqlExpression}.
	 */
	PredicateExistsSqlExpression() {
		super(Code.EXISTS);
		_predicateExists = getSQLQueryParserFactory().createPredicateExists(
				null);
	}

	/**
	 * Build an {@code PredicateExistsSqlExpression} from a {@code
	 * PredicateExists} .
	 * 
	 * @param predicateExists from which to build our {@code
	 *            PredicateExistsSqlExpression}.
	 */
	PredicateExistsSqlExpression(final PredicateExists predicateExists) {
		super(Code.EXISTS);
		_predicateExists = predicateExists;
	}

	/** {@inheritDoc} */
	@Override
	public ISqlExpression addOperand(final ISqlExp o) {
		@SuppressWarnings("unchecked")
		ISQLQueryObject<QuerySelectStatement> querySelectStatementSqlExp = (ISQLQueryObject<QuerySelectStatement>) o;
		_predicateExists.setQueryExpr(querySelectStatementSqlExp
				.getSQLQueryObject().getQueryExpr().getQuery());
		incrementOperandCount(); // Just for consistency - it's not functional.
		return this;
	}

	/** {@inheritDoc} */
	@Override
	public QuerySearchCondition getSQLQueryObject() {
		return _predicateExists;
	}
}
