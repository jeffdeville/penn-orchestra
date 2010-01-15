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

/**
 * 
 * 
 * @author Sam Donnelly
 */
abstract class AbstractQuerySearchConditionExpression extends
		AbstractDtpSqlExpression<QuerySearchCondition> {

	/**
	 * Construct a new {@code AbstractQuerySearchConditionExpression} of type
	 * {@code code}.
	 * 
	 * @param code the type of {@code ISqlExpression}.
	 */
	AbstractQuerySearchConditionExpression(final Code code) {
		super(code);
	}

	/** {@inheritDoc} */
	@Override
	public String toString() {
		// The nesting is for backward compatibility with ZQL.
		return getSQLQueryParserFactory().createNestedCondition(getSQLQueryObject()).getSQL();
	}
}
