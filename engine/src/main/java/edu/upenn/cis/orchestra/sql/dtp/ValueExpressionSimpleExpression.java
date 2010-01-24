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

import org.eclipse.datatools.modelbase.sql.query.ValueExpressionSimple;

import edu.upenn.cis.orchestra.sql.ISqlSimpleExpression;

/**
 * A {@code ValueExpresssionSimple} implementation of {@code
 * ISqlSimpleExpression} .
 * 
 * @author Sam Donnelly
 */
class ValueExpressionSimpleExpression implements ISqlSimpleExpression,
		ISQLQueryObject<ValueExpressionSimple> {

	private ValueExpressionSimple valueExpressionSimple = getSQLQueryParserFactory()
			.createSimpleExpression(null);

	@Override
	public String getValue() {
		return valueExpressionSimple.getValue();
	}

	@Override
	public ISqlSimpleExpression setValue(String value) {
		valueExpressionSimple.setValue(value);
		return this;
	}

	@Override
	public ValueExpressionSimple getSQLQueryObject() {
		return valueExpressionSimple;
	}

}
