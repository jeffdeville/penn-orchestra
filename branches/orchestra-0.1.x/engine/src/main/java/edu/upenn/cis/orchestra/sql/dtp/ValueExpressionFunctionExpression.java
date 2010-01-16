/*
 * Copyright (C) 2010 Trustees of the University of Pennsylvania
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

import org.eclipse.datatools.modelbase.sql.query.QueryValueExpression;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionFunction;

import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlExpression;

public class ValueExpressionFunctionExpression extends
		AbstractDtpSqlExpression<ValueExpressionFunction> {

	private ValueExpressionFunction valueExpressionFunction;

	public ValueExpressionFunctionExpression(final String functionName) {

		if ("AVG".equalsIgnoreCase(functionName)) {
			setCode(Code.AVG);
		} else if ("COUNT".equalsIgnoreCase(functionName)) {
			setCode(Code.COUNT);
		} else if ("MAX".equalsIgnoreCase(functionName)) {
			setCode(Code.MAX);
		} else if ("MIN".equalsIgnoreCase(functionName)) {
			setCode(Code.MIN);
		} else if ("SUM".equalsIgnoreCase(functionName)) {
			setCode(Code.SUM);
		} else {
			setCode(Code._NOT_SUPPORTED);
		}
		valueExpressionFunction = getSQLQueryParserFactory()
				.createFunctionExpression(functionName, null, null, null);
	}

	public ValueExpressionFunctionExpression(final Code code) {
		super(code);

		switch (code) {
		case AVG:
			valueExpressionFunction = getSQLQueryParserFactory()
					.createFunctionExpression("AVG", null, null, null);
		case COUNT:
			valueExpressionFunction = getSQLQueryParserFactory()
					.createFunctionExpression("COUNT", null, null, null);
			break;
		case MAX:
			valueExpressionFunction = getSQLQueryParserFactory()
					.createFunctionExpression("MAX", null, null, null);
			break;
		case MIN:
			valueExpressionFunction = getSQLQueryParserFactory()
					.createFunctionExpression("MIN", null, null, null);
			break;
		case SUM:
			valueExpressionFunction = getSQLQueryParserFactory()
					.createFunctionExpression("SUM", null, null, null);
			break;
		default:
			throw new IllegalArgumentException(code + " not supported");
		}
	}

	@Override
	public ValueExpressionFunction getSQLQueryObject() {
		return valueExpressionFunction;
	}

	@Override
	public ISqlExpression addOperand(final ISqlExp o) {
		// We downcast only as far as necessary and there may not even be any
		// "implements ISQLQueryObject<QueryValueExpression>" classes defined.
		// So
		// for example grepping "implements
		// ISQLQueryObject<QueryValueExpression> may come
		// w/ no results.
		@SuppressWarnings("unchecked")
		ISQLQueryObject<QueryValueExpression> dtpSQLQueryObject = (ISQLQueryObject<QueryValueExpression>) o;
		valueExpressionFunction.getParameterList().add(
				dtpSQLQueryObject.getSQLQueryObject());
		return this;
	}

}
