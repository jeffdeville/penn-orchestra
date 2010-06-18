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

import java.util.List;

import org.eclipse.datatools.modelbase.sql.query.ValuesRow;

import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlExpression;

/**
 * The {@code VALUES} part of an {@code INSERT} statement.
 * <p>
 * Intentionally package-private.
 * 
 * @author Sam Donnelly
 */
class ValuesRowExpression extends AbstractDtpSqlExpression<ValuesRow> {

	/** Backing DTP object. */
	private final ValuesRow _valuesRow = getSQLQueryParserFactory()
			.createValuesRow((List<?>) null);

	/**
	 * Getter.
	 * 
	 * @return the wrapped <code>ValuesRow</code>.
	 */
	ValuesRow getValuesRow() {
		return _valuesRow;
	}

	ValuesRowExpression() {
		super(ISqlExpression.Code.COMMA);
	}

	/** {@inheritDoc} */
	@Override
	public ISqlExpression addOperand(final ISqlExp o) {
		@SuppressWarnings("unchecked")
		final ISQLQueryObject<ValuesRow> dtpValuesRow = (ISQLQueryObject<ValuesRow>) o;

		@SuppressWarnings("unchecked")
		final List<Object> queryValueExpressions = _valuesRow.getExprList();
		queryValueExpressions.add(dtpValuesRow.getSQLQueryObject());

		incrementOperandCount(); // Just for consistency - the value is not
		// used.
		return this;
	}

	/** {@inheritDoc} */
	@Override
	public ValuesRow getSQLQueryObject() {
		return _valuesRow;
	}

}
