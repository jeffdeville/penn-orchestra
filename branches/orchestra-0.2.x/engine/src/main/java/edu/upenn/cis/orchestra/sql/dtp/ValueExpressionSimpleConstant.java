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

import org.eclipse.datatools.modelbase.sql.datatypes.PrimitiveType;
import org.eclipse.datatools.modelbase.sql.query.ValueExpressionSimple;
import org.eclipse.datatools.modelbase.sql.query.helper.DataTypeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.upenn.cis.orchestra.sql.ISqlConstant;

/**
 * An {@code ISqlConstant} implemented with a {@code ValueExpressionSimple}.
 * <p>
 * Intentionally package-private.
 * 
 * @author Sam Donnelly
 */
class ValueExpressionSimpleConstant extends
		AbstractSQLQueryObject<ValueExpressionSimple> implements ISqlConstant {

	/** Logger. */
	private static final Logger _logger = LoggerFactory
			.getLogger(ValueExpressionSimpleConstant.class);

	/** Wrapped DTP object. */
	private final ValueExpressionSimple _valueExpressionSimple;

	/**
	 * The type of this <code>ISqlConstant</code>.
	 * <p>
	 * Defaults to {@code ISqlConstant.Type.UNKNOWN}.
	 */
	private final Type _type;

	/**
	 * Create a new constant, given its value and type.
	 * <p>
	 * {@code type} must be one of the {@code static int}s specified in {@code
	 * ISqlConstant}.
	 * 
	 * @param value the value for the constant.
	 * @param type the type of constant.
	 */
	ValueExpressionSimpleConstant(String value, Type type) {
		switch (type) {
		case UNKNOWN:
			_valueExpressionSimple = getSQLQueryParserFactory()
			.createSimpleExpression(value);
			_valueExpressionSimple.setDataType(getSQLQueryParserFactory()
					.createDataTypeCharacterString(
							PrimitiveType.CHARACTER_VARYING, value.length(),
							null));
			break;
		// case NULL:
		// break;
		case NUMBER:
			_valueExpressionSimple = getSQLQueryParserFactory()
					.createSimpleExpression(value);
			_valueExpressionSimple.setDataType(getSQLQueryParserFactory()
					.createDataType(DataTypeHelper.TYPENAME_NUMERIC));
			break;
		case STRING:
			_valueExpressionSimple = getSQLQueryParserFactory()
					.createSimpleExpression("'" + value + "'");
			_valueExpressionSimple.setDataType(getSQLQueryParserFactory()
					.createDataTypeCharacterString(
							PrimitiveType.CHARACTER_VARYING, value.length(),
							null));
			break;
		// case LABELEDNULL:
		case DATE:
			_valueExpressionSimple = getSQLQueryParserFactory()
					.createSimpleExpression(value);
			_valueExpressionSimple.setDataType(getSQLQueryParserFactory()
					.createDataTypeDate());
			break;
		case PREPARED_STATEMENT_PARAMETER:
			final String realValue = "?";
			_valueExpressionSimple = getSQLQueryParserFactory()
					.createSimpleExpression(realValue);
			_valueExpressionSimple.setDataType(getSQLQueryParserFactory()
					.createDataTypeCharacterString(
							PrimitiveType.CHARACTER, realValue.length(),
							null));
			break;
		default:
			throw new IllegalArgumentException("Unsupported type " + type);
		}
		_logger.debug("type[{}]value[{}]", type, value);

		_type = type;
	}

	/** {@inheritDoc} */
	@Override
	public String getValue() {
		return _valueExpressionSimple.getValue();
	}

	/** {@inheritDoc} */
	@Override
	public Type getType() {
		return _type;
	}

	/** {@inheritDoc} */
	@Override
	public ValueExpressionSimple getSQLQueryObject() {
		return _valueExpressionSimple;
	}

	/** {@inheritDoc} */
	// @Override
	// public String toString() {
	// if (_type == OptimizerType.STRING) {
	// return '\'' + _val + '\'';
	// } else if (_type == OptimizerType.LABELEDNULL) {
	// return "null(\'" + _val + "\')";
	// } else if (_type == OptimizerType.DATE) {
	// return "date \'" + _val + "\'";
	// } else {
	// return _val;
	// }
	// }
}
