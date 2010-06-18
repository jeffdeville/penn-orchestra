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
package edu.upenn.cis.orchestra.sql;

/**
 * An {@code ISqlExpression} with implementation-agnostic functionality.
 * 
 * @author Sam Donnelly
 */
public abstract class AbstractSqlExpression implements ISqlExpression {

	/** For making SQL objects. */
	protected static final ISqlFactory _sqlFactory = SqlFactories
			.getSqlFactory();

	/**
	 * Make a {@code ISqlExpression} which evaluates to {@code false}.
	 * 
	 * @return a {@code ISqlExpression} which evaluates to {@code false}
	 */
	public static ISqlExpression falseExp() {
		return _sqlFactory.newExpression(ISqlExpression.Code.EQ, _sqlFactory
				.newConstant("1", ISqlConstant.Type.NUMBER), _sqlFactory
				.newConstant("2", ISqlConstant.Type.NUMBER));
	}

	/** {@inheritDoc} */
	@Override
	public ISqlExp getOperand(final int pos) {
		if (getOperands() == null || pos >= getOperands().size())
			return null;
		return getOperands().get(pos);
	}

	/** {@inheritDoc} */
	@Override
	public boolean isBoolean() {
		return (getCode() == Code.AND || getCode() == Code.OR
				|| getCode() == Code.NOT || getCode() == Code.ALL
				|| getCode() == Code.LIKE || getCode() == Code.BETWEEN
				|| getCode() == Code.EQ || getCode() == Code.NEQ
				|| getCode() == Code.NEQ2 || getCode() == Code.LT
				|| getCode() == Code.LTE || getCode() == Code.GT
				|| getCode() == Code.GTE || getCode() == Code.NOT_BETWEEN
				|| getCode() == Code.IN || getCode() == Code.NOT_IN);
	}

	/** {@inheritDoc} */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + getCode().hashCode();
		result = prime * result
				+ ((getOperands() == null) ? 0 : getOperands().hashCode());
		return result;
	}

	/** {@inheritDoc} */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof ISqlExpression))
			return false;
		ISqlExpression other = (ISqlExpression) obj;
		if (getCode() != other.getCode())
			return false;
		if (getOperands() == null) {
			if (other.getOperands() != null)
				return false;
		} else if (!getOperands().equals(other.getOperands()))
			return false;
		return true;
	}
}
