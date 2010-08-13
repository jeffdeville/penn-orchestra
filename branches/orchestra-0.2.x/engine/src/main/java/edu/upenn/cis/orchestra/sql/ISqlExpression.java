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

import java.util.List;

/**
 * An SQL Expression.
 * <p>
 * An SQL expression is an operator and one or more operands Example:
 * <code>a AND b AND c</code> -> operator is <code>AND</code>, operands are
 * <code>{a, b, c}</code>.
 * 
 * @author Sam Donnelly
 */
public interface ISqlExpression extends ISqlExp {

	/** The operator type. */
	public static enum Code {
		/** SQL {@code AND}. */
		AND("and"),

		/** SQL {@code OR}. */
		OR("or"),

		/** SQL {@CODE IN}. */
		IN("in"),

		/** SQL {@code NOT IN}. */
		NOT_IN("not in"),

		/** SQl {@code BETWEEN}. */
		BETWEEN("between"),

		/** SQL {@code NOT BETWEEN}. */
		NOT_BETWEEN("not between"),

		/** SQL {@code LIKE}. */
		LIKE("like"),

		/** SQL {@code NOT LIKE}. */
		NOT_LIKE("not like"),

		/** SQL {@code IS NULL}. */
		IS_NULL("is null"),

		/** SQL {@code IS NOT NULL}. */
		IS_NOT_NULL("is not null"),

		/** SQL {@code *}. */
		STARSSIGN("*"),

		/** SQL {@code ,}. */
		COMMA(","),

		/** SQL {@code UNION}. */
		UNION("union"),

		/** SQL {@code UNION ALL}. */
		UNION_ALL("union all"),

		/** SQL {@code INTERSECT}. */
		INTERSECT("intersect"),

		/** Unary SQL {@code -1}. As in {@code -1}. */
		MINUS("-"),

		/** SQL {@code NOT}. */
		NOT("not"),

		/** SQL {@code EXISTS}. */
		EXISTS("exists"),

		/** SQL {@code EXCEPT}. */
		EXCEPT("except"),

		/** SQL {@code PRIOR}. */
		PRIOR("prior"),

		/** SQL {@code ALL}. */
		ALL("ALL"),

		/** SQL {@code ANY}. */
		ANY("ANY"),

		/** Parameter marker. */
		QUESTIONMARK("?"),

		/** SQL {@code +}. */
		PLUSSIGN("+"),

		/** SQL {@code -}. */
		MINUSSIGN("-"),

		/** SQL {@code *}. */
		MULTSIGN("*"),

		/** SQL {@code /}. */
		DIVSIGN("/"),

		/** SQL {@code ||}. */
		PIPESSIGN("||"),

		/** SQL {@code =}. */
		EQ("="),

		/** SQL {@code <>} */
		NEQ("<>"),

		/** SQL {@code <}. */
		LT("<"),

		/** SQL {@code <=}. */
		LTE("<="),

		/** SQL {@code >}. */
		GT(">"),

		/** SQL {@code >=}. */
		GTE(">="),

		/** SQL {@code COUNT}. */
		COUNT("count"),

		/** SQL {@code SUM}. */
		SUM("sum"),

		/** SQL {@code MAX}. */
		MAX("max"),

		/** SQL {@code AVG}. */
		AVG("avg"),

		/** SQL {@code MIN}. */
		MIN("min"),

		/** SQL {@code !=}. */
		NEQ2("!="),

		/** SQL {@code #}. */
		POUND("#"),
		
		LEAST("least"),
		
		GREATEST("greatest"),

		/** No operator. */
		EMPTY(""),

		/**
		 * It seems that this value means that the expression is not one of
		 * predefined kind and the operator value should be retrieved through
		 * {@code getOperator()}.
		 */
		_NOT_SUPPORTED("_NOT_SUPPORTED");

		/** String representation of this {@code Code}. */
		private final String _operator;

		/**
		 * Construct a {@code Code} with a given operator.
		 * 
		 * @param operator the operator.
		 */
		private Code(String operator) {
			_operator = operator;
		}

		/**
		 * Get the string representation of this {@code Code}.
		 * 
		 * @return the string representation of this {@code Code}.
		 */
		@Override
		public String toString() {
			return _operator;
		}
	}

	/**
	 * Set the this expression's operator with its {@code String} value.
	 * <p>
	 * This can be used for non-predefined operators.
	 * 
	 * @param operator the {@code String} representation of the operator.
	 * @return this {@code ISqlExpression}
	 */
	ISqlExpression setOperator(String operator);

	/**
	 * Get this expression's operator as a <code>String</code>. For example, if
	 * it's {@code EQ}, return a {@code "="}.
	 * 
	 * @return the operator.
	 */
	String getOperator();

	/**
	 * Get this expression's operator.
	 * 
	 * @return the operator.
	 */
	Code getCode();

	/**
	 * Get this expression's operands.
	 * 
	 * @return the operands (as a <code>List</code> of <code>ISqlExp</code>
	 *         objects).
	 */
	List<ISqlExp> getOperands();

	/**
	 * Add an operand to the current expression.
	 * 
	 * @param o the operand to add
	 * @return this {@code ISqlExpression}
	 */
	ISqlExpression addOperand(ISqlExp o);

	/**
	 * Get an operand according to its index (position).
	 * 
	 * @param pos the operand index, starting at 0.
	 * @return the operand at the specified index, <code>null</code> if out of
	 *         bounds.
	 */
	ISqlExp getOperand(int pos);

	/**
	 * Return <code>true</code> if this <code>ISqlExpression</code> is a boolean
	 * expression. For example <code>AND</code>, <code>NOT</code>, or
	 * <code>=</code>, but not <code>UNION</code>. Returns <code>false</code>
	 * otherwise.
	 * 
	 * @return see description.
	 */
	public boolean isBoolean();

}
