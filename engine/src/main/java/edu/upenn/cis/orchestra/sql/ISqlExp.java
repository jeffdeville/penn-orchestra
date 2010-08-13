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
 * An interface for all SQL Expressions ({@link ISqlSelect}s,
 * {@link ISqlExpression}s, and {@link ISqlConstant}s are {@link ISqlExp}s).
 * 
 * @author Sam Donnelly
 */
public interface ISqlExp {
	/**
	 * Get the SQL for this {@code ISqlExp}.
	 * 
	 * @return the SQL for this {@code ISqlExp}
	 */
	@Override
	public String toString();

	public boolean isBoolean();
	
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
	ISqlExp addOperand(ISqlExp o);

	/**
	 * Get an operand according to its index (position).
	 * 
	 * @param pos the operand index, starting at 0.
	 * @return the operand at the specified index, <code>null</code> if out of
	 *         bounds.
	 */
	ISqlExp getOperand(int pos);
}
