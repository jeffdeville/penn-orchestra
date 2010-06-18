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
 * WITHOUT WARRANTIES OR CONDITIONS of ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.cis.orchestra.logicaltypes;


public abstract class OptimizerType implements Comparable<OptimizerType> {

	public static class TypeError extends Exception {
		private static final long serialVersionUID = 1L;
		public TypeError(String error) {
			super(error);
		}
	}	

	public abstract int getExpectedSize();
	public abstract int getMaximumSize();
	
	/**
	 * Convert the constant value of this type to a literal of
	 * another type
	 * 
	 * @param to			The desired type of the literal
	 * @return				A literal of the desired type
	 * @throws Expression.TypeError
	 * 						if the conversion cannot be made
	 */
	abstract Object convertLit(Object lit, OptimizerType to) throws TypeError;

	public boolean equals(Object o) {
		return (o != null && o.getClass() == this.getClass());
	}
	
	abstract public String toString();
	// If variable being typed is known to have a constant value,
	// put it here.
	private final Object constantValue;
	
	public final boolean nullable;
	public final boolean labeledNullable;
	
	OptimizerType(boolean nullable, boolean labeledNullable) {
		constantValue = null;
		this.nullable = nullable;
		this.labeledNullable = labeledNullable;
	}
	
	public OptimizerType(Object constantValue) {
		if (constantValue == null) {
			throw new NullPointerException();
		}
		this.constantValue = constantValue;
		nullable = false;
		labeledNullable = false;
	}
	
	public boolean valueKnown() {
		return (constantValue != null);
	}
	
	public Object getConstantValue() {
		return constantValue;
	}
	
	/**
	 * Create a new OptimizerType object of the same kind as this one
	 * but with the constant value from another
	 * 
	 * @param t			The type with the constant value to convert
	 * @throws Expression.TypeError
	 * 					If the conversion could not take place
	 */
	public abstract OptimizerType setConstantValue(OptimizerType t) throws TypeError;
	
	public abstract edu.upenn.cis.orchestra.datamodel.Type getExecutionType();
	
	public abstract OptimizerType getWithLabelledNullable(boolean newLabeledNullable);
	
	public abstract OptimizerType getWithNullable(boolean nullable);
}