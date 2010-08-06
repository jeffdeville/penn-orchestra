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



public class IntType extends OptimizerType {
	public static IntType create(boolean nullable, boolean labeledNullable) {
		if (nullable) {
			if (labeledNullable) {
				return FULLY_NULLABLE;
			} else {
				return ONLY_REGULAR_NULL;
			}
		} else if (labeledNullable) {
			return ONLY_LABELED_NULL;
		} else {
			return NOT_NULLABLE;
		}
	}
	
	private static final IntType NOT_NULLABLE = new IntType(false, false);
	private static final IntType ONLY_REGULAR_NULL = new IntType(true, false);
	private static final IntType ONLY_LABELED_NULL = new IntType(false, true);
	private static final IntType FULLY_NULLABLE = new IntType(true,true);
	
	private IntType(boolean nullable, boolean labeledNullable) {
		super(nullable,labeledNullable);
	}
	
	public IntType(int constantValue) {
		super(constantValue);
	}
	
	@Override
	public int getExpectedSize() {
		return Integer.SIZE / Byte.SIZE;
	}

	@Override
	public int getMaximumSize() {
		return Integer.SIZE / Byte.SIZE;
	}

	public String toString() {
		return "INT";
	}

	public int compareTo(OptimizerType t) {
		return getClass().getSimpleName().compareTo(t.getClass().getSimpleName());
	}

	@Override
	Object convertLit(Object lit, OptimizerType to) throws TypeError {
		if (!(lit instanceof Integer)) {
			throw new IllegalArgumentException("Expected integer literal");
		}
		if (to.equals(this)) {
			return lit;
		} else if (to instanceof DoubleType && (! to.valueKnown())) {
			int value = (Integer) lit;
			return new Double(value);
		} else {
			throw new TypeError("Cannot convert from " + this + " to " + to);
		}
	}

	@Override
	public edu.upenn.cis.orchestra.datamodel.IntType getExecutionType() {
		return new edu.upenn.cis.orchestra.datamodel.IntType(nullable, labeledNullable);
	}
	
	public IntType setConstantValue(OptimizerType t) throws TypeError {
		if (! t.valueKnown()) {
			throw new IllegalArgumentException("Supplied type must have a constant value");
		}
		Object newVal = t.convertLit(t.getConstantValue(), this);
		return new IntType((Integer) newVal);
	}

	@Override
	public OptimizerType getWithLabelledNullable(boolean newLabeledNullable) {
		if (this.valueKnown()) {
			throw new IllegalStateException();
		}
		return create(this.nullable, newLabeledNullable);
	}

	@Override
	public OptimizerType getWithNullable(boolean nullable) {
		if (this.valueKnown()) {
			throw new IllegalStateException();
		}
		return create(nullable, this.labeledNullable);
	}
}

