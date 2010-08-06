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



public class DoubleType extends OptimizerType {
	public static DoubleType create(boolean nullable, boolean labeledNullable) {
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
	
	private static final DoubleType NOT_NULLABLE = new DoubleType(false, false);
	private static final DoubleType ONLY_REGULAR_NULL = new DoubleType(true, false);
	private static final DoubleType ONLY_LABELED_NULL = new DoubleType(false, true);
	private static final DoubleType FULLY_NULLABLE = new DoubleType(true,true);
	
	private DoubleType(boolean nullable, boolean labeledNullable) {
		super(nullable,labeledNullable);
	}
	
	public DoubleType(double constantValue) {
		super(constantValue);
	}

	@Override
	public int getExpectedSize() {
		return Double.SIZE / Byte.SIZE;
	}

	@Override
	public int getMaximumSize() {
		return Double.SIZE / Byte.SIZE;
	}
	
	public String toString() {
		return "DOUBLE";
	}

	public int compareTo(OptimizerType t) {
		return getClass().getSimpleName().compareTo(t.getClass().getSimpleName());
	}

	@Override
	Object convertLit(Object lit, OptimizerType to) throws TypeError {
		if (!(lit instanceof Double)) {
			throw new IllegalArgumentException("Expected double literal");
		}
		if (to.equals(this)) {
			return lit;
		} else if (to instanceof IntType && (! to.valueKnown())) {
			double value = (Double) lit;
			return new Integer((int) value);
		} else {
			throw new TypeError("Cannot convert from " + this + " to " + to);			
		}
	}
	
	public edu.upenn.cis.orchestra.datamodel.DoubleType getExecutionType() {
		return new edu.upenn.cis.orchestra.datamodel.DoubleType(nullable, labeledNullable);
	}

	public DoubleType setConstantValue(OptimizerType t) throws TypeError {
		if (! t.valueKnown()) {
			throw new IllegalArgumentException("Supplied type must have a constant value");
		}
		Object newVal = t.convertLit(t.getConstantValue(), this);
		return new DoubleType((Double) newVal);
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

