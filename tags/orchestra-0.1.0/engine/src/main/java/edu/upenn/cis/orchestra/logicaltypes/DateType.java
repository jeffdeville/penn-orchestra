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


import edu.upenn.cis.orchestra.datamodel.Date;

public class DateType extends OptimizerType {
	public static DateType create(boolean nullable, boolean labeledNullable) {
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
	
	private static final DateType NOT_NULLABLE = new DateType(false, false);
	private static final DateType ONLY_REGULAR_NULL = new DateType(true, false);
	private static final DateType ONLY_LABELED_NULL = new DateType(false, true);
	private static final DateType FULLY_NULLABLE = new DateType(true,true);

	private DateType(boolean nullable, boolean labeledNullable) {
		super(nullable,labeledNullable);
	}
	
	public DateType(Date constantValue) {
		super(constantValue);
	}

	@Override
	public int getExpectedSize() {
		return Date.bytesPerDate;
	}

	@Override
	public int getMaximumSize() {
		return Date.bytesPerDate;
	}
	
	public String toString() {
		return "DATE";
	}

	public int compareTo(OptimizerType t) {
		return getClass().getSimpleName().compareTo(t.getClass().getSimpleName());
	}

	@Override
	Object convertLit(Object lit, OptimizerType to) throws TypeError {
		if (!(lit instanceof Date)) {
			throw new IllegalArgumentException("Expected date literal");
		}
		if (to.equals(this)) {
			return lit;
		}
		throw new TypeError("Cannot convert from " + this + " to " + to);			
	}
	
	public edu.upenn.cis.orchestra.datamodel.DateType getExecutionType() {
		return new edu.upenn.cis.orchestra.datamodel.DateType(nullable, labeledNullable);
	}

	public DateType setConstantValue(OptimizerType t) throws TypeError {
		if (! t.valueKnown()) {
			throw new IllegalArgumentException("Supplied type must have a constant value");
		}
		Object newVal = t.convertLit(t.getConstantValue(), this);
		return new DateType((Date) newVal);
	}

	@Override
	public OptimizerType getWithLabelledNullable(boolean newLabelledNullable) {
		if (this.valueKnown()) {
			throw new IllegalStateException();
		}
		return create(this.nullable, newLabelledNullable);
	}

	@Override
	public OptimizerType getWithNullable(boolean nullable) {
		if (this.valueKnown()) {
			throw new IllegalStateException();
		}
		return create(nullable, this.labeledNullable);
	}
}
