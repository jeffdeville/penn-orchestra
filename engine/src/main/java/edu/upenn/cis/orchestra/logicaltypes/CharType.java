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


import edu.upenn.cis.orchestra.datamodel.StringType;

public class CharType extends OptimizerType {
	public CharType(String constantValue) {
		super(constantValue);
		this.length = constantValue.length();
	}
	
	public int length;
	public CharType(int length, boolean nullable, boolean labeledNullable) {
		super(nullable, labeledNullable);
		this.length = length;
	}

	@Override
	public int getExpectedSize() {
		return length * Character.SIZE / Byte.SIZE;
	}
	@Override
	public int getMaximumSize() {
		return getExpectedSize();
	}
	
	@Override
	public boolean equals(Object o) {
		if (! super.equals(o)) {
			return false;
		}
		CharType ct = (CharType) o;
		return (length == ct.length);
	}

	public String toString() {
		return "CHAR(" + length + ")";
	}

	public int compareTo(OptimizerType t) {
		int compare = getClass().getSimpleName().compareTo(t.getClass().getSimpleName());
		if (compare != 0) {
			return compare;
		}
		CharType ct = (CharType) t;
		return length - ct.length;
	}

	@Override
	Object convertLit(Object lit, OptimizerType to) throws TypeError {
		if (!(lit instanceof String)) {
			throw new IllegalArgumentException("Expected string literal");
		}
		String s = (String) lit;
		if (to instanceof VarCharType) {
			VarCharType vct = (VarCharType) to;
			if (vct.maxLength >= s.length()) {
				return s;
			} else {
				throw new TypeError("Cannot convert from " + this + " to " + to);			
			}
		} else if (to instanceof CharType) {
			CharType ct = (CharType) to;
			if (ct.length == s.length()) {
				return s;
			} else {
				throw new TypeError("Cannot convert from " + this + " to " + to);			
			}
		} else {
			throw new TypeError("Cannot convert from " + this + " to " + to);			
		}
	}
	
	public StringType getExecutionType() {
		return new StringType(nullable, labeledNullable, false, length);
	}
	
	public CharType setConstantValue(OptimizerType t) throws TypeError {
		if (! t.valueKnown()) {
			throw new IllegalArgumentException("Supplied type must have a constant value");
		}
		Object newVal = t.convertLit(t.getConstantValue(), this);
		return new CharType((String) newVal);
	}

	@Override
	public OptimizerType getWithLabelledNullable(boolean newLabeledNullable) {
		if (this.valueKnown()) {
			throw new IllegalStateException();
		}
		if (newLabeledNullable == this.labeledNullable) {
			return this;
		}
		return new CharType(length, this.nullable, newLabeledNullable);
	}

	@Override
	public OptimizerType getWithNullable(boolean nullable) {
		if (this.valueKnown()) {
			throw new IllegalStateException();
		}
		if (nullable == this.nullable) {
			return this;
		}
		return new CharType(length, nullable, this.labeledNullable);
	}

}

