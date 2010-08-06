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
package edu.upenn.cis.orchestra.datamodel.exceptions;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.Type;


public class ValueMismatchException extends Exception {
	private static final long serialVersionUID = 1L;
	
	public ValueMismatchException() {
		super("Attempt to use a null value where a non-null value was expected");
	}
	
	public ValueMismatchException(Object o, Type t) {
		super("Attempt to use object '" + o + "' of type " + (o == null ? "null" : o.getClass().getName()) + 
				" as type " + t);
	}
	
	public ValueMismatchException(Type t, AbstractRelation schema, int col) {
		super("Attempt to use object of type " + t + 
				" as type " + schema.getColType(col) + "(" + schema.getColName(col) + " of " + schema.getName() + ")");
	}
	
	public ValueMismatchException(Object o, AbstractRelation schema, int col) {
		super("Attempt to use object '" + o + "' of type " + (o == null ? "null" : o.getClass().getName()) + 
				" as type " + schema.getColType(col) + " (" + schema.getColName(col) + " of " + schema.getName() + ")");
	}
	
	public ValueMismatchException(int label, AbstractRelation schema, int col) {
		super("Attempt to use labled null (label = " + label + ") as type " +
				schema.getColType(col) + " (" + schema.getColName(col) + " of " + schema.getName() + ")");
	}
	
	public ValueMismatchException(AbstractRelation schema, int col) {
		super("Attempt to use null as type " + schema.getColType(col) + " (" + schema.getColName(col) + " of " + schema.getName() + ")");
	}
	
	public ValueMismatchException(Type tActual, Type tExpected) {
		super("Attempt to use object of type " + tActual + 
				" as type " + tExpected);
	}
	
	public ValueMismatchException(Type tActual, int label) {
		super("Attempt to use labeled null (label = " + label + ") as type " + tActual);
	}
}