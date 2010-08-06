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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.cis.orchestra.sql;

/**
 * An {@code ISQLSimpleExpression} is for arbitrary SQL fragments and offers an
 * escape hatch from the SQL classes.
 * 
 * @author Sam Donnelly
 */
public interface ISqlSimpleExpression extends ISqlExp {

	/**
	 * Set the wrapped string
	 * 
	 * @param value
	 *            the string
	 * @return this
	 */
	ISqlSimpleExpression setValue(String value);

	/**
	 * The wrapped string
	 * 
	 * @return the wrapped string
	 */
	String getValue();
}
