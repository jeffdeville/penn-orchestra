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

package edu.upenn.cis.orchestra.localupdates.apply.sql;

import java.sql.Connection;
import java.util.List;

import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.NameNotFound;

/**
 * Given tuple is only derivable if its value for field with name {@code
 * fieldName} is found in {@code derivable values}.
 * 
 * @author John Frommeyer
 * 
 */
class DerivabilityCheckFieldCheck implements IDerivabilityCheck {

	private final String fieldName;
	private final List<? extends Object> derivableValues;

	/**
	 * Creates a {@code IDerivabilityCheck} to be used in testing.
	 * 
	 * @param fieldName the tuple field being examined
	 * @param derivableValues allowable values for the tuple field
	 */
	DerivabilityCheckFieldCheck(
			@SuppressWarnings("hiding") final String fieldName,
			@SuppressWarnings("hiding") final List<? extends Object> derivableValues) {
		this.fieldName = fieldName;
		this.derivableValues = derivableValues;
	}

	@Override
	public boolean isDerivable(Tuple tuple, Connection connection) {
		Object value = null;
		try {
			value = tuple.get(fieldName);
		} catch (NameNotFound e) {
			return false;
		}
		return derivableValues.contains(value);
	}

}
