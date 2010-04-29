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

package edu.upenn.cis.orchestra.localupdates.apply.exceptions;

import edu.upenn.cis.orchestra.datamodel.Tuple;

/**
 * Indicates that a {@code IDerivabilityCheck} encountered an error while
 * attempting to determine the derivability of a tuple.
 * 
 * @author John Frommeyer
 * 
 */
public class DerivabilityCheckException extends Exception {

	/** serialVersionUID */
	private static final long serialVersionUID = 1L;

	/**
	 * An exception for {@code IDerivabilityCheck}.
	 * 
	 * @param tuple
	 * @param throwable
	 */
	public DerivabilityCheckException(Tuple tuple, Throwable throwable) {
		super("Error while checking if " + tuple + " is derivable.", throwable);

	}

}
