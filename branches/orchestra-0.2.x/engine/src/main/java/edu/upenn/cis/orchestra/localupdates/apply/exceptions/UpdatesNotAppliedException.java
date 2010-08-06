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

/**
 * This exception is thrown when the {@code IApplier} fails to apply all the updates it has been given.
 * @author John Frommeyer
 *
 */
public class UpdatesNotAppliedException extends Exception {

	
	private static final long serialVersionUID = 1L;

	/**
	 * Constructor from {@code cause}.
	 * 
	 * @param cause
	 */
	public UpdatesNotAppliedException(Throwable cause) {
		super(cause);
	}

	/**
	 * Constructor from {@code cause} with {@code message}.
	 * 
	 * @param message
	 * @param cause
	 */
	public UpdatesNotAppliedException(String message, Throwable cause) {
		super(message, cause);
	}

}
