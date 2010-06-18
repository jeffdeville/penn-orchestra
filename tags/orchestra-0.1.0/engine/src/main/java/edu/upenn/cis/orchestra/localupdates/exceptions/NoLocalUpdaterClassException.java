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
package edu.upenn.cis.orchestra.localupdates.exceptions;

/**
 * Thrown when a {@code LocalUpdaterFactory} cannot create an {@code ILocalUpdater}.	
 * @author John Frommeyer
 *
 */
public class NoLocalUpdaterClassException extends Exception {
	
	private static final long serialVersionUID = 1L;

	/**
	 * Constructor from {@code cause}.
	 * 
	 * @param e
	 */
	public NoLocalUpdaterClassException(Throwable e) {
		super(e);
	}
	
	/**
	 * Constructor from {@code cause} with {@code message}.
	 * @param message 
	 * @param e
	 */
	public NoLocalUpdaterClassException(String message, Throwable e) {
		super(message, e);
	}

	/**
	 * Constructor from {@code message}.
	 * 
	 * @param message
	 */
	public NoLocalUpdaterClassException(String message) {
		super(message);
	}

}
