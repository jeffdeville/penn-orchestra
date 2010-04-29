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
package edu.upenn.cis.orchestra.localupdates.apply;

import edu.upenn.cis.orchestra.localupdates.apply.sql.IDerivabilityCheck;

/**
 * A factory for obtaining {@code IApplier<T>} instances.
 * 
 * @author John Frommeyer
 * @param <T> Intended to represent an object used for a connection to the
 *            underlying database.
 * 
 */
public interface IApplierFactory<T> {

	/**
	 * Returns an appropriate {@code IApplier<T>} instance.
	 * 
	 * @param derivabilityChecker
	 * @return an appropriate {@code IApplier<T>} instance
	 */
	public IApplier<T> getApplyUpdateInst(IDerivabilityCheck derivabilityChecker);

}
