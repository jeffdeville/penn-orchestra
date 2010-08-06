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

import edu.upenn.cis.orchestra.localupdates.apply.IApplier;
import edu.upenn.cis.orchestra.localupdates.apply.IApplierFactory;

/**
 * A Factory which creates {@code IApplierFactory<java.sql.Connection>}
 * instances.
 * 
 * @author John Frommeyer
 * 
 */
public class ApplierFactoryJdbc implements IApplierFactory<Connection> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.upenn.cis.orchestra.localupdates.apply.IApplierFactory#getApplyUpdateInst
	 * (IDerivabilityCheck)
	 */
	@Override
	public IApplier<Connection> getApplyUpdateInst(IDerivabilityCheck derivabilityChecker) {
		return new ApplierSql(derivabilityChecker);
	}

}
