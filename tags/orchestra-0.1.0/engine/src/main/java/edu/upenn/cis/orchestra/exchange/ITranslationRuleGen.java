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
package edu.upenn.cis.orchestra.exchange;

import java.util.List;

import edu.upenn.cis.orchestra.datamodel.ITranslationState;
import edu.upenn.cis.orchestra.mappings.Rule;

/**
 * Computes translation rules from user mappings.
 * 
 * @author John Frommeyer
 *
 */
public interface ITranslationRuleGen {

	/**
	 * Creates a set of translation rules and returns the source to target
	 * rules.
	 * 
	 * @return the source to target rules
	 * @throws Exception
	 */

	public List<Rule> computeTranslationRules() throws Exception;

	/**
	 * Returns the translation state of this rule generator.
	 * 
	 * @return the state
	 */
	public ITranslationState getState();

}
