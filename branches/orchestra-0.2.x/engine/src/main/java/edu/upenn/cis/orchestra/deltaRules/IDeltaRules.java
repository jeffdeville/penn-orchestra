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
package edu.upenn.cis.orchestra.deltaRules;

import java.util.List;

import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.datalog.DatalogEngine;
import edu.upenn.cis.orchestra.datalog.DatalogSequence;

/**
 * Interface for incremental update exchange rules.
 * 
 * @author John Frommeyer
 * 
 */
public interface IDeltaRules {

	/**
	 * Executes the rules and returns the net number
	 * of milliseconds this execution took.
	 * 
	 * @param de
	 * @return the net number of milliseconds this execution took
	 * @throws Exception
	 */
	public long execute(DatalogEngine de) throws Exception;

	public void generate(DatalogEngine de) throws Exception;

	/**
	 * Performs any necessary cleanup of prepared statements.
	 * 
	 */
	public void cleanupPreparedStmts();

	/**
	 * Returns the rules.
	 * 
	 * @return the rules
	 */
	public List<DatalogSequence> getCode();

	/**
	 * Returns a XML representation of the datalog code.
	 * 
	 * @return a XML representation of the datalog code
	 */
	public Document serialize();

	/**
	 * Returns a XML representation of the code these rules generate.
	 * 
	 * @return a XML representation of the code these rules generate
	 */
	public Document serializeAsCode();
}
