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

package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import java.util.List;

import com.sleepycat.je.Database;
import com.sleepycat.je.Environment;

import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;

/**
 * DOCUMENT ME
 * @author John Frommeyer
 *
 */
public interface IBdbStoreEnvironment {

	/**
	 * Returns the underlying {@code Environment}.
	 * 
	 * @return the underlying {@code Environment}.
	 */
	public Environment getEnv();

	/**
	 * Takes care of closing everything.
	 * 
	 */
	public void close();

	/**
	 * Returns the underlying {@code Database}s.
	 * 
	 * @return the list of {@code Database}s.
	 */
	public List<Database> getDbs();

	/**
	 * Returns a {@code ISchemaIDBinding}.
	 * 
	 * @return the schemaIDBinding.
	 */
	public ISchemaIDBinding getSchemaIDBinding();

	/**
	 * Returns the format information for this database.
	 * 
	 * @return the format information for this database.
	 */
	public BdbEnvironment getFormat();

}
