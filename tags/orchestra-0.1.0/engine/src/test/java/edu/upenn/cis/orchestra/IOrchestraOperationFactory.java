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
package edu.upenn.cis.orchestra;

import java.io.File;

import org.dbunit.JdbcDatabaseTester;

/**
 * Returns {@code IOrchestraOperation}s for client, usually an {@code
 * IOperationsExecutor}.
 * 
 * @author John Frommeyer
 * 
 */
public interface IOrchestraOperationFactory {

	/**
	 * Returns an {@code IOrchestraOperation} instance based on the name {@code
	 * operation} and file {@code datasetFile}.
	 * 
	 * @param operation an operation name this factory understands.
	 * @param datasetFile
	 * @return an {@code IOrchestraOperation}
	 */
	public IOrchestraOperation newOperation(String operation, File datasetFile);

	/**
	 * Returns an {@code IOrchestraOperation} instance based on the name {@code
	 * operation} and file {@code datasetFile}.
	 * 
	 * @param operation an operation name this factory understands.
	 * @param datasetFile
	 * @param testFrame 
	 * @return an {@code IOrchestraOperation}
	 */
	public IOrchestraOperation newOperation(String operation, File datasetFile,
			OrchestraTestFrame testFrame);

	/**
	 * Returns this factory's {@code OrchestraSchema}.
	 * 
	 * @return this factory's {@code OrchestraSchema}.
	 */
	public OrchestraSchema getOrchestraSchema();

	/**
	 * Returns the directory holding this factory's test data.
	 * 
	 * @return the directory holding this factory's test data.
	 */
	public File getTestDataDirectory();

	/**
	 * Returns this factory's {@code JdbcDatabaseTester}.
	 * 
	 * @return this factory's {@code JdbcDatabaseTester}.
	 */
	public JdbcDatabaseTester getJdbcDatabaseTester();

	/**
	 * Returns {@code true} if this factory will dump, rather than test against,
	 * DbUnit dataset files.
	 * 
	 * @return {@code true} for dump, {@code false} for test.
	 */
	public boolean getDumpDatasets();

	/**
	 * Returns the {@code OrchestraTestFrame} for the given {@code Peer}.
	 * 
	 * @param peerName
	 * @return the {@code OrchestraTestFrame} for the given {@code Peer}
	 */
	public OrchestraTestFrame getOrchestraTestFrame(String peerName);
}
