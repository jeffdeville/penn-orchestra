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
import java.io.FileFilter;
import java.util.Set;
import java.util.SortedMap;
import java.util.regex.Pattern;

import org.dbunit.operation.DatabaseOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

/**
 * Carries out a sequence of Orchestra and/or database operations. The sequence
 * of operations is specified by files with names of form {@code
 * <operation-number>-<operation-name>[-<peer-name>].xml} in a given directory.
 * The ordering is given by {@code operation-number}. The allowed values of
 * {@code operation-name} other than {@code delete}, {@code insert}, and {@code
 * update} are determined by the particular implementation of {@code
 * IOrchestraOperationFactory} being used.
 * <p>
 * Those three special {@code operation-name}s indicate three built in database
 * operations:
 * <dl>
 * <dt>delete</dt>
 * <dd>A database operation carried out by DbUnit using content of file. Deletes
 * rows from tables.</dd>
 * <dt>insert</dt>
 * <dd>A database operation carried out by DbUnit using content of file. Inserts
 * rows into tables.</dd>
 * <dt>update</dt>
 * <dd>A database operation carried out by DbUnit using content of file. Updates
 * rows in tables.</dd>
 * </dl>
 * 
 * @author John Frommeyer
 * 
 */
public class MultiSystemOrchestraOperationExecutor implements
		IOperationExecutor {

	/** The logger. */
	private final Logger logger = LoggerFactory.getLogger(getClass());

	/** The factory that makes our {@code IOrchestraOperation}s. */
	private final IOrchestraOperationFactory opFactory;
	
	/** For use in conditional breakpoints. */
	public static int operationNumber;


	/**
	 * Creates an executor which will use operations made by {@code factory}.
	 * 
	 * @param factory
	 */
	public MultiSystemOrchestraOperationExecutor(IOrchestraOperationFactory factory) {
		this.opFactory = factory;
	}

	/**
	 * Carries out all the operations.
	 * 
	 * @throws Exception
	 */
	@Override
	public final void execute() throws Exception {
		File[] operations = opFactory.getTestDataDirectory().listFiles(
				new FileFilter() {
					// Expected filename format: <number>-<operation name>-<peer
					// name>.xml
					private final Pattern pattern = Pattern
							.compile("\\d+-\\w+-\\w+\\.xml");

					@Override
					public boolean accept(File file) {
						String name = file.getName();
						boolean matches = pattern.matcher(name).matches();
						return matches;
					}
				});
		Assert.assertNotNull(operations, "No operations found in "
				+ opFactory.getTestDataDirectory());
		Assert.assertTrue(operations.length > 0, "No operations found in "
				+ opFactory.getTestDataDirectory());
		// Sort operations by filename.
		SortedMap<String, File> namesToFiles = OrchestraUtil.newTreeMap();
		for (File operation : operations) {
			namesToFiles.put(operation.getName(), operation);
		}
		Set<String> fileNames = namesToFiles.keySet();
		for (String fileName : fileNames) {
			File datasetFile = namesToFiles.get(fileName);
			String[] parsedName = fileName.replace(".xml", "").split("-", 3);
			String operation = parsedName[1];
			String peer = parsedName[2];
			OrchestraTestFrame testFrame = opFactory.getOrchestraTestFrame(peer);
			String opNumberString = parsedName[0];
			operationNumber = Integer.parseInt(opNumberString);
			logger.debug("Operation {} is {}", opNumberString, operation);
			if (operation.equalsIgnoreCase("insert")) {
				DbUnitUtil.executeDbUnitOperation(DatabaseOperation.INSERT,
						datasetFile, testFrame.getDbTester());
			} else if (operation.equalsIgnoreCase("delete")) {
				DbUnitUtil.executeDbUnitOperation(DatabaseOperation.DELETE,
						datasetFile, testFrame.getDbTester());
			} else if (operation.equalsIgnoreCase("update")) {
				DbUnitUtil.executeDbUnitOperation(DatabaseOperation.UPDATE,
						datasetFile, testFrame.getDbTester());
			} else {
				opFactory.newOperation(operation, datasetFile, testFrame)
						.execute();
			}
		}
	}
}
