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

import org.testng.Assert;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.repository.dao.RepositorySchemaDAO;
import edu.upenn.cis.orchestra.repository.dao.flatfile.FlatFileRepositoryDAO;

/**
 * @see edu.upenn.cis.orchestra.AbstractOrchestraTest
 * @author John Frommeyer
 * 
 */
public final class OrchestraTest extends AbstractOrchestraTest {

	/** The Orchestra system which will be created and tested. */
	private OrchestraSystem orchestraSystem;

	/** The factory this test will use to create Orchestra operations. */
	private OrchestraSystemOperationFactory operationFactory;

	
	/** Translates Berkeley update store into a DbUnit dataset. */
	private BdbDataSetFactory bdbDataSetFactory;

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.upenn.cis.orchestra.AbstractOrchestraTest#beforePrepare()
	 */
	@Override
	protected final void beforePrepareImpl() {}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.upenn.cis.orchestra.AbstractOrchestraTest#betweenPrepareAndTest()
	 */
	@Override
	protected void betweenPrepareAndTestImpl() throws Exception {
		RepositorySchemaDAO repositorySchemaDAO = new FlatFileRepositoryDAO(
				orchestraSchema.toDocument(dbURL, dbUser, dbPassword));
		orchestraSystem = repositorySchemaDAO.loadAllPeers();
		Assert.assertEquals(orchestraSystem.getName(), orchestraSchemaName);
		orchestraSystem.clearStoreServer();
		// The store server name should probably not be hardcoded.
		File f = new File("updateStore_env");
		if (f.exists() && f.isDirectory()) {
			String[] contents = f.list();
			Assert.assertTrue(contents.length == 0,
					"Store server did not clear.");
		}
		orchestraSystem.startStoreServer();
		Assert.assertTrue(orchestraSystem.storeServerRunning(),
				"Store server is not started.");
		
		bdbDataSetFactory = new BdbDataSetFactory(new File("updateStore_env"));
		operationFactory = new OrchestraSystemOperationFactory(orchestraSystem,
				orchestraSchema, testDataDirectory, onlyGenerateDataSets,
				dbTester, bdbDataSetFactory);
		executor = new OrchestraOperationExecutor(operationFactory);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.upenn.cis.orchestra.AbstractOrchestraTest#shutdown()
	 */
	@Override
	protected void shutdownImpl() throws Exception {
		if (operationFactory != null) {
			orchestraSystem = operationFactory.getOrchestraSystem();
			if (orchestraSystem != null) {
				orchestraSystem.stopStoreServer();
				Assert.assertFalse(orchestraSystem.storeServerRunning(),
						"Store server did not stop.");
				orchestraSystem.getMappingDb().disconnect();
				Assert.assertFalse(
						orchestraSystem.getMappingDb().isConnected(),
						"Mapping DB did not disconnect.");
				logger.debug("Shutting down Orchestra system.");
			}
		}
		if (bdbDataSetFactory != null) {
			bdbDataSetFactory.close();
		}
	}

}
