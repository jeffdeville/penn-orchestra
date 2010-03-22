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

import static edu.upenn.cis.orchestra.OrchestraUtil.newHashMap;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.Map;

import org.testng.Assert;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.reconciliation.bdbstore.BdbDataSetFactory;

/**
 * @see edu.upenn.cis.orchestra.AbstractMultiSystemOrchestraTest
 * @author John Frommeyer
 * 
 */
public final class MultiOrchestraSystemTest extends
		AbstractMultiSystemOrchestraTest {

	/** The Orchestra systems which will be created and tested. */
	private Map<String, ITestFrameWrapper<OrchestraSystem>> peerToOrchestraSystemFrame = newHashMap();

	/** The factory this test will use to create Orchestra operations. */
	private MultiOrchestraSystemOperationFactory operationFactory;

	/** Translates Berkeley update store into a DbUnit dataset. */
	private BdbDataSetFactory bdbDataSetFactory;
	
	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.AbstractMultiSystemOrchestraTest#beforePrepare()
	 */
	@Override
	protected final void beforePrepareImpl() {}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.AbstractMultiSystemOrchestraTest#betweenPrepareAndTest()
	 */
	@Override
	protected void betweenPrepareAndTestImpl() throws Exception {
		// boolean firstSystem = true;
		//File f = new File("updateStore_env");
		//if (f.exists() && f.isDirectory()) {
		//	File[] contents = f.listFiles();
		//	for (File file : contents) {
		//		file.delete();
		//	}
		//	f.delete();
		//}
		usClient.startAndClearUpdateStore();
		for (OrchestraTestFrame testFrame : testFrames) {
			File f = new File("stateStore_env_" + testFrame.getPeerName());
			if (f.exists() && f.isDirectory()) {
				File[] contents = f.listFiles();
				for (File file : contents) {
					file.delete();
				}
				f.delete();
			}
			ITestFrameWrapper<OrchestraSystem> systemFrame = new OrchestraSystemTestFrame(
					orchestraSchema, testFrame);
			Assert.assertEquals(systemFrame.getOrchestraController().getName(),
					orchestraSchemaName);
			peerToOrchestraSystemFrame
					.put(testFrame.getPeerName(), systemFrame);
		}
		assertTrue(peerToOrchestraSystemFrame.values().size() > 0);
		
		bdbDataSetFactory = new BdbDataSetFactory(new File("updateStore_env"),
				orchestraSchema.getName(), peerToOrchestraSystemFrame.keySet());
		operationFactory = new MultiOrchestraSystemOperationFactory(
				orchestraSchema, testDataDirectory, onlyGenerateDataSets,
				peerToOrchestraSystemFrame, bdbDataSetFactory);
		executor = new MultiSystemOrchestraOperationExecutor(operationFactory);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.AbstractMultiSystemOrchestraTest#shutdownImpl()
	 */
	@Override
	protected void shutdownImpl() throws Exception {
		for (ITestFrameWrapper<OrchestraSystem> frame : peerToOrchestraSystemFrame
				.values()) {
			OrchestraSystem orchestraSystem = frame.getOrchestraController();
			if (orchestraSystem != null) {
				orchestraSystem.getMappingDb().finalize();
				orchestraSystem.reset(false);
				orchestraSystem.getMappingDb().disconnect();
				Assert.assertFalse(
						orchestraSystem.getMappingDb().isConnected(),
						"Mapping DB did not disconnect.");
				orchestraSystem.disconnect();
				
				logger.debug("Shutting down Orchestra system.");
			}
		}
		//if (bdbDataSetFactory != null) {
		//	bdbDataSetFactory.close();
		//}
		usClient.clearAndStopUpdateStore();
	}

}
