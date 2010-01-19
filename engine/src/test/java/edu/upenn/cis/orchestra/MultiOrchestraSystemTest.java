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
import java.util.Collection;
import java.util.Map;

import org.testng.Assert;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Schema;

/**
 * @see edu.upenn.cis.orchestra.AbstractMultiSystemOrchestraTest
 * @author John Frommeyer
 * 
 */
public final class MultiOrchestraSystemTest extends
		AbstractMultiSystemOrchestraTest {

	/** The Orchestra systems which will be created and tested. */
	private Map<String, OrchestraSystemTestFrame> peerToOrchestraSystemFrame  = newHashMap();

	/** The factory this test will use to create Orchestra operations. */
	private MultiOrchestraSystemOperationFactory operationFactory;

	/** Translates Berkeley update store into a DbUnit dataset. */
	private BdbDataSetFactory bdbDataSetFactory;

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.AbstractOrchestraTest#beforePrepare()
	 */
	@Override
	protected final void beforePrepareImpl() {}

	/**
	 * {@inheritDoc}
	 * 
	 * @see
	 * edu.upenn.cis.orchestra.AbstractOrchestraTest#betweenPrepareAndTest()
	 */
	@Override
	protected void betweenPrepareAndTestImpl() throws Exception {
		//boolean firstSystem = true;
		File f = new File("updateStore_env");
		if (f.exists() && f.isDirectory()) {
			File[] contents = f.listFiles();
			for (File file : contents) {
				file.delete();
			}
			f.delete();
		}

		for (OrchestraTestFrame testFrame : testFrames) {
			OrchestraSystemTestFrame systemFrame = new OrchestraSystemTestFrame(
					orchestraSchema, testFrame);
			Assert.assertEquals(systemFrame.getOrchestraSystem().getName(),
					orchestraSchemaName);
			peerToOrchestraSystemFrame
					.put(testFrame.getPeerName(), systemFrame);

		}
		assertTrue(peerToOrchestraSystemFrame.values().size() > 0);
		OrchestraSystemTestFrame systemTestFrame = peerToOrchestraSystemFrame.values().iterator().next();
		Map<AbstractPeerID, Schema> peerIDToSchema = newHashMap();
		Collection<Peer> peers = systemTestFrame.getOrchestraSystem().getPeers();
		for (Peer peer : peers) {
			Collection<Schema> schemas = peer.getSchemas();
			assertTrue(schemas.size() == 1);
			peerIDToSchema.put(peer.getPeerId(), schemas.iterator().next());
		}

		bdbDataSetFactory = new BdbDataSetFactory(new File("updateStore_env"), peerIDToSchema);
		operationFactory = new MultiOrchestraSystemOperationFactory(
				peerToOrchestraSystemFrame, orchestraSchema, testDataDirectory,
				onlyGenerateDataSets, bdbDataSetFactory);
		executor = new MultiSystemOrchestraOperationExecutor(operationFactory);
	}


	/**  {@inheritDoc}
	 * @see edu.upenn.cis.orchestra.AbstractMultiSystemOrchestraTest#shutdownImpl()
	 */
	@Override
	protected void shutdownImpl() throws Exception {
		for (OrchestraSystemTestFrame frame : peerToOrchestraSystemFrame
				.values()) {
			OrchestraSystem orchestraSystem = frame.getOrchestraSystem();
			if (orchestraSystem != null) {
				orchestraSystem.stopStoreServer();
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
