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

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.repository.dao.RepositorySchemaDAO;
import edu.upenn.cis.orchestra.repository.dao.flatfile.FlatFileRepositoryDAO;

/**
 * These are the test framework items which we need on a per {@code
 * OrchestraSystem} basis.
 * 
 * @author John Frommeyer
 * 
 */
class OrchestraSystemTestFrame implements ITestFrameWrapper<OrchestraSystem> {
	/** The test frame */
	private final OrchestraTestFrame testFrame;
	private final OrchestraSystem system;

	OrchestraSystemTestFrame(OrchestraSchema orchestraSchema,
			OrchestraTestFrame orchestraTestFrame) throws Exception {
		testFrame = orchestraTestFrame;
		RepositorySchemaDAO repositorySchemaDAO = new FlatFileRepositoryDAO(
				orchestraSchema.toDocument(testFrame.getDbURL(), testFrame
						.getDbUser(), testFrame.getDbPassword(), testFrame
						.getPeerName()));
		system = repositorySchemaDAO.loadAllPeers();

	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.ITestFrameWrapper#getOrchestraController()
	 */
	@Override
	public OrchestraSystem getOrchestraController() {
		return system;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.ITestFrameWrapper#getTestFrame()
	 */
	@Override
	public OrchestraTestFrame getTestFrame() {
		return testFrame;
	}
}
