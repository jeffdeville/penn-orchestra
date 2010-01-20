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
import java.util.ArrayList;
import java.util.Map;

import org.dbunit.JdbcDatabaseTester;
import org.testng.Assert;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.exchange.IEngine;
import edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding;

/**
 * Creates {@code IOrchestraOperation}s which deal directly with a collection
 * {@code OrchestraSystem} instances indexed by peer. The idea is that each peer
 * has its own system.
 * 
 * @see edu.upenn.cis.orchestra.AbstractDsFileOperationFactory
 * @author John Frommeyer
 * 
 */
public final class MultiOrchestraSystemOperationFactory extends
		AbstractDsFileOperationFactory {

	/**
	 * An Orchestra Create operation.
	 * 
	 */
	private class CreateOperation implements IOrchestraOperation {
		/** The DbUnit dataset file we will be writing or testing against. */
		private final File datasetFile;
		private final String peerName;

		/**
		 * Creates a create operation.
		 * 
		 * @param datasetFile
		 * @param peerName
		 */
		private CreateOperation(
				@SuppressWarnings("hiding") final File datasetFile,
				@SuppressWarnings("hiding") final String peerName) {
			this.datasetFile = datasetFile;
			this.peerName = peerName;
		}

		/**
		 * @see edu.upenn.cis.orchestra.IOrchestraOperation#execute()
		 */
		@Override
		public void execute() throws Exception {
			OrchestraSystemTestFrame systemTestFrame = localPeerToSystemFrame
					.get(peerName);
			OrchestraSystem localSystem = systemTestFrame.getOrchestraSystem();

			IEngine mappingEngine = localSystem.getMappingEngine();
			mappingEngine.createBaseSchemaRelations();

			MetaDataChecker checker = new MetaDataChecker.Builder()
					.checkTypes().build();
			DbUnitUtil.dumpOrCheckAndMetaCheck(datasetFile, checker,
					orchestraSchema, dumpDatasets, systemTestFrame
							.getDbTester(), bdbDataSetFactory);
		}
	}

	/**
	 * An Orchestra Migrate operation.
	 * 
	 */
	private class MigrateOperation implements IOrchestraOperation {
		/** The DbUnit dataset file we will be writing or testing against. */
		private final File datasetFile;
		private final String peerName;

		/**
		 * Creates a migrate operation.
		 * 
		 * @param datasetFile
		 * @param peerName
		 */
		public MigrateOperation(
				@SuppressWarnings("hiding") final File datasetFile,
				@SuppressWarnings("hiding") final String peerName) {
			this.datasetFile = datasetFile;
			this.peerName = peerName;
		}

		/**
		 * @see edu.upenn.cis.orchestra.IOrchestraOperation#execute()
		 */
		@Override
		public void execute() throws Exception {
			OrchestraSystemTestFrame systemTestFrame = localPeerToSystemFrame
					.get(peerName);
			OrchestraSystem localSystem = systemTestFrame.getOrchestraSystem();
			localSystem.getMappingEngine().migrate();

			MetaDataChecker checker = new MetaDataChecker.Builder()
					.checkTypes().checkForLabeledNulls().build();
			DbUnitUtil.dumpOrCheckAndMetaCheck(datasetFile, checker,
					orchestraSchema, dumpDatasets, systemTestFrame
							.getDbTester(), bdbDataSetFactory);
		}
	}

	/**
	 * An Orchestra Import operation.
	 * 
	 */
	private class ImportOperation implements IOrchestraOperation {

		/** The DbUnit dataset file we will be writing or testing against. */
		private final File datasetFile;
		private final String peerName;

		/**
		 * Creates a new import operation.
		 * 
		 * @param datasetFile
		 * @param peerName
		 */
		private ImportOperation(
				@SuppressWarnings("hiding") final File datasetFile,
				@SuppressWarnings("hiding") final String peerName) {
			this.datasetFile = datasetFile;
			this.peerName = peerName;
		}

		/**
		 * @see edu.upenn.cis.orchestra.IOrchestraOperation#execute()
		 */
		@Override
		public void execute() throws Exception {
			ArrayList<String> succeeded = OrchestraUtil.newArrayList();
			ArrayList<String> failed = OrchestraUtil.newArrayList();
			OrchestraSystemTestFrame systemTestFrame = localPeerToSystemFrame
					.get(peerName);
			OrchestraSystem localSystem = systemTestFrame.getOrchestraSystem();

			localSystem.importUpdates(testDataDirectory.getPath(), succeeded,
					failed);

			Assert.assertFalse(succeeded.isEmpty());
			Assert.assertTrue(failed.isEmpty());

			DbUnitUtil.dumpOrCheck(datasetFile, orchestraSchema, dumpDatasets,
					systemTestFrame.getDbTester(), bdbDataSetFactory);

		}
	}

	/**
	 * An Orchestra Publication operation.
	 * 
	 */
	private class PublishOperation implements IOrchestraOperation {

		/** The DbUnit dataset file we will be writing or testing against. */
		private final File datasetFile;
		private final String peerName;

		/**
		 * Creates a new publish operation.
		 * 
		 * @param datasetFile
		 * @param peerName
		 * @param peerName
		 */
		public PublishOperation(
				@SuppressWarnings("hiding") final File datasetFile,
				@SuppressWarnings("hiding") final String peerName) {
			this.datasetFile = datasetFile;
			this.peerName = peerName;
		}

		/**
		 * @see edu.upenn.cis.orchestra.IOrchestraOperation#execute()
		 */
		@Override
		public void execute() throws Exception {
			OrchestraSystemTestFrame systemTestFrame = localPeerToSystemFrame
					.get(peerName);
			OrchestraSystem localSystem = systemTestFrame.getOrchestraSystem();
			localSystem.publishAndMap();
			DbUnitUtil.dumpOrCheck(datasetFile, orchestraSchema, dumpDatasets,
					systemTestFrame.getDbTester(), bdbDataSetFactory);
		}
	}

	/**
	 * An Orchestra Reconciliation operation.
	 * 
	 */
	private class ReconcileOperation implements IOrchestraOperation {

		/** The DbUnit dataset file we will be writing or testing against. */
		private final File datasetFile;

		/** The peer which is reconciling. */
		private final String peerName;

		/**
		 * Creates a new reconcile operation.
		 * 
		 * @param datasetFile
		 * @param peerName
		 */
		public ReconcileOperation(
				@SuppressWarnings("hiding") final File datasetFile,
				@SuppressWarnings("hiding") final String peerName) {
			this.datasetFile = datasetFile;
			this.peerName = peerName;
		}

		/**
		 * @see edu.upenn.cis.orchestra.IOrchestraOperation#execute()
		 */
		@Override
		public void execute() throws Exception {
			OrchestraSystemTestFrame systemTestFrame = localPeerToSystemFrame
					.get(peerName);
			OrchestraSystem localSystem = systemTestFrame.getOrchestraSystem();

			if (localSystem.getRecMode()) {
				localSystem.reconcile();
			} else {
				if (!localSystem.getMappingDb().isConnected()) {
					localSystem.getMappingDb().connect();
				}
				// Now run the Exchange
				localSystem.translate();

			}
			// Document translationState =
			// orchestraSystem.getMappingEngine().getState().serialize();
			// DomUtils.write(translationState, new FileWriter("ts-" +
			// datasetFile.getName()));
			DbUnitUtil.dumpOrCheck(datasetFile, orchestraSchema, dumpDatasets,
					systemTestFrame.getDbTester(), bdbDataSetFactory);
		}
	}

	/**
	 * An Orchestra Start operation.
	 * 
	 */
	private class StartOperation implements IOrchestraOperation {
		/** The DbUnit dataset file we will be writing or testing against. */
		private final File datasetFile;
		private final String peerName;

		/**
		 * Creates a start operation.
		 * 
		 * @param datasetFile
		 * @param peerName
		 */
		private StartOperation(
				@SuppressWarnings("hiding") final File datasetFile,
				@SuppressWarnings("hiding") final String peerName) {
			this.datasetFile = datasetFile;
			this.peerName = peerName;
		}

		/**
		 * @see edu.upenn.cis.orchestra.IOrchestraOperation#execute()
		 */
		@Override
		public void execute() throws Exception {
			OrchestraSystemTestFrame oldSystemFrame = localPeerToSystemFrame
					.get(peerName);
			OrchestraSystemTestFrame newSystemFrame = new OrchestraSystemTestFrame(
					orchestraSchema, oldSystemFrame.getOrchestraTestFrame());
			OrchestraSystem localSystem = newSystemFrame.getOrchestraSystem();
			if (!localSystem.storeServerRunning()) {
				localSystem.startStoreServer();
			}
			localPeerToSystemFrame.put(peerName, newSystemFrame);

			MetaDataChecker checker = new MetaDataChecker.Builder()
					.checkTypes().build();
			DbUnitUtil.dumpOrCheckAndMetaCheck(datasetFile, checker,
					orchestraSchema, dumpDatasets,
					newSystemFrame.getDbTester(), bdbDataSetFactory);
		}
	}

	/**
	 * An Orchestra Stop operation.
	 * 
	 */
	private class StopOperation implements IOrchestraOperation {
		/** The DbUnit dataset file we will be writing or testing against. */
		private final File datasetFile;
		private final String peerName;

		/**
		 * Creates a stop operation.
		 * 
		 * @param datasetFile
		 * @param peerName
		 */
		private StopOperation(
				@SuppressWarnings("hiding") final File datasetFile,
				@SuppressWarnings("hiding") final String peerName) {
			this.datasetFile = datasetFile;
			this.peerName = peerName;
		}

		/**
		 * @see edu.upenn.cis.orchestra.IOrchestraOperation#execute()
		 */
		@Override
		public void execute() throws Exception {
			OrchestraSystemTestFrame systemTestFrame = localPeerToSystemFrame
					.get(peerName);
			OrchestraSystem localSystem = systemTestFrame.getOrchestraSystem();

			localSystem.stopStoreServer();
			localSystem.getMappingDb().finalize();

			MetaDataChecker checker = new MetaDataChecker.Builder()
					.checkTypes().build();
			DbUnitUtil.dumpOrCheckAndMetaCheck(datasetFile, checker,
					orchestraSchema, dumpDatasets, systemTestFrame
							.getDbTester(), bdbDataSetFactory);

			SchemaIDBinding.resetRelationId();

		}
	}

	private final Map<String, OrchestraSystemTestFrame> localPeerToSystemFrame;

	/**
	 * Creates an {@code OrchestraSystemOperationFactory} which will use the
	 * {@code localPeerToSystem} collection to implement operations.
	 * 
	 * @param localPeerToSystemFrame
	 * @param orchestraSchema
	 * @param testDataDirectory
	 * @param dumpDatasets
	 * @param dbTester
	 * @param bdbDataSetFactory
	 */
	public MultiOrchestraSystemOperationFactory(
			@SuppressWarnings("hiding") final Map<String, OrchestraSystemTestFrame> localPeerToSystemFrame,
			@SuppressWarnings("hiding") final OrchestraSchema orchestraSchema,
			@SuppressWarnings("hiding") final File testDataDirectory,
			@SuppressWarnings("hiding") final boolean dumpDatasets,
			@SuppressWarnings("hiding") final BdbDataSetFactory bdbDataSetFactory) {
		super(orchestraSchema, testDataDirectory, dumpDatasets,
				bdbDataSetFactory);
		this.localPeerToSystemFrame = localPeerToSystemFrame;
	}

	/**
	 * The allowed values of {@code operationName} are:
	 * <dl>
	 * <dt>create</dt>
	 * <dd>Creates initial database tables.</dd>
	 * <dt>migrate</dt>
	 * <dd>Migrates existing database schema to Orchestra system.</dd>
	 * <dt>import</dt>
	 * <dd>Imports data from delimited files.</dd>
	 * <dt>start</dt>
	 * <dd>Starts the system.</dd>
	 * <dt>stop</dt>
	 * <dd>Stops the system.</dd>
	 * <dt>publish</dt>
	 * <dd>Publishes data to update store. {@code peerName} is required.</dd>
	 * <dt>reconcile</dt>
	 * <dd>Update exchange and reconciliation. {@code peerName} is required.</dd>
	 * </dl>
	 * 
	 */
	@Override
	public IOrchestraOperation operation(String operationName, String peerName,
			File datasetFile) {
		IOrchestraOperation op;
		if (operationName.equalsIgnoreCase("create")) {
			op = new CreateOperation(datasetFile, peerName);
		} else if (operationName.equalsIgnoreCase("migrate")) {
			op = new MigrateOperation(datasetFile, peerName);
		} else if (operationName.equalsIgnoreCase("import")) {
			op = new ImportOperation(datasetFile, peerName);
		} else if (operationName.equalsIgnoreCase("publish")) {
			op = new PublishOperation(datasetFile, peerName);
		} else if (operationName.equalsIgnoreCase("reconcile")) {
			op = new ReconcileOperation(datasetFile, peerName);
		} else if (operationName.equalsIgnoreCase("start")) {
			op = new StartOperation(datasetFile, peerName);
		} else if (operationName.equalsIgnoreCase("stop")) {
			op = new StopOperation(datasetFile, peerName);
		} else {
			throw new IllegalStateException("Unrecognized operation: ["
					+ operationName
					+ ((peerName == null) ? "" : "-" + peerName)
					+ "] from file: " + datasetFile);
		}

		return op;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.upenn.cis.orchestra.IOrchestraOperationFactory#getJdbcDatabaseTester
	 * ()
	 */
	@Override
	public JdbcDatabaseTester getJdbcDatabaseTester() {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.upenn.cis.orchestra.IOrchestraOperationFactory#getOrchestraTestFrame
	 * (java.lang.String)
	 */
	@Override
	public OrchestraTestFrame getOrchestraTestFrame(String peerName) {
		return localPeerToSystemFrame.get(peerName).getOrchestraTestFrame();
	}

}
