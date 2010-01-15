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

import org.dbunit.JdbcDatabaseTester;
import org.testng.Assert;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.exchange.IEngine;
import edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding;
import edu.upenn.cis.orchestra.repository.dao.RepositorySchemaDAO;
import edu.upenn.cis.orchestra.repository.dao.flatfile.FlatFileRepositoryDAO;

/**
 * Creates {@code IOrchestraOperation}s which deal directly with an {@code
 * OrchestraSystem} instance.
 * 
 * @see edu.upenn.cis.orchestra.AbstractDsFileOperationFactory
 * @author John Frommeyer
 * 
 */
public final class OrchestraSystemOperationFactory extends
		AbstractDsFileOperationFactory {

	/**
	 * An Orchestra Create operation.
	 * 
	 */
	private class CreateOperation implements IOrchestraOperation {
		/** The DbUnit dataset file we will be writing or testing against. */
		private final File datasetFile;

		/**
		 * Creates a create operation.
		 * 
		 * @param datasetFile
		 */
		private CreateOperation(
				@SuppressWarnings("hiding") final File datasetFile) {
			this.datasetFile = datasetFile;
		}

		/**
		 * @see edu.upenn.cis.orchestra.IOrchestraOperation#execute()
		 */
		@Override
		public void execute() throws Exception {

			IEngine mappingEngine = orchestraSystem.getMappingEngine();
			mappingEngine.createBaseSchemaRelations();

			MetaDataChecker checker = new MetaDataChecker.Builder().checkTypes().build();
			DbUnitUtil.dumpOrCheckAndMetaCheck(datasetFile, checker,
					orchestraSchema, dumpDatasets, dbTester, bdbDataSetFactory);
		}
	}

	/**
	 * An Orchestra Migrate operation.
	 * 
	 */
	private class MigrateOperation implements IOrchestraOperation {
		/** The DbUnit dataset file we will be writing or testing against. */
		private final File datasetFile;

		/**
		 * Creates a migrate operation.
		 * 
		 * @param datasetFile
		 */
		public MigrateOperation(
				@SuppressWarnings("hiding") final File datasetFile) {
			this.datasetFile = datasetFile;
		}

		/**
		 * @see edu.upenn.cis.orchestra.IOrchestraOperation#execute()
		 */
		@Override
		public void execute() throws Exception {
			orchestraSystem.getMappingEngine().migrate();

			MetaDataChecker checker = new MetaDataChecker.Builder()
			.checkTypes().checkForLabeledNulls().build();
			DbUnitUtil.dumpOrCheckAndMetaCheck(datasetFile, checker,
					orchestraSchema, dumpDatasets, dbTester, bdbDataSetFactory);
		}
	}

	/**
	 * An Orchestra Import operation.
	 * 
	 */
	private class ImportOperation implements IOrchestraOperation {

		/** The DbUnit dataset file we will be writing or testing against. */
		private final File datasetFile;

		/**
		 * Creates a new import operation.
		 * 
		 * @param datasetFile
		 */
		private ImportOperation(
				@SuppressWarnings("hiding") final File datasetFile) {
			this.datasetFile = datasetFile;
		}

		/**
		 * @see edu.upenn.cis.orchestra.IOrchestraOperation#execute()
		 */
		@Override
		public void execute() throws Exception {
			ArrayList<String> succeeded = OrchestraUtil.newArrayList();
			ArrayList<String> failed = OrchestraUtil.newArrayList();

			orchestraSystem.getMappingEngine().importUpdates(null,
					testDataDirectory.getPath(), succeeded, failed);

			Assert.assertFalse(succeeded.isEmpty());
			Assert.assertTrue(failed.isEmpty());

			DbUnitUtil.dumpOrCheck(datasetFile, orchestraSchema, dumpDatasets,
					dbTester, bdbDataSetFactory);

		}
	}

	/**
	 * An Orchestra Publication operation.
	 * 
	 */
	private class PublishOperation implements IOrchestraOperation {

		/** The DbUnit dataset file we will be writing or testing against. */
		private final File datasetFile;

		/**
		 * Creates a new publish operation.
		 * 
		 * @param datasetFile
		 * @param peerName
		 */
		public PublishOperation(
				@SuppressWarnings("hiding") final File datasetFile) {
			this.datasetFile = datasetFile;
		}

		/**
		 * @see edu.upenn.cis.orchestra.IOrchestraOperation#execute()
		 */
		@Override
		public void execute() throws Exception {
			orchestraSystem.fetch();
			DbUnitUtil.dumpOrCheck(datasetFile, orchestraSchema, dumpDatasets,
					dbTester, bdbDataSetFactory);
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
			if (orchestraSystem.getRecMode()) {
				orchestraSystem.reconcile();
			} else {
				if (!orchestraSystem.getMappingDb().isConnected()) {
					orchestraSystem.getMappingDb().connect();
				}
				// Now run the Exchange
				orchestraSystem.translate();

			}
			//Document translationState = orchestraSystem.getMappingEngine().getState().serialize();
			//DomUtils.write(translationState, new FileWriter("ts-" + datasetFile.getName()));
			DbUnitUtil.dumpOrCheck(datasetFile, orchestraSchema, dumpDatasets,
					dbTester, bdbDataSetFactory);
		}
	}

	/**
	 * An Orchestra Start operation.
	 * 
	 */
	private class StartOperation implements IOrchestraOperation {
		/** The DbUnit dataset file we will be writing or testing against. */
		private final File datasetFile;

		/**
		 * Creates a start operation.
		 * 
		 * @param datasetFile
		 */
		private StartOperation(
				@SuppressWarnings("hiding") final File datasetFile) {
			this.datasetFile = datasetFile;
		}

		/**
		 * @see edu.upenn.cis.orchestra.IOrchestraOperation#execute()
		 */
		@Override
		public void execute() throws Exception {
			RepositorySchemaDAO repositorySchemaDAO = new FlatFileRepositoryDAO(
					orchestraSchema.toDocument());
			orchestraSystem = repositorySchemaDAO.loadAllPeers();
			orchestraSystem.startStoreServer();

			MetaDataChecker checker= new MetaDataChecker.Builder()
			.checkTypes().build();
			DbUnitUtil.dumpOrCheckAndMetaCheck(datasetFile, checker,
					orchestraSchema, dumpDatasets, dbTester, bdbDataSetFactory);
		}
	}

	/**
	 * An Orchestra Stop operation.
	 * 
	 */
	private class StopOperation implements IOrchestraOperation {
		/** The DbUnit dataset file we will be writing or testing against. */
		private final File datasetFile;

		/**
		 * Creates a stop operation.
		 * 
		 * @param datasetFile
		 */
		private StopOperation(@SuppressWarnings("hiding") final File datasetFile) {
			this.datasetFile = datasetFile;
		}

		/**
		 * @see edu.upenn.cis.orchestra.IOrchestraOperation#execute()
		 */
		@Override
		public void execute() throws Exception {
			orchestraSystem.stopStoreServer();
			orchestraSystem.getMappingDb().finalize();
			
			MetaDataChecker checker = new MetaDataChecker.Builder()
			.checkTypes().build();
			DbUnitUtil.dumpOrCheckAndMetaCheck(datasetFile, checker,
					orchestraSchema, dumpDatasets, dbTester, bdbDataSetFactory);
			
			SchemaIDBinding.resetRelationId();

		}
	}

	/** The system we are testing. */
	private OrchestraSystem orchestraSystem;

	/**
	 * Creates an {@code OrchestraSystemOperationFactory} which will use {@code
	 * orchestraSystem} to implement operations.
	 * 
	 * @param orchestraSystem
	 * @param orchestraSchema
	 * @param testDataDirectory
	 * @param dumpDatasets
	 * @param dbTester
	 * @param bdbDataSetFactory
	 */
	public OrchestraSystemOperationFactory(
			@SuppressWarnings("hiding") final OrchestraSystem orchestraSystem,
			@SuppressWarnings("hiding") final OrchestraSchema orchestraSchema,
			@SuppressWarnings("hiding") final File testDataDirectory,
			@SuppressWarnings("hiding") final boolean dumpDatasets,
			@SuppressWarnings("hiding") final JdbcDatabaseTester dbTester,
			@SuppressWarnings("hiding") final BdbDataSetFactory bdbDataSetFactory) {
		super(orchestraSchema, testDataDirectory, dumpDatasets, dbTester,
				bdbDataSetFactory);
		this.orchestraSystem = orchestraSystem;
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
			op = new CreateOperation(datasetFile);
		} else if (operationName.equalsIgnoreCase("migrate")) {
			op = new MigrateOperation(datasetFile);
		} else if (operationName.equalsIgnoreCase("import")) {
			op = new ImportOperation(datasetFile);
		} else if (operationName.equalsIgnoreCase("publish")) {
			op = new PublishOperation(datasetFile);
		} else if (operationName.equalsIgnoreCase("reconcile")) {
			op = new ReconcileOperation(datasetFile, peerName);
		} else if (operationName.equalsIgnoreCase("start")) {
			op = new StartOperation(datasetFile);
		} else if (operationName.equalsIgnoreCase("stop")) {
			op = new StopOperation(datasetFile);
		} else {
			throw new IllegalStateException("Unrecognized operation: ["
					+ operationName
					+ ((peerName == null) ? "" : "-" + peerName)
					+ "] from file: " + datasetFile);
		}
		
		return op;
	}

	/**
	 * Returns the current {@code OrchestraSystem}.
	 * 
	 * @return the current {@code OrchestraSystem}.
	 */
	public OrchestraSystem getOrchestraSystem() {
		return orchestraSystem;
	}
}
