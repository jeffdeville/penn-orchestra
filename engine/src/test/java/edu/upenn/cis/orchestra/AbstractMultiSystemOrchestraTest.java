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

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.TestUtil.DEV_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.TestUtil.REQUIRES_DATABASE_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.TestUtil.SLOW_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.util.DomUtils.getChildElements;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.reconciliation.bdbstore.BerkeleyDBStoreStartStopClient;
import edu.upenn.cis.orchestra.util.DomUtils;

/**
 * Test the whole system.
 * <p>
 * Depending on the contents of {@code test-data-dir} (see below) this class
 * runs through some or all of the following operations:
 * <ol>
 * <li>From an Orchestra schema, create tables in a database</li>
 * <li>Migrating the database to an Orchestra system.</li>
 * <li>Importing data into the Orchestra system from delimited files.</li>
 * <li>Publishing updates from a particular peer.</li>
 * <li>Reconciliation at a particular peer.</li>
 * <li>Updates to the "local" instance.</li>
 * </ol>
 * <p>
 * The test should be run from a testng xml configuration file which specifies
 * the following parameters. The {@code run-mode} parameter is optional, and
 * should generally be left out unless you are creating a new test case for this
 * class. The {@code db-state} parameter is also optional. The Orchestra schema
 * file {@code <orchestra-schema-name>}.schema should also exist and have an
 * empty {@code engine} element, as the {@code mappings} child element will be
 * created using the values of some of these parameters.
 * <dl>
 * 
 * <dt>{@code orchestra-schema-name}</dt>
 * <dd>The name of the Orchestra schema we'll be doing our tests on. Example:
 * {@code bioTestZ}.</dd>
 * 
 * <dt>{@code test-data-dir}</dt>
 * <dd>This is the directory where test files other than the Orchestra schema
 * file are located. It is assumed be be a path relative to the directory
 * containing the Orchestra schema file.</dd>
 * <dt>{@code run-mode}</dt>
 * <dd>Optional. If the value is {@code generate}, then certain assertions will
 * be replaced by calls to write out DataSet files. These files can then be
 * placed in the same directory as the Orchestra schema file for the test and
 * this parameter removed. The default is to check the assertions and to not
 * write any DataSet files.</dd>
 * 
 * </dl>
 * <p>
 * The directory {@code test-data-dir} should include (some of) the following
 * files:
 * <ul>
 * <li>The delimited files containing the data which will be imported into the
 * system if {@code db-state} is set to {@code no-pre-existing}. Otherwise there
 * should be an SQL file for schema and table creation and a DbUnit dataset file
 * for inserting the base data.</li>
 * <li>The DbUnit DataSet XML files which will be used in assertions. See the
 * {@code run-mode} parameter for an easy way to generate these files.</li>
 * </ul>
 * </p>
 * 
 * @author Sam Donnelly
 * @author John Frommeyer
 */
@Test(groups = { SLOW_TESTNG_GROUP, REQUIRES_DATABASE_TESTNG_GROUP,
		DEV_TESTNG_GROUP })
public abstract class AbstractMultiSystemOrchestraTest {

	/**
	 * The logger.
	 */
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	/** The name of the Orchestra schema we'll be doing our tests on. */
	protected String orchestraSchemaName;

	/** The Orchestra schema we'll be doing our tests on. */
	protected OrchestraSchema orchestraSchema;

	/** The list of peers which should be started immediately. */
	protected List<String> peersToStart = newArrayList();

	/** A list of database schema names from {@code orchestraSchemaName}. */
	private Set<String> dbSchemaNames;

	/**
	 * An array of database schema name regular expressions from {@code
	 * orchestraSchemaName}.
	 */
	private String[] dbSchemaNameRegexps;

	/**
	 * This will be used as the value of the {@code workdir} property. It should
	 * also be the location of the (partial) Orchestra schema file which will be
	 * used as the starting point for creating the adapted Orchestra schema
	 * file. In addition, the delimited files which hold the data to be imported
	 * should also be in this directory.
	 */
	private String orchestraWorkdir;

	/** The directory which holds all the test data files. */
	protected File testDataDirectory;

	/**
	 * If this is true, then we create DataSet files, otherwise we assume they
	 * exist and issue assertions against them.
	 */
	protected boolean onlyGenerateDataSets;

	/** Executes Orchestra operations during the test. */
	protected IOperationExecutor executor;

	/** One test frame for each peer in the test. */
	protected List<OrchestraTestFrame> testFrames = newArrayList();

	private BerkeleyDBStoreStartStopClient usClient = new BerkeleyDBStoreStartStopClient(
			"updateStore");

	/**
	 * These are the table name extensions which do not start with "_L".
	 */
	private static final List<String> NONLOCAL_TABLE_NAME_EXTENSIONS = OrchestraUtil
			.newArrayList();

	/** These are the table name extensions which do start with "_L". */
	private static final List<String> LOCAL_TABLE_NAME_EXTENSIONS = OrchestraUtil
			.newArrayList();

	/**
	 * Sets up the test with values from the testng.xml file. Attempts to allow
	 * the use of the same class for different runs for various DBMS.
	 * 
	 * @param orchestraSchemaName
	 * @param testDataDir
	 * @param runMode
	 * 
	 * @throws Exception
	 */
	@BeforeClass
	@Parameters(value = { "orchestra-schema-name", "test-data-dir", "run-mode" })
	public final void init(
			@SuppressWarnings("hiding") final String orchestraSchemaName,
			final String testDataDir, @Optional("test") final String runMode)
			throws Exception {

		this.orchestraSchemaName = orchestraSchemaName;
		orchestraWorkdir = OrchestraUtil.getWorkingDirectory(
				orchestraSchemaName, getClass());
		testDataDirectory = new File(orchestraWorkdir + "/" + testDataDir);
		String peersFilePath = testDataDirectory.getAbsolutePath()
				+ File.separator + this.orchestraSchemaName + ".peers.xml";

		InputStream peersFileInputStream = new FileInputStream(peersFilePath);
		Document peersDoc = DomUtils.createDocument(peersFileInputStream);
		String jdbcDriver = peersDoc.getDocumentElement().getAttribute(
				"jdbcDriver");
		// When we load Config, the properties get loaded.
		// - but it is used all over the place, so it may have already been
		// loaded.

		// Here we override the working directory value with the location of
		// this class.
		Config.setJDBCDriver(jdbcDriver);
		Config.setWorkDir(orchestraWorkdir);
		Config.setTestSchemaName(orchestraSchemaName);
		Config.setDebug(false);
		orchestraSchema = new OrchestraSchema(new File(new URI(Config
				.getSchemaFile())));
		logger.debug("Using Orchestra schema file:\n{}", orchestraSchema);
		dbSchemaNames = orchestraSchema.getDbSchemaNames(false);
		dbSchemaNameRegexps = orchestraSchema.getDbSchemaNames(true).toArray(
				new String[dbSchemaNames.size()]);

		List<Element> peers = getChildElements(peersDoc.getDocumentElement());
		for (Element peerElement : peers) {
			OrchestraTestFrame testFrame = new OrchestraTestFrame(jdbcDriver,
					peerElement);
			testFrames.add(testFrame);
			if (peerElement.hasAttribute("start")
					&& "false".equalsIgnoreCase(peerElement
							.getAttribute("start"))) {
				// By default, all peers should start, so do nothing in this
				// case.
			} else {
				peersToStart.add(testFrame.getPeerName());
			}
		}

		onlyGenerateDataSets = ("generate".equalsIgnoreCase(runMode)) ? true
				: false;

		for (AtomType atomType : AtomType.values()) {
			NONLOCAL_TABLE_NAME_EXTENSIONS.add("_" + atomType.toString());
			NONLOCAL_TABLE_NAME_EXTENSIONS.add(Relation.REJECT + "_"
					+ atomType.toString());
		}
		for (String extension : NONLOCAL_TABLE_NAME_EXTENSIONS) {
			if (!extension.startsWith(Relation.REJECT + "_")) {
				LOCAL_TABLE_NAME_EXTENSIONS.add(Relation.LOCAL + extension);
			}
		}

	}

	/**
	 * This method will be called before {@code prepare()} clears and sets up
	 * the database for the test.
	 * 
	 * @throws Exception
	 * 
	 */
	@BeforeClass(dependsOnMethods = { "init" })
	public final void beforePrepare() throws Exception {
		beforePrepareImpl();
	}

	/**
	 * Allows subclasses to specify action of {@code beforePrepare()}.
	 * 
	 * @throws Exception
	 * @throws IOException
	 * @throws SAXException
	 * @throws ParserConfigurationException
	 * @throws XPathExpressionException
	 * 
	 */
	protected abstract void beforePrepareImpl() throws Exception;

	/**
	 * Attempts to drop any tables associated with {@code orchestraSchemaName}
	 * so that the test can run in a clean environment. Will then create schemas
	 * and tables if {@code preExistingTables == true}.
	 * 
	 * @throws Exception
	 */
	@BeforeClass(dependsOnMethods = { "beforePrepare" })
	public final void prepare() throws Exception {
		// Drop all tables and schemas.
		for (OrchestraTestFrame peerFrame : testFrames) {
			peerFrame.prepare(orchestraSchemaName, testDataDirectory,
					dbSchemaNames, dbSchemaNameRegexps);
		}
	}

	/**
	 * This method will be called after {@code prepare()} has setup the
	 * database, and before the test has started.
	 * 
	 * @throws Exception
	 * 
	 * @throws Exception
	 * 
	 */
	@BeforeClass(dependsOnMethods = { "prepare" })
	public final void betweenPrepareAndTest() throws Exception {
		usClient.startAndClearUpdateStore();
		betweenPrepareAndTestImpl();
	}

	/**
	 * Allows subclasses to define action of {@code betweenPrepareAndTest()}
	 * 
	 * @throws Exception
	 */
	protected abstract void betweenPrepareAndTestImpl() throws Exception;

	/**
	 * Executes the operations of the test.
	 * 
	 * @throws Exception
	 * @see edu.upenn.cis.orchestra.IOperationExecutor
	 */
	@Test
	public final void test() throws Exception {
		executor.execute();
	}

	/**
	 * Shutdown after {@code test()} is done.
	 * 
	 * @throws Exception
	 * 
	 */
	@AfterClass(alwaysRun = true)
	public final void shutdown() throws Exception {
		try {
			shutdownImpl();
		} finally {
			usClient.clearAndStopUpdateStore();
		}
	}

	/**
	 * This method will be called in {@code shutdown()}.
	 * 
	 * @throws Exception
	 */
	protected abstract void shutdownImpl() throws Exception;
}
