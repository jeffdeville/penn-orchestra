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
package edu.upenn.cis.orchestra.gui;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.fest.swing.annotation.GUITest;
import org.fest.swing.core.BasicRobot;
import org.fest.swing.core.Robot;
import org.fest.swing.edt.FailOnThreadViolationRepaintManager;
import org.fest.swing.fixture.FrameFixture;
import org.testng.Assert;
import org.xml.sax.SAXException;

import edu.upenn.cis.orchestra.AbstractOrchestraTest;
import edu.upenn.cis.orchestra.BdbDataSetFactory;
import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.IOrchestraOperationFactory;
import edu.upenn.cis.orchestra.OrchestraOperationExecutor;

/**
 * An Orchestra test via the GUI.
 * 
 * @see edu.upenn.cis.orchestra.AbstractOrchestraTest
 * @author John Frommeyer
 * 
 */
@GUITest
public final class OrchestraTestGUI extends AbstractOrchestraTest {

	/**
	 * FEST wrapper for the main ORCHESTRA frame.
	 */
	private FrameFixture window;

	/** The robot for our test. */
	private Robot robot;

	/** Translates Berkeley update store into DbUnit dataset. */
	private BdbDataSetFactory bdbDataSetFactory;

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.upenn.cis.orchestra.AbstractOrchestraTest#beforePrepareImpl()
	 */
	@Override
	protected void beforePrepareImpl() {}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.upenn.cis.orchestra.AbstractOrchestraTest#betweenPrepareAndTestImpl()
	 */
	@Override
	protected void betweenPrepareAndTestImpl() throws Exception {
		File f = new File("updateStore_env");
		if (f.exists() && f.isDirectory()) {
			File[] files = f.listFiles();
			for (File file : files) {
				file.delete();
			}
			String[] contents = f.list();
			Assert.assertTrue(contents.length == 0,
					"Store server did not clear.");
		}
		modifyOrchestraSchemaFile();
		FailOnThreadViolationRepaintManager.install();
		robot = BasicRobot.robotWithNewAwtHierarchy();
		window = GUITestUtils.launchOrchestra(robot);
		bdbDataSetFactory = new BdbDataSetFactory(new File("updateStore_env"));
		IOrchestraOperationFactory factory = new GUIOperationFactory(window,
				orchestraSchema, testDataDirectory, onlyGenerateDataSets,
				dbTester, bdbDataSetFactory);
		executor = new OrchestraOperationExecutor(factory);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.upenn.cis.orchestra.AbstractOrchestraTest#shutdownImpl()
	 */
	@Override
	protected void shutdownImpl() throws Exception {
		// This setting allows us to exit the GUI without also exiting the JVM.
		String previousGuiMode = Config.getProperty("gui.mode");
		Config.setProperty("gui.mode", "Ajax");
		window.menuItemWithPath("File", "Exit").click();
		if (previousGuiMode == null) {
			Config.removeProperty("gui.mode");
		} else {
			Config.setProperty("gui.mode", previousGuiMode);
		}
		if (window != null) {
			window.cleanUp();
		}
		if (bdbDataSetFactory != null) {
			bdbDataSetFactory.close();
		}
	}

	/**
	 * Write out the modified Orchestra schema file.
	 * 
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws XPathExpressionException
	 */
	private void modifyOrchestraSchemaFile()
			throws ParserConfigurationException, SAXException, IOException,
			URISyntaxException, XPathExpressionException {

		File file = new File(new URI(Config.getSchemaFile()));
		orchestraSchema.write(file, dbURL, dbUser, dbPassword);
	}
}
