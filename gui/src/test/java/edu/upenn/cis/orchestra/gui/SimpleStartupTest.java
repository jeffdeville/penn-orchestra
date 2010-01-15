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

import java.sql.SQLException;

import static edu.upenn.cis.orchestra.gui.GUITestUtils.createSchemasForHSQLDB;
import org.fest.swing.annotation.GUITest;

import org.fest.swing.finder.JFileChooserFinder;
import org.fest.swing.fixture.FrameFixture;
import org.fest.swing.fixture.JFileChooserFixture;
import org.fest.swing.fixture.JTextComponentFixture;

import org.fest.swing.fixture.JTabbedPaneFixture;

import org.testng.annotations.Test;
import static org.testng.Assert.assertNull;

/**
 * Startup with a simple ORCHESTRA schema file.
 * 
 * @author John Frommeyer
 * 
 */
@GUITest
@Test(groups = { "fullgui-tests" })
public class SimpleStartupTest extends GUIFestTestngTestTemplate {

	/**
	 * FEST wrapper for the main ORCHESTRA frame.
	 */
	private FrameFixture _window;

	/**
	 * The titles of the tabs which should appear.
	 */
	private static final String[] TAB_TITLES = { "All Peers", "Peer NCBI1",
			"Peer NCBI2", "Console", "Query", "Provenance" };

	/**
	 * @see org.fest.swing.testng.testcase.FestSwingTestngTestCase#onSetUp()
	 */
	@Override
	protected void launchOrchestra() {
		String schema = "bioSimpleZTest";
		_window = GUITestUtils.launchOrchestra(schema, getClass(), robot());
	}

	/**
	 * Make's sure that the GUI starts and that all the tabs are present.
	 */
	public void verifyTabTitles() {
		JTabbedPaneFixture tabPane = _window.tabbedPane();
		tabPane.requireTabTitles(TAB_TITLES);

		// tabPane.selectTab("Peer NCBI1");
		// tabPane.selectTab("Console");
		// tabPane.selectTab(1);
		// _window.menuItemWithPath("File", "Exit").click();
		// _window.optionPane().requireTitle("Error connecting to DBMS");
		// DialogFixture error = _window.dialog(DialogMatcher.any());
		// error.requireModal();
	}

	/**
	 * Here we create ORCHESTRA's local version of the databases defined in the
	 * schema file.
	 */
	@Test(dependsOnMethods = { "verifyTabTitles" })
	public void createDBFromSchema() {
		try {
			createSchemasForHSQLDB("BIOTBZTEST", new String[] { "REFSEQ",
					"TAXO" });
		} catch (ClassNotFoundException e) {
			assertNull(e, e.getLocalizedMessage());
		} catch (SQLException e) {
			assertNull(e, e.getLocalizedMessage());
		}

		_window.menuItemWithPath("Services", "Create DB from Schema").click();
	}

	/**
	 * This creates auxiliary tables which ORCESTRA needs.
	 */
	@Test(dependsOnMethods = { "createDBFromSchema" })
	public void migrateExistingDB() {

		_window.menuItemWithPath("Services", "Migrate Existing DB").click();

	}

	/**
	 * Here we click on all the tabs in the {@code MainIFrame}. After {@code
	 * migrateExistingDB()} has been called, this should cause no errors.
	 */
	@Test(dependsOnMethods = { "migrateExistingDB" })
	public void clickTabs() {
		JTabbedPaneFixture tabPane = _window.tabbedPane();
		int nTitles = TAB_TITLES.length;
		for (int i = nTitles - 1; i != -1; i--) {
			tabPane.selectTab(i);
		}
	}
	
	/**
	 * Here we load the DB with data stored in files.
	 */
	@Test(dependsOnMethods = { "clickTabs" })
	public void ImportCDSSData() {
		_window.menuItemWithPath("File", "Import CDSS Data...").click();
		JFileChooserFixture fileChooser = JFileChooserFinder.findFileChooser().using(robot());
		JTextComponentFixture fileNameBox = fileChooser.fileNameTextBox();
		String filePath = getClass().getResource("/simpleStartupTest/relationsData/outFiles").getPath();
		//if (filePath.startsWith("/")) {
		//	filePath = filePath.substring(1);
		//}
		//filePath = filePath.replace('/', File.separatorChar);
		System.err.println(filePath);
		fileNameBox.enterText(filePath);
		fileChooser.approve();
	}
}
