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

import org.fest.swing.annotation.GUITest;
import org.fest.swing.core.BasicRobot;
import org.fest.swing.core.Robot;
import org.fest.swing.edt.FailOnThreadViolationRepaintManager;
import org.fest.swing.testing.FestSwingTestCaseTemplate;
import org.fest.swing.testng.testcase.FestSwingTestngTestCase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Modeled after {@link FestSwingTestngTestCase} and
 * {@link FestSwingTestCaseTemplate}. This seemed necessary since testng
 * requires that the {@code @Before}* methods be part of the same group as the
 * {@code @Test} methods if you want to call them using the group tag.
 * 
 * @author John Frommeyer
 * 
 */
@GUITest
public abstract class GUIFestTestngTestTemplate {

	/**
	 * The FEST {@code Robot} for this test.
	 */
	private Robot _robot;


	/**
	 * This will cause a failure if any Swing components are accessed off of the
	 * EDT.
	 */
	@BeforeClass(groups = { "fullgui-tests" })
	public final void installEDTChecker() {
		FailOnThreadViolationRepaintManager.install();
	}

	/**
	 * Sets up the test's {@code Robot}.
	 */
	@BeforeClass(groups = { "fullgui-tests" }, dependsOnMethods = {"installEDTChecker"})
	public final void setUpRobot() {
		_robot = BasicRobot.robotWithNewAwtHierarchy();
		launchOrchestra();
	}

	/**
	 * Use this method to launch ORCHESTRA.
	 */
	abstract void launchOrchestra();

	/**
	 * Clean up our FEST resources
	 */
	@AfterClass(groups = { "fullgui-tests" }, alwaysRun = true)
	public final void cleanUpRobot() {
		_robot.cleanUp();
	}
	/**
	 * Returns this test's {@code Robot}.
	 * 
	 * @return this test's {@code Robot}.
	 */
	protected final Robot robot() {
		return _robot;
	}
}
