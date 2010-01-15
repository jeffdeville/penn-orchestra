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
import org.fest.swing.core.ComponentFinder;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * We had a bug in which the internal frame was selected before the GUI was visible, so
 * the selection was ignored.
 * 
 * @author John Frommeyer
 *
 */
@GUITest
@Test(groups = { "fullgui-tests" })
public class SelectIFrameTest extends GUIFestTestngTestTemplate {

	
	
	
	@Override
	protected void launchOrchestra() {
		String schema = "bioSimpleZTest";
		GUITestUtils.launchOrchestra(schema, getClass(), robot());

	}
	
	/**
	 * Make sure that the initial internal frame has been selected.
	 */
	public void verifyIFrameSelected() {
		ComponentFinder iframeFinder = robot().finder();
		MainIFrame iframe = iframeFinder.findByType(MainIFrame.class);
		Assert.assertTrue(iframe.isSelected(), "Expected inital MainIFrame to be selected, but it was not.");
		Assert.assertTrue(iframe.hasFocus(), "Expected inital MainIFrame to have focus, but it does not.");
	}

}
