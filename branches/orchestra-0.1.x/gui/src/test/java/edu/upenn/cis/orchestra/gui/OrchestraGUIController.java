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

import static edu.upenn.cis.orchestra.util.DomUtils.write;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.fest.swing.core.Robot;
import org.fest.swing.fixture.FrameFixture;
import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.Config;

/**
 * Controls an Orchestra GUI
 * 
 * @author John Frommeyer
 * 
 */
class OrchestraGUIController {
	private FrameFixture window;
	private final Robot robot;
	private final String peerName;
	private final Document orchestraSchemaDoc;

	OrchestraGUIController(@SuppressWarnings("hiding") final Robot robot,
			@SuppressWarnings("hiding") final String peerName,
			@SuppressWarnings("hiding") final Document orchestraSchemaDoc) {
		this.robot = robot;
		this.peerName = peerName;
		this.orchestraSchemaDoc = orchestraSchemaDoc;
	}

	void start() throws IOException, URISyntaxException {
		if (window == null) {
			// We need to write out the schema file without clashing with the
			// other
			// GUIs which will be created. So we temporarily change the name of
			// the
			// schema.
			String oldSchemaName = Config.getTestSchemaName();
			String tempSchemaName = oldSchemaName + "." + peerName;
			Config.setTestSchemaName(tempSchemaName);
			FileWriter writer = new FileWriter(new File(new URI(Config
					.getSchemaFile())));
			write(orchestraSchemaDoc, writer);

			window = GUITestUtils.launchOrchestra(robot, tempSchemaName);
			Config.setTestSchemaName(oldSchemaName);
		} else {
			throw new IllegalStateException("GUI for " + peerName
					+ " is already started.");
		}
	}

	void stop() {
		if (window == null) {
			throw new IllegalStateException("GUI for " + peerName
					+ " is already stoped.");
		}

		window.component().toFront();
		// This setting allows us to exit the GUI without also exiting
		// the
		// JVM.
		String previousGuiMode = Config.getProperty("gui.mode");
		Config.setProperty("gui.mode", "Ajax");
		window.menuItemWithPath("File", "Exit").click();
		if (previousGuiMode == null) {
			Config.removeProperty("gui.mode");
		} else {
			Config.setProperty("gui.mode", previousGuiMode);
		}
		if (window.robot.isActive()) {
			window.cleanUp();
		}
		window = null;

	}
	
	void cleanUp() {
		if (window != null && window.robot.isActive()) {
			window.cleanUp();
		}
	}
	
	boolean isRunning() {
		return window != null;
	}

	FrameFixture getFrameFixture() {
		return window;
	}
}
