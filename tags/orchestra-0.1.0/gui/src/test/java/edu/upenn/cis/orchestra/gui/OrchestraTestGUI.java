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

import static edu.upenn.cis.orchestra.OrchestraUtil.newHashMap;

import java.io.File;
import java.util.Map;

import org.fest.swing.annotation.GUITest;
import org.fest.swing.core.BasicRobot;
import org.fest.swing.core.Robot;
import org.fest.swing.edt.FailOnThreadViolationRepaintManager;

import edu.upenn.cis.orchestra.AbstractMultiSystemOrchestraTest;
import edu.upenn.cis.orchestra.IOrchestraOperationFactory;
import edu.upenn.cis.orchestra.ITestFrameWrapper;
import edu.upenn.cis.orchestra.MultiSystemOrchestraOperationExecutor;
import edu.upenn.cis.orchestra.OrchestraTestFrame;
import edu.upenn.cis.orchestra.reconciliation.bdbstore.BdbDataSetFactory;

/**
 * An Orchestra test via the GUI.
 * 
 * @see edu.upenn.cis.orchestra.AbstractMultiSystemOrchestraTest
 * @author John Frommeyer
 * 
 */
@GUITest
public final class OrchestraTestGUI extends AbstractMultiSystemOrchestraTest {

	/** Translates Berkeley update store into DbUnit dataset. */
	private BdbDataSetFactory bdbDataSetFactory;

	private final Map<String, ITestFrameWrapper<OrchestraGUIController>> peerNameToTestFrame = newHashMap();

	private Robot robot;

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
		FailOnThreadViolationRepaintManager.install();
		robot = BasicRobot.robotWithNewAwtHierarchy();
		for (OrchestraTestFrame testFrame : testFrames) {
			File f = new File("stateStore_env_" + testFrame.getPeerName());
			if (f.exists() && f.isDirectory()) {
				File[] contents = f.listFiles();
				for (File file : contents) {
					file.delete();
				}
				f.delete();
			}
			ITestFrameWrapper<OrchestraGUIController> guiTestFrame = new OrchestraGUITestFrame(
					orchestraSchema, testFrame, robot);
			peerNameToTestFrame.put(testFrame.getPeerName(), guiTestFrame);
			if (peersToStart.contains(testFrame.getPeerName())) {
				guiTestFrame.getOrchestraController().start();
			}
		}

		bdbDataSetFactory = new BdbDataSetFactory(new File("updateStore_env"),
				orchestraSchema.getName(), peerNameToTestFrame.keySet());
		IOrchestraOperationFactory factory = new MultiGUIOperationFactory(
				orchestraSchema, testDataDirectory, onlyGenerateDataSets,
				peerNameToTestFrame, bdbDataSetFactory);
		executor = new MultiSystemOrchestraOperationExecutor(factory);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.upenn.cis.orchestra.AbstractOrchestraTest#shutdownImpl()
	 */
	@Override
	protected void shutdownImpl() throws Exception {
		for (ITestFrameWrapper<OrchestraGUIController> guiTestFrame : peerNameToTestFrame
				.values()) {
			OrchestraGUIController controller = guiTestFrame
					.getOrchestraController();
			if (controller.isRunning()) {
				controller.stop();
			}
		}
		robot.cleanUp();
	}
}
