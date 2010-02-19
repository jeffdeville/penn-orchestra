package edu.upenn.cis.orchestra.gui;

import org.fest.swing.core.Robot;

import edu.upenn.cis.orchestra.ITestFrameWrapper;
import edu.upenn.cis.orchestra.OrchestraSchema;
import edu.upenn.cis.orchestra.OrchestraTestFrame;

/**
 * These are the test framework items which we need on a per GUI basis.
 * 
 * @author John Frommeyer
 * 
 */
public class OrchestraGUITestFrame implements
		ITestFrameWrapper<OrchestraGUIController> {
	/** The test frame */
	private final OrchestraTestFrame testFrame;
	private final OrchestraGUIController controller;

	OrchestraGUITestFrame(OrchestraSchema orchestraSchema,
			OrchestraTestFrame orchestraTestFrame, Robot robot)
			throws Exception {
		testFrame = orchestraTestFrame;
		controller = new OrchestraGUIController(robot, testFrame.getPeerName(),
				orchestraSchema.toDocument(testFrame.getDbURL(), testFrame
						.getDbUser(), testFrame.getDbPassword(), testFrame
						.getPeerName()));
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.ITestFrameWrapper#getOrchestraController()
	 */
	@Override
	public OrchestraGUIController getOrchestraController() {
		return controller;
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
