package edu.upenn.cis.orchestra.gui;

import javax.swing.JComponent;
import javax.swing.SwingUtilities;

;

/**
 * Reports on any {@code JComponent} methods which are called on a thread other
 * than the Event Dispatch Thread (EDT). Taken from Alexander Potochkin's
 * <a href="http://weblogs.java.net/blog/alexfromsun/archive/2006/02/debugging_swing.html">Blog</a>.
 * 
 * @author John Frommeyer
 * 
 */
aspect EdtRuleChecker {

	/**
	 * If {@code false} then we only report EDT rule violations when the
	 * component is showing. For new code this should be set to {@code true}
	 * because the current advice is that GUI code should always be called from
	 * the EDT thread, regardless of whether the component is visible or
	 * showing.
	 */
	private boolean isStressChecking = true;

	public pointcut anySwingMethods(JComponent c):
	         target(c) && call(* *(..));

	public pointcut threadSafeMethods():         
	         call(* repaint(..)) || 
	         call(* revalidate()) ||
	         call(* invalidate()) ||
	         call(* getListeners(..)) ||
	         call(* add*Listener(..)) ||
	         call(* remove*Listener(..));

	// calls of any JComponent method, including subclasses
	before(JComponent c): anySwingMethods(c) && 
	                          !threadSafeMethods() &&
	                          !within(EdtRuleChecker) {
		if (!SwingUtilities.isEventDispatchThread()
				&& (isStressChecking || c.isShowing())) {
			System.err.println(thisJoinPoint.getSourceLocation());
			System.err.println(thisJoinPoint.getSignature());
			System.err.println();
		}
	}

	// calls of any JComponent constructor, including subclasses
	before(): call(JComponent+.new(..)) {
		if (isStressChecking && !SwingUtilities.isEventDispatchThread()) {
			System.err.println(thisJoinPoint.getSourceLocation());
			System.err.println(thisJoinPoint.getSignature() + " *constructor*");
			System.err.println();
		}
	}
}
