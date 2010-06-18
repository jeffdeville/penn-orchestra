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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.custommonkey.xmlunit.Difference;
import org.custommonkey.xmlunit.DifferenceListener;
import org.custommonkey.xmlunit.NodeDetail;
import org.w3c.dom.Node;

import edu.upenn.cis.orchestra.sql.SqlUtil;

/**
 * <p>
 * A {@code DifferenceListener} for XMLUnit tests. When using XMLUnit to compare
 * serialized {@code Mapping}s there was a problem with the generated variable
 * names not being the same from run to run. Here we check the argument list of
 * non-identical atoms to see if they can still be considered equivalent.
 * </p>
 * 
 * <p>
 * This class also takes care of normalizing any SQL text to avoid irrelevant
 * differences.
 * </p>
 * 
 * @author John Frommeyer
 * 
 */
public class OrchestraDifferenceListener implements DifferenceListener {

	private final static Pattern ARG_LIST_START_PATTERN = Pattern
			.compile("\\(");
	private final static Pattern ARG_SEP_PATTERN = Pattern.compile("\\s*,\\s*");

	// Arguments with the form 'X<number><optional ('_' followed by alphanum)>
	private final static Pattern ARG_PATTERN = Pattern
			.compile("X(\\d+)(_\\w*)?");
	// Same as {@code ARG_PATTERN} but with a trailing ')' because it is the
	// last
	// argument of the atom.
	private final static Pattern LAST_ARG_PATTERN = Pattern
			.compile("X(\\d+)(_\\w*)?\\)");

	// Indicates to {@code argsAreEquivalent()} if it is being called for the
	// first time.
	private boolean firstIndexCheck = true;

	// The first observed difference between the control index and the test
	// index, where the index of an argument matching {@code ARG_PATTERN} or
	// {@code {LAST_ARG_PATTERN} is the number following the X, i.e., group 1 in
	// the regex.
	private int controlTestIndexDifference;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.custommonkey.xmlunit.DifferenceListener#differenceFound(org.custommonkey
	 * .xmlunit.Difference)
	 */
	@Override
	public int differenceFound(Difference difference) {
		int result = DifferenceListener.RETURN_ACCEPT_DIFFERENCE;
		NodeDetail controlDetail = difference.getControlNodeDetail();
		NodeDetail testDetail = difference.getTestNodeDetail();
		Node controlParent = controlDetail.getNode().getParentNode();
		Node testParent = testDetail.getNode().getParentNode();
		if (controlParent != null && testParent != null) {
			String controlParentName = controlParent.getNodeName();
			String testParentName = testParent.getNodeName();
			if ("atomValue".equals(testParentName)
					&& "atomValue".equals(controlParentName)) {
				String controlValue = controlDetail.getValue();
				String testValue = testDetail.getValue();
				String[] controlExpression = ARG_LIST_START_PATTERN
						.split(controlValue);
				String[] testExpression = ARG_LIST_START_PATTERN
						.split(testValue);
				if (controlExpression[0].equals(testExpression[0])) {
					String[] controlArgList = ARG_SEP_PATTERN
							.split(controlExpression[1]);
					String[] testArgList = ARG_SEP_PATTERN
							.split(testExpression[1]);
					result = compareArgLists(controlArgList, testArgList);
				}
			} else if ("statement".equals(testParentName) && "statement".equals(controlParentName)){
				String controlValue = SqlUtil.normalizeSqlStatement(controlDetail.getValue());
				String testValue = SqlUtil.normalizeSqlStatement(testDetail.getValue());
				if (controlValue.equalsIgnoreCase(testValue)){
					result = DifferenceListener.RETURN_IGNORE_DIFFERENCE_NODES_IDENTICAL;
				}
			}
		}
		return result;
	}

	/**
	 * Returns {@code DifferenceListener.RETURN_IGNORE_DIFFERENCE_NODES_SIMILAR}
	 * if we consider the argument lists equivalent, {@code
	 * DifferenceListener.RETURN_ACCEPT_DIFFERENCE} otherwise.
	 * 
	 * {@code controlArgList} is considered equivalent to {@code testArgList} if
	 * they are the same length, and each pair of corresponding arguments are
	 * equivalent according to {@code argsAreEquivalent()}.
	 * 
	 * @param controlArgList
	 * @param testArgList
	 * @return a {@code DifferenceListener} indicating result of comparison.
	 */
	private int compareArgLists(String[] controlArgList, String[] testArgList) {
		int result = DifferenceListener.RETURN_ACCEPT_DIFFERENCE;
		if (controlArgList.length == testArgList.length) {
			int lastArgIndex = controlArgList.length - 1;
			boolean allArgsEquiv = true;
			for (int i = 0; i <= lastArgIndex; i++) {
				if (!argsAreEquivalent(controlArgList[i], testArgList[i],
						i == lastArgIndex)) {
					allArgsEquiv = false;
					break;
				}
			}
			if (allArgsEquiv) {
				result = DifferenceListener.RETURN_IGNORE_DIFFERENCE_NODES_SIMILAR;
			}
		}
		return result;
	}

	/**
	 * Returns {@code true} if {@code controlArg} and {@code testArg} are
	 * equivalent, otherwise {@code false}.
	 * 
	 * <p/>
	 * If {@code controlArg} and {@code testArg} are equal, then they are
	 * equivalent. Otherwise they are equivalent if they both match {@code
	 * ARG_PATTERN} (or {@code LAST_ARG_PATTERN} if {@code last == true}) and if
	 * difference between the generated indices of the control argument and the
	 * test argument is equal to {@code controlTestIndexDifference}.
	 * 
	 * @param controlArg
	 * @param testArg
	 * @param last indicates that the two arg parameters are
	 * @return {@code true} if {@code controlArg} and {@code testArg} are
	 *         equivalent
	 */
	private boolean argsAreEquivalent(String controlArg, String testArg,
			boolean last) {
		boolean result = true;
		if (!controlArg.equals(testArg)) {
			Pattern p = (last ? LAST_ARG_PATTERN : ARG_PATTERN);
			Matcher controlMatcher = p.matcher(controlArg);
			Matcher testMatcher = p.matcher(testArg);
			boolean matches = controlMatcher.matches() && testMatcher.matches();
			if (matches) {
				/*int controlIndex = Integer.parseInt(controlMatcher.group(1));
				int testIndex = Integer.parseInt(testMatcher.group(1));
				if (firstIndexCheck) {
					controlTestIndexDifference = controlIndex - testIndex;
					firstIndexCheck = false;
				} else {
					// If this is not the first check, we can also make sure
					// that the difference in indices remains constant.
					result = controlTestIndexDifference == (controlIndex - testIndex);
				}*/
			} else {
				result = false;
			}
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.custommonkey.xmlunit.DifferenceListener#skippedComparison(org.w3c
	 * .dom.Node, org.w3c.dom.Node)
	 */
	@Override
	public void skippedComparison(Node arg0, Node arg1) {
		System.err.println("Skipped comparison of " + arg0.getNodeValue()
				+ " and " + arg1.getNodeValue());
	}

}
