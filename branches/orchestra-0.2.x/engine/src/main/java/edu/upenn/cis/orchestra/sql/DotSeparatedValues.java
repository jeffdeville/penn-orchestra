/*
 * Copyright (C) 2009 Trustees of the University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.cis.orchestra.sql;

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;

import java.util.List;
import java.util.StringTokenizer;

/**
 * For parsing up strings with {@code "."} separated values which go from right
 * to left: {@code "valueN.valueN-1. ... .value0"}. {@code "valueN"} is
 * retrieved by calling {@code get(N)}.
 * 
 * @author Sam Donnelly
 */
public class DotSeparatedValues implements IIndexedStringValues {
	/** The parsed values. */
	private final List<String> _values = newArrayList();

	/**
	 * 
	 * @param input to be parsed
	 * @param expectedMaxTokens the maximum number of tokens that {@code input}
	 *            can contain
	 */
	public DotSeparatedValues(final String input, final int expectedMaxTokens) {
		final StringTokenizer st = new StringTokenizer(input, ".");
		if (st.countTokens() > expectedMaxTokens) {
			throw new IllegalArgumentException("input [" + input + "] has "
					+ st.countTokens() + " tokens, but a max of "
					+ expectedMaxTokens + " were expected");
		}
		for (int i = 0; i < expectedMaxTokens; i++) {
			_values.add(null);
		}
		for (int i = st.countTokens() - 1; st.hasMoreTokens(); i--) {
			_values.set(i, st.nextToken());
		}
	}

	/** {@inheritDoc} */
	@Override
	public String get(final int idx) {
		return _values.get(idx);
	}
}
