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
package edu.upenn.cis.orchestra.console;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParameterParser {
	protected Pattern m_pattern;
	protected Vector<String> m_names;
	protected Pattern m_wordpat = Pattern.compile("(\\w+)(\\.\\.\\.)?");
	
	public ParameterParser(String spec) {
		// Example:
		// spec       "(peer schema)? table"        -->
		// m_pattern "(\\s*(\\b\\w+\\b)\\s*(\\b\\w+\\b))?\\s*(\\b\\w+\\b)\\s*"  ,
		// m_names   "peer, schema, table"
		m_names = new Vector<String>();
		m_names.add(null);	// increment group for the 0 (whole string) group
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < spec.length(); i++) {
			char c = spec.charAt(i);
			if (c == '(') {
				m_names.add(null);	// increment group
				buf.append(c);
			} else if (Character.isLetterOrDigit(c)) {
				Matcher mat = m_wordpat.matcher(spec);
				if (mat.find(i)) {
					if (mat.group(2) != null) {
						m_names.add(mat.group(1));
						buf.append("\\s*(.*)");
					} else {
						m_names.add(null);
						m_names.add(mat.group(1));
						m_names.add(null);
						buf.append("\\s*((\\S+)($|\\b|\\s))");
					}
					i = mat.end()-1;
				} else {
					assert(false); // shouldn't happen!
				}				
			} else if (!Character.isWhitespace(c)) {
				buf.append(c);
			}
		}
		buf.append("\\s*");
		m_pattern = Pattern.compile(buf.toString());
	}
	
	public Map<String,String> parse(String params) {
		Matcher mat = m_pattern.matcher(params);
		if (mat.matches()) {
			HashMap<String,String> map = new HashMap<String,String>();
			assert(mat.groupCount()+1 == m_names.size());
			for (int i = 0; i < mat.groupCount()+1; i++) {
				String name = m_names.get(i);
				if (name != null) {
					map.put(name, mat.group(i));
				}
			}
			return map;
		}
		return null;
	}
}
