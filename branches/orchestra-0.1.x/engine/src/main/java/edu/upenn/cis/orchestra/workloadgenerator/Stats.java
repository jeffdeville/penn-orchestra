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
package edu.upenn.cis.orchestra.workloadgenerator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Statistics of the uniprot data file - holds the attribute names and sizes of
 * the fields.
 * 
 * @author Sam Donnelly
 */
public class Stats {

	/** Hold on to the mappings. */
	private static Map<String, Integer> _stats = new HashMap<String, Integer>();
	static {
		_stats.put("FT", 38278);
		_stats.put("DE", 795);
		_stats.put("DT", 115);
		_stats.put("DR", 24337);
		_stats.put("RT", 18683);
		_stats.put("RP", 7791);
		_stats.put("RX", 7309);
		_stats.put("RG", 259);
		_stats.put("RA", 13234);
		_stats.put("RC", 1827);
		_stats.put("RL", 5857);
		_stats.put("RN", 821);
		_stats.put("AC", 911);
		_stats.put("CC", 13160);
		_stats.put("GN", 1061);
		_stats.put("ID", 44);
		_stats.put("OH", 1230);
		_stats.put("OG", 385);
		_stats.put("SQ", 58);
		_stats.put("OC", 281);
		_stats.put("KW", 585);
		_stats.put("OX", 18);
		_stats.put("OS", 144);
		_stats.put("SD", 37784);
		_stats.put("PE", 20000);
	}

	/**
	 * Get the size of the specified field.
	 * 
	 * @param att
	 *            the uniprot field we want the size of.
	 * @return the size of the specified field.
	 */
	public static int getSize(String att) {
		return _stats.get(att);
	}

	/**
	 * Return the attributes of the uniprot data file.
	 * 
	 * @return see description.
	 */
	public static Set<String> getAtts() {
		return _stats.keySet();
	}

	/**
	 * Prevent inheritance and instantiation.
	 */
	private Stats() {

	}
}
