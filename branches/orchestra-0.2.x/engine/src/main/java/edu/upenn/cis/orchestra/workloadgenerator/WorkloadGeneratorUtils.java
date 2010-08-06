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

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Utilties for the workloadgenerator package.
 * 
 * @author Sam Donnelly
 */
public class WorkloadGeneratorUtils {

	public static String spart(int j) {
		return "S" + String.valueOf(j) + "_";
	}

	public static String ppart(int i) {
		return "P" + String.valueOf(i) + "_";
	}

	public static String rpart(int k) {
		return "R" + String.valueOf(k);
	}

	public static String relname(int i, int j, int k) {
		// peer i, schema j, relation k
		return ppart(i) + spart(j) + rpart(k);
	}
	
	public static String stamp() {
		return new SimpleDateFormat("HH:mm:ss MM/dd/yy zzz").format(new Date());
	}

	/**
	 * Prevent inheritance and instantiation.
	 */
	private WorkloadGeneratorUtils() {

	}
}
