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
package edu.upenn.cis.orchestra.workload;

import java.io.FileOutputStream;
import java.io.PrintWriter;

public class PrintZipf {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Double s = Double.parseDouble(args[0]);
		int n = Integer.parseInt(args[1]);
		PrintWriter pw = new PrintWriter(new FileOutputStream(args[2]));
		
		RandomizedSet rs = new RandomizedSet(false);
		
		for (int i = 0; i < n; ++i) {
			rs.addElement(Integer.toString(i));
		}
		
		rs.makeWeightedZipfian(s);
		
		pw.print(rs.toString());
		pw.close();
	}

}
