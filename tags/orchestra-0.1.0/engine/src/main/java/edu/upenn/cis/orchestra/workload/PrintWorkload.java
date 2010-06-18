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

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.Relation;

class PrintWorkload {
	public static void main(String args[]) throws Exception {
		FileInputStream fis = new FileInputStream(args[0]);
		ObjectInputStream ois = new ObjectInputStream(fis);
		
		Relation s = (Relation) ois.readObject();
		System.out.println(s);
		
		int numPeers = (Integer) ois.readObject();
		System.out.println("Number of peers:" + numPeers);
		
		Object lastRead = ois.readObject();
		while (lastRead instanceof WorkloadAction) {
			WorkloadAction wa = (WorkloadAction) lastRead;
			System.out.println(wa);
			lastRead = ois.readObject();
		}
		
		for (int currPeer = 0; currPeer < numPeers; ++currPeer, lastRead = ois.readObject()) {
			HashMap referenceState = (HashMap) lastRead;
			Set state = referenceState.keySet();
			System.out.println("Peer " + currPeer + " state:");
			for (Object o : state) {
				System.out.println("\t" + o);
			}
		}
		
		ois.close();
	}
}
