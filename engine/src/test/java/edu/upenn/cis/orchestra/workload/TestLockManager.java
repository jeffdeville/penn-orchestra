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

public class TestLockManager {
	public static void main(String[] args) throws Exception {
		StringBuffer line = new StringBuffer();
		int c = System.in.read();
		while (c != -1) {
			if ((c == '\n' || c == '\r') && line.length() > 0) {
				int peer = Integer.parseInt(line.toString());
				LockManagerClient lmc = new LockManagerClient(null);
				System.out.print("Requesting lock " + peer + "...");
				System.out.flush();
				lmc.getLock(peer);
				System.out.println("granted, released");
				lmc.releaseLock();
				line.setLength(0);
			} else if (c != '\n' && c != '\r') {
				line.append((char) c);
			}
			c = System.in.read();
		}
	}
}
