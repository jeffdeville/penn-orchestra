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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;

public class LockManager {
	static final int LOCK_MANAGER_PORT = 1500;
	static class Waiting {
		Socket s;
		int peer;
		PrintWriter p;
		Reader r;
	}
	public static void main(String args[]) throws Exception {
		int port;

		if (args.length < 2) {
			usage();
			return;
		}
		
		try { 
			port = Integer.parseInt(args[2]);
		}
		catch (Exception e) {
			System.out.println("port = " + LOCK_MANAGER_PORT + " (default)");
			port = LOCK_MANAGER_PORT;
		}

		if (args[0].equals("-build")) {
			FileOutputStream fos = new FileOutputStream(args[1]);
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			LockServerWriter lsw = new LockServerWriter(oos, port);
			
			System.out.println("Press enter to end lock manager...");
			System.in.read();
			
			lsw.interrupt();
			lsw.join();
			oos.writeObject("EOF");
			oos.close();
		} else if (args[0].equals("-run")) {
			FileInputStream fis = new FileInputStream(args[1]);
			ObjectInputStream ois = new ObjectInputStream(fis);			
			runLockServer(ois,port);
		} else {
			usage();
		}
	}
	
	private static void usage() {
		System.err.println("Usage: LockManger -build|-run file [port]");
	}
	
	private static class LockServerWriter extends Thread {
		private ObjectOutputStream oos;
		private int port;
		private LockServerWriter(ObjectOutputStream oos, int port) {
			this.oos = oos;
			this.port = port;
			this.start();
		}
		
		public void run() {
			try {
				ServerSocket ss = new ServerSocket(port);
				ss.setSoTimeout(500);
				while (! isInterrupted()) {
					Socket s;
					try {
						s = ss.accept();
					} catch (SocketTimeoutException ste) {
						continue;
					}
					BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
					PrintWriter pw = new PrintWriter(s.getOutputStream());
					String idString = br.readLine();
					int peerID = 0;
					try {
						peerID = Integer.parseInt(idString);
					} catch (NumberFormatException nfe) {
						System.err.println("Could not parse line from client (" + idString + ") because " + nfe.getMessage());
						continue;
					}
					oos.writeObject(new Integer(peerID));
					pw.println("GRANTED");
					pw.flush();
					String responseString = br.readLine();
					if (responseString.equals("RELEASED")) {
						s.close();
					} else {
						System.err.println("Received unexpected data from client: " + responseString);
					}					
				}
			} catch (IOException ioe) {
				System.err.println("Caught IO Exception: " + ioe.getMessage());
			}
		}
	}
	
	public static void runLockServer(ObjectInputStream ois, int port) throws Exception {
		ArrayList<Integer> locks = new ArrayList<Integer>();
		Object lastRead = null; 
		for ( ; ; ) {
			lastRead = ois.readObject();
			if (! (lastRead instanceof Integer)) {
				break;
			}
			locks.add((Integer) lastRead);
		}
		ois.close();
		
		int nextLock = 0;
		final int numLocks = locks.size();
		
		ServerSocket ss = new ServerSocket(port);
		ArrayList<Waiting> waitingPeers = new ArrayList<Waiting>(); 
		
		while (nextLock < numLocks) {
			Socket s = ss.accept();
			
			Reader r = new InputStreamReader(s.getInputStream());
			PrintWriter p = new PrintWriter(s.getOutputStream());
			
			StringBuilder sb = new StringBuilder();
			int c = r.read();
			while (c != '\n' && c != '\r' && c != -1) {
				sb.append((char) c);
				c = r.read();
			}
			int num = Integer.parseInt(sb.toString());
			Waiting wp = new Waiting();
			wp.peer = num;
			wp.s = s;
			wp.r = r;
			wp.p = p;
			
			waitingPeers.add(wp);
			
			boolean noAvailablePeer = false;
			
			while (! noAvailablePeer) {
				noAvailablePeer = true;
				for (int i = 0; i < waitingPeers.size(); ++i) {
					wp = waitingPeers.get(i);
					if (wp.peer == locks.get(nextLock)) {
						s = wp.s;
						p = wp.p;
						r = wp.r;
						
						p.println("GRANTED");
						p.flush();
						
						sb = new StringBuilder();
						c = r.read();
						while ((c == '\n' || c == '\r') && c != -1) {
							c = r.read();
						}
						while (c != '\n' && c != '\r' && c != -1) {
							sb.append((char) c);
							c = r.read();
						}
						
						if (sb.toString().equals("RELEASED")) {
							s.close();
							waitingPeers.remove(i);
							++nextLock;
							noAvailablePeer = false;
						} else {
							throw new RuntimeException("Unexpected input received: " + sb.toString());
						}
						break;
					}
				}
			}
		}
	}
}
