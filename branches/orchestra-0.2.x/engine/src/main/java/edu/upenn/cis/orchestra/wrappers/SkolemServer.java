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
package edu.upenn.cis.orchestra.wrappers;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.net.SocketTimeoutException;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.dbms.sql.vendors.GenerateDB2SkolemUDFs;

public class SkolemServer extends Thread {
	private static int _newSkVal = -2;
	public static final int PORT = 7770;
	public static String CONNECTION = "/hsqldb";
	private static boolean quit = false;
	private static boolean isActive = false;
	
	private static Connection db;

	private static synchronized int getNextSkolem() {
		return _newSkVal--;
	}
	
	public static void quitServer() {
		if (isActive) {
			quit = true;
			try {
			while (isActive)
				Thread.sleep(10);
			} catch (InterruptedException i) {
				
			}
			System.out.println("Skolem server shut down");
		}
	}
	
	public boolean isEnabled() {
		return isActive;
	}
	
	public static void main(String[] parms) {
		Config.parseCommandLine(parms);
		SkolemServer s = new SkolemServer();
		s.start();
	}
	
	/**
	 * Main daemon thread:  wait for a request on the socket and respond.
	 * Also reads/writes last value of Skolem.
	 * 
	 * @throws SQLException
	 */
	public void run() {
		try {
			isActive = true;
			System.out.println("Server daemon initializing...");
			ServerSocket s = new ServerSocket(PORT);
			try {
				Class.forName ("org.hsqldb.jdbcDriver");
			} catch (ClassNotFoundException cnf) {
				cnf.printStackTrace();
				System.exit(1);
			}
			String conn = "jdbc:hsqldb:file:" + Config.getWorkDir() + CONNECTION;
			System.out.println("Connecting to " + conn);
			db = DriverManager.getConnection(conn);
			
			try {
				BufferedReader f = new BufferedReader(new FileReader(Config.getWorkDir() + "/last.val"));
	
				String str = f.readLine();
				
				_newSkVal = Integer.valueOf(str);
				f.close();
			} catch (FileNotFoundException fne) {
				// Skip if no file
				System.out.println("First time invocation: creating SQL tables");
				try {
					createTables(Config.getUDFDepth());
				} catch (SQLException se) {
					System.err.println("SQL exception creating tables");
					se.printStackTrace();
					isActive = false;
					System.exit(1);
				}
			}
			System.out.println("Skolem server IDs begin at " + _newSkVal);
			
			s.setSoTimeout(1000);
			do {
				Socket req = null;
				try {
					req = s.accept();

					SkolemHandler handler = new SkolemHandler(req);
					handler.run();
				} catch (SocketTimeoutException soe) {
					// Timeout should be caught
					req = null;
				}
			} while (!quit);
			quit = false;
			s.close();
			System.out.println("Server daemon closing down...");
			
			try {
				PrintWriter f = new PrintWriter(new FileWriter("last.val"));
			
				f.println(String.valueOf(_newSkVal));
				f.close();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (SQLException sql) {
			sql.printStackTrace();
		}
		isActive = false;
	}
	
	private static class SkolemHandler extends Thread { 
		private Socket _sock = null;
			
		public SkolemHandler(Socket s) {
			_sock = s;
		}
		
		public void run() {
			System.out.println("Handling request from " + _sock.getRemoteSocketAddress().toString());
			try {
				ObjectOutputStream ret = new ObjectOutputStream(_sock.getOutputStream());
				ObjectInputStream br = new ObjectInputStream(_sock.getInputStream());
				
				StringBuffer nam = new StringBuffer("skolem");
//				Class[] c;
				Object[] o;
				Integer numParms;
				Integer retType;
				//Integer returnBad = new Integer(0);
				
				try {
					do {
						numParms = (Integer)br.readObject();
						
						if (numParms == -1) {
							quit = true;
							return;
						}
						nam = new StringBuffer("");//skolem");
//						c = new Class[numParms + 1];
						o = new Object[numParms + 1];
//						c[0] = String.class;
						for (int i = 1; i < numParms + 1; i++) {
							retType = (Integer)br.readObject();
							switch (retType.intValue()) {
							case 0:
								nam.append("Str");
//								c[i] = String.class;
								break;
							case 1:
								nam.append("Int");
//								c[i] = Integer.class;
								break;
							case 2:
								nam.append("Dat");
//								c[i] = Date.class;
								break;
							default:
								System.err.println("Illegal parameter type");						
							}
						}
			
						Integer returnThis = null;
						try {
							for (int i = 0; i <= numParms; i++) {
								o[i] = br.readObject();
							}
							
							returnThis = new Integer(skolem(nam.toString(), o));//(Integer)m.invoke(this, o);
							ret.writeObject(returnThis);
						} catch (SQLException s) {
							s.printStackTrace();							
						}
						ret.flush();
						if (returnThis != null)
							System.out.println("Sent response " + returnThis.toString());
						
						break;
					} while (true);
				} catch (IOException ie) {
		//			System.err.println("IO exception");
		//			ie.printStackTrace();
				} catch (ClassNotFoundException cnf) {
					System.err.println("IO exception");
					cnf.printStackTrace();
				}
				_sock.close();
			} catch (IOException ioe) {
	//			System.err.println("IO exception");
	//			ioe.printStackTrace();
				
			}
		}
		
		public static int skolem(String nam, Object[] parms) throws SQLException {
			int skVal;
			String stmt = "SELECT * FROM Skolems." + nam + " WHERE Func = ?";//AND StrVal0 = ?";
			
			System.out.print(parms[0].toString() + "(");
			int pos = 0;
			for (int i = 1; i < parms.length; i++) {
				if (i > 1)
					System.out.print(",");
				if (parms[i] != null) {
					System.out.print(parms[i].toString());
				}
				stmt = stmt + " AND ";
				
				/*
				if (parms[i] instanceof String)
					stmt = stmt + "Str";
				else if (parms[i] instanceof Integer)
					stmt = stmt + "Int";
				else if (parms[i] instanceof Date)
					stmt = "Dat";
				else 
					throw new SQLException("Unable to translate type " + parms[i]);
					*/
				stmt = stmt + nam.substring(pos, pos + 3);
				pos += 3;
				
				stmt = stmt + "Val" + String.valueOf(i - 1) + " = ?";
			}
			System.out.println(")");
	
			PreparedStatement ps = db.prepareStatement(stmt);
	
			for (int i = 0; i < parms.length; i++)
				if (parms[i] instanceof String)
					ps.setString(i+1, (String)parms[i]);
				else if (parms[i] instanceof Integer)
					ps.setInt(i+1, ((Integer)parms[i]).intValue());
				else if (parms[i] instanceof Date)
					ps.setDate(i+1, (Date)parms[i]);
	
			ResultSet rs = ps.executeQuery();
	
			if (rs.next()) {
				skVal = rs.getInt(parms.length+1);
			} else {
				skVal = getNextSkolem();
	
				String upd = "INSERT INTO Skolems." + nam + " VALUES (?";//?,?,?)";
				
				for (int i = 0; i < parms.length; i++)
					upd = upd + ",?";
				upd = upd + ")";
				
				ps = db.prepareStatement(upd);
	
				for (int i = 0; i < parms.length; i++)
					if (parms[i] instanceof String)
						ps.setString(i+1, (String)parms[i]);
					else if (parms[i] instanceof Integer)
						ps.setInt(i+1, ((Integer)parms[i]).intValue());
					else if (parms[i] instanceof Date)
						ps.setDate(i+1, (Date)parms[i]);
	
				ps.setInt(parms.length+1, skVal);
	
				ps.executeUpdate();
				if (ps != null) ps.close();
			}
			if (rs != null) rs.close();
			if (ps != null) ps.close();
			return skVal;
		}
	}

	public static void createTables(int depth) throws SQLException {
		ArrayList<SkolemParameters> statements = new ArrayList<SkolemParameters>();
		statements.add(new SkolemParameters());
		
		statements = GenerateDB2SkolemUDFs.getAttribStatements(statements, 0, depth);

		createTableDDL(statements);
	}

	/**
	 * Creates the Skolem function tables, with up to <b>depth</b> parameters
	 * 
	 * @param depth Maximum number of attributes
	 * 
	 * @throws SQLException
	 */
	public static void createTableDDL(ArrayList<SkolemParameters> statements) throws SQLException {
		Statement st = db.createStatement();
		try {
			st.execute("CREATE SCHEMA Skolems AUTHORIZATION DBA");
		} catch (SQLException schemaE) {
			
		}
		for (SkolemParameters s : statements) {
			String statement = "CREATE CACHED TABLE Skolems." + s.getFnName() + "(Func VARCHAR(10) NOT NULL";
			
			String pKey = "PRIMARY KEY (Func";
			
			for (int i = 0; i < s.getNumAttribs(); i++) {
				statement = statement + ", " + s.getAttribNameAt(i) + " " + s.getTypeDefNNAt(i);
				
				pKey = pKey + ", " + s.getAttribNameAt(i);
			}
			statement = statement + ", SkVal INTEGER";
			statement = statement + ", " + pKey +"));";

			try {
				st.execute(statement);
				System.out.println("Creating " + s.getFnName());
			} catch (SQLException schemaE) {
				schemaE.printStackTrace();
			}
		}
		st.close();
	}
	
}
