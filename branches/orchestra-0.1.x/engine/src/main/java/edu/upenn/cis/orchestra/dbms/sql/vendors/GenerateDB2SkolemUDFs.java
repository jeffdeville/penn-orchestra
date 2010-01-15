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
package edu.upenn.cis.orchestra.dbms.sql.vendors;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.wrappers.SkolemParameters;

/**
 * Skolem function Java UDF generator.
 * 
 * @author zives
 *
 */
public class GenerateDB2SkolemUDFs {
	/**
	 * Skolem parameter types
	 * @author zives
	 *
	 */
	static enum AType {STR//, INT, DAT
	};

	
	/**
	 * Longest string
	 */
	final static int maxStr = 128;
	
	static String namespace = "";//"edu.upenn.cis.orchestra";
	static String className = "Skolem";

	
	/**
	 * Create the SQL DDL info for the Skolem parameters and function name
	 * 
	 * @param existingString
	 * @param typ
	 * @param curDepth
	 * @return
	 */
	private static SkolemParameters getAttribDDL(SkolemParameters existingString, AType typ, int curDepth) {
		SkolemParameters ret = new SkolemParameters(existingString);
		if (typ == AType.STR) {
			ret.add("Str", "StrVal" + Integer.toString(curDepth), "VARCHAR(" + maxStr + ")", "String");
//		} else if (typ == AType.INT) {
//			ret.add("Int", "IntVal" + Integer.toString(curDepth), "INTEGER", "int");
//		} else if (typ == AType.DAT) {
//			ret.add("Dat", "DatVal" + Integer.toString(curDepth), "DATE", "Date");
		} else {
			throw new RuntimeException("Illegal type");
		}
		return ret;
	}
	
	public static ArrayList<SkolemParameters> getAttribStatements(ArrayList<SkolemParameters> aList, int curDepth, int maxDepth) {
		if (curDepth == maxDepth)
			return new ArrayList<SkolemParameters>();
		
		ArrayList<SkolemParameters> ret = new ArrayList<SkolemParameters>();
		for (SkolemParameters a : aList) {
			ret.add(getAttribDDL(a, AType.STR, curDepth));
//			ret.add(getAttribDDL(a, AType.INT, curDepth));
//			ret.add(getAttribDDL(a, AType.DAT, curDepth));
		}
		
		ret.addAll(getAttribStatements(ret, curDepth + 1, maxDepth));
		
		return ret;
	}
	
	/**
	 * Auto-generate the Java function for a particular Skolem parameter combination
	 * 
	 * @param s Skolem parameters
	 * @param out Destination for statements
	 * 
	 * @throws java.io.IOException
	 */
	public static void createJavaFunction(SkolemParameters s, PrintStream out) 
	throws java.io.IOException {
		StringBuffer def = new StringBuffer(
				"/* This code is automatically generated */\n" +
				"\t/**\n\t * Compute skolem function\n\t */\n" +
				"\tpublic static int skolem");
		
		def.append(s.getFnName() + "(String skolemName");
		
		for (int i = 0; i < s.getNumAttribs(); i++) {
			def.append(", " + s.getJavaTypeAt(i) + " " + s.getAttribNameAt(i));
		}
		def.append(") throws IOException {\n");

		/*
		def.append(	"\t\tObject[] parms = new Object[" + (s.getNumAttribs() + 1) + "];\n\n");
		
		def.append(	"\t\tparms[0] = skolemName;\n");
		
		for (int i = 0; i < s.getNumAttribs(); i++) {
			String attr = s.getAttribNameAt(i);
			if (s.getJavaTypeAt(i).equals("int"))
				def.append("\t\tparms[" + (i + 1) + "] = new Integer("+ attr + ");\n");
			else
				def.append("\t\tparms[" + (i + 1) + "] = "+ attr + ";\n");
		}*/

		def.append("\t\ttry {\n");
		def.append("\t\t\tint skVal = skolem(\"" + s.getFnName() + "\", skolemName");
		for (int i = 0; i < s.getNumAttribs(); i++) {
			def.append(", ");
			String attr = s.getAttribNameAt(i);
			def.append(attr);
		}
		def.append(");\n");
		def.append("\t\t\tlog.println(\"Returned \" + skVal);\n");
		def.append("\t\t\treturn skVal;\n");
		def.append("\t\t} catch (Exception ce) {\n");
		def.append("\t\t\tce.printStackTrace(log);\n");
		def.append("\t\t\treturn 0;\n");
		def.append("\t\t}\n");
		def.append("\t}\n");
		
		out.print(def.toString());
	}
	
	public static void createFunctionDDL(SkolemParameters s, PrintStream out) throws java.io.IOException {
		StringBuffer def = new StringBuffer("CREATE FUNCTION Skolem.skolem" + s.getFnName() + " (\n");
		
		def.append("\tFunc VARCHAR(10)");
		
		for (int i = 0; i < s.getNumAttribs(); i++) {
			def.append(",\n\t" + s.getAttribNameAt(i) + " " + s.getTypeDefAt(i));
		}
		def.append(" )\nRETURNS INTEGER\n");
		def.append("LANGUAGE JAVA\n");
		def.append("DISALLOW PARALLEL\n");
		def.append("EXTERNAL ACTION\n");
		def.append("NOT DETERMINISTIC\n");
		//def.append("MODIFIES SQL DATA\n");
		def.append("FENCED NOT THREADSAFE\n");
		if (namespace.equals(""))
			def.append("EXTERNAL NAME '" + className + s.getFnName() + "!skolem" + s.getFnName() + "'\n");
		else
			def.append("EXTERNAL NAME '" + namespace + "." + className + s.getFnName() + "!skolem" + s.getFnName() + "'\n");
		def.append("PARAMETER STYLE JAVA ;\n\n");
		
		out.print(def.toString());
	}
	
	public static void createSourceFile(String name, int position, int max, PrintStream ps) 
	throws java.io.IOException {
		if (!namespace.equals(""))
			ps.println("package " + namespace + ".Skolem" + name + ";\n");

		ps.println("import java.io.ByteArrayOutputStream;");
		ps.println("import java.io.File;");
		ps.println("import java.io.FileWriter;");
		ps.println("import java.io.IOException;");
		ps.println("import java.io.ObjectOutputStream;");
		ps.println("import java.io.PrintWriter;");
		ps.println("import java.sql.Date;\n");

		ps.println("import com.sleepycat.je.Database;");
		ps.println("import com.sleepycat.je.DatabaseConfig;");
		ps.println("import com.sleepycat.je.DatabaseEntry;");
		ps.println("import com.sleepycat.je.DatabaseException;");
		ps.println("import com.sleepycat.je.Environment;");
		ps.println("import com.sleepycat.je.EnvironmentConfig;");
		ps.println("import com.sleepycat.je.OperationStatus;\n");
		
		ps.println("public class " + className + name + " extends COM.ibm.db2.app.UDF {");
		
		ps.println("\tprivate static int FIRST_SKOLEM = " + (position * ((Integer.MIN_VALUE + 2) / max) - 2) + ";");
		ps.println("\tpublic static String CONNECTION = \"skolem" + name + "\";");

		ps.println("\tprivate static final byte INT = 1, STRING = 2, DATE = 3, DOUBLE = 4;");
		ps.println("\tprivate static boolean isActive = false;\n");

		ps.println("\tprivate static PrintWriter log;");
		ps.println("\tprivate static Database _db;");
		ps.println("\tprivate static Environment _env;\n");

		ps.println("\tprivate static synchronized int getNextSkolem() throws DatabaseException {");
		ps.println("\t	DatabaseEntry key = new DatabaseEntry(new String(\"##SKVAL##\").getBytes()), value = new DatabaseEntry();");
		ps.println("\t	OperationStatus os = _db.get(null, key, value, null);");
		ps.println("\t	int retval;");
		ps.println("\t	if (os == OperationStatus.NOTFOUND) {");
		ps.println("\t		retval = FIRST_SKOLEM;");
		ps.println("\t	} else {");
		ps.println("\t		retval = getValFromBytes(value.getData()) - 1;");
		ps.println("\t	}");
		ps.println("\t	value.setData(getBytes(retval));");
		ps.println("\t	_db.put(null, key, value);");
		ps.println("\t	return retval;");
		ps.println("\t}\n");

		ps.println("\tpublic Skolem" + name + "() {");
		ps.println("\t	super();");
		ps.println("\t	try {");
		ps.println("\t		log = new PrintWriter(new FileWriter(\"skolems.log\", true), true);");
		ps.println("\t		log.println(\"Server daemon initializing...\");");
		ps.println("\t	} catch (Exception e) {");
		ps.println("\t		e.printStackTrace();");
		ps.println("\t	}");
		ps.println("\t}\n");

		ps.println("\tpublic static boolean connect() throws DatabaseException, IOException {");
		ps.println("\t	if (!isActive) {");
		ps.println("\t		try {");
		ps.println("\t			log = new PrintWriter(new FileWriter(\"skolems.log\", true), true);");
		ps.println("\t			EnvironmentConfig envCfg = new EnvironmentConfig();");
		ps.println("\t			envCfg.setAllowCreate(true);");
		ps.println("\t			envCfg.setTransactional(false);");
		ps.println("\t			File file = new File(CONNECTION);");
		ps.println("\t			file.mkdir();");
		ps.println("\t			_env = new Environment(file, envCfg);");
		ps.println("\t			DatabaseConfig databaseConfig = new DatabaseConfig();");
		ps.println("\t			databaseConfig.setAllowCreate(true);");
		ps.println("\t			databaseConfig.setSortedDuplicates(false);\n");

		ps.println("\t			_db = _env.openDatabase(null, CONNECTION + File.separator + CONNECTION, databaseConfig);");
		ps.println("\t		} catch (DatabaseException dnf) {");
		ps.println("\t			dnf.printStackTrace(log);");
		ps.println("\t			throw dnf;");
		ps.println("\t		}\n");

		ps.println("\t		isActive = true;");
		ps.println("\t	}");
		ps.println("\t	return isActive;");
		ps.println("\t}\n");
				
		ps.println("\tpublic static void closeDown() {");
		ps.println("\t	log.println(\"Closing\");");
		ps.println("\t	try {");
		ps.println("\t		_db.close();");
		ps.println("\t		_env.close();");
		ps.println("\t	} catch (DatabaseException e) {");
		ps.println("\t		e.printStackTrace(log);");
		ps.println("\t	}");
		ps.println("\t	isActive = false;");
		ps.println("\t}\n");

		ps.println("\tpublic static synchronized int skolem(String function, Object... args) {");
		ps.println("\t	byte[] keyBytes;");
		ps.println("\t	try {");
		ps.println("\t		connect();");
		ps.println("\t		ByteArrayOutputStream bytes = new ByteArrayOutputStream();");
		ps.println("\t		ObjectOutputStream out = new ObjectOutputStream(bytes);");

		ps.println("\t		byte[] funcBytes = function.getBytes(\"UTF-8\");");
		ps.println("\t		out.writeInt(funcBytes.length);");
		ps.println("\t		out.write(funcBytes);");

		ps.println("\t		for (Object o : args) {");
		ps.println("\t			if (o instanceof Integer) {");
		ps.println("\t				out.writeByte(INT);");
		ps.println("\t			} else if (o instanceof String) {");
		ps.println("\t				out.writeByte(STRING);");
		ps.println("\t			} else if (o instanceof Double) {");
		ps.println("\t				out.writeByte(DOUBLE);");
		ps.println("\t			} else if (o instanceof Date) {");
		ps.println("\t				out.writeByte(DATE);");
		ps.println("\t			} else {");
		ps.println("\t				throw new RuntimeException(\"Don't know what to do with object \" + o + \" of type \" + o.getClass().getCanonicalName());");
		ps.println("\t			}");
		ps.println("\t		}");

		ps.println("\t				for (Object o : args) {");
		ps.println("\t			if (o instanceof Integer) {");
		ps.println("\t				out.writeInt((Integer) o);");
		ps.println("\t			} else if (o instanceof String) {");
		ps.println("\t				byte[] strBytes = ((String) o).getBytes(\"UTF-8\");");
		ps.println("\t				out.writeInt(strBytes.length);");
		ps.println("\t				out.write(strBytes);");
		ps.println("\t			} else if (o instanceof Double) {");
		ps.println("\t				out.writeDouble((Double) o);");
		ps.println("\t			} else if (o instanceof Date) {");
		ps.println("\t				out.writeLong(((Date) o).getTime());");
		ps.println("\t			} else {");
		ps.println("\t				throw new RuntimeException(\"Don't know what to do with object \" + o + \" of type \" + o.getClass().getCanonicalName());");
		ps.println("\t			}");
		ps.println("\t		}");
		ps.println("\t		out.flush();");
		ps.println("\t		keyBytes = bytes.toByteArray();");
		ps.println("\t	} catch (IOException ioe) {");
		ps.println("\t		ioe.printStackTrace(log);");
		ps.println("\t		throw new RuntimeException(ioe);");
		ps.println("\t	} catch (DatabaseException dbe) {");
		ps.println("\t		dbe.printStackTrace(log);");
		ps.println("\t		throw new RuntimeException(dbe);");
		ps.println("\t	}");

		ps.println("\t	DatabaseEntry key = new DatabaseEntry(keyBytes), value = new DatabaseEntry();");

		ps.println("\t	try {");
		ps.println("\t		OperationStatus os = _db.get(null, key, value, null);");
		ps.println("\t		if (os == OperationStatus.SUCCESS) {");
		ps.println("\t			closeDown();");
		ps.println("\t			return getValFromBytes(value.getData());");
		ps.println("\t		} else {");
		ps.println("\t			int retval = getNextSkolem();");
		ps.println("\t			value.setData(getBytes(retval));");
		ps.println("\t			_db.put(null, key, value);");
		ps.println("\t			closeDown();");
		ps.println("\t			return retval;");
		ps.println("\t		}");
		ps.println("\t	} catch (DatabaseException de) {");
		ps.println("\t		throw new RuntimeException(de);");
		ps.println("\t	}");
		ps.println("\t}");

		ps.println("\tpublic static byte[] getBytes(int value) {");
		ps.println("\t	byte[] retval = new byte[4];");
		ps.println("\t	for (int i = 0; i < 4; ++i) {");
		ps.println("\t		retval[i] = (byte) (value >>> ((4 - i - 1) * 8));");
		ps.println("\t	}");
		ps.println("\t	return retval;");
		ps.println("\t}");

		ps.println("\tpublic static int getValFromBytes(byte[] bytes) {");
		ps.println("\t	if (bytes.length != 4) {");
		ps.println("\t		throw new RuntimeException(\"Byteified integers must have length 4\");");
		ps.println("\t	}");
		ps.println("\t	int value = 0;");

		ps.println("\t	for (int i = 0; i < 4; ++i) {");
		ps.println("\t		value |= ((int) (bytes[i] & 0xFF)) << ((4 - i - 1) * 8);");		
		ps.println("\t	}");

		ps.println("\t	return value;");
		ps.println("\t}");
		
	}

	public static void createJavaSource(String filePrefix, ArrayList<SkolemParameters> statements) 
	throws java.io.IOException {
		int i = 0;
		for (SkolemParameters sk : statements) {
			PrintStream ps = new PrintStream(new FileOutputStream(filePrefix + sk.getFnName() + ".java"));
			
			createSourceFile(sk.getFnName(), i, statements.size(), ps);
			createJavaFunction(sk, ps);
			ps.println("}");
			ps.close();
			
			i++;
		}
	}
	
	final static int defaultDepth = 5;
	
	public static void main(String[] args) throws java.io.FileNotFoundException {
		System.out.println("Orchestra Skolem function creator, zives 4/30/07\n\n");
		
		Config.parseCommandLine(args);
	
		int depth = Config.getUDFDepth();
		
		PrintStream psS = System.out;
		
		String fn = Config.getSkolemClass();
		psS = new PrintStream(new FileOutputStream(fn + ".sql"));
		className = fn;
		System.out.println("Writing UDF output to " + fn + ".{java,sql}");

		System.out.println("Creating Skolem functions to depth " + depth);

		ArrayList<SkolemParameters> statements = new ArrayList<SkolemParameters>();
		statements.add(new SkolemParameters());
		
		statements = getAttribStatements(statements, 0, depth);
	
		try {
			createJavaSource(fn, statements);
			
			for (SkolemParameters sk : statements)
				createFunctionDDL(sk, psS);
			
			psS.close();

			for (SkolemParameters sk : statements) {
			
				String[] args2 = new String[1];
				args2[0] = fn + sk.getFnName() + ".java";
	
				int ret = -1;
				try {
//					Process p = Runtime.getRuntime().exec("javac " + args2[0]);
//					greg: need to have jdbc and Berkeley DB jars in classpath ...
					Process p = Runtime.getRuntime().exec("C:\\Progra~1\\IBM\\SQLLIB\\java\\jdk\\bin\\javac " + args2[0]);
					InputStream stderr = p.getErrorStream();
		            InputStreamReader isr = new InputStreamReader(stderr);
		            BufferedReader br = new BufferedReader(isr);
		            String line = null;
		            
		            while ( (line = br.readLine()) != null)
		                System.out.println(line);
		            
					ret = p.waitFor();
				} catch (Throwable t) {
					t.printStackTrace();
				}
				/*
				int ret = com.sun.tools.javac.Main.compile(new String[] {
			            "-classpath", "bin",
			            "-d", "/temp/dynacode_classes",
			            className + ".java" });*/
				
				if (ret == 0)
					System.out.println("Java file " + fn + sk.getFnName() + ".java compiled successfully");
				else {
					System.out.println("ERROR: Java file failed to compile");
					System.exit(1);
				}
			}
			
			System.out.println("Now you need to: (1) Copy " + className + ".class to C:\\Program Files\\IBM\\sqllib\\function");
			System.out.println("                 (2) Run " + className + ".sql from the DB2 CLI");
		} catch (java.io.IOException i) {
			i.printStackTrace();
		}
	}
}
