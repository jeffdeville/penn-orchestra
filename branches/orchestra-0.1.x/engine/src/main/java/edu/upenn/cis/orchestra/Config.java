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
package edu.upenn.cis.orchestra;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.security.InvalidParameterException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@code Config} manages and provides access to a list of properties
 * determining the system's runtime configuration. These properties can be
 * specified in three ways:
 * <ol>
 * <li>in the global.properties file in {@code edu.upenn.cis.orchestra}</li>
 * <li>in the local.properties file in {@code edu.upenn.cis.orchestra}</li>
 * <li>as command-line arguments of the form {@code -key=value}</li>
 * </ol>
 * These are in order of increasing precedence (command-line arguments have
 * highest precedence, global.properties lowest).
 * 
 * @author gkarvoun, tjgreen
 */
public class Config {
	protected static Properties s_props = new Properties(System.getProperties());

	static {
		InputStream global = Config.class.getResourceAsStream("global.properties");
		InputStream local = Config.class.getResourceAsStream("local.properties");
		if (local == null)
		{ 
			File f = new File ("local.properties");
			try
			{
				local = new FileInputStream (f);
			} catch (FileNotFoundException ex) {}
		}
		if (global != null) {
			try { s_props.load(global); } catch (IOException e) { assert(false); }
		}
		if (local != null) {
			try { s_props.load(local); } catch (IOException e) { assert(false); }
		}
		expandVariables();
	}

	protected static void expandVariables() {
		Pattern pat = Pattern.compile("\\$\\{([^\\{\\}]+)\\}");
		StringBuffer buf = new StringBuffer();
		for (Object obj : s_props.keySet()) {
			String key = (String)obj;
			String value = (String)s_props.get(key);
			Matcher mat = pat.matcher(value);
			while (mat.find()) {
				buf.setLength(0);
				String var = mat.group(1);
				buf.append(value.substring(0, mat.start()));
				String prop = (String)s_props.getProperty(var);
				buf.append(prop);
				buf.append(value.substring(mat.end()));
				value = buf.toString();
				mat = pat.matcher(value);
			}
			s_props.put(key, value);
		}
	}

	public static boolean getBoolean(String name) {
		return Boolean.parseBoolean(getProperty(name));
	}
	
	public static void setBoolean(String name, boolean value) {
		s_props.setProperty(name, Boolean.toString(value));
	}
	
	public static int getInteger(String name) {
		return Integer.parseInt(getProperty(name));
	}
	
	public static void setInteger(String name, int value) {
		s_props.setProperty(name, Integer.toString(value));
	}

	public static float getFloat(String name) {
		return Float.parseFloat(getProperty(name));
	}

	public static void setFloat(String name, float value) {
		s_props.setProperty(name, Float.toString(value));
	}
	
	public static String getProperty(String name) {
//		Debug.println("GET: KEY=" + name + ", VALUE=" + s_props.getProperty(name));
		return s_props.getProperty(name);
	}
	
	public static Object setProperty(String key, String value) {
		Debug.println("SET: KEY=" + key + ", VALUE=" + s_props.getProperty(key));
		return s_props.setProperty(key, value);
	}
	
	public static void removeProperty(String key) {
		s_props.remove(key);
	}

	public static void parseCommandLine(String[] args) {
		for (int i = 0; i < args.length; i++) {
			int index = args[i].indexOf("=");
			if (args[i].startsWith("-") && index != -1) {
				String name = args[i].substring(1, index);
				String value = args[i].substring(index+1);
				s_props.put(name, value);
			} else {
				throw new InvalidParameterException("Invalid command-line argument: " + args[i]);
			}
		}
		expandVariables();
	}

	public static void dumpParams(PrintStream out) {
		out.println("PARAMETERS:");
		for (Enumeration<Object> e = s_props.keys(); e.hasMoreElements(); ) {
			Object key = e.nextElement();
			out.println(key + "=" + s_props.get(key));
		}
	}

	public static boolean isDB2() {
		return getJDBCDriver().contains("db2");
	}
	
	public static boolean isOracle() {
		return getJDBCDriver().contains("oracle");
	}

	public static boolean isHsql() {
		return getJDBCDriver().contains("hsql");
	}
	public static boolean isMYSQL() {
		return getJDBCDriver().contains("mysql");
	}
	
	public static void setJDBCDriver(String driver) {
		setProperty("jdbc-driver", driver);
	}

	public static String getJDBCDriver() {
		return getProperty("jdbc-driver");
	}
	
	public static String getUSJDBCDriver() {
		return getProperty("US-jdbc-driver");
	}
	
	public static void setUSJDBCDriver(String driver) {
		setProperty("US-jdbc-driver", driver);
	}

	public static float getDBMSversion() {
		return getFloat("DBMSversion");
	}

	public static void setDBMSversion(float version) {
		setFloat("DBMSversion", version);
	}
	
	public static void setWorkloadPrefix(String prefix) {
		setProperty("workload", prefix);
	}

	public static String getWorkloadPrefix() {
		return getProperty("workload");
	}

	public static void setWorkDir(String workdir) {
		setProperty("workdir", workdir);
	}

	public static String getWorkDir() {
		return getProperty("workdir");
	}

	public static void setTestSchemaName(String schema) {
		setProperty("schema", schema);
	}

	public static String getTestSchemaName() {
		return getProperty("schema");
	}
	
	public static void setTempTables(boolean temps) {
		setBoolean("temptables", temps);
	}
	
	public static boolean getTempTables() {
		return getBoolean("temptables");
	}

	public static void setFullDebug(boolean fulldebug) {
		setBoolean("fulldebug", fulldebug);
	}

	public static boolean getFullDebug() {
		return getBoolean("fulldebug");
	}

	public static void setLocalPeer(String peer) {
		setProperty("localpeer", peer);
	}

	public static String getLocalPeer() {
		return getProperty("localpeer");
	}

	public static void setLocalSchema(String schema) {
		setProperty("localschema", schema);
	}

	public static String getLocalSchema() {
		return getProperty("localschema");
	}

	/**
	 * Prepare statements before executing
	 */
	public static void setPrepare(boolean prepare) {
		setBoolean("prepare", prepare);
	}

	public static boolean getPrepare() {
		return getBoolean("prepare");
	}

	public static String getSchemaFile() {
		return "file://" + getProperty("workdir") + "/" + getTestSchemaName() + ".schema";
	}

	/** Apply SQL to DBMS
	 */

	public static void setApply(boolean apply) {
		setBoolean("apply", apply);
	}

	public static boolean getApply() {
		return getBoolean("apply");
	}

	/** Create outer unions, versus individual
	 * mapping relations
	 */

	public static void setOuterUnion(boolean outerunion) {
		setBoolean("outerunion", outerunion);
	}

	public static boolean getOuterUnion() {
		return getBoolean("outerunion");
	}
	
	public static void setOuterJoin(boolean outerjoin) {
		setBoolean("outerjoin", outerjoin);
	}

	public static boolean getOuterJoin() {
		return getBoolean("outerjoin");
	}

	public static void setMigrate(boolean migrate) {
		setBoolean("migrate", migrate);
	}

	public static boolean getMigrate() {
		return getBoolean("migrate");
	}
	
	/** Reset the tables at the end */
	public static void setReset(boolean reset) {
		setBoolean("reset", reset);
	}

	public static boolean getReset() {
		return getBoolean("reset");
	}

	/** 
	 * Don't do incremental maintenance -- instead
	 * simply recompute.  Disables DO_INSERT, DO_DELETE.
	 */
	public static void setNonIncremental(boolean nonincremental) {
		setBoolean("incremental", !nonincremental);
	}

	public static boolean getNonIncremental() {
		return !getBoolean("incremental");
	}

	/** Autocommit  */
	public static void setAutocommit(boolean autocommit) {
		setBoolean("autocommit", autocommit);
	}

	public static boolean getAutocommit() {
		return getBoolean("autocommit");
	}

	/** Debug  */
	public static void setDebug(boolean debug) {
		setBoolean("debug", debug);
	}

	public static boolean getDebug() {
		return getBoolean("debug");
	}

	/** Put multiple queries in one batch */

	public static void setBatch(boolean batch) {
		setBoolean("batch", batch);
	}

	public static boolean getBatch() {
		return getBoolean("batch");
	}

	/** Put rules with the same body in one union query */
	public static void setUnion(boolean union) {
		setBoolean("union", union);
	}

	public static boolean getUnion() {
		return getBoolean("union");
	}

	//	Don't remember what exactly false means ... maybe not to include labeled nulls in index?
	public static void setIndexAllFields(boolean indexall) {
		setBoolean("indexall", indexall);
	}

	public static boolean getIndexAllFields() {
		return getBoolean("indexall");
	}

	public static void setEstTableSize(int tablesize) {
		setInteger("tablesize", tablesize);
	}

	public static int getEstTableSize() {
		return getInteger("tablesize");
	}

	public static void setUser(String user) {
		setProperty("user", user);
	}

	public static String getUser() {
		String user = getProperty("user");
		if (user == null) {
			user = getProperty("db2user");
		}
		return user;
	}

	public static void setPassword(String password) {
		setProperty("password", password);
	}

	public static String getPassword() {
		String password = getProperty("password");
		if (password == null) {
			password = getProperty("db2pwd");
		}
		return password;
	}

	public static void setSQLServer(String server) {
		setProperty("sqlserver", server);
	}

	public static String getSQLServer() {
		String server = getProperty("sqlserver");
		if (server == null) {
			server = getProperty("db2server");
		}
		return server;
	}
	
	public static void setSQLSchema(String schema) {
		setProperty("sqlschema", schema);
	}
	
	public static String getSQLSchema() {
		return getProperty("sqlschema");
	}

	public static void setDBName(String dbname) {
		setProperty("dbname", dbname);
	}

	public static String getDBName() {
		return getProperty("dbname");
	}

	public static void setSetSemantics(boolean set) {
		setBoolean("setsemantics", set);
	}

	public static boolean getSetSemantics() {
		return getBoolean("setsemantics");
	}

	public static void setStratified(boolean stratified) {
		setBoolean("stratified", stratified);
	}

	public static boolean getStratified() {
		return getBoolean("stratified");
	}
	
	public static String getImportExtension() {
		return getProperty("importExtension");
	}

	public static void setCygwinHome(String home) {
		setProperty("cygwinhome", home);
	}

	public static String getCygwinHome() {
		return getProperty("cygwinhome");
	}

	public static void setUDFHome(String udfhome) {
		setProperty("udfhome", udfhome);
	}

	/**
	 * DB2 home directory for user defined Skolems
	 * 
	 * @return
	 */
	public static String getUDFHome() {
		return getProperty("udfhome");
	}
	
	public static void setUDFDepth(int d) {
		setInteger("udfdepth", d);
	}

	/**
	 * @deprecated
	 * @return
	 */
	public static String getLoggingMsg() {
		if (isDB2()){
			return " NOT LOGGED INITIALLY";
		} else if (isOracle()) {
			return " NOLOGGING";
		} else {
			return null;
		}
	}

	/**
	 * Maximum number of arguments to a Skolem user defined function
	 * 
	 * @return
	 */
	public static int getUDFDepth() {
		return getInteger("udfdepth");
	}

	public static void setSkolemClass(String cname) {
		setProperty("skolemclass", cname);
	}
	
	/**
	 * Skolem file class name
	 * 
	 * @return
	 */
	public static String getSkolemClass() {
		return getProperty("skolemclass");
	}

	/**
	 * Skolem server hostname
	 * 
	 * @return
	 */
	public static String getSkolemServerHost() {
		return getProperty("skolemhost");
	}

	public static void setSkolemServerHost(String cname) {
		setProperty("skolemhost", cname);
	}
	
	
	/***************
	 * Gui options
	 ***************
	 */
	

	/**
	 * Show the tree menu at startup?
	 * @return
	 */
	public static boolean getGuiShowTreeMenu ()
	{
		return getBoolean("showtreemenu");
	}
	
	/**
	 * Show the tree menu at startup?
	 * @param showTreeMenu
	 */
	public static void setGuiShowTreeMenu (boolean showTreeMenu)
	{
		setBoolean("showtreemenu", showTreeMenu);
	}

	
	/**
	 * Cache data to avoid to load all relation data in data editor
	 * @return
	 */
	public static boolean getGuiCacheRelEditData ()
	{
		return getBoolean("guiCacheRelEditData");
	}
	
	/**
	 * Cache data to avoid to load all relation data in data editor
	 */
	public static void setGuiCacheRelEditData (boolean cache)
	{
		setBoolean("guiCacheRelEditData", cache);
	}
	
	/**
	 * Temporary directory
	 */
	public static String getTempDir() {
		return getProperty("tempdir");
	}
	
	/**
	 * Temporary schema file
	 */
	public static String getTempSchemaFile() {
		return  getProperty("tempdir") + "/" + "test.schema";
	}
	
	/**
	 * Current Schema name
	 */
	public static void setCurrSchemaName(String currSchemaName) {
		setProperty("currschema",currSchemaName);
	}

	
	/**
	 * Current Schema name
	 * @return
	 */
	public static String getCurrSchemaName() {
		return getProperty("currschema");
	}
	
	/**
	 * Current Schema File Location
	 * @return
	 */
	public static String getCurrSchemaFile() {
		return "file://" + getProperty("workdir") + "\\" + getCurrSchemaName() + ".schema";
	}
	
	/*
	 * "Chase", i.e., reuse existing tuples in mappings' target
	 * or create skolems always
	 */
	
	
	/**
	 * 
	 * @return true, if side effects are allowed, false if not
	 */
	public static boolean getAllowSideEffects ()
	{
		return getBoolean("allowSideEffects");
	}
	
	/**
	 * 
	 */
	public static void setAllowSideEffects (boolean se)
	{
		setBoolean("allowSideEffects", se);
	}
	

	public static void setEdbbits(boolean edbbits) {
		setBoolean("edbbits", edbbits);
	}

	public static boolean getEdbbits() {
		return getBoolean("edbbits");
	}
	
	public static void setValueProvenance(boolean valueProvenance) {
		setBoolean("valueProvenance", valueProvenance);
	}

	public static boolean getValueProvenance() {
		return getBoolean("valueProvenance");
	}
	
	public static void setAcyclicSchema(boolean acyclicSchema) {
		setBoolean("acyclicSchema", acyclicSchema);
	}

	public static boolean isAcyclicSchema() {
		return getBoolean("acyclicSchema");
	}
	
	public static void setRunStatistics(boolean runStatistics) {
		setBoolean("runStatistics", runStatistics);
	}

	public static boolean getRunStatistics() {
		return getBoolean("runStatistics");
	}

	public static void setQueryCutoff(int queryCutoff) {
		setInteger("queryCutoff", queryCutoff);
	}

	public static int getQueryCutoff() {
		return getInteger("queryCutoff");
	}
	
	public static void setTransactionCutoff(int transactionCutoff) {
		setInteger("transactionCutoff", transactionCutoff);
	}

	public static int getTransactionCutoff() {
		return getInteger("transactionCutoff");
	}
	
	public static void setRejectionTables(boolean rejectionTables) {
		setBoolean("rejectionTables", rejectionTables);
	}

	public static boolean getRejectionTables() {
		return getBoolean("rejectionTables");
	}
	
	public static void setNotExists(boolean notExists) {
		setBoolean("notExists", notExists);
	}

	public static boolean getNotExists() {
		return getBoolean("notExists");
	}

	/**
	 * Option: Only create the minimal set of labeled nulls
	 * 
	 * @return
	 */
	public static boolean useCompactNulls() {
		return getBoolean("compactNulls");
	}
	
	public static void setCompactNulls(boolean cns) {
		setBoolean("compactNulls", cns);
	}
	
	public static String setUpdateStoreExecutable(String executable) {
		return (String) setProperty("updateStoreExecutable", executable);
	}
	
	public static String getUpdateStoreExecutable() {
		return getProperty("updateStoreExecutable");
	}
}
