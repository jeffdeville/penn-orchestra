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
package edu.upenn.cis.orchestra.gui;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.fest.swing.core.Robot;
import org.fest.swing.finder.WindowFinder;
import org.fest.swing.fixture.FrameFixture;
import org.fest.swing.launcher.ApplicationLauncher;

import edu.upenn.cis.orchestra.OrchestraUtil;

/**
 * Utilities for testing the ORCHESTRA GUI.
 * 
 * @author John Frommeyer
 * 
 */
public class GUITestUtils {

	
	/** Qualified name of HSQL JDBC driver class. */
	static final String HSQL_DRIVER = "org.hsqldb.jdbcDriver";
	
	/** Qualified name of DB2 JDBC driver class. */
	static final String DB2_DRIVER = "com.ibm.db2.jcc.DB2Driver";

	/**
	 * Private Constructor.
	 */
	private GUITestUtils() {
	}

	/**
	 * Returns a FEST {@code FrameFixture} which wraps a running ORCHESTRA
	 * {#code MainFrm}. ORCHESTRA is launched with the 'schema' property set to
	 * {@code schema} and an appropriate value of 'workdir' set on the command
	 * line, overriding the values of these two properties set in
	 * global.properties or local.properties.
	 * 
	 * @param schema
	 *            the name of the schema (without the '.schema' extension).
	 * @param testClass
	 *            the test {@code Class} which will be using the fixture.
	 * @param robot
	 *            testClass' robot.
	 * 
	 * @return a {@code FrameFixture} of a running ORCHESTRA session.
	 */
	public static FrameFixture launchOrchestra(String schema,
			Class<?> testClass, Robot robot) {
		ApplicationLauncher.application(MainFrm.class).withArgs(
				"-schema=" + schema,
				"-workdir=" + OrchestraUtil.getWorkingDirectory(schema, testClass)).start();
		FrameFixture fixture = WindowFinder.findFrame(MainFrm.class).using(
				robot);
		return fixture;
	}

	/**
	 * Returns a FEST {@code FrameFixture} which wraps a running ORCHESTRA
	 * {#code MainFrm}. 
	 * 
	 * @param robot testClass' robot.
	 * 
	 * @return a {@code FrameFixture} of a running ORCHESTRA session.
	 */
	public static FrameFixture launchOrchestra(Robot robot) {
		ApplicationLauncher.application(MainFrm.class).start();
		FrameFixture fixture = WindowFinder.findFrame(MainFrm.class).using(
				robot);
		return fixture;
	}
	
	/**
	 * Returns a FEST {@code FrameFixture} which wraps a running ORCHESTRA
	 * {#code MainFrm}. 
	 * 
	 * @param robot testClass' robot.
	 * @param frameName the name of the frame which will be launched.
	 * 
	 * @return a {@code FrameFixture} of a running ORCHESTRA session.
	 */
	public static FrameFixture launchOrchestra(Robot robot, String frameName) {
		ApplicationLauncher.application(MainFrm.class).start();
		FrameFixture fixture = WindowFinder.findFrame(frameName).using(
				robot);
		return fixture;
	}
	
	/**
	 * This will connect to the in-memory HSQL DB {@code dbName} and issue
	 * {@code CREATE SCHEMA} commands for each schema named in {@code
	 * schemaNames}.
	 * 
	 * <p>
	 * The HSQLDB (unlike DB2?) does not seem to handle implicitly created DB
	 * schemas, so if A does not already exist, things like {@code CREATE TABLE
	 * A.B} ... fail instead of causing A to be created.
	 * </p>
	 * 
	 * @param dbName the name of the database.
	 * @param schemaNames the names of the schemas which need to be created.
	 * 
	 * @throws ClassNotFoundException if the HSQL JDBC driver class cannot be found.
	 * @throws SQLException if there are SQL problems. 
	 */
	public static void createSchemasForHSQLDB(String dbName,
			String[] schemaNames) throws ClassNotFoundException, SQLException {
		Class.forName(HSQL_DRIVER);

		Connection conn = DriverManager.getConnection("jdbc:hsqldb:mem:"
				+ dbName, "sa", "");
		Statement createSchema = conn.createStatement();
		for (String schema : schemaNames) {
			createSchema.executeUpdate("CREATE SCHEMA " + schema
					+ " AUTHORIZATION dba");
		}
	}
}
