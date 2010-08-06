/*
 * Copyright (C) 2009 Trustees of the University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.cis.orchestra.sql;

/**
 * {@code edu.upenn.cis.orchestra.sql} utilities.
 * 
 * @author Sam Donnelly
 */
public class SqlUtil {

	/** Backing class. */
	private static final ISqlUtil _sqlUtils = SqlFactories.getSqlFactory()
			.newSqlUtils();

	/**
	 * See {@link ISqlUtil#normalizeSqlStatement(String)}.
	 * 
	 * @param sqlStatement
	 *            see {@link ISqlUtil#normalizeSqlStatement(String)}
	 * 
	 * @return see {@link ISqlUtil#normalizeSqlStatement(String)}
	 */
	public static String normalizeSqlStatement(String sqlStatement) {
		return _sqlUtils.normalizeSqlStatement(sqlStatement);
	}

	/**
	 * See {@link ISqlUtil#normalizeSqlFragment(String)}.
	 * 
	 * @param sqlFragment
	 *            see {@link ISqlUtil#normalizeSqlFragment(String)}
	 * 
	 * @return see {@link ISqlUtil#normalizeSqlFragment(String)}
	 */
	public static String normalizeSqlFragment(String sqlFragment) {
		return _sqlUtils.normalizeSqlFragment(sqlFragment);
	}

	/** Prevent inheritance and instantiation. */
	private SqlUtil() {
	}
}
