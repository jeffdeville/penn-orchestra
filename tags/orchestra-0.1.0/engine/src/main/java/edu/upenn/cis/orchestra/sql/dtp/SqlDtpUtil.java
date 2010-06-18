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
package edu.upenn.cis.orchestra.sql.dtp;

import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParserFactory;

/**
 * {@code edu.upenn.cis.orchestra.sql.dtp} utility class.
 * 
 * @author Sam Donnelly
 */
class SqlDtpUtil {

	/** For creating DTP SQL objects. */
	private static SQLQueryParserFactory _queryParserFactory;

	/**
	 * Get the {@code SQLQueryParserFactory} which we use for creating DTP AST
	 * objects.
	 * 
	 * @return the <code>SQLQueryParserFactory</code>
	 */
	static SQLQueryParserFactory getSQLQueryParserFactory() {
		if (null == _queryParserFactory) {
			_queryParserFactory = new SQLQueryParserFactory();
		}
		return _queryParserFactory;
	}

	/**
	 * Prevent inheritance and instantiation.
	 * 
	 * @throws AssertionError always
	 */
	private SqlDtpUtil() {
		throw new AssertionError("Can't instantiate an SqlDtpUtil");
	}
}
