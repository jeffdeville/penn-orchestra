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

import edu.upenn.cis.orchestra.sql.dtp.DtpSqlFactory;

/**
 * {@code ISQLFactory} factory.
 * 
 * @author Sam Donnelly
 */
public class SqlFactories {

	/** The wrapped abstract factory. */
	private static ISqlFactory _sqlFactory;

	/**
	 * Get the default {@code ISqlFactory}.
	 * 
	 * @return the default {@code ISqlFactory}.
	 */
	public static ISqlFactory getSqlFactory() {
		if (_sqlFactory == null) {
			// NOTE: this introduces a circular dependency between the
			// {@code sql} and {@code sql.dtp} packages:
			_sqlFactory = new DtpSqlFactory();
		}
		return _sqlFactory;
	}

	/** Prevent inheritance and instantiation. */
	private SqlFactories() {}
}
