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

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;

import java.util.List;

/**
 * A simple container implementation of an {@code ISqlCreate}.
 * <p>
 * Intentionally package-private.
 * 
 * @author gkarvoun
 * @author Sam Donnelly
 */
class SqlCreateTable implements ISqlCreateTable {

	/** The name of the table, in {@code "[schema.]table"}. */
	private final String _name;

	/** In case this is a {@code CREATE TABLE AS}. */
	private final ISqlSelect _asQuery;

	/** The {@code NOT LOGGED INITIALLY} parameter. */
	// Is this DB2 specific?
	private final String _noLogging;

	/**
	 * For a {@code CREATE TEMPORARY TABLE} or {@code CREATE GLOBAL TEMPORARY
	 * TABLE} this is the {@code "TEMPORARY"} or {@code "GLOBAL TEMPORARY"}
	 * part.
	 */
	private final String _type;

	/** The columns for the table we are creating. */
	private final List<ISqlColumnDef> _columns;

	/**
	 * Construct an{@code SqlCreateTable}: {@code
	 * "CREATE type TABLE name (columns) noLogMsg"}.
	 * 
	 * @param name the name of the table, in {@code "[schema.]table"} form.
	 * @param type the type of table, for example {@code "TEMPORARY"} or {@code
	 *            "GLOBAL TEMPORARY"}.
	 * @param columns the columns of the table we're creating
	 * @param noLogMsg no logging string, for example {@code
	 *            "NOT LOGGED INITIALLY"}.
	 */
	SqlCreateTable(final String name, final String type,
			final List<? extends ISqlColumnDef> columns, final String noLogMsg) {
		_name = name;
		_asQuery = null;
		if (type != "")
			_type = type + " ";
		else
			_type = type;
		_noLogging = noLogMsg;
		_columns = newArrayList(columns);
	}
	
	/**
	 * Construct an {@code SqlCreateTable}: {@code
	 * "CREATE TABLE name (columns)"}.
	 * 
	 * @param name the name of the table, in {@code "[schema.]table"} form.
	 * @param columns the columns of the table we're creating
	 * 
	 */
	SqlCreateTable(final String name, final List<? extends ISqlColumnDef> columns) {
		_name = name;
		_asQuery = null;
		_type = "";
		_noLogging = null;
		_columns = newArrayList(columns);
	}
	
	/**
	 * For a {@code CREATE TABLE AS} statement.
	 * 
	 * @param name the name of the table, in {@code "[schema.]table"} form
	 * @param asQuery the {@code SELECT} part of the {@code CREATE TABLE AS}
	 */
	SqlCreateTable(final String name, final ISqlSelect asQuery) {
		_name = name;
		_asQuery = asQuery;
		_type = "";
		_noLogging = null;
		_columns = newArrayList();
	}

	/** {@inheritDoc} */
	@Override
	public String toString() {
		if (_asQuery == null) {
			StringBuffer cols = new StringBuffer();
			for (int i = 0; i < _columns.size(); i++) {
				if (i > 0) {
					cols.append(", ");
				}
				cols.append(_columns.get(i).toString());
			}
			if (_noLogging != null)
				return "CREATE " + _type + "TABLE " + _name + "(" + cols + ")"
						+ _noLogging;
			else
				return "CREATE " + _type + "TABLE " + _name + "(" + cols + ")";
		} else {
			if (_noLogging != null)
				// Is this DB2 specific?
				return "CREATE " + _type + "TABLE " + _name + " as ("
						+ _asQuery.toString() + ") DEFINITION ONLY"
						+ _noLogging;
			else
				return "CREATE " + _type + "TABLE " + _name + " as ("
						+ _asQuery.toString() + ") DEFINITION ONLY";
		}
	}
}
