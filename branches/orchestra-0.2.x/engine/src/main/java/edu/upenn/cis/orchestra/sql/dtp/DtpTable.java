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

import static edu.upenn.cis.orchestra.sql.dtp.SqlDtpUtil.getSQLQueryParserFactory;

import org.eclipse.datatools.modelbase.sql.query.TableInDatabase;

import edu.upenn.cis.orchestra.sql.DotSeparatedValues;
import edu.upenn.cis.orchestra.sql.IIndexedStringValues;
import edu.upenn.cis.orchestra.sql.ITable;

/**
 * A DTP-backed {@code ITable}.
 * <p>
 * Intentionally package-private.
 * 
 * @author Sam Donnelly
 */
class DtpTable extends AbstractSQLQueryObject<TableInDatabase> implements
		ITable {

	/** Wrapped DTP object. */
	private final TableInDatabase _tableInDatabase;

	/**
	 * Construct a {@code DtpTable} from a {@code TableReference}.
	 * 
	 * @param tableInDatabase from which we construct this {@code DtpTable}
	 */
	DtpTable(final TableInDatabase tableInDatabase) {
		_tableInDatabase = tableInDatabase;
	}

	/**
	 * Create a {@code DtpTable} from a {@code [schema.]table} string.
	 * 
	 * @param table a {@code [schema.]table} string
	 */
	DtpTable(final String table) {
		IIndexedStringValues indexedStringValues = new DotSeparatedValues(
				table, 2);
		_tableInDatabase = getSQLQueryParserFactory().createSimpleTable(
				indexedStringValues.get(1), indexedStringValues.get(0));
	}

	/**
	 * Create a new {@code ITable}.
	 * 
	 * @param tableName the name of the new table. Must be in the form {@code
	 *            [schema.]table}.
	 * @param alias alias for the table
	 */
	public DtpTable(final String tableName, final String alias) {
		this(tableName);
		_tableInDatabase.setTableCorrelation(getSQLQueryParserFactory()
				.createTableCorrelation(alias));
	}

	/** {@inheritDoc} */
	@Override
	public String getSchemaName() {
		if (_tableInDatabase.getDatabaseTable() != null) {
			return _tableInDatabase.getDatabaseTable().getSchema().getName();
		} else {
			return null;
		}
	}

	/** {@inheritDoc} */
	@Override
	public String getName() {
		return _tableInDatabase.getName();
	}

	/** {@inheritDoc} */
	@Override
	public TableInDatabase getSQLQueryObject() {
		return _tableInDatabase;
	}
}
