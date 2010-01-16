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

import java.util.List;

/**
 * Manufactures {@code edu.upenn.orchestra.cis} SQL objects.
 * 
 * @author Sam Donnelly
 */
public abstract class AbstractSqlFactory implements ISqlFactory {

	/** {@inheritDoc} */
	@Override
	public ISqlAlter newAlter(final String table) {
		return new SqlAlter(table);
	}

	/** {@inheritDoc} */
	@Override
	public ISqlCreateTable newSqlCreateTable(final String table,
			final String type, final List<? extends ISqlColumnDef> columnDefs,
			final String noLogging) {
		return new SqlCreateTable(table, type, columnDefs, noLogging);
	}

	/** {@inheritDoc} */
	@Override
	public ISqlCreateTable newSqlCreateTable(final String table,
			final List<? extends ISqlColumnDef> columnDefs) {
		return new SqlCreateTable(table, columnDefs);
	}

	/** {@inheritDoc} */
	@Override
	public ISqlCreateTable newSqlCreateTable(final String table,
			final ISqlSelect asSelect) {
		return new SqlCreateTable(table, asSelect);
	}

	/** {@inheritDoc} */
	@Override
	public ISqlCreateTempTable newSqlCreateTempTable(String name, String type,
			List<? extends ISqlColumnDef> cols) {
		return new SqlCreateTempTable(name, type, cols);
	}

	/** {@inheritDoc} */
	@Override
	public ISqlColumnDef newColumnDef(final String name, final String type) {
		return new SqlColumnDef(name, type);
	}

	/** {@inheritDoc} */
	@Override
	public ISqlColumnDef newSqlColumnDef(final String name, final String type,
			final String defaultValue) {
		return new SqlColumnDef(name, type, defaultValue);
	}

	/** {@inheritDoc} */
	@Override
	public ISqlDrop newSqlDrop(final String table) {
		return new SqlDrop(table);
	}

	/** {@inheritDoc} */
	@Override
	public ISqlDropSchema newSqlDropSchema(final String schema) {
		return new SqlDropSchema(schema);
	}

	/** {@inheritDoc} */
	@Override
	public ISqlCreateIndex newSqlCreateIndex(final String indName,
			final String tabName, final List<? extends ISqlColumnDef> columns) {
		return new SqlCreateIndex(indName, tabName, columns);
	}

	/** {@inheritDoc} */
	@Override
	public ISqlMove newSqlMove(final String dest, final String source,
			final boolean soft) {
		return new SqlMove(dest, source, soft);
	}

	/** {@inheritDoc} */
	@Override
	public ISqlRename newSqlRename(final ITable source, final ITable destination) {
		return new SqlRename(source, destination);
	}

}
