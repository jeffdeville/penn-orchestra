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
 * Simple {@code ISqlCreateIndex}.
 * 
 * @author Sam Donnelly
 */
class SqlCreateIndex implements ISqlCreateIndex {

	/** The name of the index we are creating. */
	private final String _indexName;

	/** The name of the table on which the index is being created. */
	private final String _tableName;

	/** The columns that make up the index. */
	private final List<ISqlColumnDef> _columns;

	/**
	 * Create a new {@code CREATE INDEX} statement.
	 * <p>
	 * As in: {@code CREATE INDEX indexName on tableName (columns.get(0),
	 * columns.get(1),...,columns.get(N)}.
	 * 
	 * @param indexName the name of the index
	 * @param tableName the table on which we're creating the index
	 * @param columns the columns that make up the index
	 */
	public SqlCreateIndex(final String indexName, final String tableName,
			final List<? extends ISqlColumnDef> columns) {
		_indexName = indexName;
		_tableName = tableName;
		_columns = newArrayList(columns);
	}

	/** {@inheritDoc} */
	@Override
	public String toString() {
		final StringBuffer cols = new StringBuffer();
		for (int i = 0; i < _columns.size(); i++) {
			if (i > 0) {
				cols.append(", ");
			}
			cols.append(_columns.get(i).toString());
		}
		// String stmt = "CREATE " + _cluster + " INDEX " + indexName + " ON "
		String stmt = "CREATE INDEX " + _indexName + " ON " + _tableName + " ("
				+ cols + ")";

		return stmt;
	}
}
