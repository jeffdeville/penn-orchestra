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
 * An {@code ISqlMove}.
 * 
 * @author Sam Donnelly
 */
class SqlMove implements ISqlMove {

	/** For making {@code SQL} objects. */
	private static final ISqlFactory _sqlFactory = SqlFactories.getSqlFactory();

	/**
	 * {@code true} means
	 * <ol>
	 * <li>{@code DELETE FROM _destTable}
	 * <li>{@code INSERT INTO _destTable SELECT * FROM _sourceTable}
	 * <li>{@code DELETE FROM _sourceTable}
	 * </ol>
	 * <p>
	 * {@code false} means
	 * <ol>
	 * <li>{@code DROP _destTable}
	 * <li>{@code RENAME _sourceTable TO _destTable}
	 * <li>{@code CREATE _sourceTable}
	 * </ol>
	 * </ol>
	 */
	private final boolean _soft;

	/** Source table. */
	private final ITable _sourceTable;

	/** Destination table. */
	private final ITable _destTable;

	/**
	 * Move all rows from {@code source} into {@code dest}.
	 * 
	 * @param dest destination table
	 * @param source source table
	 * @param soft if {@code true} then do a {@code DROP dest}
	 */
	SqlMove(final String dest, final String source, final boolean soft) {
		_sourceTable = _sqlFactory.newTable(source);
		_destTable = _sqlFactory.newTable(dest);
		_soft = soft;
	}

	/** {@inheritDoc} */
	@Override
	public List<String> toStringList() {
		final List<String> ret = newArrayList();

		if (_soft) {
			// DELETE FROM _destTable
			final ISqlDelete deleteDestTable = _sqlFactory
					.newDelete(_destTable);

			// INSERT INTO _destTable SELECT * FROM _sourceTable
			final ISqlInsert insertIntoOldTable = _sqlFactory
					.newInsert(_destTable);
			insertIntoOldTable.addValueSpec(_sqlFactory.newSelect(
					_sqlFactory.newSelectItem("*"), _sqlFactory
							.newFromItem(_sourceTable.toString())));

			// DELETE FROM _sourceTable
			final ISqlDelete deleteSourceTable = _sqlFactory
					.newDelete(_sourceTable);

			ret.add(deleteDestTable.toString());
			ret.add(insertIntoOldTable.toString());
			ret.add(deleteSourceTable.toString());
		} else {
			// DROP OLD_TABLE
			final ISqlDrop dr = _sqlFactory.newDrop(_destTable.toString());

			// RENAME TABLE NEW_TABLE TO OLD_TABLE
			final ISqlRename renameSrcToDest = _sqlFactory.newRename(
					_sourceTable, _destTable);

			// CREATE source_table
			final ISqlSelectItem star = _sqlFactory.newSelectItem("*");
			final ISqlFromItem fr = _sqlFactory.newFromItem(_destTable
					.toString());
			final ISqlExpression w = AbstractSqlExpression.falseExp();
			final ISqlSelect cq = _sqlFactory.newSelect(star, fr, w);
			final ISqlCreateTable createSrcTable = _sqlFactory
					.newCreateTable(_sourceTable.toString(), cq);

			ret.add(dr.toString());
			ret.add(renameSrcToDest.toString());
			ret.add(createSrcTable.toString());
		}
		return ret;
	}

	/**
	 * Because this object generates a list of statements and not a single
	 * statement, this method is not implemented - call {@code toStringList()}
	 * instead.
	 * 
	 * @throws UnsupportedOperationException always
	 * @return nothing
	 */
	@Override
	public String toString() {
		throw new UnsupportedOperationException(
				"This method is not implemented for ISqlMove - call ISqlMove.toStringList() instead.");
	}
}
