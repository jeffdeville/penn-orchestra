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
 * An SQL {@code INSERT} statement.
 * 
 * @author Sam Donnelly
 */
public interface ISqlInsert extends ISqlStatement {

	/**
	 * Specify the {@code VALUES} part or SQL sub-query of the {@code INSERT}
	 * statement.
	 * <p>
	 * Previous {@code ISqlExp}s are overwritten with {@code e}.
	 * 
	 * @param e an SQL expression or a {@code SELECT} statement.
	 *            <p>
	 *            If it is a list of SQL expressions, {@code e} should be
	 *            represented by <em>one</em> SQL expression with operator
	 *            {@code ISqlExpression.COMMA} and operands the expressions in
	 *            the list.
	 *            <p>
	 *            If it is a {@code SELECT} statement, <code>e</code> should be
	 *            an <code>ISqlSelect</code> object.
	 * 
	 * @return this {@code ISqlInsert}.
	 */
	ISqlInsert addValueSpec(ISqlExp e);

	/**
	 * Specify the target columns for this insert.
	 * 
	 * @param columnNames a list of column names into which the values should be
	 *            inserted. Each constant must have type {@code
	 *            ISqlConstant.Type.COLUMNNAME}.
	 * @return this {@code ISqlInsert}
	 */
	ISqlInsert addTargetColumns(List<? extends ISqlConstant> columnNames);

}
