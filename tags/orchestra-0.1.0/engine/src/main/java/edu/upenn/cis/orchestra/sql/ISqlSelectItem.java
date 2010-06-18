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
 * An item in the <code>SELECT</code> part of an SQL query. (The
 * <code>SELECT</code> part of a query is a {@code List} of
 * <code>ISqlSelectItem</code>.) In other words, in the {@code SELECT x1,
 * x2,..., xn} part of a {@code SELECT} statement, this is the <code>x</code>s.
 * <p>
 * For backward compatibility with ZQL, this interface takes
 * non-Liskov-Substitution-Principle liberties with some of the {@code
 * ISqlAliasedName}-defined methods: see the documentation for {@code
 * setAlias()}, {@code getColumn()}, and {@code getTable()}.
 * <p>
 * {@code setAlias()} is <em>not</em> backward compatible with ZQL: ZQL allowed
 * the alias to be set on a wildcard {@code SELECT} item, {@code ISqlSelectItem}
 * does not.
 * 
 * @author Sam Donnelly
 */
public interface ISqlSelectItem extends ISqlAliasedName {

	/**
	 * {@inheritDoc}
	 * 
	 * @throws IllegalStateException
	 *             if this {@code ISqlSelectItem} is a wildcard. Note: this
	 *             behavior is <em>not</em> backward compatible and violates the
	 *             LSP.
	 */
	@Override
	ISqlSelectItem setAlias(String alias);

	/**
	 * {@inheritDoc}
	 * <p>
	 * Always returns {@code null} if this {@code ISqlSelectItem} is a wildcard.
	 * 
	 * @return the alias associated to the current name, or {@code null} if this
	 *         {@code ISqlSelectItem} is a wildcard
	 */
	@Override
	String getAlias();

	/**
	 * {@inheritDoc}
	 * <p>
	 * Always returns {@code null} if this {@code ISqlSelectItem} is a udf or
	 * cast expression. This violates the LSP.
	 * 
	 * @return the table associated with this {@code ISqlSelectItem} or {@code
	 *         null} if there is no such table
	 */
	@Override
	String getTable();

	/**
	 * {@inheritDoc}
	 * <p>
	 * If this {@code ISqlSelectItem} is a udf or cast expression, then this
	 * method is equivalent to {@code toString()}. This violates the LSP.
	 * 
	 * @return {@inheritDoc}
	 *         <p>
	 *         if this {@code ISqlSelectItem} is a udf or cast expression, then
	 *         this method is equivalent to {@code toString()}
	 */
	@Override
	String getColumn();

	/**
	 * Return an {@code ISqlExpression} if this {@code SELECT} item is an
	 * expression, an {@code ISqlConstant} if it is a column name, {@code null}
	 * if it is a wildcard.
	 * 
	 * @return an {@code ISqlExpression} if this {@code SELECT} item is an
	 *         expression, an {@code ISqlConstant} if it is a column name,
	 *         {@code null} if it is a wildcard
	 */
	ISqlExp getExpression();

	/**
	 * Initialize this SELECT item as an SQL expression (not a column name nor
	 * wildcard) Example: SELECT a+b FROM table1; (a+b is an expression)
	 * 
	 * @param expression
	 * @return this
	 */
	ISqlSelectItem setExpression(ISqlExp expression);

}