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
 * An SQL {@code SELECT} statement.
 * 
 * @author Sam Donnelly
 */
public interface ISqlSelect extends ISqlExp, ISqlStatement {

	/**
	 * Add in the {@code SELECT column_name(s)} part of the {@code SELECT}
	 * statement. Overwrites whatever was already there.
	 * 
	 * @param selectClause the {@code SELECT column_name(s)} part
	 */
	void addSelectClause(List<? extends ISqlSelectItem> selectClause);

	/**
	 * Add in the <code>FROM table(s)</code> part of the <code>SELECT</code>
	 * statement. Overwrites whatever was already there.
	 * 
	 * @param fromClause the <code>FROM table(s)</code> part
	 */
	void addFromClause(List<? extends ISqlFromItem> fromClause);

	/**
	 * Insert a <code>WHERE</code> clause.
	 * 
	 * @param whereClause an SQL Expression
	 */
	void addWhere(ISqlExp whereClause);

	/**
	 * Insert a <code>SET</code> clause (generally <code>UNION</code>,
	 * <code>INTERSECT</code>, or <code>MINUS</code>).
	 * 
	 * @param setClause an SQL Expression (generally <code>UNION</code>,
	 *            <code>INTERSECT</code>, or <code>MINUS</code>)
	 */
	void addSet(ISqlExpression setClause);

	/**
	 * Get the <code>SELECT</code> part of the statement.
	 * 
	 * @return a {@code List<ISqlSelectItem>} which is the <code>SELECT</code>
	 *         part of this <code>ISqlSelect</code>
	 */
	List<ISqlSelectItem> getSelect();

	/**
	 * Get the <code>FROM</code> part of the statement.
	 * 
	 * @return a <code>List</code> of <code>ISqlFromItem</code>s
	 */
	List<ISqlFromItem> getFrom();

	/**
	 * Get the <code>WHERE</code> part of the statement.
	 * 
	 * @return an SQL Expression or sub-query (an <code>ISqlExpression</code> or
	 *         <code>ISqlSelect</code>
	 */
	ISqlExp getWhere();

	/**
	 * If <code>distinct == true</code>, make this a
	 * <code>SELECT DISTINCT...</code> statement. Otherwise, make it an un
	 * <code>DISTINCT</code> statement.
	 * 
	 * @param distinct see description
	 */
	void setDistinct(boolean distinct);

	/**
	 * Get the {@code GROUP BY} part of this {@code SELECT} statement.
	 * 
	 * @return the {@code GROUP BY} part of this {@code SELECT} statement
	 */
	ISqlGroupBy getGroupBy();

	/**
	 * Add in the <code>ORDER BY column(s)</code> part of the
	 * <code>SELECT</code> statement. Overwrites whatever was already there.
	 * 
	 * @param orderBy the <code>ORDER BY column(s)</code> part
	 * @return this {@code ISqlSelect}
	 *            
	 */
	ISqlSelect addOrderBy(final List<? extends ISqlOrderByItem> orderBy);

}
