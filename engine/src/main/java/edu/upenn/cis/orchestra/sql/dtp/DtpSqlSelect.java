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

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;
import static edu.upenn.cis.orchestra.sql.dtp.SqlDtpUtil.getSQLQueryParserFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import org.eclipse.datatools.modelbase.sql.query.QueryCombined;
import org.eclipse.datatools.modelbase.sql.query.QueryResultSpecification;
import org.eclipse.datatools.modelbase.sql.query.QuerySearchCondition;
import org.eclipse.datatools.modelbase.sql.query.QuerySelect;
import org.eclipse.datatools.modelbase.sql.query.QuerySelectStatement;
import org.eclipse.datatools.modelbase.sql.query.TableReference;

import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlExpression;
import edu.upenn.cis.orchestra.sql.ISqlFromItem;
import edu.upenn.cis.orchestra.sql.ISqlGroupBy;
import edu.upenn.cis.orchestra.sql.ISqlOrderByItem;
import edu.upenn.cis.orchestra.sql.ISqlSelect;
import edu.upenn.cis.orchestra.sql.ISqlSelectItem;

/**
 * An SQL {@code SELECT} statement implemented with DTP classes.
 * <p>
 * Intentionally package-private: only other DTP-backed {@code
 * edu.upenn.cis.orchestra.sql} objects should have access to this object.
 * 
 * @author Sam Donnelly
 */
class DtpSqlSelect extends AbstractSQLQueryObject<QuerySelectStatement>
		implements ISqlSelect {

	/** Backing DTP object. */
	private final QuerySelectStatement _querySelectStatement;

	/**
	 * Create a new <code>SELECT</code> statement.
	 */
	DtpSqlSelect() {
		_querySelectStatement = getSQLQueryParserFactory()
				.createSelectStatement(
						getSQLQueryParserFactory().createQuerySelect(null,
								null, null, null, null, null), (List<?>) null);
	}

	/**
	 * Create a new {@code ISqlSelect} with {@code SELECT} and <code>FROM</code>
	 * clauses.
	 * 
	 * @param selectItem <code>SELECT ...</code> part.
	 * @param fromItem <code>FROM ...</code> part.
	 */
	DtpSqlSelect(final ISqlSelectItem selectItem, final ISqlFromItem fromItem) {
		this();
		addSelectClause(newArrayList(selectItem));
		addFromClause(newArrayList(fromItem));
	}

	/**
	 * Create a new {@code ISqlSelect} with {@code SELECT}, <code>FROM</code>,
	 * and <code>WHERE</code> clauses.
	 * 
	 * @param selectItem <code>SELECT ...</code> part.
	 * @param fromItem <code>FROM ...</code> part.
	 * @param whereItem <code>WHERE ...</code> part.
	 */
	DtpSqlSelect(final ISqlSelectItem selectItem, final ISqlFromItem fromItem,
			final ISqlExpression whereItem) {
		this(selectItem, fromItem);
		addWhere(whereItem);
	}

	/**
	 * Construct a {@code DtpSqlSelect} from a {@code QuerySelectStatement}.
	 * 
	 * @param selectStatement source.
	 */
	DtpSqlSelect(final QuerySelectStatement selectStatement) {
		_querySelectStatement = selectStatement;
	}

	/** {@inheritDoc} */
	@Override
	public void addWhere(final ISqlExp where) {
		final QuerySelect querySelect = (QuerySelect) _querySelectStatement
				.getQueryExpr().getQuery();
		if (where == null) {
			querySelect.setWhereClause(null);
		} else {
			@SuppressWarnings("unchecked")
			final ISQLQueryObject<QuerySearchCondition> expressionQuerySearchCondition = (ISQLQueryObject<QuerySearchCondition>) where;
			querySelect
					.setWhereClause(getSQLQueryParserFactory()
							.createNestedCondition(
									expressionQuerySearchCondition
											.getSQLQueryObject()));
		}
	}

	/** {@inheritDoc} */
	@Override
	public ISqlExp getWhere() {
		final QuerySelect querySelect = (QuerySelect) _querySelectStatement
				.getQueryExpr().getQuery();
		return AbstractDtpSqlExpression.newSqlExpression(querySelect
				.getWhereClause());
	}

	/** {@inheritDoc} */
	@Override
	public void addSet(final ISqlExpression s) {
		@SuppressWarnings("unchecked")
		final ISQLQueryObject<QueryCombined> queryCombinedSqlExpression = (ISQLQueryObject<QueryCombined>) s;
		final QueryCombined queryCombined = queryCombinedSqlExpression
				.getSQLQueryObject();
		queryCombined.setLeftQuery(_querySelectStatement.getQueryExpr()
				.getQuery());
		_querySelectStatement.setQueryExpr(getSQLQueryParserFactory()
				.createQueryExpressionRoot(queryCombined, null));
	}

	/** {@inheritDoc} */
	@Override
	public List<ISqlSelectItem> getSelect() {
		@SuppressWarnings("unchecked")
		final List<Object> queryResultSpecifications = ((QuerySelect) _querySelectStatement
				.getQueryExpr().getQuery()).getSelectClause();
		// If it's null or empty then it's a '*'. This semantics is taken from
		// org.eclipse.datatools.modelbase.sql.query.util.SQLQuerySourceWriter
		if (queryResultSpecifications == null
				|| queryResultSpecifications.isEmpty()) {
			return new Vector<ISqlSelectItem>(Arrays
					.asList(new DtpSqlSelectItem("*")));
		}
		final List<ISqlSelectItem> sqlSelectItems = newArrayList();

		for (final Object o : queryResultSpecifications) {
			final QueryResultSpecification queryResultSpecification = (QueryResultSpecification) o;
			sqlSelectItems.add(new DtpSqlSelectItem(queryResultSpecification));
		}
		return sqlSelectItems;
	}

	/** {@inheritDoc} */
	@Override
	public List<ISqlFromItem> getFrom() {
		@SuppressWarnings("unchecked")
		final List<Object> tableReferences = ((QuerySelect) _querySelectStatement
				.getQueryExpr().getQuery()).getFromClause();
		final List<ISqlFromItem> sqlFromItems = newArrayList();
		for (final Object o : tableReferences) {
			TableReference tableReference = (TableReference) o;
			sqlFromItems.add(new DtpSqlFromItem(tableReference));
		}
		return sqlFromItems;
	}

	/** {@inheritDoc} */
	@Override
	public void setDistinct(final boolean distinct) {
		((QuerySelect) _querySelectStatement.getQueryExpr().getQuery())
				.setDistinct(distinct);
	}

	/** {@inheritDoc} */
	@Override
	public void addSelectClause(final List<? extends ISqlSelectItem> selectItems) {
		for (final ISqlSelectItem selectItem : selectItems) {
			@SuppressWarnings("unchecked")
			final ISQLQueryObject<QueryResultSpecification> queryResultSpecSelectItem = (ISQLQueryObject<QueryResultSpecification>) selectItem;

			// Not sure if there's a point to using
			// createSelectClause() vs. just adding queryResultSpec
			// to querySelect.getSelectClause().
			getSQLQueryParserFactory().createSelectClause(
					((QuerySelect) _querySelectStatement.getQueryExpr()
							.getQuery()).getSelectClause(),
					queryResultSpecSelectItem.getSQLQueryObject());
		}
	}

	/** {@inheritDoc} */
	@Override
	public void addFromClause(final List<? extends ISqlFromItem> fromItems) {
		((QuerySelect) _querySelectStatement.getQueryExpr()
				.getQuery()).getFromClause().clear();
		for (final ISqlFromItem fromItem : fromItems) {
			@SuppressWarnings("unchecked")
			final ISQLQueryObject<TableReference> tableReferenceFromItem = (ISQLQueryObject<TableReference>) fromItem;

			// Not sure if there's a point to using createFromClause() vs.
			// just adding queryResultSpec to querySelect.getFromClause().
			getSQLQueryParserFactory().createFromClause(
					((QuerySelect) _querySelectStatement.getQueryExpr()
							.getQuery()).getFromClause(),
					tableReferenceFromItem.getSQLQueryObject());
		}
	}

	/** {@inheritDoc} */
	@Override
	public QuerySelectStatement getSQLQueryObject() {
		return _querySelectStatement;
	}

	/**
	 * This method is not yet implemented.
	 * 
	 * @throws UnsupportedOperationException if this method is called
	 * @returns nothing
	 */
	@Override
	public ISqlGroupBy getGroupBy() {
		throw new UnsupportedOperationException("not yet implemented");
	}

	/** {@inheritDoc} */
	public ISqlSelect addOrderBy(final List<? extends ISqlOrderByItem> orderBy) {
		_querySelectStatement.getOrderByClause().clear();
		for (final ISqlOrderByItem orderByItem : orderBy) {
			final DtpSqlOrderByItem dtpOrderByItem = (DtpSqlOrderByItem) orderByItem;

			getSQLQueryParserFactory().createOrderByClause(
					_querySelectStatement.getOrderByClause(),
					dtpOrderByItem.getSQLQueryObject());
		}
		return this;
	}

	public boolean isBoolean() { return false; }

	public ISqlExp addOperand(ISqlExp e) {
		return null;
	}
	
	public ISqlExp getOperand(int i) {
		return null;
	}
	
	public List<ISqlExp> getOperands() {
		return new ArrayList<ISqlExp>();
	}
}
