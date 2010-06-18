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

import edu.upenn.cis.orchestra.sql.ISqlFromItem.Join;

/**
 * Abstract Factory for making <code>edu.upenn.cis.orchestra.sql</code> objects.
 * All implementations should be thread safe.
 * 
 * @author Sam Donnelly
 */
public interface ISqlFactory {

	/**
	 * Make a {@code IColumnExpression} with an optionally {@code
	 * [schema.]table} qualified column name.
	 * 
	 * @param column
	 *            an optionally-qualified column name in the form {@code
	 *            [[schema.]table.]column}
	 * @return the new {@code IColumnExpression}
	 */
	IColumnExpression newColumnExpression(String column);

	/**
	 * Create a {@code ALTER TABLE table} statement.
	 * <p>
	 * {@code table} is passed through to {@code toString()} SQL generation as
	 * free text.
	 * 
	 * @param table
	 *            target of the {@code ALTER TABLE}
	 * @return a new {@code ALTER TABLE table} statement
	 */
	ISqlAlter newAlter(String table);

	/**
	 * Create a column definition with a column name and type.
	 * <p>
	 * {@code type} will just be passed straight through to the create
	 * statement.
	 * 
	 * @param name
	 *            the name of the column.
	 * @param type
	 *            the type of column.
	 * @return the new {@code ISqlColumnDef}.
	 */
	ISqlColumnDef newColumnDef(String name, String type);

	/**
	 * Create a column definition with a column name, type, and default value.
	 * <p>
	 * {@code type} will just be passed straight through to the create
	 * statement.
	 * 
	 * @param name
	 *            the name of the column.
	 * @param type
	 *            the type of column.
	 * @param defaultValue
	 *            the default value of the column.
	 * @return the new {@code ISqlColumnDef}.
	 */
	ISqlColumnDef newColumnDef(String name, String type, String defaultValue);

	/**
	 * Create a new constant, given its value and type.
	 * 
	 * @param value
	 *            the value for the constant.
	 * @param type
	 *            the type of constant.
	 * @return the new constant.
	 */
	ISqlConstant newConstant(String value, ISqlConstant.Type type);

	/**
	 * Create a new {@code CREATE INDEX} statement.
	 * <p>
	 * As in: {@code CREATE INDEX indexName on tableName (columns.get(0),
	 * columns.get(1),...,columns.get(N)}.
	 * 
	 * @param indexName
	 *            the name of the index
	 * @param tableName
	 *            the table on which we're creating the index
	 * @param columns
	 *            the columns that make up the index
	 * 
	 * @return the new {@code CREATE INDEX} statement
	 */
	ISqlCreateIndex newCreateIndex(String indexName, String tableName,
			List<? extends ISqlColumnDef> columns);

	/**
	 * For a {@code CREATE TABLE AS} statement.
	 * <p>
	 * Example {@code create temp table cta4 as select * from cta1 where a > 1;}.
	 * 
	 * @param name
	 *            the name of the table, in {@code "[schema.]table"} form.
	 * @param asQuery
	 *            the {@code SELECT} part of the {@code CREATE TABLE AS}. the
	 *            new {@code ISqlCreate}.
	 * @return the new {@code ISqlCreate}.
	 */
	ISqlCreateTable newCreateTable(String name, ISqlSelect asQuery);

	/**
	 * Construct an{@code ISqlCreateTable}: {@code
	 * "CREATE type TABLE name (columns) noLogMsg"}.
	 * 
	 * @param name
	 *            the name of the table, in {@code "[schema.]table"} form.
	 * @param type
	 *            the type of table, for example {@code "TEMPORARY"} or {@code
	 *            "GLOBAL TEMPORARY"}.
	 * @param columns
	 *            the columns of the table we're creating.
	 * @param noLogMsg
	 *            no logging string, for example {@code "NOT LOGGED INITIALLY"}.
	 * @return the new {@code ISqlCreate}.
	 */
	ISqlCreateTable newCreateTable(String name, String type,
			List<? extends ISqlColumnDef> columns, String noLogMsg);

	/**
	 * Construct an{@code SqlCreateTable}: {@code "CREATE TABLE name (columns)"}
	 * .
	 * 
	 * @param name
	 *            the name of the table, in {@code "[schema.]table"} form.
	 * @param columns
	 *            the columns of the table we're creating.
	 * 
	 * @return the new {@code ISqlCreate}.
	 */
	ISqlCreateTable newSqlCreateTable(String name,
			List<? extends ISqlColumnDef> columns);

	/**
	 * Create a new {@code DELETE} statement.
	 * <p>
	 * As in: {@code DELETE table}.
	 * 
	 * @param table
	 *            the table to be deleted.
	 * @return the new {@code DELETE} statement
	 */
	ISqlDelete newDelete(ITable table);

	/**
	 * Construct an {@code SqlDelete} from the target table.
	 * 
	 * @param tableName
	 *            the name of the table in the {@code FROM} part of the {@code
	 *            DELETE} statement.
	 * @return an {@code SqlDelete} from the target table.
	 */
	ISqlDelete newSqlDelete(String tableName);

	/**
	 * Construct an {@code SqlDelete} who's {@code FROM} part has a table and an
	 * alias.
	 * <p>
	 * For example: {@code DELETE LBI FROM lightboxes.dbo.lightboxItem AS LBI
	 * where LBI.UserName = "jdoe";}.
	 * 
	 * 
	 * @param tableName
	 *            table name.
	 * @param alias
	 *            alias for the table.
	 * 
	 * @return an {@code SqlDelete} who's {@code FROM} part has a table and an
	 *         alias.
	 */
	ISqlDelete newDelete(String tableName, String alias);

	/**
	 * Create a new {@code DROP} statement.
	 * 
	 * @param table
	 *            the name of the table we're dropping.
	 * @return the new {@code DROP} statement.
	 */
	ISqlDrop newDrop(String table);

	/**
	 * Create a new {@code DROP SCHEMA} statement.
	 * 
	 * @param schema
	 *            the name of the schema we're dropping.
	 * @return the new {@code DROP SCHEMA} statement.
	 */
	ISqlDropSchema newDropSchema(String schema);

	/**
	 * Create an SQL Expression given the operator.
	 * 
	 * @param code
	 *            the type of <code>ISqlExpression</code>, see the
	 *            <code>static final int</code>s in <code>ISqlExpression</code>.
	 * 
	 * @return an SQL Expression given the operator.
	 */
	ISqlExpression newExpression(ISqlExpression.Code code);

	/**
	 * Create an SQL Expression given the operator and 1st operand.
	 * 
	 * @param code
	 *            the operator.
	 * @param o1
	 *            the 1st operand.
	 * 
	 * @return see description.
	 */
	ISqlExpression newExpression(ISqlExpression.Code code, ISqlExp o1);

	/**
	 * Create an SQL Expression given the operator, 1st and 2nd operands.
	 * 
	 * @param code
	 *            the operator.
	 * @param o1
	 *            the 1st operand.
	 * @param o2
	 *            the 2nd operand.
	 * 
	 * @return see description.
	 */
	ISqlExpression newExpression(ISqlExpression.Code code, ISqlExp o1,
			ISqlExp o2);

	/**
	 * Create an SQL Expression given the function name and 1st operand.
	 * 
	 * @param functionName
	 *            the name of the function
	 * @param o1
	 *            the 1st operand.
	 * 
	 * @return see description.
	 */
	ISqlExpression newExpression(String functionName, ISqlExp... o1);

	/**
	 * Construct a join-type {@code FROM} clause.
	 * 
	 * @param type
	 *            the type of join.
	 * @param left
	 *            the left side of the join.
	 * @param right
	 *            the right side of the join.
	 * @param cond
	 *            {@code WHERE} part of the {@code FROM} clause.
	 * @return the new {@code ISqlFromItem}.
	 */
	ISqlFromItem newFromItem(Join type, ISqlFromItem left,
			ISqlFromItem right, ISqlExp cond);

	/**
	 * Create a new {@code FROM} clause on a given table.
	 * <p>
	 * {@code fullName} can either be schema-unqualified, as in {@code SELECT *
	 * FROM t}, or schema-qualified, as in {@code SELECT * FROM s.t}.
	 * 
	 * @param tableName
	 *            the table name
	 * @return see description
	 * @throws IllegalArgumentException
	 *             if {@code tableName} is not the correct format
	 */
	ISqlFromItem newFromItem(String tableName);

	/**
	 * Create a new {@code ISqlInsert} object.
	 * 
	 * @param table
	 *            the table we're inserting into
	 * 
	 * @return a new <code>ISqlInsert</code> object
	 */
	ISqlInsert newInsert(ITable table);

	/**
	 * Create a new <code>ISqlInsert</code> object.
	 * 
	 * @param tableName
	 *            the name of the table we're inserting into
	 * 
	 * @return a new <code>ISqlInsert</code> object
	 */
	ISqlInsert newInsert(String tableName);

	/**
	 * Move the rows from the table {@code source} to the table {@code dest}.
	 * 
	 * @param dest
	 *            destination table
	 * @param source
	 *            source table
	 * @param soft
	 *            {@code true} means
	 *            <ol>
	 *            <li>{@code DELETE FROM _destTable}
	 *            <li>{@code INSERT INTO _destTable SELECT * FROM _sourceTable}
	 *            <li>{@code DELETE FROM _sourceTable}
	 *            </ol>
	 *            <p>
	 *            {@code false} means
	 *            <ol>
	 *            <li>{@code DROP _destTable}
	 *            <li>{@code RENAME _sourceTable TO _destTable}
	 *            <li>{@code CREATE _sourceTable}
	 *            </ol>
	 *            </ol>
	 * @return the new {@code ISqlMove}
	 */
	ISqlMove newMove(String dest, String source, boolean soft);

	/**
	 * Get a new {@code ISqlParser}.
	 * 
	 * @return the new parser.
	 */
	ISqlParser newParser();

	/**
	 * Create a {@code RENAME TABLE source TO dest} statement.
	 * 
	 * @param source
	 *            see description
	 * @param dest
	 *            see description
	 * @return a {@code RENAME TABLE source TO dest} statement.
	 */
	ISqlRename newRename(ITable source, ITable dest);

	/**
	 * Create a new <code>ISqlSelect</code>.
	 * 
	 * @return a new <code>ISqlSelect</code>.
	 */
	ISqlSelect newSelect();

	/**
	 * @param selectItem
	 *            <code>SELECT...</code> part
	 * @param fromItem
	 *            <code>FROM...</code> part
	 * 
	 * @return a new <code>ISqlSelect</code> with the given <code>SELECT</code>
	 *         and {@code FROM} clauses.
	 */
	ISqlSelect newSelect(ISqlSelectItem selectItem, ISqlFromItem fromItem);

	/**
	 * Construct a new {@code ISqlSelect} with the given {@code SELECT}, {@code
	 * FROM}, and {@code WHERE} clauses.
	 * 
	 * @param selectItem
	 *            {@code SELECT...} part
	 * @param fromItem
	 *            {@code FROM...} part
	 * @param whereItem
	 *            {@code WHERE...} part
	 * @return a new {@code ISqlSelect} with the given {@code SELECT}, {@code
	 *         FROM}, and {@code WHERE} clauses
	 */
	ISqlSelect newSelect(ISqlSelectItem selectItem, ISqlFromItem fromItem,
			ISqlExpression whereItem);

	ISqlSelectItem newSelectItem();

	/**
	 * Create a new {@code SELECT} item, given its name (for column names and
	 * wildcards).
	 * 
	 * @param fullName
	 *            a string that represents a column name or wildcard (examples:
	 *            {@code "SCHEMA.TABLE.*"}, {@code "TABLE.*"}, {@code "*"},
	 *            {@code "SCHEMA.TABLE.COLUMN"}, {@code "TABLE.COLUMN"}, {@code
	 *            "COLUMN"}).
	 * 
	 * @return the newly created {@code ISqlSelectItem}.
	 */
	ISqlSelectItem newSelectItem(String fullName);

	/**
	 * Create a table with an optionally schema-qualified table name.
	 * 
	 * @param schemaTable
	 *            the table name. Must be in the form {@code [schema.]table}.
	 * 
	 * @return a table with the given optionally schema-qualified table name
	 */
	ITable newTable(String schemaTable);

	/**
	 * Create a new {@code ITable}.
	 * 
	 * @param tableName
	 *            the name of the new table. Must be in the form {@code
	 *            [schema.]table}.
	 * @param alias
	 *            alias for the table
	 * 
	 * @return the new {@code ITable}
	 */
	ITable newTable(String tableName, String alias);

	/**
	 * Create a new {@code ISqlUtil}.
	 * 
	 * @return a new {@code ISqlUtil}
	 */
	ISqlUtil newSqlUtils();

	/**
	 * Construct an {@code ISqlCreateTempTable}: {@code
	 * "CREATE TEMP type TABLE name (columns)"}.
	 * 
	 * @param name
	 *            the name of the table, in {@code "[schema.]table"} form.
	 * @param type
	 *            the type of table, for example {@code "TEMPORARY"} or {@code
	 *            "GLOBAL TEMPORARY"}.
	 * @param cols
	 *            the columns of the table we're creating.
	 * @return the new {@code ISqlCreateTempTable}
	 */
	ISqlCreateTempTable newCreateTempTable(String name, String type,
			List<? extends ISqlColumnDef> cols);

	/**
	 * Returns an expression which evaluates to false.
	 * 
	 * @return an expression which evaluates to false
	 */
	ISqlExpression newFalseExpression();

	/**
	 * Construct an {@code ISqlOrderByItem} for use in an {@code ORDER BY}
	 * clause.
	 * 
	 * @param orderByName
	 *            the name of the item to order by
	 * @return the new {@code ISqlOrderByItem}
	 */
	ISqlOrderByItem newOrderByItem(ISqlConstant orderByName);

	/**
	 * Construct an {@code ISqlOrderByItem} for use in an {@code ORDER BY}
	 * clause.
	 * 
	 * @param orderByName
	 *            the name of the item to order by
	 * @param orderType
	 *            indicates if the order is ascending or descending
	 * @param nullOrderType
	 *            indicates if nulls should be first or last
	 * @return the new {@code ISqlOrderByItem}
	 */
	ISqlOrderByItem newOrderByItem(ISqlConstant orderByName,
			ISqlOrderByItem.OrderType orderType,
			ISqlOrderByItem.NullOrderType nullOrderType);
	
	ISqlSimpleExpression newSimpleExpression(String value);
		

}
