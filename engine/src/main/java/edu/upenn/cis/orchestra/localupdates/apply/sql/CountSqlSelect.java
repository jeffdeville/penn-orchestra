/*
 * Copyright (C) 2010 Trustees of the University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS of ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.upenn.cis.orchestra.localupdates.apply.sql;

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.NameNotFound;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.exchange.sql.SqlEngine;
import edu.upenn.cis.orchestra.sql.ISqlConstant;
import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlFactory;
import edu.upenn.cis.orchestra.sql.ISqlSelect;
import edu.upenn.cis.orchestra.sql.SqlFactories;
import edu.upenn.cis.orchestra.sql.ISqlConstant.Type;
import edu.upenn.cis.orchestra.sql.ISqlExpression.Code;

/**
 * Creates unparameterized SQL statements of the form {@code SELECT COUNT(*)
 * FROM ... WHERE ...}.
 * 
 * @author John Frommeyer
 * 
 */
class CountSqlSelect {
	private final ISqlFactory sqlFactory = SqlFactories.getSqlFactory();

	private final static Logger logger = LoggerFactory
			.getLogger(CountSqlSelect.class);

	@SuppressWarnings("unchecked")
	ISqlSelect getSqlSelect(Tuple tuple) throws NameNotFound,
			ValueMismatchException {
		return getSqlSelect(tuple.getSchema().getFullQualifiedDbId(), tuple,
				Collections.EMPTY_MAP);
	}

	@SuppressWarnings("unchecked")
	ISqlSelect getSqlSelect(String selectRelationName, Tuple tuple)
			throws NameNotFound, ValueMismatchException {
		return getSqlSelect(selectRelationName, tuple, Collections.EMPTY_MAP);
	}

	/**
	 * Count how many rows in {@code selectRelationName} match the data in
	 * {@code tuple}. The attributes in the {@code WHERE} clause are
	 * either the {@code Tuple}'s field names, or if the field name is a key in
	 * {@code tupleColumnToSqlColumn}, then the corresponding value is used
	 * instead.
	 * 
	 * 
	 * @param selectRelationName the table being examined
	 * @param tuple the tuple containing the data
	 * @param tupleColumnToSqlColumn maps column names from the {@code Tuple}'s
	 *            schema to {@code selectRelationName}'s schema
	 * @return a {@code SELECT COUNT(*) ...} statement
	 * @throws NameNotFound
	 * @throws ValueMismatchException
	 */
	ISqlSelect getSqlSelect(String selectRelationName, Tuple tuple,
			Map<String, String> tupleColumnToSqlColumn) throws NameNotFound,
			ValueMismatchException {
		ISqlSelect countSelect = sqlFactory.newSelect();
		countSelect.addSelectClause(Collections.singletonList(sqlFactory
				.newSelectItem().setExpression(
						sqlFactory.newExpression(Code.COUNT))));

		countSelect.addFromClause(Collections.singletonList(sqlFactory
				.newFromItem(selectRelationName)));

		ISqlExp whereClause = createWhereClause(tuple, tupleColumnToSqlColumn);
		countSelect.addWhere(whereClause);
		logger.debug("countSelect: {}", countSelect);

		return countSelect;
	}

	private ISqlExp createWhereClause(Tuple tuple,
			Map<String, String> tupleColumnToSqlColumn) throws NameNotFound,
			ValueMismatchException {
		final List<ISqlExp> conditions = newArrayList();
		final List<RelationField> fields = tuple.getSchema().getFields();
		for (RelationField field : fields) {
			String tupleFieldName = field.getName();
			String sqlColName = tupleColumnToSqlColumn.get(tupleFieldName);
			if (sqlColName == null) {
				sqlColName = tupleFieldName;
			}
			edu.upenn.cis.orchestra.datamodel.Type orchestraType = field
					.getType();
			ISqlConstant.Type sqlConstType = orchestraType.getSqlConstantType();
			if (tuple.isLabeledNullable(tupleFieldName)) {
				if (tuple.isLabeledNull(tupleFieldName)) {
					int lnValue = tuple.getLabeledNull(tupleFieldName);
					conditions.add(sqlFactory
							.newExpression(Code.IS_NULL, sqlFactory
									.newConstant(sqlColName, Type.COLUMNNAME)));
					conditions.add(sqlFactory.newExpression(Code.EQ, sqlFactory
							.newConstant(sqlColName
									+ RelationField.LABELED_NULL_EXT,
									Type.COLUMNNAME), sqlFactory.newConstant(
							Integer.toString(lnValue), Type.NUMBER)));
				} else {
					Object value = tuple.get(tupleFieldName);
					conditions.add(sqlFactory.newExpression(Code.EQ, sqlFactory
							.newConstant(sqlColName, Type.COLUMNNAME),
							sqlFactory.newConstant(orchestraType
									.getStringRep(value), sqlConstType)));
					conditions.add(sqlFactory.newExpression(Code.EQ, sqlFactory
							.newConstant(sqlColName
									+ RelationField.LABELED_NULL_EXT,
									Type.COLUMNNAME), sqlFactory.newConstant(
							Integer.toString(SqlEngine.LABELED_NULL_NONVALUE),
							Type.NUMBER)));
				}
			} else {
				Object value = tuple.get(tupleFieldName);
				conditions.add(sqlFactory.newExpression(Code.EQ, sqlFactory
						.newConstant(sqlColName, Type.COLUMNNAME), sqlFactory
						.newConstant(orchestraType.getStringRep(value),
								sqlConstType)));
			}
		}
		ISqlExp whereClause = conditions.get(0);
		for (int i = 1; i < conditions.size(); i++) {
			whereClause = sqlFactory.newExpression(Code.AND, whereClause,
					conditions.get(i));
		}
		return whereClause;
	}
}
