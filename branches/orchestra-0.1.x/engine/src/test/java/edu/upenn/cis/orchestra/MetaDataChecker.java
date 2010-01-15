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
package edu.upenn.cis.orchestra;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITableMetaData;
import org.testng.Assert;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;

/**
 * Checks that the metadata in the actual table matches what should be the case
 * based on the contents of the Orchestra schema file.
 * 
 * @author John Frommeyer
 * 
 */
public class MetaDataChecker {
	/**
	 * Private Constructor.
	 */
	private MetaDataChecker(Builder builder) {
		checkLabeledNulls = builder.checkLabeledNulls;
		checkTypes = builder.checkTypes;
		checkPrimaryKeys = builder.checkPrimaryKeys;
		checkLabeledNullable = builder.checkLabeledNullable;
		checkMaterialized = builder.checkMaterialized;
		checkNullable = builder.checkNullable;
	}

	/**
	 * Indicates that we should check for a labeled null column for each base
	 * column.
	 */
	private final boolean checkLabeledNulls;

	/** Indicates that we should check the type of each column. */
	private final boolean checkTypes;

	/** Indicates that we should check the primary keys of the table. */
	private final boolean checkPrimaryKeys;

	/**
	 * Indicates that we should check that the labeledNullable attribute is
	 * being honored.
	 */
	private final boolean checkLabeledNullable;

	/**
	 * Indicates that we should check that the materialized attribute is being
	 * honored.
	 */
	private final boolean checkMaterialized;

	/**
	 * Indicates that we should check if {@code relation/field/@nullable} is
	 * being honored.
	 */
	private final boolean checkNullable;

	/** The metadata of the actual table. */
	private ITableMetaData metaData;

	/**
	 * The Orchestra extension of the actual table, or {@code ""} if it is not
	 * an Orchestra table.
	 */
	private String extension;

	/**
	 * The relation element from the Orchestra schema file corresponding to the
	 * actual table.
	 */
	private Element relation;

	/** Maps column names to column objects in the actual table. */
	private Map<String, Column> nameToColumn;

	/**
	 * Maps primary key column names to primary key column objects in the actual
	 * table.
	 */
	private Map<String, Column> nameToPKColumn;

	/**
	 * Indicates that the Orchestra schema implies that the actual table should
	 * have labeled null columns.
	 */
	private boolean shouldHaveLabeledNulls;

	/** The name of the actual table. */
	private String actualTableName;

	/** The value of {@code relation/@materialized}. */
	private boolean materialized;

	/** These are the table name extensions used by Orchestra. */
	private static final List<String> TABLE_NAME_EXTENSIONS = OrchestraUtil
			.newArrayList();

	static {
		for (AtomType atomType : AtomType.values()) {
			TABLE_NAME_EXTENSIONS.add(Relation.LOCAL + "_"
					+ atomType.toString());
			TABLE_NAME_EXTENSIONS.add(Relation.REJECT + "_"
					+ atomType.toString());
			TABLE_NAME_EXTENSIONS.add("_" + atomType.toString());
		}

		// Since some extensions are suffixes of other extensions, we sort
		// to
		// get the longest extensions first. This is probably overkill since
		// Relation.LOCAL.length() == Relation.REJECT.length().
		Collections.sort(TABLE_NAME_EXTENSIONS, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				int len1 = o1.length();
				int len2 = o2.length();
				if (len1 > len2) {
					return -1;
				} else if (len1 < len2) {
					return 1;
				}
				return 0;
			}
		});
	}

	/**
	 * Checks that the information in {@code metaData} is consistent with that
	 * in {@code relation}, perhaps also taking into account the value of
	 * {@code actualExtension}.
	 * 
	 * @param actualMetaData the {@code ITableMetaData} of the actual table
	 *            being checked.
	 * @param actualExtension the "Orchestra extension" of the actual table, or
	 *            the empty string if the actual table is not a special
	 *            Orchestra table.
	 * @param relationElement
	 * @param relation the base relation {@code Element} corresponding to the
	 *            actual table.
	 * @throws DataSetException
	 * 
	 */
	public void check(ITableMetaData actualMetaData, String actualExtension,
			Element relationElement) throws DataSetException {
		this.metaData = actualMetaData;
		this.extension = actualExtension;
		this.relation = relationElement;

		init();
		doMaterializedCheck();
		doFieldChecks();
	}

	/**
	 * For each base relation name in {@code orchestraSchemaRelations}, the
	 * metadata contained in {@code actual} of any associated table in {@code
	 * actualTableNames} is checked against the content of {@code
	 * orchestraSchema}.
	 * 
	 * 
	 * @param orchestraSchemaRelations the base relation names from the
	 *            Orchestra schema file represented by {@code orchestraSchema}.
	 * @param actualTableNames the table names in an actual database instance.
	 * @param orchestraSchema represents an Orchestra schema file.
	 * @param actual metadata which is being checked.
	 * @throws DataSetException
	 */
	public void check(Collection<String> orchestraSchemaRelations,
			Collection<String> actualTableNames,
			OrchestraSchema orchestraSchema, IDataSet actual)
			throws DataSetException {
		for (String relationName : orchestraSchemaRelations) {
			for (String actualTable : actualTableNames) {
				String[] parsedActualTable = parseTableName(actualTable);
				if (relationName.equalsIgnoreCase(parsedActualTable[0])) {
					check(actual.getTableMetaData(actualTable),
							parsedActualTable[1], orchestraSchema
									.getElementForName(relationName));
				}
			}
		}
	}

	/**
	 * For each base relation name in {@code orchestraSchema}, the metadata
	 * contained in {@code actual} of any associated table is checked against
	 * the content of {@code orchestraSchema}.
	 * 
	 * @param orchestraSchema represents an Orchestra schema file.
	 * @param actual metadata which is being checked.
	 * @throws DataSetException
	 */
	public void check(OrchestraSchema orchestraSchema, IDataSet actual)
			throws DataSetException {
		// dumpOrCheck(...) has verified that all, and only, the tables
		// which
		// should exist do, so we don't worry about that here.

		Set<String> orchestraSchemaRelations = orchestraSchema
				.getDbTableNames();
		List<String> actualTableNames = Arrays.asList(actual.getTableNames());

		check(orchestraSchemaRelations, actualTableNames, orchestraSchema,
				actual);
	}

	/**
	 * Checks that the materialized attribute of relation is being handled
	 * correctly.
	 * 
	 */
	private void doMaterializedCheck() {
		if (checkMaterialized) {
			// Not sure what to test here.
		}
	}

	/**
	 * Performs the various checks on the actual table columns.
	 * 
	 * @param relation
	 * 
	 */
	private void doFieldChecks() {
		NodeList fields = relation.getElementsByTagName("field");
		for (int i = 0; i < fields.getLength(); i++) {
			Element field = (Element) fields.item(i);
			String name = doColumnNameCheck(field);

			doPrimaryKeyCheck(field, name);
			doTypeCheck(field, name);
			doLabeledNullableCheck(field, name);
			doNullableCheck(field, name);
		}
	}

	/**
	 * Checks that the labeledNullable attribute of relation/field is being
	 * handled correctly.
	 * 
	 * @param field
	 * @param fieldName
	 */
	private void doLabeledNullableCheck(Element field, String fieldName) {
		if (checkLabeledNullable) {
			// Not sure what to test for here.
			String labeledNullable = field.getAttribute("labeledNullable");
		}
	}

	/**
	 * Checks that {@code field} has the correct type.
	 * 
	 * @param field
	 * @param fieldName
	 * 
	 */

	private void doTypeCheck(Element field, String fieldName) {
		if (checkTypes) {
			String typeInSchemaFile = field.getAttribute("type").toUpperCase();

			int expectedTypeCode = orchestraTypeToSqlTypeCode(typeInSchemaFile);
			int actualTypeCode = nameToColumn.get(fieldName).getDataType()
					.getSqlType();
			Assert.assertEquals(actualTypeCode, expectedTypeCode,
					"Actual type was ["
							+ nameToColumn.get(fieldName).getSqlTypeName()
							+ "], Orchestra schema file type was ["
							+ typeInSchemaFile + "].");
		}
	}

	/**
	 * Checks that the primary keys are correct.
	 * 
	 * @param field
	 * @param fieldName
	 */
	private void doPrimaryKeyCheck(Element field, String fieldName) {
		if (checkPrimaryKeys) {
			// We do not seem to be creating primary keys.
			// Also need to figure out how this should interact with
			// primaryKey
			// child of relation.
			String key = field.getAttribute("key");
		}
	}

	/**
	 * Returns the expected name of {@code field}. Checks for the correct column
	 * names, and if required the correct labeled null columns.
	 * 
	 * @param field
	 * @return the expected name of {@code field}.
	 */
	private String doColumnNameCheck(Element field) {
		String name = field.getAttribute("name").toUpperCase();
		Assert.assertTrue(nameToColumn.containsKey(name), "Actual table: "
				+ actualTableName + " missing column: " + name);

		if (checkLabeledNulls && shouldHaveLabeledNulls) {
			Assert.assertTrue(nameToColumn.containsKey(name
					+ RelationField.LABELED_NULL_EXT), "Actual table: "
					+ actualTableName + " missing labelled null column for: "
					+ name);
		}
		return name;
	}

	/**
	 * Checks that columns which should be nullable/non-nullable are actuall
	 * nullable/non-nullable.
	 * 
	 * @param field
	 * @param fieldName
	 */
	private void doNullableCheck(Element field, String fieldName) {
		if (checkNullable) {
			// nullable defaults to true.
			String nullable = field.getAttribute("nullable");
			if ("false".equalsIgnoreCase(nullable)) {
				Assert.assertTrue(nameToColumn.get(fieldName).getNullable()
						.equals(Column.NO_NULLS), "Field: " + fieldName
						+ " in table: " + actualTableName
						+ " should not be nullable.");
			} else {
				Assert.assertTrue(nameToColumn.get(fieldName).getNullable()
						.equals(Column.NULLABLE), "Field: " + fieldName
						+ " in table: " + actualTableName
						+ " should be nullable.");
			}
		}
	}

	/**
	 * Sets up for the various checks.
	 * 
	 * @throws DataSetException
	 * 
	 */
	private void init() throws DataSetException {
		actualTableName = metaData.getTableName().toUpperCase();
		Column[] actualColumns = metaData.getColumns();
		nameToColumn = OrchestraUtil.newHashMap();
		for (Column actualColumn : actualColumns) {
			nameToColumn.put(actualColumn.getColumnName().toUpperCase(),
					actualColumn);
		}

		Column[] actualPKColumns = metaData.getPrimaryKeys();
		nameToPKColumn = OrchestraUtil.newHashMap();
		for (Column actualPKColumn : actualPKColumns) {
			nameToPKColumn.put(actualPKColumn.getColumnName().toUpperCase(),
					actualPKColumn);
		}

		shouldHaveLabeledNulls = !"true".equalsIgnoreCase(relation
				.getAttribute("noNulls"));
		materialized = Boolean.parseBoolean(relation
				.getAttribute("materialized"));
	}

	/**
	 * Returns the {@code java.sql.Types} code which corresponds to {@code}
	 * type. Probably not the ideal implementation with respect to fragility.
	 * 
	 * @param type
	 * @return the {@code java.sql.Types} code which corresponds to {@code}
	 *         type.
	 */
	private static int orchestraTypeToSqlTypeCode(String type) {
		String trimmedType = type.trim();
		if ("date".equalsIgnoreCase(trimmedType)) {
			return java.sql.Types.DATE;
		} else if ("timestamp".equalsIgnoreCase(trimmedType)) {
			return java.sql.Types.TIMESTAMP;
		} else if ("double".equalsIgnoreCase(trimmedType)) {
			return java.sql.Types.DOUBLE;
		} else if ("float".equalsIgnoreCase(trimmedType)) {
			return java.sql.Types.DOUBLE;
		} else if ("integer".equalsIgnoreCase(trimmedType)) {
			return java.sql.Types.INTEGER;
		} else if ("int".equalsIgnoreCase(trimmedType)) {
			return java.sql.Types.INTEGER;
		} else if ("bigint".equalsIgnoreCase(trimmedType)) {
			return java.sql.Types.BIGINT;
		} else if ("long".equalsIgnoreCase(trimmedType)) {
			return java.sql.Types.BIGINT;
		} else if ("boolean".equalsIgnoreCase(trimmedType)) {
			return java.sql.Types.BOOLEAN;
		} else if ("bool".equalsIgnoreCase(trimmedType)) {
			return java.sql.Types.BOOLEAN;
		} else if (trimmedType.toLowerCase().startsWith("char(")) {
			return java.sql.Types.CHAR;
		} else if (trimmedType.toLowerCase().startsWith("varchar(")) {
			return java.sql.Types.VARCHAR;
		} else if (trimmedType.startsWith("clob(")) {
			return java.sql.Types.CLOB;
		} else {
			throw new IllegalStateException(
					"Unrecognized type name in Orchestra schema file: ["
							+ trimmedType + "].");
		}
	}

	/**
	 * Returns a two element {@code String[]} containing a base name of {@code
	 * tableName} in the first element. The second element is the Orchestra
	 * extension of {@code tableName}, or the empty string if {@code tableName}
	 * has no Orchestra extension.
	 * 
	 * @param tableName
	 * 
	 * @return a two element {@code String[]}.
	 */
	private String[] parseTableName(String tableName) {
		for (String ext : TABLE_NAME_EXTENSIONS) {
			if (tableName.endsWith(ext)) {
				// This should be safe since TABLE_NAME_EXTENSIONS is
				// ordered
				// with longest extension first.
				return new String[] {
						tableName.substring(0, tableName.length()
								- ext.length()), ext };
			}
		}
		return new String[] { tableName, "" };
	}
	
	/**
	 * Creates {@code MetaDataChecker}s.
	 * @author John Frommeyer
	 *
	 */
	public static class Builder {
		private boolean checkLabeledNulls = false;
		private boolean checkTypes = false;
		private boolean checkPrimaryKeys = false;
		private boolean checkLabeledNullable = false;
		private boolean checkMaterialized = false;
		private boolean checkNullable = false;
		
		/**
		 * Returns the newly built {@code MetaDataChecker}.
		 * 
		 * @return the newly built {@code MetaDataChecker}
		 */
		public MetaDataChecker build() {
			return new MetaDataChecker(this);
		}
		/**
		 * Indicates that the built checker should check for labeled null columns.
		 * 
		 * @return this builder.
		 */
		public Builder checkForLabeledNulls() {
			checkLabeledNulls = true;
			return this;
		}

		/**
		 * Indicates that the built checker should check the types of the columns in
		 * the actual table.
		 * 
		 * @return this builder.
		 */
		public Builder checkTypes() {
			checkTypes = true;
			return this;
		}

		/**
		 * Indicates that the built checker should check the primary keys of the
		 * actual table.
		 * 
		 * @return this builder.
		 */
		public Builder checkPrimaryKeys() {
			checkPrimaryKeys = true;
			return this;
		}

		/**
		 * Indicates that the built checker should check that the {@code
		 * labeledNullable} attribute of {@code relation/field} is being handled
		 * correctly.
		 * 
		 * @return this builder.
		 */
		public Builder checkLabeledNullable() {
			checkLabeledNullable = true;
			return this;
		}

		/**
		 *Indicates that the built checker should check that the {@code
		 * materialized} attribute of {@code relation} is being handled correctly.
		 * 
		 * @return this builder.
		 */
		public Builder checkMaterialized() {
			checkMaterialized = true;
			return this;
		}

		/**
		 *Indicates that the built checker should check that the {@code nullable}
		 * attribute of {@code relation/field} is being handled correctly.
		 * 
		 * @return this builder.
		 */
		public Builder checkNullable() {
			checkNullable = true;
			return this;
		}
	}
}
