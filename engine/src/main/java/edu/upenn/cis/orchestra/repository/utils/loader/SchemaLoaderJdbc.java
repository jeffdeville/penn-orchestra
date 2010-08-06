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
package edu.upenn.cis.orchestra.repository.utils.loader;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.support.DatabaseMetaDataCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.MetaDataAccessException;

import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.datamodel.ForeignKey;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.PrimaryKey;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.RelationIndexNonUnique;
import edu.upenn.cis.orchestra.datamodel.RelationIndexUnique;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicatePeerIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateRelationIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateSchemaIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.repository.utils.loader.exceptions.SchemaLoaderException;

/**
 * Load relations from a JDBC connection, and add these relations to a given
 * Orchestra schema
 * 
 * @author Olivier Biton
 * @author Sam Donnelly
 */
public class SchemaLoaderJdbc implements ISchemaLoader {

	/** Datasource from which the metadata is to be loaded */
	private DataSource _ds;

	/**
	 * Configuration for this <code>SchemaLoader</code>.
	 */
	private ISchemaLoaderConfig _schemaLoaderConfig;

	/**
	 * Create a new JDBC schema loader
	 * 
	 * @param ds Datasource from which the metadata is to be loaded
	 * @param schemaLoaderConfig configuration for this {@code ISchemaLoader}.
	 */
	public SchemaLoaderJdbc(DataSource ds,
			ISchemaLoaderConfig schemaLoaderConfig) {
		_ds = ds;
		_schemaLoaderConfig = schemaLoaderConfig;
	}

	/**
	 * Load the relations according to the filters passed to the constructor and
	 * add the new relations to the schema
	 * 
	 * @param catalogPattern Filter on the catalog from which tables should be
	 *            loaded, can be null (no filter)
	 * @param schemaPattern Filter on the schema from which tables should be
	 *            loaded, can be null (no filter)
	 * @param tableNamePattern Filter on the table names to load, can be null
	 *            (no filter)
	 * @throws SchemaLoaderException Thrown if an error occurs while reading
	 *             database metadata
	 * @throws DuplicateSchemaIdException if such an exception it thrown when
	 *             building the <code>OrchestraSystem</code>
	 * @throws DuplicatePeerIdException if such an exception it thrown when
	 *             building the <code>OrchestraSystem</code>
	 */
	public OrchestraSystem buildSystem(String orchestraSystemName,
			String catalogPattern, String schemaPattern, String tableNamePattern)
			throws SchemaLoaderException, DuplicateSchemaIdException,
			DuplicatePeerIdException {

		try {
			// Add the relations to the schema, with no constraints

			List<Schema> schemas = new ArrayList<Schema>();

			MetaLoadTables loadTables = new MetaLoadTables(schemas,
					catalogPattern, schemaPattern, tableNamePattern);
			String err = (String) JdbcUtils.extractDatabaseMetaData(_ds,
					loadTables);

			if (err != null)
				throw new SchemaLoaderException(
						"An error occured while loading a relation: " + err);

			// Now that all tables have been created, we can load the
			// constraints
			refreshSchemaConstraintsAndStatistics(schemas);
			OrchestraSystem orchestraSystem = new OrchestraSystem();
			orchestraSystem.setName(orchestraSystemName);

			for (Schema schema : schemas) {
				Peer peer = new Peer(schema.getSchemaId(), "localhost", "");
				peer.addSchema(schema);
				orchestraSystem.addPeer(peer);
			}

			return orchestraSystem;
		} catch (MetaDataAccessException ex) {
			throw new SchemaLoaderException("Unable to load relations: "
					+ ex.getMessage(), ex);
		}
	}

	/**
	 * For a given schema (which has to be stored in the database referenced by
	 * connection _ds), refresh all constraints and statistics (tables row
	 * counts, indexes unique values counts, foreign keys...) Statistics are
	 * loaded using database statistics, might not work with some RDBMS such as
	 * MySQL
	 * 
	 * @param schema Schema for which constraints and statistics must be updated
	 * @throws SchemaLoaderException If any error occurs while extracting
	 *             metadata
	 */
	public void refreshSchemaConstraintsAndStatistics(Schema schema)
			throws SchemaLoaderException {
		try {
			// Now that all tables have been created, we can load the
			// constraints
			MetaLoadConstraints loadConstraints = new MetaLoadConstraints(
					schema);
			JdbcUtils.extractDatabaseMetaData(_ds, loadConstraints);
		} catch (MetaDataAccessException ex) {
			throw new SchemaLoaderException("Unable to load relations: "
					+ ex.getMessage(), ex);
		}
	}

	/**
	 * For a given schema (which has to be stored in the database referenced by
	 * connection _ds), refresh all constraints and statistics (tables row
	 * counts, indexes unique values counts, foreign keys...) Statistics are
	 * loaded using database statistics, might not work with some RDBMS such as
	 * MySQL
	 * 
	 * @param schema Schema for which constraints and statistics must be updated
	 * @throws SchemaLoaderException If any error occurs while extracting
	 *             metadata
	 */
	public void refreshSchemaConstraintsAndStatistics(List<Schema> schemas)
			throws SchemaLoaderException {
		try {
			// Now that all tables have been created, we can load the
			// constraints
			MetaLoadConstraints loadConstraints = new MetaLoadConstraints(
					schemas);
			JdbcUtils.extractDatabaseMetaData(_ds, loadConstraints);
		} catch (MetaDataAccessException ex) {
			throw new SchemaLoaderException("Unable to load relations: "
					+ ex.getMessage(), ex);
		}
	}

	/**
	 * Metadata callback class used to load tables/columns. But no constraints
	 * Based on the Spring framework / JDBC module
	 * 
	 * @author Olivier Biton
	 */
	private class MetaLoadTables implements DatabaseMetaDataCallback {
		// private Schema _schema;

		private List<Schema> _schemas;
		/** Filter on the catalog from which tables should be loaded */
		private String _catalogPattern = null;
		/** Filter on the schema from which tables should be loaded */
		private String _schemaPattern = null;
		/** Filter on the table names to load */
		private String _tableNamePattern = null;

		/**
		 * @param catalogPattern Filter on the catalog from which tables should
		 *            be loaded, can be null (no filter)
		 * @param schemaPattern Filter on the schema from which tables should be
		 *            loaded, can be null (no filter)
		 * @param tableNamePattern Filter on the table names to load, can be
		 *            null (no filter)
		 * @param schema
		 */
		// public MetaLoadTables(Schema schema, String catalogPattern,
		// String schemaPattern, String tableNamePattern) {
		// _schema = schema;
		// _catalogPattern = catalogPattern;
		// _schemaPattern = schemaPattern;
		// _tableNamePattern = tableNamePattern;
		//
		// }
		/**
		 * @param catalogPattern Filter on the catalog from which tables should
		 *            be loaded, can be null (no filter)
		 * @param schemaPattern Filter on the schema from which tables should be
		 *            loaded, can be null (no filter)
		 * @param tableNamePattern Filter on the table names to load, can be
		 *            null (no filter)
		 * @param schema
		 */
		public MetaLoadTables(List<Schema> schemas, String catalogPattern,
				String schemaPattern, String tableNamePattern) {
			_schemas = schemas;
			_catalogPattern = catalogPattern;
			_schemaPattern = schemaPattern;
			_tableNamePattern = tableNamePattern;
		}

		/**
		 * Will complete the list of relations existing in the schema by
		 * extracting the list of tables according to the catalog, schema, and
		 * table name patterns. <BR>
		 * The columns are also extracted and added to these relations but the
		 * constraints are not loaded.
		 * 
		 * @return null, return not used
		 * @throws UnsupportedTypeException
		 */
		public Object processMetaData(DatabaseMetaData dbmd)
				throws SQLException, MetaDataAccessException {
			ResultSet rs = dbmd.getTypeInfo();

			// Extract only tables (we don't want to get views, not system
			// tables and synonyms...)
			rs = dbmd.getTables(_catalogPattern, _schemaPattern,
					_tableNamePattern, new String[] { "TABLE" });
			List<TabName> lstTab = new ArrayList<TabName>();
			while (rs.next()) {
				TabName tab = new TabName(rs.getString("TABLE_CAT"), rs
						.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME"),
						rs.getString("REMARKS"));
				lstTab.add(tab);
			}

			// Will now loop on the tables list to load the columns
			for (TabName tableName : lstTab) {
				if (Relation.isInternalRelationName(tableName._tabName)
						|| AtomType.NONE != Atom.getSuffix(tableName._tabName)
						|| Relation.isMappingTableName(tableName._tabName)
						|| !_schemaLoaderConfig.filterOnSchemaName(dbmd
								.getDatabaseProductName(),
								tableName._schemaName)
						|| !_schemaLoaderConfig
								.filterOnTableName(tableName._tabName)) {
					// It's an Orchestra table or excluded, so skip it
					continue;
				}
				List<RelationField> fields = new ArrayList<RelationField>();
				rs = dbmd.getColumns(tableName._catalogName,
						tableName._schemaName, tableName._tabName, null);

				boolean containsLabeledNull = false;

				while (rs.next()) {

					String colName = rs.getString("COLUMN_NAME");

					if (RelationField.isLabeledNull(colName)) {
						containsLabeledNull = true;
					} else {
						// Ignore labeled null columns (if the database has
						// already
						// been prepared for labeled null

						String dbType = rs.getString("TYPE_NAME");
						if (dataTypeNeedsWidth(rs.getInt("DATA_TYPE"))
								&& columnSizeNeedsWidth(rs
										.getInt("COLUMN_SIZE"))) {
							// if (rs.getInt("COLUMN_SIZE")>0 &&
							// !types.get(dbType)) {
							dbType = dbType + "(" + rs.getInt("COLUMN_SIZE");
							if (rs.getInt("DECIMAL_DIGITS") > 0)
								dbType = dbType + ","
										+ rs.getInt("DECIMAL_DIGITS");
							dbType = dbType + ")";
						}

						try {
							// TODO: test against DB2 width CLOB... to check
							// data types!!
							RelationField fld = new RelationField(rs
									.getString("COLUMN_NAME"), rs
									.getString("REMARKS"), rs.getString(
									"IS_NULLABLE").equals("YES"), dbType);
							fields.add(fld);
						} catch (UnsupportedTypeException ute) {
							System.err
									.println("Ignoring column: unsupported type "
											+ ute.getMessage());
							// throw new MetaDataAccessException(
							// "Unsupported type " + ute.getMessage());
						}
					}
				}
				// create the relation
				try {
					Schema schema = null;
					String schemaName = (tableName._schemaName == null ? ""
							: tableName._schemaName);
					if (1 == _schemas.size() && 0 == _stringToSchema.size()) {
						// This is to support the original addToSchema(Schema,
						// ...)
						// method.
						schema = _schemas.get(0);
					} else if (_stringToSchema.get(schemaName) == null) {
						schema = new Schema(schemaName, "");
						_stringToSchema.put(schemaName, schema);
						_schemas.add(schema);
					} else {
						schema = _stringToSchema.get(schemaName);
					}
					Relation relation = new Relation(tableName._catalogName,
							tableName._schemaName, tableName._tabName,
							tableName._tabName, tableName._comments, true,
							true, fields);
					relation.setLabeledNulls(containsLabeledNull);
					schema.addRelation(relation);
				} catch (DuplicateRelationIdException ex) {
					return ex.getMessage();
				}
			}

			return null;
		}

		/**
		 * Maps the schema ID's (<code>schema.getIdSchema()</code>) to
		 * <code>Schema</code>s.
		 */
		private Map<String, Schema> _stringToSchema = new HashMap<String, Schema>();

		/**
		 * Return <code>true</code> if <code>dataType</code> is such that a
		 * width is required for the data type. For example VARCHAR(5) has a
		 * width but INT doesn't.
		 * 
		 * @param dataType an SQL data type, as in the values specified in
		 *            <code>java.sql.Types</code>.
		 * @return see description.
		 */
		private boolean dataTypeNeedsWidth(int dataType) {
			if (Types.INTEGER == dataType || Types.BIGINT == dataType
					|| Types.SMALLINT == dataType || Types.TINYINT == dataType
					|| Types.TIMESTAMP == dataType || Types.TIME == dataType
					|| Types.DATE == dataType || Types.DOUBLE == dataType
					|| Types.LONGVARBINARY == dataType
					|| Types.OTHER == dataType) {
				return false;
			}
			return true;
		}

		/**
		 * Return <code>true</code> if <code>columnSize</code> is such that a
		 * width is required for the data type.
		 * 
		 * @param columnSize COLUMN_SIZE of the column.
		 * @return see description.
		 */
		private boolean columnSizeNeedsWidth(int columnSize) {
			if (0 == columnSize || Integer.MAX_VALUE == columnSize
					|| Integer.MIN_VALUE == columnSize) {
				return false;
			}
			return true;
		}

	}

	/**
	 * Metadata callback class used to load tables constraints. Will load
	 * primary keys first and then foreign keys. <BR>
	 * This will be done for all the relations in the orchestra schema (not only
	 * whose added by the latest call to MetaLoadTables). <BR>
	 * Based on the Spring framework / JDBC module
	 * 
	 * @author Olivier Biton
	 */
	private class MetaLoadConstraints implements DatabaseMetaDataCallback {
		private Schema _schema;
		private List<Schema> _schemas = new ArrayList<Schema>();

		public MetaLoadConstraints(Schema schema) {
			_schema = schema;
			_schemas.add(_schema);
		}

		public MetaLoadConstraints(List<Schema> schemas) {
			_schemas.addAll(schemas);
		}

		/**
		 * 
		 * 
		 * @return null, return not used
		 */
		public Object processMetaData(DatabaseMetaData dbmd)
				throws SQLException, MetaDataAccessException {

			try {
				for (Schema schema : _schemas) {

					// First step: load all the primary keys and indexes
					for (Relation rel : schema.getRelations()) {

						// Remove all former indexes and foreign keys
						rel.clearForeignKeys();
						rel.clearUniqueIndexes();
						rel.clearNonUniqueIndexes();

						/***********************************************************
						 * Load primary keys
						 * *****************************************
						 */
						// Extract primary key data from the metadata for this
						// relation
						ResultSet rs = dbmd.getPrimaryKeys(rel.getDbCatalog(),
								rel.getDbSchema(), rel.getName());
						// Load list of fields (ordered) + primary key name
						// (will be
						// empty if no primary key)
						String pkName = "";
						List<String> fields = new ArrayList<String>();
						while (rs.next()) {
							String colName = rs.getString("COLUMN_NAME");
							pkName = rs.getString("PK_NAME");
							// We want to insert the fields following the order
							// defined in the database
							int ind = rs.getInt("KEY_SEQ") - 1;
							if (ind < fields.size())
								fields.set(ind, colName);
							else {
								for (int i = fields.size(); i < ind; i++)
									fields.add("");
								fields.add(colName);
							}
						}
						// If a primary key has been found
						if (pkName.length() > 0) {
							// Remove the labeled null fields (it was easier to
							// load
							// them and remove
							// at the end to be able to preserve the columns
							// sequence)
							removeLabeledNulls(fields);
							// Create the primary key
							rel.setPrimaryKey(new PrimaryKey(pkName, rel,
									fields));
						}

						/***********************************************************
						 * Load indexes (unique & non unique)
						 * *****************************************
						 */
						// TODO: use the KEY_SEQ attribute as done for primary
						// key
						// Extract the index informations from the database
						// This will also return a virtual index of type
						// tableIndexStatistic which
						// gives table statistics
						rs = dbmd.getIndexInfo(rel.getDbCatalog(), rel
								.getDbSchema(), rel.getName(), false, /**
						 * Get
						 * both unique and non unique
						 */
						false /** Load actual values from db, do not use cache */
						);
						String prevIndexName = "";
						fields = null;
						boolean isPrevUniqueIndex = false;
						while (rs.next()) {
							// If this is an actual index ...
							if (rs.getInt("TYPE") != DatabaseMetaData.tableIndexStatistic) {
								// Do not include the primary key (already
								// loaded
								// previously)
								if (!rs.getString("INDEX_NAME").equals(pkName)) {

									String colName = rs
											.getString("COLUMN_NAME");

									// If the index information does not concern
									// the
									// same index as the one
									// loaded in the previous row...
									if (!prevIndexName.equals(rs
											.getString("INDEX_NAME"))) {
										// If a previous row was loaded (not
										// first
										// step in the loop)
										if (prevIndexName.length() > 0) {
											// Remove the labeled null columns
											// (was
											// easier to load them to
											// preserve columns order
											// Create the index
											removeLabeledNulls(fields);
											if (isPrevUniqueIndex)
												rel
														.addUniqueIndex(new RelationIndexUnique(
																prevIndexName,
																rel, fields));
											else
												rel
														.addNonUniqueIndex(new RelationIndexNonUnique(
																prevIndexName,
																rel, fields));
										}
										// Update the index properties that are
										// not
										// field specific
										prevIndexName = rs
												.getString("INDEX_NAME");
										isPrevUniqueIndex = !rs
												.getBoolean("NON_UNIQUE");
										fields = new ArrayList<String>();
									}
									// Add the field to this index list of
									// fields,
									// preserve the order
									// We want to insert the fields following
									// the
									// order defined in the database
									int ind = rs.getInt("ORDINAL_POSITION") - 1;
									if (ind < fields.size())
										fields.set(ind, colName);
									else {
										for (int i = fields.size(); i < ind; i++)
											fields.add("");
										fields.add(colName);
									}
								}

							}
						}
						// Add the last index scanned
						if (prevIndexName.length() > 0) {
							removeLabeledNulls(fields);

							if (isPrevUniqueIndex)
								rel.addUniqueIndex(new RelationIndexUnique(
										prevIndexName, rel, fields));
							else
								rel
										.addNonUniqueIndex(new RelationIndexNonUnique(
												prevIndexName, rel, fields));
						}
					}

					// We have now all the primary keys, we will load all the
					// foreign keys that reference a
					// primary key loaded in the orchestra schema and are stored
					// for
					// a relation also known in this
					// schema
					for (Relation rel : schema.getRelations()) {
						ResultSet rs = dbmd.getImportedKeys(rel.getDbCatalog(),
								rel.getDbSchema(), rel.getName());
						String prevFkName = "";
						List<String> fields = null;
						Relation referredRel = null;
						List<String> refFields = null;
						// TODO: use the KEYSEQ attribute as done for primary
						// key
						while (rs.next()) {
							if (!rs.getString("FK_NAME").equals(prevFkName)) {
								if (prevFkName.length() > 0
										&& referredRel != null)
									rel.addForeignKey(new ForeignKey(
											prevFkName, rel, fields,
											referredRel, refFields));
								prevFkName = rs.getString("FK_NAME");
								fields = new ArrayList<String>();
								referredRel = schema.getRelation(rs
										.getString("PKTABLE_CAT"), rs
										.getString("PKTABLE_SCHEM"), rs
										.getString("PKTABLE_NAME"));
								refFields = new ArrayList<String>();

							}
							if (referredRel != null) {
								// Do not include labeled null fields1
								String fkColName = rs
										.getString("FKCOLUMN_NAME");
								String refColName = rs
										.getString("FKCOLUMN_NAME");
								if (!RelationField.isLabeledNull(fkColName)
										&& !RelationField
												.isLabeledNull(refColName)) {
									fields.add(rs.getString("FKCOLUMN_NAME"));
									refFields
											.add(rs.getString("PKCOLUMN_NAME"));
								}
								// TODO: raise an actual exception if those two
								// fields are not labeled nulls!
								// It can happen and should not be checked with
								// an
								// assert
								else
									assert (RelationField
											.isLabeledNull(fkColName) && RelationField
											.isLabeledNull(refColName)) : "A labeled null field should reference a labeled null field only in foreign keys ("
											+ rel.getName() + ")";

							}
						}
						if (prevFkName.length() > 0)
							rel.addForeignKey(new ForeignKey(prevFkName, rel,
									fields, referredRel, refFields));
					}
				}
			} catch (UnknownRefFieldException ex) {
				// Should not happen coming from a RDBMS!
				// TODO: Logger + terminate
				System.out.println("UNEXPECTED EXCEPTION : " + ex.getMessage());
				ex.printStackTrace();
			}

			return null;
		}

		/**
		 * Removes labeled null fields from <code>fields</code>.
		 * 
		 * @param fields from which we're doing the removing.
		 */
		private void removeLabeledNulls(List<String> fields) {
			for (int i = 0; i < fields.size();) {
				if (RelationField.isLabeledNull(fields.get(i)))
					fields.remove(i);
				else
					i++;
			}
		}

	}

	/**
	 * Class used for internal purpose (store tables properties while fields are
	 * not known (thus while RelationSchema cannot be instantiated).
	 * 
	 * @author Olivier Biton
	 * 
	 */
	private class TabName {
		protected String _catalogName;
		protected String _schemaName;
		protected String _tabName;
		protected String _comments;

		protected TabName(String catalogName, String schemaName,
				String tabName, String comments) {
			_catalogName = catalogName;
			_schemaName = schemaName;
			_tabName = tabName;
			if (comments == null)
				_comments = _tabName;
			else
				_comments = comments;
		}
	}

}
