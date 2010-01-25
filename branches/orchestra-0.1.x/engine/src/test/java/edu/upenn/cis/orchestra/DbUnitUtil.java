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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.dbunit.Assertion;
import org.dbunit.DatabaseUnitException;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.CompositeDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.SortedDataSet;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.dbunit.dataset.filter.ITableFilter;
import org.dbunit.dataset.filter.IncludeTableFilter;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.ext.db2.Db2DataTypeFactory;
import org.dbunit.ext.hsqldb.HsqldbDataTypeFactory;
import org.dbunit.ext.mysql.MySqlDataTypeFactory;
import org.dbunit.ext.oracle.OracleDataTypeFactory;
import org.dbunit.operation.DatabaseOperation;

import edu.upenn.cis.orchestra.reconciliation.BdbDataSetFactory;


/**
 * Utility methods for using DbUnit.
 * 
 * @author John Frommeyer
 * 
 */
public class DbUnitUtil {
	/**
	 * Prevent instantiation.
	 * 
	 */
	private DbUnitUtil() {}

	/**
	 * Performs the operation indicated by {@code op} with {@code dataSetFile}
	 * as the input dataset.
	 * 
	 * @param op
	 * @param dataSetFile
	 * @param dbUnitConnection
	 * @throws IOException
	 * @throws DatabaseUnitException
	 * @throws SQLException
	 */
	public static void executeDbUnitOperation(DatabaseOperation op,
			File dataSetFile, IDatabaseConnection dbUnitConnection)
			throws IOException, DatabaseUnitException, SQLException {
		IDataSet dataSet = new FlatXmlDataSet(dataSetFile);
		op.execute(dbUnitConnection, dataSet);
	}

	/**
	 * Performs the operation indicated by {@code op} with {@code dataSetFile}
	 * as the input dataset.
	 * 
	 * @param op
	 * @param dataSetFile
	 * @param dbTester
	 * @throws Exception
	 */
	public static void executeDbUnitOperation(DatabaseOperation op,
			File dataSetFile, JdbcDatabaseTester dbTester) throws Exception {
		IDatabaseConnection dbUnitConnection = getConfiguredDbUnitConnection(dbTester);
		try {
			IDataSet dataSet = new FlatXmlDataSet(dataSetFile);
			op.execute(dbUnitConnection, dataSet);
		} finally {
			dbUnitConnection.close();
		}
	}

	/**
	 * Returns a dataset representing the actual state of the database. Uses
	 * DbUnit to write an xml representation of the database.
	 * 
	 * @param dsName the desired filename of the dataset.
	 * @param tableFilter a filter for the dataset file.
	 * @param dbTester
	 * @param factory
	 * @return the actual state of the database.
	 * 
	 * @throws Exception if cannot get a connection.
	 */
	public static IDataSet dumpDataSet(String dsName, ITableFilter tableFilter,
			JdbcDatabaseTester dbTester, BdbDataSetFactory factory)
			throws Exception {

		IDatabaseConnection dbUnitConnection = getConfiguredDbUnitConnection(dbTester);
		IDataSet output = null;
		try {
			IDataSet rdb = new FilteredDataSet(tableFilter, dbUnitConnection
					.createDataSet());
			if (factory == null) {
				output = rdb;
			} else {
				IDataSet bdb = factory.getDataSet();
				output = new CompositeDataSet(rdb, bdb);
			}
			FileOutputStream out = new FileOutputStream(dsName);
			FlatXmlDataSet.write(output, out);

			// IDataSet metaData = metaDataToDataSet(output);
			// FileOutputStream mdOut = new FileOutputStream("md-" + dsName);
			// FlatXmlDataSet.write(metaData, mdOut);
		} finally {
			dbUnitConnection.close();
		}
		return output;
	}

	/**
	 * Returns a DbUnit dataset version of the metadata found in {@code output}.
	 * 
	 * @param output
	 * @return a DbUnit dataset version of the metadata found in {@code output}.
	 * @throws DataSetException
	 */
	public static IDataSet metaDataToDataSet(IDataSet output)
			throws DataSetException {
		Column name = new Column("name", DataType.VARCHAR);
		Column type = new Column("type", DataType.VARCHAR);
		Column primaryKey = new Column("primaryKey", DataType.BOOLEAN);
		Column nullable = new Column("nullable", DataType.BOOLEAN);
		DefaultTable mdTable = new DefaultTable("metadata", new Column[] {
				name, type, primaryKey, nullable });
		String[] tableNames = output.getTableNames();
		for (String tableName : tableNames) {
			ITableMetaData tableMetaData = output.getTableMetaData(tableName);
			List<Column> primaryKeys = OrchestraUtil.newArrayList(tableMetaData
					.getPrimaryKeys());
			Column[] columns = tableMetaData.getColumns();
			for (Column column : columns) {
				String cName = tableName + "." + column.getColumnName();
				String cType = column.getSqlTypeName();
				Boolean isPK = Boolean.valueOf(primaryKeys.contains(column));
				Boolean isNullable = Boolean.valueOf(column.getNullable()
						.equals(Column.NULLABLE));
				mdTable.addRow(new Object[] { cName, cType, isPK, isNullable });
			}
		}
		IDataSet md = new DefaultDataSet(mdTable);
		return md;
	}

	/**
	 * Returns an {@code IDataSet} representing the current state of the
	 * database if the assertion passes. Compares the current state of the
	 * database to the expected state (as determined by a DbUnit XML file).
	 * 
	 * @param expectedDataSetFile the expected state of the database.
	 * @param tableFilter a filter for the actual state.
	 * @param dbTester
	 * @param factory
	 * @return an {@code IDataSet} representing the current state of the
	 *         database.
	 * 
	 * @throws IOException
	 * @throws DataSetException
	 * @throws Exception
	 * @throws SQLException
	 * @throws DatabaseUnitException
	 */
	public static IDataSet checkDatabase(File expectedDataSetFile,
			ITableFilter tableFilter, JdbcDatabaseTester dbTester,
			BdbDataSetFactory factory) throws IOException, DataSetException,
			Exception, SQLException, DatabaseUnitException {
		IDataSet actual = null;
		IDataSet expected = new FlatXmlDataSet(expectedDataSetFile);
		// It appears to be necessary to get a new connection for each
		// assertion, otherwise DbUnit reports a mismatch in the expected and
		// actual number of tables. The _tableMap in DatabaseDataSet looks to be
		// cached.
		IDatabaseConnection dbUnitConnection = getConfiguredDbUnitConnection(dbTester);
		try {
			IDataSet rdb = new FilteredDataSet(tableFilter, dbUnitConnection
					.createDataSet());
			if (factory == null) {
				actual = rdb;
			} else {
				IDataSet bdb = factory.getDataSet();
				actual = new CompositeDataSet(rdb, bdb);
			}
			Assertion.assertEquals(new SortedDataSet(expected),
					new SortedDataSet(actual));
		} finally {
			dbUnitConnection.close();
		}
		return actual;
	}

	/**
	 * Returns a DbUnit {@code IDatabaseConnection} configured for {@code
	 * dbTester}.
	 * 
	 * @param dbTester
	 * @return a DbUnit {@code IDatabaseConnection} configured for {@code
	 *         dbTester}.
	 * 
	 * @throws Exception if {@code dbTester} cannot get a connection.
	 */
	public static IDatabaseConnection getConfiguredDbUnitConnection(
			JdbcDatabaseTester dbTester) throws Exception {
		IDatabaseConnection c = dbTester.getConnection();
		DatabaseConfig config = c.getConfig();
		config.setFeature("http://www.dbunit.org/features/qualifiedTableNames",
				true);
		IDataTypeFactory typeFactory = null;
		if (Config.isDB2()) {
			typeFactory = new Db2DataTypeFactory();
		} else if (Config.isHsql()) {
			typeFactory = new HsqldbDataTypeFactory();
		} else if (Config.isMYSQL()) {
			typeFactory = new MySqlDataTypeFactory();
		} else if (Config.isOracle()) {
			typeFactory = new OracleDataTypeFactory();
		}
		if (typeFactory != null) {
			config.setProperty(
					"http://www.dbunit.org/properties/datatypeFactory",
					typeFactory);
		}

		return c;
	}

	/**
	 * Returns an {@code IDataSet} representing the current state of the
	 * database. Examines the value of {@code dump} to determine if we should
	 * create a DbUnit XML DataSet file, or check the current state of database
	 * against such a file (which should already exist).
	 * 
	 * @param dataSetFile
	 * @param tableFilter a filter for actual/dumped dataset.
	 * @param dump if {@code true} then the actual dataset is dumped to {@code
	 *            dataSetFile}, otherwise it is compared to it.
	 * @param dbTester
	 * @param factory
	 * @return an {@code IDataSet} representing the current state of the
	 *         database.
	 * @throws Exception
	 */
	public static IDataSet dumpOrCheck(File dataSetFile,
			IncludeTableFilter tableFilter, boolean dump,
			JdbcDatabaseTester dbTester, BdbDataSetFactory factory)
			throws Exception {
		IDataSet currentState = null;
		if (dump) {
			//recover test-data-dir to keep things organized.
			File dataDir = new File(dataSetFile.getParentFile().getName());
			if (!dataDir.exists()) {
				dataDir.mkdir();
			}
			currentState = dumpDataSet(dataDir.getName() + "/"
					+ dataSetFile.getName(), tableFilter, dbTester, factory);
		} else {
			currentState = checkDatabase(dataSetFile, tableFilter, dbTester,
					factory);
		}
		return currentState;
	}

	/**
	 * A convenience method for calling {@code DbUnit.dumpOrCheck(...)}.
	 * 
	 * @param datasetFile
	 * @param orchestraSchema
	 * @param dumpDatasets
	 * @param dbTester
	 * @param factory
	 * @return an {@code IDataSet} representing the current state of the
	 *         database.
	 * @throws Exception
	 */
	public static IDataSet dumpOrCheck(File datasetFile,
			OrchestraSchema orchestraSchema, boolean dumpDatasets,
			JdbcDatabaseTester dbTester, BdbDataSetFactory factory)
			throws Exception {
		String[] regexps = orchestraSchema.getDbSchemaNames(true).toArray(
				new String[0]);
		return DbUnitUtil.dumpOrCheck(datasetFile, new IncludeTableFilter(
				regexps), dumpDatasets, dbTester, factory);
	}

	/**
	 * Returns a list of table names found in the database used for the test. It
	 * only returns those tables with names matching {@code tableFilter}.
	 * 
	 * @param tableFilter the filter to apply to the returned table names.
	 * @param dbTester
	 * @return a list of table names.
	 * 
	 * @throws Exception
	 */
	public static List<String> getFilteredTableNames(ITableFilter tableFilter,
			JdbcDatabaseTester dbTester) throws Exception {
		IDatabaseConnection dbUnitConnection = getConfiguredDbUnitConnection(dbTester);
		try {
			IDataSet output = dbUnitConnection.createDataSet();
			IDataSet filteredOutput = new FilteredDataSet(tableFilter, output);
			String[] tableNames = filteredOutput.getTableNames();
			return Arrays.asList(tableNames);
		} finally {
			dbUnitConnection.close();
		}
	}

	/**
	 * (Dump or Check) and Meta Check.
	 * 
	 * @param datasetFile
	 * @param checker 
	 * @param orchestraSchema
	 * @param dumpDatasets
	 * @param dbTester
	 * @param factory
	 * @return an {@code IDataSet} representing the current state of the
	 *         database.
	 * @throws Exception
	 */
	public static IDataSet dumpOrCheckAndMetaCheck(File datasetFile,
			MetaDataChecker checker, OrchestraSchema orchestraSchema,
			boolean dumpDatasets, JdbcDatabaseTester dbTester,
			BdbDataSetFactory factory) throws Exception {
		IDataSet actual = dumpOrCheck(datasetFile, orchestraSchema,
				dumpDatasets, dbTester, factory);

		checker.check(orchestraSchema, actual);
		return actual;
	}
}
