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

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.List;

import org.eclipse.datatools.modelbase.sql.query.QueryDeleteStatement;
import org.eclipse.datatools.modelbase.sql.query.QueryInsertStatement;
import org.eclipse.datatools.modelbase.sql.query.QuerySelectStatement;
import org.eclipse.datatools.modelbase.sql.statements.SQLStatement;
import org.eclipse.datatools.sqltools.parsers.sql.SQLParseResult;
import org.eclipse.datatools.sqltools.parsers.sql.SQLParserException;
import org.eclipse.datatools.sqltools.parsers.sql.SQLParserInternalException;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParserManager;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParserManagerProvider;

import edu.upenn.cis.orchestra.sql.ISqlParser;
import edu.upenn.cis.orchestra.sql.ISqlStatement;
import edu.upenn.cis.orchestra.sql.ParseException;

/**
 * An {@code ISqlParser} implemented with DTP classes.
 * <p>
 * NOTE: this implementation does not support DDL statements.
 * 
 * @author Sam Donnelly
 */
class DtpSqlParser implements ISqlParser {

	/** That which we are parsing. */
	private Reader _reader;

	/** With which we'll do our parsing. */
	private static final SQLQueryParserManager _parserManager = SQLQueryParserManagerProvider
			.getInstance().getParserManager(null, null);

	/** How we iterate over the stored results of a parse. */
	private Iterator<ISqlStatement> _sqlStatementsItr = null;

	/** {@inheritDoc} */
	@Override
	public ISqlStatement readStatement() throws ParseException {
		if (_sqlStatementsItr != null) {
			if (_sqlStatementsItr.hasNext()) {
				return _sqlStatementsItr.next();
			} else {
				return null;
			}
		}
		final StringBuilder sb = new StringBuilder();
		int c = -1;
		try {
			while ((c = _reader.read()) != -1) {
				sb.append((char) c);
			}
		} catch (IOException e) {
			throw new ParseException(e);
		}
		try {
			@SuppressWarnings("unchecked")
			final List<Object> sqlStatements = _parserManager.parseScript(sb
					.toString());
			final List<ISqlStatement> dtpSqlStatements = newArrayList();
			for (Object o : sqlStatements) {
				final SQLStatement sqlStatement = ((SQLParseResult) o)
						.getSQLStatement();
				if (sqlStatement instanceof QueryDeleteStatement) {
					dtpSqlStatements.add(new DtpSqlDelete(
							(QueryDeleteStatement) sqlStatement));
				} else if (sqlStatement instanceof QuerySelectStatement) {
					dtpSqlStatements.add(new DtpSqlSelect(
							(QuerySelectStatement) sqlStatement));
				} else if (sqlStatement instanceof QueryInsertStatement) {
					dtpSqlStatements.add(new DtpSqlInsert(
							(QueryInsertStatement) sqlStatement));
				} else {
					throw new IllegalStateException(
							"Shouldn't have gotten here: sqlStatement is an unsupported type: "
									+ sqlStatement.getClass());
				}
			}
			_sqlStatementsItr = dtpSqlStatements.iterator();
			return _sqlStatementsItr.next();
		} catch (SQLParserException e) {
			throw new ParseException(e);
		} catch (SQLParserInternalException e) {
			throw new ParseException(e);
		}
	}

	/** {@inheritDoc} */
	@Override
	public void initParser(final Reader reader) {
		_reader = reader;
		_sqlStatementsItr = null;
	}
}
