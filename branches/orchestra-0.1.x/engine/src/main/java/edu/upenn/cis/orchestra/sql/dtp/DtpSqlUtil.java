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

import org.eclipse.datatools.modelbase.sql.query.QueryStatement;
import org.eclipse.datatools.modelbase.sql.query.helper.StatementHelper;
import org.eclipse.datatools.modelbase.sql.query.util.SQLQuerySourceFormat;
import org.eclipse.datatools.sqltools.parsers.sql.SQLParserException;
import org.eclipse.datatools.sqltools.parsers.sql.SQLParserInternalException;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParseResult;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParserManager;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParserManagerProvider;

import edu.upenn.cis.orchestra.sql.ISqlUtil;

/**
 * DTP-backed {@code ISqlUtil}.
 * 
 * @author Sam Donnelly
 */
public class DtpSqlUtil implements ISqlUtil {

	/** {@inheritDoc} */
	@Override
	public String normalizeSqlFragment(String sqlFragment) {
		return StatementHelper.stripWhiteSpace(upcaseNonDelimitedText(
				StatementHelper.removeCommentsInSQL(sqlFragment, '"'), '"'),
				'"');
	}

	/** {@inheritDoc} */
	@Override
	public String upcaseNonDelimitedText(final String sql, final char delimiter) {
		final StringBuilder sqlUpperCased = new StringBuilder();
		boolean withinDelimetedText = false;
		for (int i = 0; i < sql.length(); i++) {
			final char charAtI = sql.charAt(i);
			if (charAtI == delimiter) {
				withinDelimetedText = !withinDelimetedText;
			}
			if (withinDelimetedText) {
				sqlUpperCased.append(charAtI);
			} else {
				sqlUpperCased.append(Character.toUpperCase(charAtI));
			}
		}
		return sqlUpperCased.toString();
	}

	/** {@inheritDoc} */
	@Override
	public String normalizeSqlStatement(String sqlStatement) {
		final SQLQueryParserManager parserManager = SQLQueryParserManagerProvider
				.getInstance().getParserManager(null, null);

		SQLQueryParseResult parseResultNew = null;
		try {
			parseResultNew = parserManager.parseQuery(sqlStatement);
		} catch (SQLParserException e) {
			throw new RuntimeException(e);
		} catch (SQLParserInternalException e) {
			throw new RuntimeException(e);
		}
		final QueryStatement queryStatement = parseResultNew
				.getQueryStatement();
		queryStatement.getSourceInfo().getSqlFormat().setQualifyIdentifiers(
				SQLQuerySourceFormat.QUALIFY_IDENTIFIERS_WITH_SCHEMA_NAMES);
		return normalizeSqlFragment(queryStatement.getSQL());
	}

}
