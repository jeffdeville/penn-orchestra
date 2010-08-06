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

import java.io.Reader;

/**
 * Parse up the SQL from a {@code Reader}.
 * 
 * @author Sam Donnelly
 */
public interface ISqlParser {

	/**
	 * Parse a set of SQL Statements from the parser's input stream. The
	 * available statements are returned one at a time by this method.
	 * 
	 * @return the next {@code ISqlStatement} on the stream
	 * 
	 * @throws ParseException if the statements in {@code initParser.reader} are
	 *             not well formed.
	 */
	ISqlStatement readStatement() throws ParseException;

	/**
	 * Initialize (or re-initialize) the input stream for the parser.
	 * 
	 * @param reader that which we're parsing.
	 */
	void initParser(Reader reader);
}
