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

/**
 * {@code edu.upenn.cis.orchestra.sql} utilities.
 * 
 * @author Sam Donnelly
 */
public interface ISqlUtil {

	/**
	 * As much as possible, put {@code sqlStatement} into a standardized format.
	 * The specific format will be implementation specific, but that's okay: the
	 * purpose of this method is to facilitate comparison of SQL statements
	 * normalized through the same implementation.
	 * 
	 * @param sqlStatement
	 *            to be normalized.
	 * 
	 * @return normalized {@code sqlStatement}
	 */
	String normalizeSqlStatement(String sqlStatement);

	/**
	 * Strips out extraneous white space from the input, eliminates comments,
	 * and upcases non-{@code "'"} delimited text.
	 * <p>
	 * Implementations of this method should not require the input to be a
	 * complete SQL statement.
	 * 
	 * @param sqlFragment
	 *            input SQL fragment
	 * 
	 * @return {@code sqlFragment} with no extra white space and no comments
	 */
	String normalizeSqlFragment(String sqlFragment);

	/**
	 * Upcases the characters in the given string except the parts that are
	 * delimited by {@code delimiter}.
	 * 
	 * @param s
	 *            input
	 * @param delimiter
	 *            the delimiter
	 * @return {@code s} all upcased, except the delimited parts which remains
	 *         untouched
	 */
	String upcaseNonDelimitedText(final String s, final char delimiter);

}
