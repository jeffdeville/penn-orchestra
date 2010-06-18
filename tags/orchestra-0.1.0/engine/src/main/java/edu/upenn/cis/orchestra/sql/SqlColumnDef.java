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
 * A simple container {@code ISqlColumnDef}.
 * <p>
 * Intentionally package-private.
 * 
 * @author Sam Donnelly
 */
class SqlColumnDef implements ISqlColumnDef {

	/** The name of the column. */
	private final String _name;

	/** The type of the column. */
	private final String _type;

	/** The default value of the column. */
	private final String _defaultValue;

	/**
	 * Create a column definition with a column name and type.
	 * <p>
	 * {@code type} will just be passed straight through to the create statement
	 * as free text.
	 * 
	 * @param name the name of the column.
	 * @param type the type of column.
	 */
	SqlColumnDef(final String name, final String type) {
		this(name, type, null);
	}

	/**
	 * Create a column definition with a column name and type.
	 * <p>
	 * {@code type} will just be passed straight through to the create statement
	 * as free text.
	 * 
	 * @param name the name of the column.
	 * @param type the type of column.
	 * @param defaultValue the default value of the column.
	 */
	SqlColumnDef(final String name, final String type, final String defaultValue) {
		_name = name;
		_type = type;
		_defaultValue = defaultValue;
	}

	/** {@inheritDoc} */
	@Override
	public String toString() {
		return new StringBuilder(_name).append(" ").append(_type).toString();
	}

	/** {@inheritDoc} */
	@Override
	public String getDefault() {
		return _defaultValue;
	}

	/** {@inheritDoc} */
	@Override
	public String getName() {
		return _name;
	}

	/** {@inheritDoc} */
	@Override
	public String getType() {
		return _type;
	}

}