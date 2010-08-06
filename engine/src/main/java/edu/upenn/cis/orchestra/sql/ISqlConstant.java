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
 * SQL constants.
 * 
 * @author Sam Donnelly
 */
public interface ISqlConstant extends ISqlExp {

	/** {@code ISqlConstant} types. */
	public static enum Type {

		/** Unknown. */
		UNKNOWN("UNKNOWN"),

		/** Column name. */
		COLUMNNAME("COLUMNNAME"),

		/** SQL {code NULL}. */
		NULL("NULL"),

		/** An SQL number. */
		NUMBER("NUMBER"),

		/** An SQL string. */
		STRING("STRING"),

		/** A labeled null. */
		LABELEDNULL("LABELEDNULL"),

		/** An SQL date. */
		DATE("DATE"),
		
		/** A JDBC prepared statement parameter. */
		PREPARED_STATEMENT_PARAMETER("PREPARED_STATEMENT_PARAMETER");
		
		/** String representation of this {@code Type}. */
		private final String _type;

		/**
		 * Construct a {@code Type} with the given type string.
		 * 
		 * @param type the {@code toString()} value
		 */
		private Type(String type) {
			_type = type;
		}

		/**
		 * String representation of this {@code Type}.
		 * 
		 * @return string representation of this {@code Type}
		 */
		@Override
		public String toString() {
			return _type;
		}
	}

	
	/**
	 * Get this constant's value.
	 * 
	 * @return this constant's value.
	 */
	String getValue();

	/**
	 * Get this constant's type.
	 * 
	 * @return this constant type.
	 */
	Type getType();

}