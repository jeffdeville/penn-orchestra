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
 * The <code>FROM</code> clause of a <code>SELECT</code> statement.
 * 
 * @author Sam Donnelly
 */
public interface ISqlFromItem extends ISqlAliasedName {

	/** The types of join. */
	public enum Join {
		/** No join. */
		// NONE,
		/** Inner join. */
		INNERJOIN,
		/** Natural join. */
		// NATURALJOIN,
		/** Left outer join. */
		LEFTOUTERJOIN,
		/** Right outer join. */
		RIGHTOUTERJOIN,
		/** Full outer join. */
		FULLOUTERJOIN
	}

}