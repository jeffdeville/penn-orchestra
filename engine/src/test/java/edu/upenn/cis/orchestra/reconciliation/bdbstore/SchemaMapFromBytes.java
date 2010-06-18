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
package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import java.io.IOException;

import edu.upenn.cis.orchestra.reconciliation.bdbstore.SchemaIDBinding.SchemaMap;

/**
 * Helper for {@code BdbDataSetFactory}. Restores a {@code StaticMap} instance
 * from a {@code byte} array.
 * 
 * @author John Frommeyer
 * 
 */
public class SchemaMapFromBytes {
	/**
	 * Returns a {@code String} representation of the {@code SchemaMap} instance
	 * held in {@code bytes}.
	 * 
	 * @param bytes
	 * @return a {@code String} representation of a {@code SchemaMap}.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static String readSchemaMap(byte[] bytes) throws IOException,
			ClassNotFoundException {
		Object o = BdbDataSetFactory.readObject(bytes);
		SchemaMap map = (SchemaMap) o;
		StringBuffer sb = new StringBuffer();
		sb.append("Peer to Schema Map: " + map._peerSchemaMap);
		sb.append("\nSchema to Relation to Integer Map: "
				+ map._schemaRelationIdMap);
		return sb.toString();
	}
}
