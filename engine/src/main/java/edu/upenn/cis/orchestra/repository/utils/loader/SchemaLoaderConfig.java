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

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 * Implementation of <code>ISchemaLoaderConfig</code> that gets its config info
 * from an XML file which it expects to be returned by {@code SchemaLoaderConfig.class.getResourceAsStream("SchemaLoaderConfig.xml").
 * 
 * @author Sam Donnelly
 */
public class SchemaLoaderConfig implements ISchemaLoaderConfig {

	/**
	 * The configuration file.
	 */
	private Document _config;

	/**
	 * Construct a <code>SchemaLoaderConfig</code>.
	 */
	public SchemaLoaderConfig() {
		try {
			InputStream configFileStream = SchemaLoaderConfig.class
					.getResourceAsStream("SchemaLoaderConfig.xml");
			_config = new SAXReader().read(new InputStreamReader(
					configFileStream));
		} catch (DocumentException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean filterOnSchemaName(String databaseProductName,
			String schemaName) {
		// Look for any SCHEMA Exclude's with @name ==
		// schemaName.
		if (0 < (_config
				.selectNodes("/SchemaLoaderConfig/DBSpecific/DB[@productName='"
						+ databaseProductName
						+ "']/Excludes/Exclude[@type='SCHEMA' and @name='"
						+ schemaName + "']").size())) {
			return false;
		}
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean filterOnTableName(String tableName) {
		for (Iterator<?> itr = _config
				.selectNodes(
						"/SchemaLoaderConfig/DBGeneric/Excludes/Exclude[@type='TABLE']")
				.iterator(); itr.hasNext();) {
			if (tableName
					.matches(((Element) itr.next()).attributeValue("name"))) {
				return false;
			}
		}
		return true;
	}

}
