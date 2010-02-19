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
package edu.upenn.cis.orchestra.repository.dao.flatfile;

import java.io.File;
import java.io.FileInputStream;

import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.w3c.dom.Document;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.repository.dao.RepositorySchemaDAOWithMemoryCache;


/**
 * DAO implementation using a flat file to load the system in memory.
 * TODO: Add an option to constructor to write the new system in a file when unloaded 
 * 
 * @author Olivier Biton
 *
 */
public class FlatFileRepositoryDAO extends RepositorySchemaDAOWithMemoryCache {

	
	public FlatFileRepositoryDAO (String filePath)
			throws Exception
	{
		DefaultResourceLoader loader = new DefaultResourceLoader();
		File file = new File(filePath);
		if (!file.exists()) {
			Resource ress = loader.getResource(filePath);
			file = ress.getFile();
		}
		FileInputStream stream = new FileInputStream(file);
		setSystem(OrchestraSystem.deserialize(stream));

	}

	/**
	 * Load the system from a {@code Document}.
	 * 
	 * @param document a {@code Document} representing an Orchestra schema file.
	 * @throws Exception if cannot deserialize {@code document}.
	 */
	public FlatFileRepositoryDAO(Document document) throws Exception {
		setSystem(OrchestraSystem.deserialize(document));
	}	
}
