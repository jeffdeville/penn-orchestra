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
package edu.upenn.cis.orchestra.repository.dao;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;

/**
 * Abstract implementation of the repository DAO, to extend for any DAO
 * implementation that uses an in-memory cache of the repository objects.
 * 
 * @author Olivier Biton
 */
public class RepositorySchemaDAOWithMemoryCache implements RepositorySchemaDAO {
	protected OrchestraSystem _inMemoryRepos = null;

	protected void setSystem(OrchestraSystem system) {
		_inMemoryRepos = system;
	}

	public OrchestraSystem loadAllPeers() {
		return _inMemoryRepos;
	}
}
