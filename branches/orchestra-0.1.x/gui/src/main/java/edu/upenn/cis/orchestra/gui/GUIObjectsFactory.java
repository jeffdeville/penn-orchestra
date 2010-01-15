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
package edu.upenn.cis.orchestra.gui;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.upenn.cis.orchestra.repository.dao.RepositorySchemaDAO;

public class GUIObjectsFactory {

	private static GUIObjectsFactory _singleton=null;
	
	private final ApplicationContext _ctx = new ClassPathXmlApplicationContext ("edu/upenn/cis/orchestra/gui/SpringConfig.xml");
	
	private RepositorySchemaDAO _reposDAO = null;
	
	
	
	/**
	 * This is a singleton, use getInstance() instead
	 *
	 */ 
	private GUIObjectsFactory ()
	{		
	}
	
	/**
	 * Get the factory singleton
	 * @return
	 */
	public static GUIObjectsFactory getInstance ()
	{
		if (_singleton == null)
			_singleton = new GUIObjectsFactory ();
		return _singleton;
	}
	
	/**
	 * Get the default repository DAO.
	 * On first call this is loaded using Spring, then the same object is cached and returned.
	 * @return Default repository DAO object
	 */
	public RepositorySchemaDAO getRepositoryDAO ()
	{
		if (_reposDAO == null)
			_reposDAO = (RepositorySchemaDAO) _ctx.getBean("defaultReposDAO");
		return _reposDAO;
	}
	
	
}
