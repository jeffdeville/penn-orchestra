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
package edu.upenn.cis.orchestra.obsolete;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicatePeerIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateSchemaIdException;
import edu.upenn.cis.orchestra.reconciliation.bdbstore.SchemaIDBinding;
import edu.upenn.cis.orchestra.repository.utils.loader.exceptions.SchemaLoaderException;

public class TestConverter extends TestCase {

	
	/*
	private DataSource _provDs;
	
	public void setUp ()
	{
    	ApplicationContext ctx = new ClassPathXmlApplicationContext ("edu/upenn/cis/orchestra/repository/utils/SpringConfig.xml");
		
		_provDs = (DataSource) ctx.getBean("provenanceConn");
		
		
		
	}
	
	public void testManual () throws SchemaLoaderException
	{
		SchemaLoaderJdbc loader = new SchemaLoaderJdbc (_provDs);
		Schema sc = new Schema ("schemaProv", "schemaProv");
		loader.addToSchema(sc, null, "PROVENANCE", null);
		System.out.println (sc.toString());
		
		SchemaConverterJDBC conv = new SchemaConverterJDBC (_provDs, sc);
		conv.runConversion(true);
		
	}
	*/
	
	private DataSource _orchestraTestDs;
	
	
	@Override
	@Before
	public void setUp ()
	{
    	ApplicationContext ctx = new ClassPathXmlApplicationContext ("edu/upenn/cis/orchestra/repository/utils/SpringConfig.xml");
		
    	_orchestraTestDs = (DataSource) ctx.getBean("orchestraDB2TestConn");
	}
	
	@Test
	public void testLoadSpoolAndConvertDatabase () 
		throws SchemaLoaderException, DuplicateSchemaIdException, DuplicatePeerIdException, SQLException,
		DatabaseException
				
	{
		File f = new File("dbenv");
		if (f.exists()) {
			File[] files = f.listFiles();
			for (File file : files) {
				file.delete();
			}
		} else {
			f.mkdir();
		}
		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setAllowCreate(true);
		ec.setTransactional(true);
		Environment e = new Environment(f, ec);
		List<Schema> schemas = new ArrayList<Schema>();
		Map<AbstractPeerID,Integer> peerMap = new HashMap<AbstractPeerID,Integer>();
		
		SchemaIDBinding scm = new SchemaIDBinding(e); 
		Peer p = new Peer ("dbserv", "localhost:50000", "DB group database server");
		
		//SchemaLoaderJdbc loader = new SchemaLoaderJdbc (_orchestraTestDs);
		Schema sc = new Schema ("schema1", "This is schema 1");
		//loader.addToSchema(sc, null, "DB2USER", null);		
		p.addSchema(sc);
		
		sc = new Schema ("schema2", "This is schema 2");
		//loader.addToSchema(sc, null, "SCHEMA2", null);		
		p.addSchema(sc);
		
		schemas.addAll(p.getSchemas());
		scm.registerAllSchemas("test", schemas, peerMap);
		peerMap.put(p.getPeerId(), 0);
		OrchestraSystem system = new OrchestraSystem (scm);
		system.addPeer(p);
		
		System.out.println (system.toString());
		
		for (Schema sc2 : p.getSchemas())
		{
			SchemaConverterJDBC conv = new SchemaConverterJDBC (_orchestraTestDs, "db2", sc2);
			conv.runConversion(true, true, "db2", false, system.isBidirectional());
		}
		
	}
	
}
