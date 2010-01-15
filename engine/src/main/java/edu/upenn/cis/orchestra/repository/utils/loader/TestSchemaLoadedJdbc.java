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

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.util.DomUtils;

public class TestSchemaLoadedJdbc {

	public static void main(String[] args) throws Throwable {

		System.out.println("M: " + Relation.isMappingTableName("M"));
		System.out.println("M0: " + Relation.isMappingTableName("M0"));
		System.out.println("M34: " + Relation.isMappingTableName("M34"));
		System.out.println("M9485M: " + Relation.isMappingTableName("M9485M"));
		System.out.println("someString: "
				+ Relation.isMappingTableName("someString"));

		ApplicationContext ctx = new ClassPathXmlApplicationContext(
				"edu/upenn/cis/orchestra/repository/utils/loader/SpringConfig.xml");
		// Eventually want we'd want is a mapping from schema to peers.
		// I suppose we could pass it right in through the addToSchema method.
		// What about the creation of peers.
//		DataSource ds = (DataSource) ctx.getBean("db2Conn");
		//DataSource ds = (DataSource) ctx.getBean("mysqlConn");
		// DataSource ds = (DataSource) ctx.getBean("mysqlConnLocal");
		DataSource ds = (DataSource) ctx.getBean("ppod");
		SchemaLoaderJdbc loader = new SchemaLoaderJdbc(ds,
				new SchemaLoaderConfig());

		Schema schTest = new Schema("loadedSchema",
				"Schema loaded with JDBC loader demo");

		// try
		// {
		List<Peer> peers = new ArrayList<Peer>();
		List<Schema> schemas = new ArrayList<Schema>();
		loader.buildSystem(null, null, null, null);
		for (Schema schema : schemas) {
			System.out.println(schema.toString());
			DocumentBuilderFactory builderFactory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder = builderFactory.newDocumentBuilder();
			Document doc = builder.newDocument();
			Element cat = doc.createElement("schema");
			doc.appendChild(cat);
			schema.serialize(doc, doc.getDocumentElement());
			DomUtils.write(doc, System.out);
		}
		// } catch (Throwable ex)
		// {
		// System.out.println ("Exception while loading schema: " +
		// ex.toString());
		// }

	}

}
