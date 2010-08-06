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

import java.io.FileOutputStream;

import javax.sql.DataSource;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;

/**
 * Schema loader to be called from a console.
 * 
 * @author Sam Donnelly
 */
public class SchemaLoaderCLI {

	/**
	 * Run it.
	 * 
	 * @param args <p>
	 *            {@code [0]} the name of the bean to use in
	 *            "edu/upenn/cis/orchestra/repository/utils/loader/SpringConfig.xml"
	 *            <p>
	 *            {@code [1]} name for the output file. Name should be {@code
	 *            X.extension}. The {@code X} will be used in the output file
	 *            for the value of {@code catalog/@name}
	 */
	public static void main(String[] args) throws Throwable {
		String beanId = args[0], outputFile = args[1];
		ApplicationContext ctx = new ClassPathXmlApplicationContext(
				"edu/upenn/cis/orchestra/repository/utils/loader/SpringConfig.xml");

		DataSource ds = (DataSource) ctx.getBean(beanId);
		ISchemaLoader loader = new SchemaLoaderJdbc(ds,
				new SchemaLoaderConfig());
		OrchestraSystem orchestraSystem = loader.buildSystem(outputFile
				.substring(0, outputFile.indexOf('.')), null, null, null);
		orchestraSystem.serialize(new FileOutputStream(outputFile));
	}
}
