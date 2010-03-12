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

import static edu.upenn.cis.orchestra.TestUtil.FAST_TESTNG_GROUP;
import static edu.upenn.cis.orchestra.util.DomUtils.createDocument;
import static edu.upenn.cis.orchestra.util.DomUtils.getChildElementByName;
import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;
import edu.upenn.cis.orchestra.util.XMLParseException;

/**
 * DOCUMENT ME
 * 
 * @author John Frommeyer
 * 
 */
@Test(groups = { FAST_TESTNG_GROUP  /*,DEV_TESTNG_GROUP*/})
public class StartStopServerTest {
	private Document orchestraSchema;
	private Element updateStoreElement;
	private UpdateStore.Factory usFactory;
	private Process server;

	/**
	 * Setup common objects.
	 * 
	 * @throws IOException
	 * @throws XMLParseException
	 */
	@BeforeClass
	public void readSchema() throws IOException, XMLParseException {
		InputStream in = Config.class
				.getResourceAsStream("ppodLN/ppodLNHash.schema");
		orchestraSchema = createDocument(in);
		in.close();
		Element store = getChildElementByName(orchestraSchema
				.getDocumentElement(), "store");
		updateStoreElement = getChildElementByName(store, "update");
		usFactory = UpdateStore.deserialize(updateStoreElement);
	}

	/**
	 * Start the server.
	 * 
	 * @throws USException
	 * @throws IOException
	 * 
	 */
	public void startTest() throws USException, IOException {
		usFactory.startUpdateStoreServer();
		// startUpdateStoreServerExec();
		/*server = TestUtil
				.startUpdateStoreServerExec(
						9999,
						"updateStore_env",
						".");*/
		usFactory.resetStore(null);
		ISchemaIDBindingClient client = usFactory.getSchemaIDBindingClient();
		client.reconnect();
		Set<String> hostedSystems = client.getHostedSystems();
		client.disconnect();
		assertEquals(hostedSystems.size(), 0);
	}

	/**
	 * Stop the server.
	 * 
	 * @throws USException
	 * @throws InterruptedException 
	 * @throws IOException 
	 * 
	 */
	// @Test(dependsOnMethods = "startTest", groups = { FAST_TESTNG_GROUP,
	// DEV_TESTNG_GROUP })
	@AfterClass
	public void stopTest() throws USException, InterruptedException, IOException {
		/*BufferedReader reader = new BufferedReader(new InputStreamReader(server.getInputStream()));
		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				System.out.println(line);
			}
		} catch (IOException e) {
			System.err.println("Error reading output from update store process.");
		}
		reader.close();
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(server.getOutputStream()));
		writer.append("q\n");
		writer.flush();
		writer.close();*/
		//server.destroy();
		
		usFactory.stopUpdateStoreServer();
		//server.waitFor();
	}

	private static void main(String[] args) throws IOException,
			XMLParseException, USException, InterruptedException {
		StartStopServerTest test = new StartStopServerTest();
		test.readSchema();
		test.startTest();
		test.stopTest();

		/*TestListenerAdapter tla = new TestListenerAdapter();
		TestNG testng = new TestNG();
		testng.setTestClasses(new Class[] { StartStopServerTest.class });
		testng.addListener(tla);
		testng.run();*/

	}
}
