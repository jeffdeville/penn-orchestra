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
package edu.upenn.cis.orchestra.reconciliation.p2pstore;


import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.TrustConditions;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient;
import edu.upenn.cis.orchestra.reconciliation.USDump;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;

public class PastryStoreFactory implements UpdateStore.Factory {
	PastryNodeFactory nf;
	rice.environment.Environment env;
	Environment dbEnv;
	String epochDbName;
	String transactionDbName;
	String reconciliationDbName;
	String decisionDbName;
	String pastryIdsDbName;
	int replicationFactor;
	
	int createdCount = 0;

	List<P2PStore> stores = new ArrayList<P2PStore>();
	
	// Is the factory creating a local update store?  Here we say yes --
	// there is a local object representing the factory
	public boolean isLocal() {
		return true;
	}
	
	public PastryStoreFactory(int port, Environment dbEnv, int replicationFactor,
			String epochDbName, String transactionDbName,
			String reconciliationDbName, String decisionDbName, String pastryIdsDbName) {
		env = new rice.environment.Environment();
		// Queue up a lot of messages, if necessary
		env.getParameters().setInt("pastry_socket_writer_max_queue_length", 50000);
		// Replication interval, in milliseconds
		env.getParameters().setInt("p2p_replication_maintenance_interval", 100000);
		// Buffer messages that can't be delivered
		env.getParameters().setBoolean("pastry_messageDispatch_bufferIfNotReady", true);
		// Buffer this many messages before we start dropping messages
		env.getParameters().setInt("pastry_messageDispatch_bufferSize", 128);
		this.dbEnv = dbEnv;
		this.epochDbName = epochDbName;
		this.transactionDbName = transactionDbName;
		this.reconciliationDbName = reconciliationDbName;
		this.decisionDbName = decisionDbName;
		this.pastryIdsDbName = pastryIdsDbName;
		this.replicationFactor = replicationFactor;
		nf = new PastryNodeFactory(env, port);
	}
	
	public void shutdownEnvironment() {
		env.destroy();
	}
	
	public P2PStore getUpdateStore(AbstractPeerID pid, ISchemaIDBinding sch, Schema s, TrustConditions tc) throws USException {
		++createdCount;
		P2PStore retval = new P2PStore(nf, pid, tc, sch, s, dbEnv, replicationFactor,
				epochDbName + createdCount, transactionDbName + createdCount,
				reconciliationDbName + createdCount, decisionDbName + createdCount,
				pastryIdsDbName + createdCount);
		stores.add(retval);
		return retval;
	}

	public void serialize(Document doc, Element update) {
		update.setAttribute("type", "p2p");
		update.setAttribute("port", Integer.toString(nf.port));
		try {
			update.setAttribute("workdir", dbEnv.getHome().getPath());
		} catch (DatabaseException e) {
			assert(false);	// shouldn't happen
		}
		update.setAttribute("replicationFactor", Integer.toString(replicationFactor));
		update.setAttribute("epochDbName", epochDbName);
		update.setAttribute("transactionDbName", transactionDbName);
		update.setAttribute("reconciliationDbName", reconciliationDbName);
		update.setAttribute("decisionDbName", decisionDbName);
		update.setAttribute("pastryIdsDbName", pastryIdsDbName);
	}

	static public PastryStoreFactory deserialize(Element update) throws DatabaseException {
		int port = Integer.parseInt(update.getAttribute("port"));
		String workdir = update.getAttribute("workdir");
		Environment dbEnv = new Environment(new File(workdir), null);
		int replicationFactor = Integer.parseInt(update.getAttribute("replicationFactor"));
		String epochDbName = update.getAttribute("epochDbName");
		String transactionDbName = update.getAttribute("transactionDbName");
		String reconciliationDbName = update.getAttribute("reconciliationDbName");
		String decisionDbName = update.getAttribute("decisionDbName");
		String pastryIdsDbName = update.getAttribute("pastryIdsDbName");
		return new PastryStoreFactory(port, dbEnv, replicationFactor, epochDbName, transactionDbName, 
				reconciliationDbName, decisionDbName, pastryIdsDbName);
	}

	public void resetStore(Schema s) throws USException {
		throw new UnsupportedOperationException("reset is not supported for P2PStores (yet)");
	}

	public USDump dumpUpdateStore(ISchemaIDBinding binding, Schema s) throws USException {
		throw new UnsupportedOperationException();
	}

	public void restoreUpdateStore(USDump d) throws USException {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#getSchemaIDBindingClient(edu.upenn.cis.orchestra.datamodel.AbstractPeerID, edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding)
	 */
	@Override
	public ISchemaIDBindingClient getSchemaIDBindingClient() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#startUpdateStoreServer()
	 */
	@Override
	public void startUpdateStoreServer() throws USException {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#stopUpdateStoreServer()
	 */
	@Override
	public void stopUpdateStoreServer() throws USException {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#updateStoreServerIsRunning()
	 */
	@Override
	public boolean updateStoreServerIsRunning() {
		// TODO Auto-generated method stub
		return false;
	}
}
