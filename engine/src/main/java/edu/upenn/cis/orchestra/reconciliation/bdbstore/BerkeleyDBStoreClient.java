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

import static edu.upenn.cis.orchestra.OrchestraUtil.newArrayList;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.TrustConditions;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.iterators.FlatteningIterator;
import edu.upenn.cis.orchestra.datamodel.iterators.IntegerIterator;
import edu.upenn.cis.orchestra.datamodel.iterators.IteratorException;
import edu.upenn.cis.orchestra.datamodel.iterators.ListIteratorResultIterator;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;
import edu.upenn.cis.orchestra.reconciliation.Benchmark;
import edu.upenn.cis.orchestra.reconciliation.Decision;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBindingClient;
import edu.upenn.cis.orchestra.reconciliation.ReconciliationEpoch;
import edu.upenn.cis.orchestra.reconciliation.StateStore;
import edu.upenn.cis.orchestra.reconciliation.TxnChain;
import edu.upenn.cis.orchestra.reconciliation.USDump;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore;

public class BerkeleyDBStoreClient extends UpdateStore {
	private InetSocketAddress host;
	private Socket socket;
	private ObjectOutputStream oos;
	private ObjectInputStream ois;
	private Map<AbstractPeerID, Schema> schemas;
	private TrustConditions tc;
	private Benchmark b;
	private ISchemaIDBinding _mapStore;

	private final static Logger logger = LoggerFactory
			.getLogger(BerkeleyDBStoreClient.class);

	public static class Factory implements UpdateStore.Factory {
		public final InetSocketAddress host;

		public Factory(InetSocketAddress host) {
			this.host = host;
		}

		// Is the factory creating a local update store?
		public boolean isLocal() {
			try {
				return host.getHostName().equals("localhost")
						|| host.getAddress().equals(InetAddress.getLocalHost());
			} catch (UnknownHostException u) {
				return false;
			}
		}

		public UpdateStore getUpdateStore(AbstractPeerID pid,
				ISchemaIDBinding sch, Schema s, TrustConditions tc)
				throws USException {
			if (!pid.equals(tc.getOwner())) {
				throw new IllegalArgumentException("Supplied peer ID " + pid
						+ " does not match owner of trust conditions "
						+ tc.getOwner());
			}
			return new BerkeleyDBStoreClient(host, sch, s, tc);// s, tc);
		}

		public void serialize(Document doc, Element update) {
			update.setAttribute("type", "bdb");
			update.setAttribute("hostname", host.getHostName());
			update.setAttribute("port", Integer.toString(host.getPort()));
		}

		static public Factory deserialize(Element update) {
			String hostname = update.getAttribute("hostname");
			int port = Integer.parseInt(update.getAttribute("port"));
			InetSocketAddress address = new InetSocketAddress(hostname, port);
			return new Factory(address);
		}

		public void resetStore(Schema s) throws USException {
			try {
				Socket socket = new Socket(host.getAddress(), host.getPort());
				ObjectOutputStream oos = new ObjectOutputStream(socket
						.getOutputStream());
				ObjectInputStream ois = new ObjectInputStream(socket
						.getInputStream());
				oos.writeObject(new Reset());
				oos.writeObject(new EndOfStreamMsg());
				oos.flush();
				Object response = ois.readObject();

				oos.close();
				socket.close();
				if (response instanceof Ack) {
					return;
				} else if (response instanceof Exception) {
					throw new USException(
							"Error restoring BDB store from dump",
							(Exception) response);
				} else {
					throw new USException("Recevied unexpected reply of type "
							+ response.getClass().getName() + ": " + response);
				}

			} catch (IOException ioe) {
				throw new USException("Error resetting BDB store", ioe);
			} catch (ClassNotFoundException e) {
				throw new USException(
						"Could not deserialize response from BDB store", e);
			}
		}

		public USDump dumpUpdateStore(ISchemaIDBinding binding, Schema schema)
				throws USException {
			try {
				Socket socket = new Socket(host.getAddress(), host.getPort());
				ObjectOutputStream oos = new ObjectOutputStream(socket
						.getOutputStream());
				ObjectInputStream ois = new ObjectInputStream(socket
						.getInputStream());
				oos.writeObject(new DumpMsg(schema));
				oos.writeObject(new EndOfStreamMsg());
				oos.flush();

				Object response = ois.readObject();
				ois.close();
				oos.close();
				socket.close();
				if (response instanceof USDump) {
					return (USDump) response;
				} else if (response instanceof Exception) {
					throw new USException("Error dumping BDB store",
							(Exception) response);
				} else {
					throw new USException("Recevied unexpected reply of type "
							+ response.getClass().getName() + ": " + response);
				}
			} catch (IOException ioe) {
				throw new USException("Error dumping BDB store", ioe);
			} catch (ClassNotFoundException e) {
				throw new USException(
						"Could not deserialize response from BDB store", e);
			}
		}

		public void restoreUpdateStore(USDump d) throws USException {
			try {
				Socket socket = new Socket(host.getAddress(), host.getPort());
				ObjectOutputStream oos = new ObjectOutputStream(socket
						.getOutputStream());
				ObjectInputStream ois = new ObjectInputStream(socket
						.getInputStream());
				oos.writeObject(new Reset(d));
				oos.flush();

				Object response = ois.readObject();
				oos.close();
				ois.close();
				socket.close();
				if (response instanceof Ack) {
					return;
				} else if (response instanceof Exception) {
					throw new USException(
							"Error restoring BDB store from dump",
							(Exception) response);
				} else {
					throw new USException("Recevied unexpected reply of type "
							+ response.getClass().getName() + ": " + response);
				}
			} catch (IOException ioe) {
				throw new USException("Error restoring BDB store from Dump",
						ioe);
			} catch (ClassNotFoundException e) {
				throw new USException(
						"Could not deserialize response from BDB store", e);
			}

		}

		/*
		 * (non-Javadoc)
		 * 
		 * @seeedu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#
		 * getSchemaIDBindingClient
		 * (edu.upenn.cis.orchestra.datamodel.AbstractPeerID,
		 * edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding)
		 */
		@Override
		public ISchemaIDBindingClient getSchemaIDBindingClient() {
			return new SchemaIDBindingBerkeleyDBStoreClient(host);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @seeedu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#
		 * startUpdateStoreServer()
		 */
		@Override
		public Process startUpdateStoreServer() throws USException {
			Process process = null;
			if (isLocal()) {
				try {
					String storeName = "updateStore";
					File f = new File(storeName + "_env");
					if (!f.exists()) {
						f.mkdir();
					}
					process = startUpdateStoreServerExec(host.getPort(), f
							.getAbsolutePath(), ".");
					// EnvironmentConfig ec = new EnvironmentConfig();
					// ec.setAllowCreate(true);
					// ec.setTransactional(true);
					// Environment env = new Environment(f, ec);
					// storeServer = new BerkeleyDBStoreServer(env,
					// host.getPort());
				} catch (Exception e) {
					throw new USException(e);
				}
			}
			return process;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @seeedu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#
		 * stopUpdateStoreServer()
		 */
		@Override
		public void stopUpdateStoreServer() throws USException {
			try {
				Socket socket = new Socket(host.getAddress(), host.getPort());
				ObjectOutputStream oos = new ObjectOutputStream(socket
						.getOutputStream());
				ObjectInputStream ois = new ObjectInputStream(socket
						.getInputStream());
				oos.writeObject(new StopUpdateStore());
				// Server writes an EndOfStream message.
				ois.readObject();
				oos.flush();

				oos.close();
				ois.close();
				socket.close();
			} catch (IOException ioe) {
				throw new USException("Error stopping BDB store", ioe);
			} catch (ClassNotFoundException e) {
				throw new USException("Error stopping BDB store", e);
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @seeedu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory#
		 * updateStoreServerIsRunning()
		 */
		@Override
		public boolean updateStoreServerIsRunning() {
			try {
				Socket socket = new Socket(host.getAddress(), host.getPort());
				ObjectOutputStream oos = new ObjectOutputStream(socket
						.getOutputStream());
				ObjectInputStream ois = new ObjectInputStream(socket
						.getInputStream());
				oos.writeObject(new Ping());
				oos.flush();
				Object response = ois.readObject();
				oos.writeObject(new EndOfStreamMsg());
				oos.flush();
				ois.readObject();// EndOfStreamMsg as a response to our
									// EndOfStreamMsg.
				oos.close();
				socket.close();
				if (response instanceof Ack) {
					return true;
				}
				return false;

			} catch (Exception e) {
				return false;
			}
		}
	}

	BerkeleyDBStoreClient(InetSocketAddress host, ISchemaIDBinding sch,
			Schema s, TrustConditions tc) throws USException {
		schemas = new HashMap<AbstractPeerID, Schema>();
		schemas.put(tc.getOwner(), s);
		_mapStore = sch;
		this.tc = tc;
		this.host = host;

		reconnect();

	}

	private synchronized Object sendRequest(Object o, Class<?>... classes)
			throws USException {
		Object reply;
		try {
			oos.writeObject(o);
			oos.flush();
			reply = ois.readObject();
		} catch (Exception e) {
			throw new USException(e);
		}

		if (reply instanceof Exception) {
			throw new USException((Exception) reply);
		}

		if (reply instanceof BadMsg) {
			throw new USException("Server received unexpected message: "
					+ ((BadMsg) reply).o);
		}

		for (Class<?> c : classes) {
			if (c.isInstance(reply)) {
				return reply;
			}
		}

		throw new USException("Received object is of unexpected type "
				+ reply.getClass().getName());
	}

	@Override
	public void disconnect() throws USException {
		if (socket == null) {
			// Already disconnected
			return;
		}
		sendRequest(new EndOfStreamMsg(), EndOfStreamMsg.class);
		try {
			oos.close();
			ois.close();
			socket.close();
		} catch (Exception e) {
			throw new USException(
					"Could not disconnect from update store server", e);
		}
		oos = null;
		ois = null;
		socket = null;
	}

	@Override
	public void reconnect() throws USException {
		if (socket != null) {
			// Already connected
			return;
		}
		try {
			socket = new Socket(host.getAddress(), host.getPort());
			oos = new ObjectOutputStream(socket.getOutputStream());
			ois = new ObjectInputStream(socket.getInputStream());
		} catch (Exception e) {
			throw new USException("Could not connect to update store server", e);
		}

		sendRequest(new SendTrustConditions(tc, _mapStore, schemas.get(tc
				.getOwner())), Ack.class);

	}

	@Override
	public boolean isConnected() {
		return (socket != null);
	}

	@Override
	public Benchmark getBenchmark() {
		return b;
	}

	@Override
	public int getCurrentRecno() throws USException {
		if (b != null) {
			resetElapsedTime();
		}
		int lastRecno = (Integer) sendRequest(new GetLastReconciliation(),
				Integer.class);
		if (b != null) {
			b.getCurrentRecnoNet += getElapsedTime();
		}
		return lastRecno + 1;
	}

	@Override
	public void getReconciliationData(int recno,
			Set<TxnPeerID> ownAcceptedTxns,
			Map<Integer, List<TxnChain>> trustedTxns, Set<TxnPeerID> mustReject)
			throws USException {
		if (b != null) {
			resetElapsedTime();
		}
		ReconciliationData rd = (ReconciliationData) sendRequest(
				new GetReconciliationData(recno, ownAcceptedTxns),
				ReconciliationData.class);
		if (b != null) {
			b.getReconciliationDataNet += getElapsedTime(true);
		}

		rd.beginReading(_mapStore);// getSchema(tc.getOwner()));

		ReconciliationData.Entry rde;

		while ((rde = rd.readEntry()) != null) {
			if (rde.tc == null) {
				mustReject.add(rde.tpi);
			} else {
				List<TxnChain> txnsForPrio = trustedTxns.get(rde.prio);
				if (txnsForPrio == null) {
					txnsForPrio = new ArrayList<TxnChain>();
					trustedTxns.put(rde.prio, txnsForPrio);
				}
				txnsForPrio.add(rde.tc);
			}
		}
		if (b != null) {
			b.getReconciliationData += getElapsedTime();
		}
	}

	@Override
	public void publish(List<List<Update>> txns) throws USException {
		sendRequest(new PublishMsg(txns), Ack.class);
	}

	@Override
	public void recordReconcile(boolean empty) throws USException {
		if (b != null) {
			resetElapsedTime();
		}
		sendRequest(new RecordReconcileMsg(empty), Ack.class);
		if (b != null) {
			b.recordReconcileNet += getElapsedTime();
		}
	}

	protected void recordTxnDecisionsImpl(Iterable<Decision> decisions)
			throws USException {
		if (b != null) {
			resetElapsedTime();
		}
		sendRequest(new RecordDecisions(decisions), Ack.class);
		if (b != null) {
			b.recordTxnDecisionsNet += getElapsedTime();
		}
	}

	@Override
	public void setBenchmark(Benchmark b) throws USException {
		this.b = b;
	}

	private long startTime = 0;

	private void resetElapsedTime() {
		getElapsedTime(true);
	}

	private long getElapsedTime() {
		return getElapsedTime(false);
	}

	private long getElapsedTime(boolean reset) {
		long currTime = System.nanoTime();
		long retval = currTime - startTime;
		if (reset) {
			startTime = currTime;
		}
		return retval;
	}

	@Override
	protected TxnStatus getTxnStatus(TxnPeerID tpi) throws USException {
		return (TxnStatus) sendRequest(new GetStatusMsg(tpi), TxnStatus.class);
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Decision> getDecisions(int recno) throws USException {
		return (List<Decision>) sendRequest(new GetDecisions(recno), List.class);
	}

	@Override
	public ResultIterator<ReconciliationEpoch> getReconciliations()
			throws USException {
		int lastRecno = (Integer) sendRequest(new GetLastReconciliation(),
				Integer.class);

		return new IntegerIterator<ReconciliationEpoch>(lastRecno) {
			@Override
			protected ReconciliationEpoch getData(int recno)
					throws IteratorException {
				int epoch;
				try {
					epoch = (Integer) sendRequest(new GetRecnoEpoch(recno),
							Integer.class);
				} catch (USException e) {
					throw new IteratorException(e.getMessage(), e.getCause());
				}
				return new ReconciliationEpoch(recno, epoch);
			}
		};
	}

	@SuppressWarnings("unchecked")
	public ResultIterator<ReconciliationEpoch> getReconciliations(boolean buffer)
			throws USException {
		if (buffer) {
			// This implementation buffers the entire list of reconciliations
			// and
			// epochs, which could be bad for large numbers of reconciliations.
			List<Integer> epochs = (List<Integer>) sendRequest(
					new GetRecnoEpochs(), List.class);
			final int numRecons = epochs.size();

			List<ReconciliationEpoch> retvalList = new ArrayList<ReconciliationEpoch>(
					numRecons);

			for (int i = 0; i < numRecons; ++i) {
				retvalList.add(new ReconciliationEpoch(i, epochs.get(i)));
			}

			return new ListIteratorResultIterator<ReconciliationEpoch>(
					retvalList.listIterator());
		} else {
			return getReconciliations();
		}
	}

	@Override
	public ResultIterator<Update> getPublishedUpdatesForRelation(String relname)
			throws USException {
		int lastEpoch = (Integer) sendRequest(new GetLastEpoch(), Integer.class);

		try {
			return new FlatteningIterator<Update>(new EpochIterator(lastEpoch,
					relname));
		} catch (Exception e) {
			throw new USException("Error creating updates iterator", e);
		}
	}

	private class EpochIterator extends IntegerIterator<List<Update>> {
		private final int relId;
		private final boolean hasRelId;

		EpochIterator(int lastEpoch, String relname) throws USException {
			super(lastEpoch);
			if (relname == null) {
				relId = 0;
				hasRelId = false;
			} else {
				relId = getSchema(tc.getOwner()).getIDForName(relname);
				hasRelId = true;
			}
		}

		EpochIterator(int lastEpoch) throws USException {
			this(lastEpoch, null);
		}

		@Override
		protected List<Update> getData(int epoch) throws IteratorException {
			byte[] data;
			try {
				data = (byte[]) sendRequest(new GetEpochContents(epoch),
						byte[].class);
			} catch (USException e) {
				throw new IteratorException(e.getMessage(), e.getCause());
			}
			ByteBufferReader bbr = new ByteBufferReader(_mapStore, data);
			List<Update> retval = new ArrayList<Update>();
			while (!bbr.hasFinished()) {
				TxnPeerID tpi = bbr.readTxnPeerID();
				int size = bbr.readInt();
				try {
					// bbr.setSchema(getSchema(tpi.getPeerID()));
					while (size > 0) {
						--size;
						Update u = bbr.readUpdate();
						if (hasRelId
								&& ((!getSchema(u.getLastTid().getPeerID())
										.equals(getSchema(tc.getOwner()))))
								|| u.getRelationID() != relId) {
							continue;
						}
						retval.add(u);
					}
				} catch (USException e) {
					throw new IteratorException(e);
				}
			}
			return retval;
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Update> getTransaction(TxnPeerID txn) throws USException {
		byte[] data = (byte[]) sendRequest(txn, byte[].class);

		List<Update> retval = new ArrayList<Update>();
		ByteBufferReader bbr = new ByteBufferReader(_mapStore/*
															 * getSchema(txn.getPeerID
															 * ())
															 */, data);
		while (!bbr.hasFinished()) {
			retval.add(bbr.readUpdate());
		}
		return retval;
	}

	@Override
	public ResultIterator<TxnPeerID> getTransactionsForReconciliation(int recno)
			throws USException {
		int currentRecno = getCurrentRecno();

		int firstEpoch, lastEpoch;

		if (currentRecno == StateStore.FIRST_RECNO || recno >= currentRecno) {
			lastEpoch = (Integer) sendRequest(new GetLastEpoch(), Integer.class);

			if (currentRecno == StateStore.FIRST_RECNO) {
				firstEpoch = BerkeleyDBStoreServer.FIRST_EPOCH;
			} else {
				firstEpoch = ((Integer) sendRequest(new GetRecnoEpoch(
						currentRecno - 1), Integer.class)) + 1;
			}
		} else {
			lastEpoch = (Integer) sendRequest(new GetRecnoEpoch(recno),
					Integer.class);
			if (recno == StateStore.FIRST_RECNO) {
				firstEpoch = BerkeleyDBStoreServer.FIRST_EPOCH;
			} else {
				firstEpoch = ((Integer) sendRequest(
						new GetRecnoEpoch(recno - 1), Integer.class)) + 1;
			}
		}

		try {
			return new FlatteningIterator<TxnPeerID>(new EpochTxnsIterator(
					firstEpoch, lastEpoch));
		} catch (IteratorException e) {
			throw new USException(e.getMessage(), e.getCause());
		}
	}

	private class EpochTxnsIterator extends IntegerIterator<List<TxnPeerID>> {

		public EpochTxnsIterator(int firstEpoch, int lastEpoch) {
			super(firstEpoch, lastEpoch);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected List<TxnPeerID> getData(int epoch) throws IteratorException {
			try {
				return (List<TxnPeerID>) sendRequest(new GetEpochTransactions(
						epoch), List.class);
			} catch (USException e) {
				throw new IteratorException(e.getMessage(), e.getCause());
			}
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<TxnPeerID, List<Update>> getTransactionsAcceptedAtRecno(int recno)
			throws USException {
		HashSet<TxnPeerID> accepted = new HashSet<TxnPeerID>();
		List<Decision> decisions = (List<Decision>) sendRequest(
				new GetDecisions(recno), List.class);

		for (Decision d : decisions) {
			if (d.recno != recno) {
				throw new USException("Received decision for recno " + d.recno
						+ ", expected " + recno);
			}
			if (d.accepted) {
				accepted.add(d.tpi);
			}
		}

		byte[] updateContents = (byte[]) sendRequest(new GetTxns(accepted),
				byte[].class);

		Map<TxnPeerID, List<Update>> retval = new HashMap<TxnPeerID, List<Update>>();

		ByteBufferReader bbr = new ByteBufferReader(_mapStore, updateContents);

		while (!bbr.hasFinished()) {
			TxnPeerID tid = bbr.readTxnPeerID();
			int size = bbr.readInt();
			List<Update> txn = new ArrayList<Update>();
			retval.put(tid, txn);
			// bbr.setSchema(getSchema(tid.getPeerID()));
			while (size > 0) {
				txn.add(bbr.readUpdate());
				--size;
			}
		}

		return retval;
	}

	@Override
	public int getLargestTidForPeer() throws USException {
		return (Integer) sendRequest(new GetLargestTidForPeer(), Integer.class);
	}

	private Schema getSchema(AbstractPeerID pid) throws USException {
		Schema s = schemas.get(pid);
		if (s != null) {
			return s;
		}

		s = (Schema) sendRequest(new GetSchema(pid), Schema.class);
		if (s == null) {
			throw new USException("Schema for peer " + pid + " is not known");
		}
		schemas.put(pid, s);
		return s;
	}

	/**
	 * Returns the {@code Process} resulting from the attempt to start a {@code
	 * BerkeleyDBStoreServer} in a new process. The class path is {@code
	 * java.class.path}.
	 * 
	 * @param port the port the server should listen on
	 * @param serverDirectoryName the directory where the Berkeley database
	 *            files should be kept
	 * @param workingDirectoryName the working directory of the process.
	 * 
	 * @return a {@code BerkeleyDBStoreServer} {@code Process}
	 * 
	 * @throws IOException
	 */
	private static Process startUpdateStoreServerExec(int port,
			String serverDirectoryName, String workingDirectoryName)
			throws IOException {
		String[] pathToScript = Config.getUpdateStoreExecutable();
		List<String> cmdList = newArrayList(pathToScript);
		cmdList.addAll(newArrayList( "-port",
				Integer.toString(port), serverDirectoryName));
		ProcessBuilder builder = new ProcessBuilder();
		builder.command(cmdList);
		builder.redirectErrorStream(true);
		builder.directory(new File(workingDirectoryName));
		final Process process = builder.start();
		
	
		new Thread() {
			@Override
			public void run() {
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(process.getInputStream()));
				String line = null;
				try {
					while ((line = reader.readLine()) != null) {
						logger.debug(line);
					}
				} catch (IOException e) {
					logger.error(
							"Error reading output from update store process.",
							e);
				}
			}
	
		}.start();
	
		return process;
	}
}
