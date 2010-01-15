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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.NodeHandleSet;
import rice.p2p.commonapi.RangeCannotBeDeterminedException;
import rice.p2p.commonapi.RouteMessage;
import rice.pastry.socket.SocketNodeHandle;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DeadlockException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.TrustConditions;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.iterators.FlatteningIterator;
import edu.upenn.cis.orchestra.datamodel.iterators.IntegerIterator;
import edu.upenn.cis.orchestra.datamodel.iterators.IteratorException;
import edu.upenn.cis.orchestra.datamodel.iterators.MappingIterator;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;
import edu.upenn.cis.orchestra.reconciliation.Benchmark;
import edu.upenn.cis.orchestra.reconciliation.Decision;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.PidAndRecno;
import edu.upenn.cis.orchestra.reconciliation.ReconciliationEpoch;
import edu.upenn.cis.orchestra.reconciliation.TransactionDecisions;
import edu.upenn.cis.orchestra.reconciliation.TxnChain;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore;
import edu.upenn.cis.orchestra.reconciliation.p2pstore.PeerController.ReconciliationRecordException;


public class P2PStore extends UpdateStore implements Application {
	ISchemaIDBinding _mapStore;
	
	// Instance name of Application
	public static final String instanceName = "P2PStore";
	// Number of times to retry an operation that fails
	static int DEFAULT_NUM_RETRIES = 2;
	// Delay between failure and retry, in milliseconds
	static int DEFAULT_RETRY_DELAY = 500;
	// Time to wait for a reply before timing out, in milliseconds
	static int DEFAULT_MESSAGE_TIMEOUT = 500;

	static int INITIAL_NUM_PROCESSING_THREADS = 5;
	// Time to wait between checking for a non-waiting
	// thread, in milliseconds
	static int THREAD_CHECK_INTERVAL = 250;

	static int NUM_DEADLOCK_RETRIES = 5;

	// ID of peer owning this P2PStore object
	private AbstractPeerID pid;
	// The schema of the shared database
	private Schema schema;

	// Data members relating to the P2P network
	private NodeFactory nodeFactory;
	private Node node;
	private IdFactory idFactory;
	private Endpoint endpoint;

	// BerkeleyDb databases to store local data
	private Environment env;
	// Mapping from epoch to multiple TxnPeerIDs
	private String epochDbName;
	// Mapping from TxnPeerID to multiple updates
	private String transactionDbName;
	// Mapping from (TnxPeerID, reconciliation) to epoch
	private String peerDbName;
	// Mapping from (TxnPeerID, PeerID) -> nothing(rejected) or to recno, order (accepted)
	private String decisionDbName;
	// Mapping from Pastry ID -> set of epochs, peer ids, and txn ids
	private String pastryIdsDbName;

	// Thread group to which all of our service threads belong
	private ThreadGroup threadGroup;

	// Queue of messages waiting to be delivered
	private BlockingQueue<P2PMessage> msgQueue;
	// Threads that processes incoming messages
	private List<MessageProcessorThread> msgProcessingThreads;
	private MonitorThread monitorThread;

	// The epoch allocator running at this node. Note that this epoch allocator
	// won't receive any messages unless this node is the node closest to the
	// hash of epochAllocatorIdInt
	EpochAllocator epochAllocator;

	// A fixed location for the epoch allocator
	static final int epochAllocatorIdInt = 0;

	// The epoch controller running at this node
	EpochController epochController;

	// The transaction controller running at this node
	TransactionController transactionController;

	// The peer coordinator running at this node
	PeerController peerController;

	// The database to keep track of the Pastry IDs of data stored at this node
	ReplicationController replicationController;

	// Table to store reply data
	private Map<Long,ReplyData> replyData;

	// How many peers to send neighbor messages to
	static final int numNeighborMessages = 10;

	// Incremented after a participant retrieves transactions
	// from other participants and decides which to accept and reject
	private int currentRecno;
	private int lastReconEpoch;


	public static class P2PStoreException extends USException {
		private static final long serialVersionUID = 1L;
		P2PStoreException(Exception e) {
			super(e);
		}
		P2PStoreException(String msg) {
			super(msg);
		}
		P2PStoreException(String msg, Throwable cause) {
			super(msg,cause);
		}
	}

	P2PStore(NodeFactory nf, AbstractPeerID p, TrustConditions tc, ISchemaIDBinding sch, Schema s, Environment e,
			int replicationFactor, String epochDbName, String transactionDbName,
			String peerDbName, String decisionDbName,
			String pastryIdsDbName) throws P2PStoreException {
		this(nf,p,tc,sch,s,e,replicationFactor,epochDbName,transactionDbName,peerDbName,
				decisionDbName,pastryIdsDbName,PeerController.FIRST_RECNO);
	}

	P2PStore(NodeFactory nf, AbstractPeerID p, TrustConditions tc, ISchemaIDBinding sch, Schema s, Environment e,
			int replicationFactor, String epochDbName, String transactionDbName,
			String peerDbName, String decisionDbName,
			String pastryIdsDbName, int currentRecno) throws P2PStoreException {
		msgQueue = new LinkedBlockingQueue<P2PMessage>();
		this.nodeFactory = nf;
		idFactory = nf.getIdFactory();
		this.node = nf.getNode();
		this.pid = p;
		this.schema = s;
		this.env = e;
		this.epochDbName = epochDbName;
		this.transactionDbName = transactionDbName;
		this.peerDbName = peerDbName;
		this.decisionDbName = decisionDbName;
		this.pastryIdsDbName = pastryIdsDbName;
		
		_mapStore = sch;

		this.currentRecno = currentRecno;

		reconnect();
		threadGroup = new ThreadGroup(pid + " P2PStore Services");
		try {
			epochController = new EpochController(threadGroup, this, e, this.epochDbName);
			transactionController = new TransactionController(threadGroup, this, sch, schema, env,
					this.transactionDbName);
			peerController = new PeerController(threadGroup, this, e, this.peerDbName, this.decisionDbName);
			replicationController = new ReplicationController(threadGroup, replicationFactor, env, this.pastryIdsDbName, node, this, idFactory);
			epochAllocator = new EpochAllocator(this, getId(epochAllocatorIdInt));
		} catch (Exception exception) {
			throw new P2PStoreException("Error while starting controllers", exception);
		}
		msgProcessingThreads = new ArrayList<MessageProcessorThread>(INITIAL_NUM_PROCESSING_THREADS);
		for (int i = 0; i < INITIAL_NUM_PROCESSING_THREADS; ++i) {
			MessageProcessorThread mpt = new MessageProcessorThread(threadGroup, i);
			mpt.start();
			msgProcessingThreads.add(mpt);
		}
		monitorThread = new MonitorThread(threadGroup);
		monitorThread.start();

		replyData = new HashMap<Long,ReplyData>();

		this.endpoint = node.registerApplication(this, instanceName);

		replicationController.shareTrustConditions(pid, tc.getBytes(_mapStore));

		// Needed data from replica set will be sent by other peers as soon
		// as they notice that the new node has joined

		if (currentRecno == PeerController.FIRST_RECNO) {
			lastReconEpoch = EpochAllocator.FIRST_EPOCH - 1;
		} else {
			try {
				lastReconEpoch = peerController.getReconciliationEpoch(pid, currentRecno - 1, currentRecno - 1, null);
			} catch (Exception ex) {
				throw new P2PStoreException("Error determining epoch for previous reconciliaiton", ex);
			}
		}
	}

	@Override
	public void disconnect() throws P2PStoreException {
		try {
			// This order is important, since we want to make sure that nothing
			// accesses the databases after they are closed
			monitorThread.interrupt();
			monitorThread.join();
			for (MessageProcessorThread mpt : msgProcessingThreads) {
				mpt.interrupt();
				mpt.join();
			}
			replicationController.halt();
			epochController.halt();
			transactionController.halt();
			peerController.halt();
			nodeFactory.shutdownNode(node);
			node = null;
		} catch (Exception e) {
			throw new P2PStoreException(e);
		}
	}

	@Override
	public void reconnect() throws P2PStoreException {
		try {
			// TODO: restart threads; reconnect to network; retrieve necessary state
			throw new UnsupportedOperationException("Need to implemenent P2PStore.reconnect()");
		} catch (Exception e) {
			throw new P2PStoreException(e);
		}
	}
	
	protected void clearSubclassState() throws P2PStoreException {
		// TODO: implement P2PStore.clearSubclassState
		throw new RuntimeException("Need to implement P2PStore.clearSubclassState");
	}
	
	@Override
	public boolean isConnected() {
		return (node != null);
	}

	/**
	 * Get the PeerID associated with this object
	 * 
	 * @return The PeerID
	 */
	AbstractPeerID getPeerID() {
		return pid;
	}


	private int publishNextEpoch(List<TxnPeerID> tids) throws P2PStoreException, InterruptedException {
		// First, request the most recent epoch from the epoch allocator
		int epoch;
		try {
			epoch = epochAllocator.getLastEpoch();
		} catch (UnexpectedReply e) {
			throw new P2PStoreException("Error requesting last epoch", e);
		}

		// Then start at the next epoch
		++epoch;

		// Then try to publish the list of transactions, starting with the
		// suggested epoch, until we succeed

		ReplyHolder<Integer> replies = new ReplyHolder<Integer>(1);

		for ( ; ; ) {
			PublishEpoch pe = new PublishEpoch(epoch, tids, getId(epoch));

			sendMessageAwaitReply(pe, new SimpleReplyContinuation<Integer>(1,replies), ReplySuccess.class, CouldNotPublishEpoch.class);

			replies.waitUntilFinished();
			P2PMessage reply = replies.getReply(1);

			if (reply instanceof ReplySuccess) {
				epochAllocator.lastEpochIs(epoch);
				epochAllocator.shareLastEpoch();
				return epoch;
			} else if (reply instanceof CouldNotPublishEpoch) {
				epoch = ((CouldNotPublishEpoch) reply).lastEpoch + 1;
				replies.reset(1);
			} else if (reply instanceof ReplyException) {
				ReplyException re = (ReplyException) reply;
				if (re.e != null) {
					throw new P2PStoreException(re.message, re.e);
				} else {
					throw new P2PStoreException(re.message);
				}
			} else {
				throw new P2PStoreException("Received unexpected reply to PublishEpoch(" + epoch + "): " + reply);
			}
		}
	}

	@Override
	public void publish(List<List<Update>> txns) throws P2PStoreException {
		try {

			if (benchmark != null) {
				resetElapsedTime();
			}

			List<TxnPeerID> tids = new ArrayList<TxnPeerID>(txns.size());

			for (List<Update> txn : txns) {
				tids.add(txn.get(0).getLastTid());
			}

			// Publish the epoch
			publishNextEpoch(tids);

			// Then publish the transactions
			ReplyHolder<TxnPeerID> replies = new ReplyHolder<TxnPeerID>(txns.size());

			for (List<Update> txn : txns) {
				TxnPeerID tid = txn.get(0).getLastTid();
				PublishedTxn pt = new PublishedTxn(txn, getId(tid));
				sendMessageAwaitReply(pt, new SimpleReplyContinuation<TxnPeerID>(tid,replies), ReplySuccess.class, ReplyFailure.class);
			}

			replies.waitUntilFinished();

			for (TxnPeerID tid : replies.getKeys()) {
				P2PMessage reply = replies.getReply(tid);
				if (! (reply instanceof ReplySuccess)) {
					throw new P2PStoreException("Couldn't publish transaction " + tid + ": " + reply);
				}
			}

			if (benchmark != null) {
				benchmark.publishNet += getElapsedTime();
			}

		} catch (InterruptedException e) {
			throw new P2PStoreException("Interrupted while publishing transactions", e);
		}


	}

	@Override
	protected TxnStatus getTxnStatus(TxnPeerID tpi) throws P2PStoreException {
		try {
			int lastRecno = getCurrentRecno() - 1;
			return peerController.getTxnStatus(pid, lastRecno, tpi, lastRecno, null);
		} catch (InterruptedException e) {
			throw new P2PStoreException("Interrupted while determining transaction status", e);
		} catch (Exception e) {
			throw new P2PStoreException("Error determining transaction status", e);
		}
	}

	@Override
	protected void recordTxnDecisionsImpl(Iterable<Decision> decisions)
	throws P2PStoreException {
		try {
			if (benchmark != null) {
				resetElapsedTime();
			}

			// TODO: change to use reconciliation number specified in TransactionDecision
			if (true) {
				throw new RuntimeException("Need to change to use reconciliation number specified in TransactionDecision");
			}
			
			int recno;
			int epoch;
			synchronized (this) {
				recno = currentRecno;
				epoch = lastReconEpoch;
			}

			List<TxnPeerID> accepted = new ArrayList<TxnPeerID>(), rejected = new ArrayList<TxnPeerID>();

			for (Decision td : decisions) {
				if (td.accepted) {
					accepted.add(td.tpi);
				} else {
					rejected.add(td.tpi);
				}
			}

			RecordReconciliation rr = new RecordReconciliation(pid, recno, accepted, rejected, epoch, getId(new PidAndRecno(pid, recno)));

			ReplyHolder<Integer> replies = new ReplyHolder<Integer>(1);
			sendMessageAwaitReply(rr, new SimpleReplyContinuation<Integer>(1, replies), ReplySuccess.class, ReplyFailure.class);

			replies.waitUntilFinished();
			if (benchmark != null) {
				benchmark.recordTxnDecisionsNet += getElapsedTime();
			}
			P2PMessage reply = replies.getReply(1);
			if (reply instanceof ReplySuccess) {
				// Everything OK, can return
			} else if (reply instanceof ReplyFailure) {
				throw new P2PStoreException("Could not publish transaction decisions for " + recno + ": " + ((ReplyFailure) reply).msg);
			} else if (reply instanceof ReplyException) {
				ReplyException re = (ReplyException) reply;
				if (re.e != null) {
					throw new P2PStoreException(re.message, re.e);
				} else {
					throw new P2PStoreException(re.message);
				}
			} else {
				throw new UnexpectedReply(rr,reply);
			}

			synchronized (this) {
				++currentRecno;
			}
		} catch (InterruptedException ie) {
			throw new P2PStoreException(ie);
		}
	}

	@Override
	public void recordReconcile(boolean empty) throws P2PStoreException {
		try {
			if (benchmark != null) {
				resetElapsedTime();
			}

			synchronized (this) {
				if (empty) {
					if (currentRecno < PeerController.FIRST_RECNO) {
						throw new IllegalArgumentException("Cannot have an empty first reconciliation");
					}
				} else {
					List<TxnPeerID> emptyEpoch = Collections.emptyList();
					lastReconEpoch = publishNextEpoch(emptyEpoch);
				}
			}

			if (benchmark != null) {
				benchmark.recordReconcileNet += getElapsedTime();;
			}

		} catch (InterruptedException ie) {
			throw new P2PStoreException("Interrupted while recording reconciliation", ie);
		}
	}

	private int getRecnoEpoch(int recno) throws P2PStoreException {
		if (recno == (PeerController.FIRST_RECNO - 1)) {
			return EpochAllocator.FIRST_EPOCH - 1;
		}
		try {
			return peerController.getReconciliationEpoch(pid, recno, getCurrentRecno() - 1, null);
		} catch (Exception e) {
			throw new P2PStoreException("Error determining reconciliation epoch", e);
		}
	}

	@Override
	public void getReconciliationData(int recno, final Set<TxnPeerID> ownAcceptedTxns,
			final Map<Integer, List<TxnChain>> trustedTxns, final Set<TxnPeerID> mustReject)
	throws USException, P2PStoreException {
		try {
			if (benchmark != null) {
				resetElapsedTime();
			}

			TransactionDecisions td = new TransactionDecisions() {

				public boolean hasAcceptedTxn(TxnPeerID tpi) throws USException {
					return (ownAcceptedTxns.contains(tpi) || P2PStore.this.hasAcceptedTxn(tpi));
				}

				public boolean hasRejectedTxn(TxnPeerID tpi) throws USException {
					return P2PStore.this.hasRejectedTxn(tpi);
				}

			};

			final HashMap<TxnPeerID,Integer> priorities = new HashMap<TxnPeerID,Integer>();
			final HashMap<TxnPeerID,List<Update>> relevantTransactions = new HashMap<TxnPeerID,List<Update>>();

			TransactionSource ts = new TransactionSource() {
				public List<Update> getTxn(TxnPeerID tpi) {
					return relevantTransactions.get(tpi);
				}
			};

			int lastRecno;
			synchronized (this) {
				// We haven't yet recorded the now current reconciliation
				lastRecno = currentRecno - 1;
			}
			int prevEpoch = getRecnoEpoch(recno - 1);
			int currentEpoch;
			if (recno == lastRecno + 1) {
				synchronized (this) {
					currentEpoch = lastReconEpoch;
				}
			} else if (recno <= lastRecno) {
				currentEpoch = getRecnoEpoch(recno);
			} else {
				throw new USException("Cannot request reconciliation data for future reconciliation");
			}

			ReplyHolder<Integer> replies = new ReplyHolder<Integer>(currentEpoch - prevEpoch);

			for (int i = prevEpoch + 1; i <= currentEpoch; ++i) {
				final P2PMessage rpe = new RequestPublishedEpoch(i, getId(i));
				sendMessageAwaitReply(rpe, new SimpleReplyContinuation<Integer>(i,replies), PublishEpoch.class);
			}

			replies.waitUntilFinished();

			final HashSet<TxnPeerID> neededTxns = new HashSet<TxnPeerID>();
			for (int i = prevEpoch + 1; i <= currentEpoch; ++i) {
				P2PMessage reply = replies.getReply(i);
				if (reply instanceof PublishEpoch) {
					neededTxns.addAll(((PublishEpoch) reply).getTids());
				} else {
					throw new P2PStoreException("Unexpected reply to RequestPublishedEpoch(" + i + "):" + reply);
				}
			}

			Iterator<TxnPeerID> it = neededTxns.iterator();
			while (it.hasNext()) {
				TxnPeerID tid = it.next();
				if (ownAcceptedTxns.contains(tid) || hasRejectedTxn(tid) || hasAcceptedTxn(tid)) {
					it.remove();
				}
			}

			final HashSet<TxnPeerID> unfinishedTxns = new HashSet<TxnPeerID>(neededTxns);

			final Map<TxnPeerID,Integer> retryCounts = new HashMap<TxnPeerID,Integer>();
			final List<String> failureMessages = Collections.synchronizedList(new ArrayList<String>());

			for (final TxnPeerID tid : neededTxns) {
				final RequestTxnForReconciliation rtfr = new RequestTxnForReconciliation(tid, pid, lastRecno, lastRecno, ownAcceptedTxns, true, getId(tid));


				ReplyContinuation rc = new ReplyContinuation() {
					// use unreceivedTxns as lock for both of them
					Set<TxnPeerID> unreceivedTxns = new HashSet<TxnPeerID>();
					{ unreceivedTxns.add(tid); }
					Set<TxnPeerID> receivedTxns = new HashSet<TxnPeerID>();
					boolean finished = false;
					public void processReply(P2PMessage m, MessageProcessorThread mpt) {
						synchronized (this) {
							if (finished) {
								return;
							}
						}
						TxnPeerID msgTpi = null;
						boolean mustFinish = false;
						if (m instanceof RetrievedTxn) {
							RetrievedTxn rt = (RetrievedTxn) m;
							List<Update> txn = rt.getTxn(_mapStore);//schema);
							msgTpi = txn.get(0).getLastTid();
							if (rt.hasPriority()) {
								synchronized (priorities) {
									priorities.put(msgTpi.duplicate(), rt.getTxnPriority());
								}
							}
							synchronized (unreceivedTxns) {
								for (TxnPeerID tpi : rt.getRequestedAntecedents()) {
									if (! receivedTxns.contains(tpi)) {
										unreceivedTxns.add(tpi);
									}
								}
							}
							synchronized(relevantTransactions) {
								relevantTransactions.put(msgTpi.duplicate(), txn);
							}
						} else if (m instanceof TxnNotNeeded) {
							msgTpi = ((TxnNotNeeded) m).getTpi();
						} else if (m instanceof TxnAlreadyRejected) {
							// The root transaction has been rejected, so
							// no more computation is needed. Since we never set
							// its priority, it will not be considered any more
							if (! ((TxnAlreadyRejected) m).getTpi().equals(tid)) {
								System.err.println("Should only receive a TxnAlreadyRejected for a root transaction");
							}
							mustFinish = true;
						} else if (m instanceof AntecedentTxnRejected) {
							synchronized (mustReject) {
								mustReject.add(tid);
							}
							mustFinish = true;
						} else if (m instanceof CheckForTxn) {
							TxnPeerID errTid = ((CheckForTxn) m).toCheck;
							boolean shouldRetry = false;
							synchronized (retryCounts) {
								Integer count = retryCounts.get(errTid);
								if (count == null) {
									retryCounts.put(errTid, 1);
									shouldRetry = true;
								} else if (count >= DEFAULT_NUM_RETRIES) {
									mustFinish = true;
									failureMessages.add("Couldn't retrieve " + errTid + " for root txn " + tid);
								} else {
									retryCounts.put(errTid, count + 1);
									shouldRetry = true;
								}
							}
							if (shouldRetry) {
								sendMessage(new RequestTxnForReconciliation(errTid, rtfr, getId(errTid)));
								sendSelfMessageAfterTimeoutDelay(new CheckForTxn(rtfr, tid));
							}
						} else {
							// An unexpected reply, such as a ReplyException
							// We'll identify the failure later when we receive the
							// CheckForTxn message
							System.err.println("Received unexpected reply to " + rtfr + ": " + m);
						}
						if (mustFinish) {
							synchronized (this) {
								finished = true;
							}
							synchronized (unfinishedTxns) {
								unfinishedTxns.remove(tid);
								if (unfinishedTxns.isEmpty()) {
									unfinishedTxns.notify();
								}
							}
						} else if (msgTpi != null) {
							synchronized (unreceivedTxns) {
								unreceivedTxns.remove(msgTpi);
								receivedTxns.add(msgTpi);
								if (unreceivedTxns.isEmpty()) {
									// The root transaction has finished
									synchronized (this) {
										finished = true;
									}
									synchronized (unfinishedTxns) {
										unfinishedTxns.remove(tid);
										if (unfinishedTxns.isEmpty()) {
											unfinishedTxns.notify();
										}
									}
								}
							}
						}
					}

					synchronized public boolean isFinished() {
						return finished;
					}

				};

				sendMessageAwaitReply(rtfr, rc, 0, 0, 0, P2PMessage.class);
				sendSelfMessageAfterTimeoutDelay(new CheckForTxn(rtfr, tid));
			}

			synchronized (unfinishedTxns) {
				while (! unfinishedTxns.isEmpty()) {
					unfinishedTxns.wait();
				}
			}

			if (! failureMessages.isEmpty()) {
				throw new P2PStoreException("Error retrieving transaction data: " + failureMessages);
			}

			// We're done receiving data over the network!
			if (benchmark != null) {
				benchmark.getReconciliationDataNet += getElapsedTime();
			}

			// Build the transaction chains for the fully trusted transactions with
			// no rejected antecedents
			for (Map.Entry<TxnPeerID, Integer> trustedTxn : priorities.entrySet()) {
				try {
					TxnChain tc = new TxnChain(trustedTxn.getKey(), ts, td);
					List<TxnChain> txns = trustedTxns.get(trustedTxn.getValue());
					if (txns == null) {
						txns = new ArrayList<TxnChain>();
						trustedTxns.put(trustedTxn.getValue(), txns);
					}
					txns.add(tc);

					ByteBufferWriter bbw = new ByteBufferWriter();
					bbw.addToBuffer(tc.getHead());
					for (TxnPeerID tpi : tc.getTail()) {
						bbw.addToBuffer(tpi);
					}

				} catch (AlreadyRejectedAntecedent ara) {
					mustReject.add(trustedTxn.getKey());
				} catch (USException e) {
					throw e;
				} catch (Exception e) {
					throw new P2PStoreException(e);
				}
			}
		} catch (InterruptedException ie) {
			throw new P2PStoreException(ie);
		}
	}

	@Override
	public int getCurrentRecno() throws P2PStoreException {
		synchronized (this) {
			return currentRecno;
		}
	}

	public boolean forward(RouteMessage routeMessage) {
		return true;
	}

	public void deliver(Id id, Message message) {
		if (! (message instanceof P2PMessage)) {
			System.err.println("P2PStore received message of invalid type: " + message);
		}
		P2PMessage m = (P2PMessage) message;
		try {
			msgQueue.put(m);
		} catch (InterruptedException ie) {
			System.out.println("Not enqueuing message due to InterruptedException: " + m);
		}
	}

	IdRange getRange(NodeHandle nh, int replicationFactor) {
		try {
			return endpoint.range(nh, replicationFactor, nh.getId(), true);
		} catch (RangeCannotBeDeterminedException e) {
			// This node doesn't have enough information to determine
			// another node's range
		}

		try {
			return replicationController.getRemoteRange(nh);
		} catch (Exception e) {
			System.err.println("Couldn't determine range for " + nh + ": " + e);
			e.printStackTrace();
		}

		return null;
	}

	IdRange getLocalRange(int replicationFactor) throws RangeCannotBeDeterminedException {
		return endpoint.range(node.getLocalNodeHandle(), replicationFactor, node.getId(), true);
	}

	private final TransactionConfig tc = new TransactionConfig();
//	{ tc.setReadUncommitted(true); }

	Transaction beginTxn() throws DatabaseException {
		Transaction t = env.beginTransaction(null, tc);
		t.setLockTimeout(100000);
		return t;
	}

	private void processMessage(P2PMessage m, MessageProcessorThread processor) {
		try {
			if (m.isReply()) {
				// Dispatch through reply table
				ReplyData rd = replyData.get(m.getRequestMessageId());
				if (rd == null) {
					// Silently ignore replies we're not interested in
					return;
				}
				synchronized (rd) {
					// Avoid race condition two replies arrive simultaneously,
					// and one causes success or failure
					if (rd.hasFinished) {
						return;
					}
					if (rd.checkForTimeout != null) {
						rd.checkForTimeout.cancel();
					}
					boolean isSuccess = false;
					for (Class<?> c : rd.successReplies) {
						if (c.isInstance(m)) {
							isSuccess = true;
							break;
						}
					}
					if (rd.retriesRemaining == 0 || isSuccess) {
						rd.rc.processReply(m, processor);
						if (rd.rc.isFinished() || ((! isSuccess) && rd.retriesRemaining == 0)) {
							rd.hasFinished = true;
							replyData.remove(m.getRequestMessageId());
						} else {
							if (rd.checkForTimeout != null) {
								rd.checkForTimeout = endpoint.scheduleMessage(new ReplyTimeout(rd.m), rd.messageTimeout);
							}
						}
					} else {
						if (rd.checkForTimeout != null) {
							rd.checkForTimeout = endpoint.scheduleMessage(new ReplyTimeout(rd.m), rd.messageTimeout);
						}
						--rd.retriesRemaining;
						endpoint.scheduleMessage(rd.m, rd.retryDelay);
					}
				}
				return;
			}
		} catch (Exception e) {
			System.err.println("Caught exception processing reply: " + m);
			e.printStackTrace();
			return;
		}

		int retryCount = 0;

		for ( ; ; ) {
			Transaction t = null;
			P2PMessage reply;

			try {
				t = beginTxn();
				if (m instanceof EpochAllocatorMessage) {
					reply = epochAllocator.processMessage((EpochAllocatorMessage) m, t, processor);
				} else if (m instanceof EpochControllerMessage) {
					reply = epochController.processMessage((EpochControllerMessage) m, t, processor);
				} else if (m instanceof TransactionControllerMessage) {
					reply = transactionController.processMessage((TransactionControllerMessage) m, t, processor);
				} else if (m instanceof PeerControllerMessage) {
					reply = peerController.processMessage((PeerControllerMessage) m, t, processor);
				} else if (m instanceof ReplicationControllerMessage) {
					reply = replicationController.processMessage((ReplicationControllerMessage) m, t, processor);
				} else {
					System.err.println("P2PStore doesn't know what to do with message: " + m);
					reply = null;
				}
				t.commit();
				if (reply != null) {
					sendMessage(reply);
				}
			} catch (DeadlockException de) {
				try {
					t.abort();
				} catch (DatabaseException e) {
					System.err.println("Chouldn't abort transaction while processing " + m);
					e.printStackTrace();
					return;
				}
				++retryCount;
				if (retryCount < NUM_DEADLOCK_RETRIES) {
					continue;
				} else {
					System.err.println("Too many deadlocks while processing " + m);
					de.printStackTrace();
					sendMessage(new ReplyException("BerkeleyDB deadlock processing message", de, m));
				}
			} catch (DatabaseException de) {
				sendMessage(new ReplyException("BerkeleyDB error processing message", de, m));
				System.err.println("BerkeleyDB error processing " + m);
				de.printStackTrace();
				try {
					t.abort();
				} catch (DatabaseException e) {
					System.err.println("Chouldn't abort transaction after catching exception while processing " + m);
					e.printStackTrace();
				}
			} catch (InterruptedException ie) {
				System.err.println("Interrupted while processing " + m);
				ie.printStackTrace();
				sendMessage(new ReplyException("Interrupted while processing message", ie, m));
			} catch (Exception e) {
				System.err.println("Error while processing " + m);
				e.printStackTrace();
				sendMessage(new ReplyException("Error processing message", e, m));
			}
			break;
		}
	}

	public void update(NodeHandle handle, boolean joined) {
		if (! joined) {
			// Only deal with nodes joining. We could also perform replica set maintenance
			// here, but for now let's defer that to periodic background maintenance using
			// the pastry replica manager
			return;
		}
		try {
			replicationController.replicateToNewPeer(handle);
		} catch (Exception e) {
			System.err.println("Caught exception while performing P2PStore.update(" + handle + "," + joined +")");
			e.printStackTrace();
		}
	}

	private static class ReplyData {
		ReplyContinuation rc;
		P2PMessage m;
		int retryDelay;
		int messageTimeout;
		int retriesRemaining;
		CancellableTask checkForTimeout;
		Set<Class<?>> successReplies;
		boolean hasFinished;

		ReplyData(ReplyContinuation rc, P2PMessage m, int retryDelay, int numRetries, int messageTimeout, CancellableTask checkForTimeout, Set<Class<?>> successReplies) {
			this.rc = rc;
			this.m = m;
			this.retryDelay = retryDelay;
			this.retriesRemaining = numRetries;
			this.messageTimeout = messageTimeout;
			this.checkForTimeout = checkForTimeout;
			this.successReplies = successReplies;
			hasFinished = false;
		}
	}

	void sendMessageAwaitReply(P2PMessage m, ReplyContinuation rc, int retryDelay, int numRetries, int messageTimeout,
			Class<?>... successReplies) {
		Set<Class<?>> success = new HashSet<Class<?>>(successReplies.length);
		for (Class<?> c : successReplies) {
			success.add(c);
		}
		sendMessageAwaitReply(m, rc, retryDelay, numRetries, messageTimeout, success);
	}

	void sendMessageAwaitReply(P2PMessage m, ReplyContinuation rc, int retryDelay, int numRetries, int messageTimeout,
			Set<Class<?>> successReplies) {
		CancellableTask checkForTimeout = null;
		if (messageTimeout > 0) {
			checkForTimeout = endpoint.scheduleMessage(new ReplyTimeout(m), messageTimeout);
		}

		ReplyData rd = new ReplyData(rc, m, retryDelay, numRetries, messageTimeout, checkForTimeout, successReplies);
		synchronized (replyData) {
			replyData.put(m.getMessageId(), rd);
		}
		sendMessage(m);
	}

	void sendMessageAwaitReply(P2PMessage m, ReplyContinuation rc,
			Class<?>... successReplies) {
		sendMessageAwaitReply(m, rc, DEFAULT_RETRY_DELAY, DEFAULT_NUM_RETRIES, DEFAULT_MESSAGE_TIMEOUT, successReplies);
	}

	void sendSelfMessageAfterTimeoutDelay(P2PMessage m) {
		endpoint.scheduleMessage(m, DEFAULT_MESSAGE_TIMEOUT);
	}

	void sendMessage(P2PMessage m) {
		m.send(endpoint);
	}

	NodeHandleSet getReplicaSet(Id id, int replicationFactor) {
		return endpoint.replicaSet(id, replicationFactor);
	}

	SocketNodeHandle getNodeHandle() {
		return (SocketNodeHandle) endpoint.getLocalNodeHandle();
	}

	class MonitorThread extends Thread {
		private MonitorThread(ThreadGroup tg) {
			super(tg, "P2P Store " + pid + " Monitor Thread");
		}

		public void run() {
			try {
				while (! isInterrupted()) {
					sleep(THREAD_CHECK_INTERVAL);
					if (msgQueue.isEmpty()) {
						continue;
					}
					boolean foundNonWaiting = false;
					for (MessageProcessorThread mpt : msgProcessingThreads) {
						if (! mpt.isAwaitingReply()) {
							foundNonWaiting = true;
						}
					}
					if (! foundNonWaiting) {
						msgProcessingThreads.add(new MessageProcessorThread(threadGroup, msgProcessingThreads.size()));
					}
				}
			} catch (InterruptedException ie) {
				return;
			}
		}
	}

	class MessageProcessorThread extends Thread {
		private boolean isAwaitingReply = false;
		private boolean isBusy = false;

		private boolean isAwaitingMessage = false;
		private boolean isInterrupted = false;

		synchronized void startAwaitingReply() {
			if (isAwaitingReply) {
				throw new RuntimeException("Was already awaiting reply");
			}
			isAwaitingReply = true;
		}

		synchronized void stopAwaitingReply() {
			if (! isAwaitingReply) {
				throw new RuntimeException("Was not awaiting reply");
			}
			isAwaitingReply = false;
		}

		synchronized boolean isAwaitingReply() {
			return isAwaitingReply;
		}

		synchronized boolean isBusy() {
			return isBusy;
		}

		private MessageProcessorThread(ThreadGroup tg, int i) {
			super(tg, "P2P Store " + pid + " Message Processor Thread #" + i);
		}

		public synchronized void interrupt() {
			isInterrupted = true;
			if (isAwaitingMessage) {
				super.interrupt();
			}
		}

		public void run() {
			for ( ; ; ) {
				synchronized (this) {
					if (isInterrupted) {
						return;
					}
					isAwaitingMessage = true;
				}
				P2PMessage m;
				try {
					m = msgQueue.take();
				} catch (InterruptedException e) {
					return;
				}
				synchronized (this) {
					isAwaitingMessage = false;
					if (isInterrupted) {
						return;
					}
					isBusy = true;
				}

				processMessage(m, this);
				synchronized (this) {
					isBusy = false;
				}
			}

		}
	}

	Id getId(AbstractPeerID pid) {
		return idFactory.getIdFromContent(pid.getBytes());
	}

	Id getId(TxnPeerID tpi) {
		return idFactory.getIdFromContent(tpi.getBytes());
	}

	Id getId(int i) {
		return idFactory.getIdFromContent(IntType.getBytes(i));
	}

	Id getId(PidAndRecno par) {
		return idFactory.getIdFromContent(par.getBytes());
	}

	Id deserializeId(byte[] serializedId) {
		return idFactory.getIdFromByteArray(serializedId);
	}

	Benchmark benchmark;

	public Benchmark getBenchmark() {
		return benchmark;
	}

	public void setBenchmark(Benchmark b) {
		benchmark = b;
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
	public List<Decision> getDecisions(int recno) throws P2PStoreException {
		int lastRecno = getCurrentRecno() - 1;
		try {
			return peerController.getDecisions(pid, recno, lastRecno, true);
		} catch (ReconciliationRecordException e) {
			throw new P2PStoreException(e);
		} catch (InterruptedException e) {
			throw new P2PStoreException(e);
		}
	}

	@Override
	public ResultIterator<ReconciliationEpoch> getReconciliations() throws P2PStoreException {
		int lastRecno = getCurrentRecno() - 1;
		return new IntegerIterator<ReconciliationEpoch>(lastRecno) {

			@Override
			protected ReconciliationEpoch getData(int recno) throws IteratorException {
				try {
					int epoch = peerController.getReconciliationEpoch(pid, recno, getCurrentRecno(), null);
					return new ReconciliationEpoch(recno, epoch);
				} catch (UnexpectedReply e) {
					throw new IteratorException(e);
				} catch (P2PStoreException e) {
					throw new IteratorException(e.getMessage(), e.getCause());
				} catch (ReconciliationRecordException e) {
					throw new IteratorException(e);
				} catch (InterruptedException ie) {
					throw new IteratorException(ie);
				}
			}

		};
	}

	/**
	 * Determine the most recent epoch that has been published. Unlike
	 * {@link EpochAllocator.getLastEpoch()}, it also checks
	 * to make sure that nothing later has been stored in the
	 * distributed database.
	 * 
	 * @return The number of the most recent published epoch
	 * @throws UnexpectedReply
	 * @throws InterruptedException
	 */
	int getMostRecentEpoch() throws UnexpectedReply, InterruptedException {
		int mostRecentEpoch;
		int foundEpoch = epochAllocator.getLastEpoch();

		do {
			mostRecentEpoch = foundEpoch;
			P2PMessage m = new RequestMostRecentEpoch(getId(mostRecentEpoch));
			ReplyHolder<Integer> replies = new ReplyHolder<Integer>(1);
			sendMessageAwaitReply(m, new SimpleReplyContinuation<Integer>(1, replies), MostRecentEpochIs.class);

			replies.waitUntilFinished();
			P2PMessage reply = replies.getReply(1);
			if (reply instanceof MostRecentEpochIs) {
				foundEpoch = ((MostRecentEpochIs) reply).mostRecentEpoch;
			} else {
				throw new UnexpectedReply(m, reply);
			}
		} while (foundEpoch > mostRecentEpoch);


		return mostRecentEpoch;
	}

	@Override
	public ResultIterator<Update> getPublishedUpdatesForRelation(String relname) throws P2PStoreException {
		int mostRecentEpoch;
		try {
			mostRecentEpoch = getMostRecentEpoch();
		} catch (InterruptedException e) {
			throw new P2PStoreException("Interrupted while awaiting most recent epoch", e);
		}
		try {
			return new FlatteningIterator<Update>(new TransactionIterator(new FlatteningIterator<TxnPeerID>(new TransactionIdIterator(mostRecentEpoch)), relname));
		} catch (Exception e) {
			throw new P2PStoreException("Error creating update iterator", e);
		}
	}

	class TransactionIterator extends MappingIterator<List<Update>, TxnPeerID> {
		private final String relName;		

		public TransactionIterator(ResultIterator<TxnPeerID> subIterator) {
			this(subIterator,null);
		}

		public TransactionIterator(ResultIterator<TxnPeerID> subIterator, String relName) {
			super(subIterator);
			this.relName = relName;
		}

		@Override
		public List<Update> convert(TxnPeerID input) throws IteratorException {
			List<Update> txn;
			try {
				txn = getTransaction(input);
			} catch (USException e) {
				throw new IteratorException(e.getMessage(), e.getCause());
			}
			if (relName == null) {
				return txn;
			} else {
				ArrayList<Update> filtered = new ArrayList<Update>();
				for (Update u : txn) {
					if (u.getRelationName().equals(relName)) {
						filtered.add(u);
					}
				}
				return filtered;
			}
		}

	}

	class TransactionIdIterator extends IntegerIterator<List<TxnPeerID>> {

		TransactionIdIterator(int lastEpoch) {
			super(lastEpoch);
		}

		@Override
		protected List<TxnPeerID> getData(int pos) throws IteratorException {
			P2PMessage request = new RequestPublishedEpoch(pos, getId(pos));

			ReplyHolder<Integer> replies = new ReplyHolder<Integer>(1);
			sendMessageAwaitReply(request, new SimpleReplyContinuation<Integer>(1, replies),
					PublishEpoch.class);

			try {
				replies.waitUntilFinished();
			} catch (InterruptedException e) {
				throw new IteratorException(e);
			}
			P2PMessage reply = replies.getReply(1);
			if (reply instanceof PublishEpoch) {
				return ((PublishEpoch) reply).getTids();
			} else {
				throw new IteratorException(new UnexpectedReply(request, reply));
			}
		}

	}

	@Override
	public List<Update> getTransaction(TxnPeerID txn) throws P2PStoreException {
		ReplyHolder<Integer> replies = new ReplyHolder<Integer>(1);
		RequestTxn rt = new RequestTxn(txn, getId(txn));
		sendMessageAwaitReply(rt, new SimpleReplyContinuation<Integer>(1, replies), PublishedTxnIs.class);

		try {
			replies.waitUntilFinished();
		} catch (InterruptedException ie) {
			throw new P2PStoreException("Interrupted while awaiting reply", ie);
		}

		P2PMessage m = replies.getReply(1);
		if (m instanceof PublishedTxnIs) {
			return ((PublishedTxnIs) m).getTxn(_mapStore);//schema);
		} else {
			throw new UnexpectedReply(rt, m);
		}
	}

	@Override
	public ResultIterator<TxnPeerID> getTransactionsForReconciliation(int recno) throws P2PStoreException {
		int currentRecno = getCurrentRecno();

		int firstEpoch, lastEpoch;

		try {
			if (currentRecno == PeerController.FIRST_RECNO || recno >= currentRecno) {
				lastEpoch = getMostRecentEpoch();
				if (currentRecno == PeerController.FIRST_RECNO) {
					firstEpoch = EpochAllocator.FIRST_EPOCH;
				} else {
					firstEpoch = peerController.getReconciliationEpoch(pid, recno - 1, currentRecno - 1, null);
				}
			} else {
				lastEpoch = peerController.getReconciliationEpoch(pid, recno, currentRecno - 1, null);
				if (recno == PeerController.FIRST_RECNO) {
					firstEpoch = EpochAllocator.FIRST_EPOCH;
				} else {
					firstEpoch = peerController.getReconciliationEpoch(pid, recno - 1, currentRecno - 1, null);
				}
			}
		} catch (InterruptedException ie) {
			throw new P2PStoreException("Interrupted while retrieving reconciliation epoch", ie);
		} catch (PeerController.ReconciliationRecordException rre) {
			throw new P2PStoreException(rre);
		}


		try {
			return new FlatteningIterator<TxnPeerID>(new TxnsForEpoch(firstEpoch,lastEpoch));
		} catch (IteratorException e) {
			if (e.getCause() instanceof P2PStoreException) {
				throw ((P2PStoreException) e.getCause());
			} else {
				throw new P2PStoreException(e.getMessage(), e.getCause());
			}
		}
	}

	private class TxnsForEpoch extends IntegerIterator<List<TxnPeerID>> {
		public TxnsForEpoch(int first, int last) {
			super(first, last);
		}

		@Override
		protected List<TxnPeerID> getData(int epoch) throws IteratorException {
			P2PMessage m = new RequestPublishedEpoch(epoch, getId(epoch));
			ReplyHolder<Integer> replies = new ReplyHolder<Integer>(1);			
			sendMessageAwaitReply(m, new SimpleReplyContinuation<Integer>(1, replies));
			try {
				replies.waitUntilFinished();
			} catch (InterruptedException e) {
				throw new IteratorException("Interrupted while awaiting contents of epoch " + epoch, e);
			}

			P2PMessage reply = replies.getReply(1);
			if (reply instanceof PublishEpoch) {
				return ((PublishEpoch) reply).getTids();
			} else {
				throw new IteratorException(new UnexpectedReply(m, reply));
			}
		}

	}

	@Override
	public Map<TxnPeerID, List<Update>> getTransactionsAcceptedAtRecno(int recno) throws P2PStoreException {
		List<Decision> decisions;
		try {
			decisions = peerController.getDecisions(pid, recno, getCurrentRecno() - 1, false);
		} catch (ReconciliationRecordException e) {
			throw new P2PStoreException(e);
		} catch (InterruptedException e) {
			throw new P2PStoreException(e);
		}
		HashSet<TxnPeerID> accepted = new HashSet<TxnPeerID>();
		
		for (Decision d : decisions) {
			if (d.recno != recno) {
				throw new P2PStoreException("Recevied unexpected decision for reconciliation " + d.recno + ", expected " + recno);
			}
			if (d.accepted) {
				accepted.add(d.tpi);
			}
		}
		
		return getTxns(accepted);
	}
	
	private Map<TxnPeerID,List<Update>> getTxns(Collection<TxnPeerID> tpis ) throws P2PStoreException {
		ReplyHolder<TxnPeerID> replies = new ReplyHolder<TxnPeerID>(tpis.size());
		
		Map<TxnPeerID,P2PMessage> requests = new HashMap<TxnPeerID,P2PMessage>();
		
		for (TxnPeerID tpi : tpis) {
			P2PMessage m = new RequestTxn(tpi, getId(tpi));
			requests.put(tpi,m);
			sendMessageAwaitReply(m, new SimpleReplyContinuation<TxnPeerID>(tpi, replies), PublishedTxnIs.class);
		}
		
		try {
			replies.waitUntilFinished();
		} catch (InterruptedException ie) {
			throw new P2PStoreException("Interrupted while requesting transaction contents", ie);
		}
		
		Map<TxnPeerID,List<Update>> retval = new HashMap<TxnPeerID,List<Update>>(tpis.size());

		for (TxnPeerID tpi : tpis) {
			P2PMessage response = replies.getReply(tpi);
			if (response instanceof PublishedTxnIs) {
				retval.put(tpi, ((PublishedTxnIs) response).getTxn(_mapStore));//schema));
			} else {
				throw new UnexpectedReply(requests.get(tpi), response);
			}
		}
		
		return retval;
	}

	@Override
	public int getLargestTidForPeer() throws USException {
		// TODO: implement P2PStore.getLargestTidForPeer
		throw new RuntimeException("Need to implement P2PStore.getLargestTidForPeer");
	}
}
