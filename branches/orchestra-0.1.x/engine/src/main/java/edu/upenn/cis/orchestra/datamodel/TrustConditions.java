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
package edu.upenn.cis.orchestra.datamodel;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.predicate.Byteification;
import edu.upenn.cis.orchestra.predicate.Predicate;
import edu.upenn.cis.orchestra.predicate.XMLification;
import edu.upenn.cis.orchestra.reconciliation.DbException;
import edu.upenn.cis.orchestra.reconciliation.ISchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.Db.IllegalPeer;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;


public class TrustConditions {
	private static final long serialVersionUID = 1L;

	// Mapping from relation id to priority to Trust object. It uses a reverse ordering
	// on priorities so higher priorities come first
	private final Map<Integer,TreeMap<Integer,Set<Trusts>>> trustConds;

	// ID of peer that owns this object
	private final AbstractPeerID id;

	public static final int MAX_PRIORITY = 100000;
	public static final int OWN_TXN_PRIORITY = MAX_PRIORITY + 1;
	
	
	public TrustConditions(AbstractPeerID id) {
		this.id = id.duplicate();
		trustConds = new HashMap<Integer,TreeMap<Integer,Set<Trusts>>>();
	}
	
	public TrustConditions(TrustConditions tc) {
		this(tc.id);
		
		for (Map.Entry<Integer, TreeMap<Integer,Set<Trusts>>> me : tc.trustConds.entrySet()) {
			int relId = me.getKey();
			TreeMap<Integer,Set<Trusts>> tcs = new TreeMap<Integer,Set<Trusts>>(Collections.reverseOrder());
			trustConds.put(relId, tcs);
			
			for (Map.Entry<Integer, Set<Trusts>> me2 : me.getValue().entrySet()) {
				HashSet<Trusts> trusts = new HashSet<Trusts>(me2.getValue());
				tcs.put(me2.getKey(), trusts);
			}
		}
	}
	
	public AbstractPeerID getOwner() {
		return id.duplicate();
	}
	
	/**
	 * @author Nick Taylor
	 * 
	 * Class to represent a trust condition in the P2P database.
	 */
	public static class Trusts {
		private static final long serialVersionUID = 1L;
		/**
		 * Creates a new trust condition
		 * 
		 * @param trusted The peer who's update is being accepted
		 * @param cond The tuple-level predicate over that must be satisified for the update
		 * to be accepted, or <code>null</code> if all tuples should be accepted 
		 */
		private Trusts(AbstractPeerID trusted, Predicate cond) {
			trustedPeer = trusted;
			condition = cond;
			
		}
		
		public String toString() {
			return trustedPeer + ":" + condition;
		}
				
		public final AbstractPeerID trustedPeer;
		public final Predicate condition;
		
		public int hashCode() {
			return trustedPeer.hashCode();
		}
		
		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass()) {
				return false;
			}
			
			Trusts t = (Trusts) o;
			
			if (! t.trustedPeer.equals(trustedPeer)) {
				return false;
			}
			if (t.condition == null) {
				return (condition == null);
			} else {
				return t.condition.equals(condition);
			}
		}
	}
	
	/**
	 * Class to represent an attempt to use an improper priority
	 * for a trust condition.
	 * 
	 * @author Nick Taylor
	 */
	public static class BadPrio extends DbException {
		private static final long serialVersionUID = 1L;
	
		public BadPrio(int prio) {
			super("Attempt to assign out of range priority " + prio
					+ "to a trust condition");
		}
	}

	public int getTuplePriority(AbstractPeerID trustedPeer, Tuple tuple) throws CompareMismatch {
		if (id.equals(trustedPeer)) {
			return OWN_TXN_PRIORITY;
		}
		
		
		TreeMap<Integer,Set<Trusts>> tc = trustConds.get(tuple.getRelationID());
		
		if (tc == null || tc.isEmpty()) {
			return 1;
		}

		// Since the map for each relation is sorted by decreasing order of priority,
		// we can just use the first matching trust condition
		for (Map.Entry<Integer,Set<Trusts>> me : tc.entrySet()) {
			for (Trusts t : me.getValue()) {
				if (! (t.trustedPeer.equals(trustedPeer)))
					continue;
				if (t.condition == null || t.condition.eval(tuple)) {
					return me.getKey();
				}
			}
		}

		return 0;
	}
	
	public int getUpdatePriority(Update u) throws CompareMismatch {
		
		int prio = 0;

		AbstractPeerID pi = u.getLastTid().getPeerID();
		
		
		if (u.getOldVal() != null) {
			int oldPrio = getTuplePriority(pi, u.getOldVal());
			if (oldPrio > prio) {
				prio = oldPrio;
			}
		}
		if (u.getNewVal() != null) {
			int newPrio = getTuplePriority(pi, u.getNewVal());
			if (newPrio > prio) {
				prio = newPrio;
			}
		}
		
		return prio;
	}
	
	public int getTxnPriority(List<Update> txn) throws CompareMismatch {
		int prio = 0;
		
		for (Update u : txn) {
			int updatePrio = getUpdatePriority(u);
			if (updatePrio == 0) {
				return 0;
			} else if (updatePrio > prio) {
				prio = updatePrio;
			}
		}
		
		return prio;
	}

	/**
	 * Register a new trust condition.
	 * 
	 * @param trusted	Peer that performed the updates
	 * @param s			The peer's schema
	 * @param relName	Name of the relation to consider
	 * @param cond		Tuple-level predicate the updates much satisfy, or <code>null</code>
	 * 					if all updates satisfy predicate
	 * @param prio		Condition priority > 0, larger = higher priority
	 * @throws TrustConditions.BadPrio
	 * @throws IllegalPeer
	 */
	public void addTrustCondition(AbstractPeerID trusted, Schema s, String relName, Predicate cond, int prio) throws BadPrio, IllegalPeer {
		int relId = s.getIDForName(relName);
		addTrustCondition(trusted, relId, cond, prio);
	}
	
	/**
	 * Register a new trust condition.
	 * 
	 * @param trusted	Peer that performed the updates
	 * @param relationId
	 * 					Id of the relation to consider
	 * @param cond		Tuple-level predicate the updates much satisfy, or <code>null</code>
	 * 					if all updates satisfy predicate
	 * @param prio		Condition priority > 0, larger = higher priority
	 * @throws TrustConditions.BadPrio
	 * @throws IllegalPeer
	 */
	public void addTrustCondition(AbstractPeerID trusted, int relationId, Predicate cond, int prio) throws BadPrio, IllegalPeer {
		if (prio <= 0 || prio > TrustConditions.MAX_PRIORITY) {
			throw new TrustConditions.BadPrio(prio);
		}

		TreeMap<Integer,Set<Trusts>> tc = trustConds.get(relationId);
		if (tc == null) {
			tc = new TreeMap<Integer,Set<Trusts>>(Collections.reverseOrder());
			trustConds.put(relationId, tc);
		}
		Set<Trusts> trusts = tc.get(prio);
		if (trusts == null) {
			trusts = new HashSet<Trusts>();
			tc.put(prio, trusts);
		}
		trusts.add(new Trusts(trusted, cond));
	}

	public TrustConditions duplicate() {
		return new TrustConditions(this);
	}
	
	public String toString() {
		return toString(null);
	}
	
	public String toString(Schema s) {
		StringBuilder retval = new StringBuilder();
		for (Integer relID : trustConds.keySet()) {
			if (s == null) {
				retval.append("Relation " + relID + ":\n");
			} else {
				retval.append(s.getNameForID(relID) + ":\n");
			}
			for (Map.Entry<Integer, Set<Trusts>> me : trustConds.get(relID).entrySet()) {
				retval.append("\t" + me.getKey() + ":");
				for (Trusts t : me.getValue()) {
					retval.append(" " + t);
				}
				retval.append("\n");
			}
		}
		return retval.toString();
	}
	
	/**
	 * Get the trust conditions
	 * 
	 * @return A mapping from relation id to priority to Trust object. It uses a reverse
	 * ordering on priorities so higher priorities come first

	 */
	public Map<Integer,TreeMap<Integer,Set<Trusts>>> getConditions() {
		return trustConds;
	}
	
	/**
	 * Determine if this trust conditions object is empty or not
	 * 
	 * @return <code>true</code> if this trust conditions object is empty,
	 * <code>false</code> if it is not
	 */
	public boolean empty() {
		return (trustConds.isEmpty());
	}
	
	public byte[] getBytes(ISchemaIDBinding s) {
		ByteBufferWriter bbw = new ByteBufferWriter();
		bbw.addToBuffer(id);
		
		for (Map.Entry<Integer,TreeMap<Integer,Set<Trusts>>> outerEntry : trustConds.entrySet()) {
			int relId = outerEntry.getKey();
//			Relation rs = s.getRelationSchema(relId);
			Relation rs = s.getRelationFor(relId);
			bbw.addToBuffer(relId);
			TreeMap<Integer,Set<Trusts>> rel = outerEntry.getValue();
			bbw.addToBuffer(rel.size());
			for (Map.Entry<Integer, Set<Trusts>> innerEntry : rel.entrySet()) {
				int prio = innerEntry.getKey();
				Set<Trusts> entries = innerEntry.getValue();
				bbw.addToBuffer(prio);
				bbw.addToBuffer(entries.size());
				for (Trusts t : entries) {
					bbw.addToBuffer(t.trustedPeer);
					bbw.addToBuffer(Byteification.getPredicateBytes(rs, t.condition));
				}
			}
		}
		
		return bbw.getByteArray();
	}
	
	public TrustConditions(byte[] bytes, ISchemaIDBinding s) {
		trustConds = new HashMap<Integer,TreeMap<Integer,Set<Trusts>>>();
		
		ByteBufferReader bbr = new ByteBufferReader(s, bytes);
		id = bbr.readPeerID();

		while (! bbr.hasFinished()) {
			int relId = bbr.readInt();
			Relation rs = s.getRelationFor(relId);
			for (int numPrios = bbr.readInt(); numPrios > 0; --numPrios) {
				int prio = bbr.readInt();
				for (int numEntries = bbr.readInt(); numEntries > 0; --numEntries) {
					AbstractPeerID trusted = bbr.readPeerID();
					try {
						Predicate cond = Byteification.getPredicateFromBytes(rs, bbr.readByteArray());
						addTrustCondition(trusted, relId, cond, prio);
					} catch (Exception e) {
						throw new RuntimeException("Error decoding trust conditions: " + e);
					}
				}
			}
			
		}
	}
	
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		
		TrustConditions tc = (TrustConditions) o;
		
		if (! id.equals(tc.id)) {
			return false;
		}
		
		return (id.equals(tc.id) && trustConds.equals(tc.trustConds));
	}
	
	public void serialize(Document d, Element el, Schema s) {
		for (int relId : trustConds.keySet()) {
			Map<Integer,Set<Trusts>> priorities = trustConds.get(relId);
			String relName = s.getNameForID(relId);
			Relation rs = s.getRelationSchema(relId);
			for (int priority : priorities.keySet()) {
				Set<Trusts> conds = priorities.get(priority);
				for (Trusts t : conds) {
					Element trusts = DomUtils.addChild(d, el, "trusts");
					trusts.setAttribute("priority", Integer.toString(priority));
					trusts.setAttribute("relation", relName);
					t.trustedPeer.serialize(d, trusts);
					if (t.condition == null) {
						trusts.setAttribute("noPred", Boolean.toString(true));
					} else {
						XMLification.serialize(t.condition, d, trusts, rs);
					}
				}
			}
		}
	}
	
	/**
	 * Appends to {@code el} an XML representation of each trust condition in
	 * this {@code TrustConditions}.
	 * 
	 * <p>
	 * Unlike {@link TrustConditions#serialize(Document, Element, Schema)
	 * serialize(Document, Element, Schema)} this uses {@code schemaIDBinding}
	 * and not a {@code Schema} to look up relation names. This method was added to help
	 * {@link TestTrustConditions#testXMLification() testXMLification()} and it
	 * is not clear if it is actually correct. See <a href="https://dbappserv.cis.upenn.edu/bugzilla/show_bug.cgi?id=123"> bug 123</a>
	 * </p>
	 * 
	 * @param d
	 * @param el
	 * @param schemaIDBinding
	 * @see TrustConditions#serialize(Document, Element, Schema)
	 */
	public void serialize(Document d, Element el, ISchemaIDBinding schemaIDBinding) {
		for (int relId : trustConds.keySet()) {
			Map<Integer,Set<Trusts>> priorities = trustConds.get(relId);
			Relation rs = schemaIDBinding.getRelationFor(relId);
			String relName = rs.getName();
			for (int priority : priorities.keySet()) {
				Set<Trusts> conds = priorities.get(priority);
				for (Trusts t : conds) {
					Element trusts = DomUtils.addChild(d, el, "trusts");
					trusts.setAttribute("priority", Integer.toString(priority));
					trusts.setAttribute("relation", relName);
					t.trustedPeer.serialize(d, trusts);
					if (t.condition == null) {
						trusts.setAttribute("noPred", Boolean.toString(true));
					} else {
						XMLification.serialize(t.condition, d, trusts, rs);
					}
				}
			}
		}
	}
	
	public static TrustConditions deserialize(Element el, Collection<Peer> peers, AbstractPeerID owner) throws XMLParseException {
		TrustConditions retval = new TrustConditions(owner);
		List<Element> trustsEls = DomUtils.getChildElementsByName(el, "trusts");
		for (Element trusts : trustsEls) {
			if (! trusts.hasAttribute("priority")) {
				throw new XMLParseException("Trusts element must have attribute 'priority'", trusts);
			}
			int priority;
			try {
				priority = Integer.parseInt(trusts.getAttribute("priority"));
			} catch (NumberFormatException nfe) {
				throw new XMLParseException("Could not parse priority", nfe, trusts);
			}
			if (! trusts.hasAttribute("relation")) {
				throw new XMLParseException("Trusts element must have attribute 'relation'", trusts);
			}
			
			AbstractPeerID trusted = AbstractPeerID.deserialize(trusts);

			// Find the schema for the relation in the peer
			Relation rs = null;
			for (Peer p : peers) {
				if (p.getPeerId().equals(trusted)) {
					for (Schema sc: p.getSchemas()) {
						rs = sc.getRelationSchema(trusts.getAttribute("relation"));
						if (rs != null)
							break;
					}
				}
				if (rs != null)
					break;
			}
			if (rs == null) {
				throw new XMLParseException("Relation " + trusts.getAttribute("relation") + " not found in any schema on " + trusted.toString(), trusts);
			}
			boolean noPred = Boolean.parseBoolean(trusts.getAttribute("noPred"));
			Predicate condition;
			if (noPred) {
				condition = null;
			} else if (! trusts.hasAttribute(XMLification.predTypeAttribute)) {
				// Backwards compatibility
				condition = null;
			} else {
				condition = XMLification.deserialize(trusts, rs);
			}
			try {
				retval.addTrustCondition(trusted, rs.getRelationID(), condition, priority);
			} catch (TrustConditions.BadPrio e) {
				throw new XMLParseException(e,trusts);
			} catch (IllegalPeer e) {
				throw new XMLParseException(e,trusts);
			}
		}
		return retval;
	}
	
	public void addTrustConditions(TrustConditions tc) {
		for (int relId : tc.trustConds.keySet()) {
			Map<Integer,Set<Trusts>> priorities = tc.trustConds.get(relId);
			for (int priority : priorities.keySet()) {
				Set<Trusts> conds = priorities.get(priority);
				for (Trusts t : conds) {
					try {
						this.addTrustCondition(t.trustedPeer, relId, t.condition, priority);
					} catch (TrustConditions.BadPrio e) {
						throw new RuntimeException("Shouldn't get expection when copying trust conditions", e);
					} catch (IllegalPeer e) {
						throw new RuntimeException("Shouldn't get expection when copying trust conditions", e);
					}
				}
			}
		}
	}
}
