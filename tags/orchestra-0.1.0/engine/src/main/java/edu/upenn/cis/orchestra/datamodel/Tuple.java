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

import java.util.SortedSet;

import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;



/**
 * Class to represent a tuple that exists inside a schema of multiple
 * relations.
 * 
 * @author netaylor
 */
public class Tuple extends AbstractMutableTuple<Relation> {
	static final long serialVersionUID = 1L;
	
	/** The logical-level relation from which this tuple originated */
	private RelationContext _peerRelationContext;
	
	private String _provenanceExpr = null;
	

	/**
	 * Create a new, empty tuple for the enclosing schema.
	 * @param schema		The schema of the relation that this tuple conforms to
	 */
	public Tuple(Relation schema) {
		super(schema);
	}
	
	/**
	 * Create a new Tuple that is a deep copy of the specified tuple
	 * 
	 * @param t		The copy
	 */
	public Tuple(Tuple t) {
		super(t);
	}
	
	/**
	 * Create a deep copy of this Tuple.
	 * 
	 * @return		The copy
	 */
	public Tuple duplicate() {
		return new Tuple(this);
	}
			
	public int getRelationID() {
		return getSchema().getRelationID();
	}
	
	/**
	 * Generate a byte string containing the relation ID
	 * followed by the values of the key columns
	 * 
	 * @return		The byte string
	 */
	public byte[] getKeyColumnBytes() {
		return getBytes(false);
	}
	
	public byte[] getBytes() {
		return getBytes(true);
	}
	
	private byte[] getBytes(boolean allCols) {
		final int numCols = getSchema().getNumCols();
		
		ByteBufferWriter bbw = new ByteBufferWriter();

		// First write relation ID
		bbw.addToBuffer(getRelationID());

		// Then, for each field, write
		//		* true if labeled null, false if not
		//		* if labeled null, label (4 byte int)
		//		* if not labeled null, length of data
		//		  and then data
		Relation relation = getSchema();
		SortedSet<Integer> keyCols = relation.getKeyCols();

		for (int i = 0; i < numCols; ++i) {
			Type t = relation.getColType(i);
			boolean isKey = false;
			if (! allCols) {
				isKey = keyCols.contains(Integer.valueOf(i));
			}
			if (allCols || isKey) {
				if (isLabeledNull(i)) {
					bbw.addToBuffer(true);
					try {
						bbw.addToBuffer(getLabeledNull(i));
					} catch (IsNotLabeledNull e) {
						throw new RuntimeException("Error encoding tuple", e);
					}
				} else {
					Object o = get(i);
					bbw.addToBuffer(false);
					bbw.addToBuffer(o == null ? null : t.getBytes(o));
				}
			}
		}
		
		return bbw.getByteArray();
	}
	
	/**
	 * Decode a serialized Tuple object. Note that the relation ID has already been
	 * determined by Schema.getTupleFromBytes and is therefore before the offset
	 * argument
	 * 
	 * @param rs			The schema of this Tuple
	 * @param bytes			An array which contains the serialized data
	 * @param offset		Where to begin reading the serialized data
	 * @param length		The length of the serialized data.
	 */
	public Tuple(Relation rs, byte[] bytes, int offset, int length) {
		this(rs);
		// Relation ID has been pulled out by Schema.getTupleFromBytes
		final int bytesPerInt = IntType.bytesPerInt;
		final Relation schema = getSchema();
		final int numCols = schema.getNumCols();
		for (int i = 0; i < numCols; ++i) {
			// Determine if we're processing a labeled null or not
			boolean isLabeledNull = (bytes[offset] > 0);
			++offset;
			--length;
			if (isLabeledNull) {
				int label = IntType.getValFromBytes(bytes, offset);
				offset += bytesPerInt;
				length -= bytesPerInt;
				setLabeledNull(i, label);
			} else {
				int bytesLength = IntType.getValFromBytes(bytes, offset);
				offset += bytesPerInt;
				length -= bytesPerInt;
				try {
				if (bytesLength < 0) {
					set(i, null);
				} else {
					Object o = schema.getColType(i).fromBytes(bytes, offset, bytesLength);
					length -= bytesLength;
					offset += bytesLength;
					set(i,o);
				}
				} catch (ValueMismatchException vm) {
					throw new RuntimeException("Error while deserializing tuple", vm);
				}
			}
		}
		
		if (length != 0) {
			throw new RuntimeException("Extra data left after debyteifying Tuple for a " + getRelationName());
		}
	}

	@Override
	public boolean sameSchemaAs(AbstractTuple<Relation> t) {
		// It is assumed they're from the same schema of relations
		return getRelationID() == t.getSchema().getRelationID();
	}
	
	@Override
	public boolean hasSchema(Relation schema) {
		// It is assumed they're from the same schema of relations
		return getRelationID() == schema.getRelationID();
	}


	/**
	 * The peer from which this tuple originated
	 * 
	 * @return the peer
	 */
	public Peer getOriginatingPeer() {
		return _peerRelationContext.getPeer();
	}

	/**
	 * Get the logical-level relation from which this tuple originated
	 * 
	 * @return the peerRelation
	 */
	public AbstractRelation getOriginatingPeerRelation() {
		return _peerRelationContext.getRelation();
	}

	/**
	 * The schema from which this tuple originated, using the Update Exchange
	 * schema -- rather than the Reconciliation schema.
	 * 
	 * @return the peerSchema
	 */
	public Schema getOriginatingPeerSchema() {
		return _peerRelationContext.getSchema();
	}
	
	public RelationContext getOrigin() {
		return _peerRelationContext;
	}
	
	public void setOrigin(RelationContext sc) {
		_peerRelationContext = sc;
	}
	
	public void setProvenance(String prov) {
		_provenanceExpr = prov;
	}
	
	public String getProvenance() { 
		return _provenanceExpr;
	}

	public String getProvString() {
		if (_provenanceExpr != null)
			return _provenanceExpr;
		else
			return super.getProvString();
	}

	public Subtuple getKeySubtuple() {
		return new Subtuple(this, schema.getKeyCols());
	}
}