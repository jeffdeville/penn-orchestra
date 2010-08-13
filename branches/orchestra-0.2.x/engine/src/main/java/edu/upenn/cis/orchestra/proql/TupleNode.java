package edu.upenn.cis.orchestra.proql;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Schema;

public class TupleNode {
	Peer _peer;
	Schema _schema;
	Relation _relation;
	
	public TupleNode(Peer p, Schema s, Relation r) {
		_peer = p;
		_schema = s;
		_relation = r;
	}
	
	public String getName() {
		return (new RelationContext(_peer, _schema, _relation)).toString();
	}
	
	public int hashCode() {
		return _relation.hashCode();
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof TupleNode))
			return false;
		
		TupleNode t2 = (TupleNode)o;
		return (_peer.getId().equals(t2._peer.getId())) &&
			(_schema.getSchemaId().equals(t2._schema.getSchemaId())) &&
			(_relation.getName().equals(t2._relation.getName()));
	}
	
	/**
	 * Returns true if the relation matches the string name
	 * 
	 * @param s
	 * @return
	 */
	public boolean matches(String s) {
		String peer = "";
		String schema = "";
		String relation = "";
		
		if (s.contains(".")) {
			int inx = s.indexOf('.');
			
			int inx2 = s.indexOf('.', inx + 1);
			
			if (inx2 > inx) {
				peer = s.substring(0, inx);
				schema = s.substring(inx + 1, inx2);
				relation = s.substring(inx2 + 1);
			} else {
				schema = s.substring(0, inx);
				relation = s.substring(inx + 1);
			}
		} else {
			relation = s;
		}
		return (peer.isEmpty() || peer.equals(_peer.getId())) &&
			(schema.isEmpty() || schema.equals(_schema.getSchemaId())) &&
			relation.equals(_relation.getName());
	}
	
	public String toString() {
		return "[" + ((_peer != null) ? (_peer.getId() + ".") : "") + 
		((_schema != null) ? _schema.getSchemaId() + "." : "") + _relation.getName() + "]";
	}
}
