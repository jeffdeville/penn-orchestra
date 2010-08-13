package edu.upenn.cis.orchestra.proql;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;

public class DerivationNode {
	List<TupleNode> _sources;
	List<TupleNode> _targets;
	
	Mapping _mapping;
	
	String _name;

	public DerivationNode(Mapping m) {
		Set<RelationContext> targets = new HashSet<RelationContext>();
		for (Atom a : m.getMappingHead()) {
			targets.add(a.getRelationContext());
		}
		Set<RelationContext> sources = new HashSet<RelationContext>();
		for (Atom a : m.getBody()) {
			sources.add(a.getRelationContext());
		}
		_mapping = m;
		
		_sources = new ArrayList<TupleNode>();
		_targets = new ArrayList<TupleNode>();
		
		for (RelationContext src : sources) {
			_sources.add(new TupleNode(src.getPeer(), src.getSchema(), src.getRelation()));			
		}
		_name = m.getId();
//		_mappingRelation = mappingRelation;
		
		for (RelationContext trg : targets) {
			_targets.add(new TupleNode(trg.getPeer(), trg.getSchema(), trg.getRelation()));			
		}
	}
	
	public String getName() {
		return _name;
	}
	
	public List<TupleNode> getSources() {
		return _sources;
	}
	
	public List<TupleNode> getTargets() {
		return _targets;
	}
	
	public boolean matches(String name) {
		return name.isEmpty() || _name.equals(name);
	}
	
	public String toString() {
		StringBuffer buf = new StringBuffer("(");
		
		for (TupleNode tn : getTargets()) {
			buf.append(tn.toString());
		}
		
		buf.append(" <" + _name + "< ");

		for (TupleNode tn : getSources()) {
			buf.append(tn.toString());
		}
		
		buf.append(")");
		
		return new String(buf);
	}
}
