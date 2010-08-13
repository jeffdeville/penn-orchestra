package edu.upenn.cis.orchestra.proql;

import java.util.HashSet;
import java.util.Set;

public class TupleMatch {
	String _name;
	String _var;
	
	public TupleMatch(String name, String var) {
		_name = name;
		_var = var;
	}
	
	public String getName() {
		return _name;
	}
	
	public boolean matches(TupleNode tn) {
		return _name.isEmpty() || (tn.matches(_name));
	}
	
	public Set<TupleNode> matchesSources(DerivationNode dn) {
		Set<TupleNode> ret = new HashSet<TupleNode>();
		for (TupleNode tn : dn.getSources())
			if (matches(tn))
				ret.add(tn);
		
		return ret;
	}

	public Set<TupleNode> matchesTargets(DerivationNode dn) {
		Set<TupleNode> ret = new HashSet<TupleNode>();
		for (TupleNode tn : dn.getTargets())
			if (matches(tn))
				ret.add(tn);
		
		return ret;
	}
	
	public String toString() {
		return "[" + ((_name.isEmpty()) ? "" : _name + " ") +
			((_var.isEmpty()) ? "" : _var) + "]";
	}
}
