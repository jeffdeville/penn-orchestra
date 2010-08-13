package edu.upenn.cis.orchestra.proql;

import java.util.HashSet;
import java.util.Set;

public abstract class PatternStep {
	String _variableBinding;
	Pattern _pattern;
	Set<TupleMatch> _sourceNodes;
	Set<TupleMatch> _targetNodes;
	
	
	public PatternStep(Pattern p, String var) {
		_pattern = p;
		_variableBinding = var;
		_sourceNodes = new HashSet<TupleMatch>();
		_targetNodes = new HashSet<TupleMatch>();
	}
	
	public String getVariable() {
		return _variableBinding;
	}
	
	public PatternStep getNextPatternStep() {
		return _pattern.getNextPatternStep(this);
	}
	
	public boolean isEndOfPattern() {
		return _pattern.getNextPatternStep(this) == null;
	}
	
	public void addSourceMatch(String nam, String var) {
		_sourceNodes.add(new TupleMatch(nam,var));
	}
	
	public void addSourceMatch(TupleMatch t) {
		_sourceNodes.add(t);
	}
	
	public void addTargetMatch(String nam, String var) {
		_targetNodes.add(new TupleMatch(nam,var));
	}
	
	public void addTargetMatch(TupleMatch t) {
		_targetNodes.add(t);
	}
	
	public Set<TupleMatch> getTargetSet() {
		return _targetNodes;
	}
	
	public abstract String getStepString();
	
	public String toString() {
		StringBuffer buf = new StringBuffer("(");
		
		for (TupleMatch tm : _targetNodes)
			buf.append(tm.toString());
		
		
		buf.append(" " + getStepString() + " ");
		
		for (TupleMatch tm : _sourceNodes)
			buf.append(tm.toString());

		if (!_variableBinding.isEmpty())
			buf.append(" " + _variableBinding);
		
		buf.append(")");
		
		return new String(buf);
	}
	
	//Set<Condition> _restrictions;
	
	public enum MATCH_RESULT {NOMATCH_ADVANCE, MATCH_ADVANCE, NOMATCH_NOADVANCE, MATCH_NOADVANCE};
	
	public abstract MATCH_RESULT match(DerivationNode d, //Set<DerivationNode> visited,
			Set<TupleNode> connectors);
}
