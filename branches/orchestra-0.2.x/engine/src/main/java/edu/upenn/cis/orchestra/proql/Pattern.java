package edu.upenn.cis.orchestra.proql;

import java.util.List;

public class Pattern {
	List<PatternStep> _steps;
	
	public Pattern(List<PatternStep> pattern) {
		_steps = pattern;
	}
	
	/**
	 * Get the first step in the pattern
	 * 
	 * @return
	 */
	public PatternStep getPatternStart() {
		return _steps.get(0);
	}
	
	/**
	 * Get the next step in the pattern
	 * 
	 * @param p
	 * @return
	 */
	public PatternStep getNextPatternStep(PatternStep p) {
		int inx = _steps.indexOf(p);
		
		if (inx == _steps.size() - 1)
			return null;
		else
			return _steps.get(inx + 1);
	}
	
	public String toString() {
		StringBuffer buf = new StringBuffer("QUERY: ");
		for (PatternStep st : _steps)
			buf.append(st.toString());
		
		return new String(buf);
	}
}
