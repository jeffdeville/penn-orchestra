package edu.upenn.cis.orchestra.proql;

import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;

public class QueryParser {
	public static Pattern getPatternFor(Reader r) throws IOException {
		StreamTokenizer st = new StreamTokenizer(r);
	    List<PatternStep> steps = new ArrayList<PatternStep>();
	    
		Pattern p = new Pattern(steps);
		
		st.wordChars('A', 'Z'); 
	    st.wordChars('a', 'z');
	    st.wordChars('0', '9');
	    st.wordChars('$','$');
	    st.wordChars('.','.');
	    st.wordChars('_','_');
	    st.ordinaryChar('-');
	    st.ordinaryChar('>');
	    st.ordinaryChar('<');
	    st.ordinaryChar('*');
	    st.ordinaryChars('[', ']');

	    st.slashSlashComments(true);
	    st.slashStarComments(true);
	    int type; 
	    
	    List<String> tokens = new ArrayList<String>();
	    
	    while  ((type = st.nextToken (  )) != StreamTokenizer.TT_EOF) {  
	    	if (type == StreamTokenizer.TT_WORD )  
	             tokens.add ( st.sval ) ;
	    	else {
	    		char c = (char)st.ttype;
	    		tokens.add(String.valueOf(c));
	    	}
//	    	else if (type == StreamTokenizer.TT_NUMBER)
//	    		tokens.add(st.nval)
	    }
	    
//	    for (String str : tokens)
//	    	System.err.println(str);
	    
	    List<TupleMatch> leftMatch = new ArrayList<TupleMatch>(); 
	    List<TupleMatch> rightMatch;
	    
	    int cursor = 0;
    	cursor = getNodes(tokens, cursor, leftMatch);
	    while (cursor < tokens.size()) {
		    boolean chain = false;
		    if (!tokens.get(cursor).equals("<") && !tokens.get(cursor).equals("*"))
				throw new RuntimeException("Edge delimiter '<' expected at position " + cursor + " instead of " + tokens.get(cursor));
		    else {
		    	chain = !tokens.get(cursor).equals("<"); 
		    	cursor++;
		    }

		    String mappingName = "";
		    String mappingVar = "";
			if (!chain && !tokens.get(cursor).equals("-") && !tokens.get(cursor).startsWith("$")) {
				mappingName = tokens.get(cursor);
				cursor++;
			}
			if (!tokens.get(cursor).equals("-")) {
				if (!tokens.get(cursor).startsWith("$"))
					throw new RuntimeException("Variable prefix '$' expected at position " + cursor + " instead of " + tokens.get(cursor));
				mappingVar = tokens.get(cursor);
				cursor++;
			}
			if (!tokens.get(cursor).equals("-"))
				throw new RuntimeException("Edge delimiter '-' expected at position " + cursor + " instead of " + tokens.get(cursor));
			else
				cursor++;
				
		    rightMatch = new ArrayList<TupleMatch>(); 
			cursor = getNodes(tokens, cursor, rightMatch);
			
			if (chain)
				steps.add(new ChainStep(p, leftMatch, rightMatch, mappingVar));
			else
				steps.add(new DerivationStep(p, leftMatch, mappingName, rightMatch, mappingVar));

			leftMatch = new ArrayList<TupleMatch>();
	    	leftMatch.addAll(rightMatch);
	    }
	    
		return p;
	}
	
	static int getNodes(List<String> tokens, int cursor, List<TupleMatch> sourceMatch) {
    	while (cursor < tokens.size() && tokens.get(cursor).equals("[")) {
    		cursor++;
	    	String srcName = "";
	    	String srcVar = "";
    		if (!tokens.get(cursor).equals("]")) {
    			srcName = tokens.get(cursor++);
    		}
    		
    		if (!tokens.get(cursor).equals("]")) {
    			srcVar = tokens.get(cursor++);
    		}
    		if (!tokens.get(cursor).equals("]")) {
				throw new RuntimeException("Close-node delimiter ']' expected at position " + cursor);
    		} else
    			cursor++;
    		sourceMatch.add(new TupleMatch(srcName, srcVar));
//    		System.err.println("Matched " + srcName + " and left with cursor = " + cursor);
    	}
    	return cursor;		
	}
	
	public static String getAnnotationType(String s){
		java.util.regex.Pattern p = java.util.regex.Pattern.compile("EVALUATE ([A-Za-z]*) ASSIGNING EACH\n(.*)");
		Matcher m = p.matcher(s);
		if(m.find()){
			return m.group(1);
		}else{
			return null;
		}
	}
	
	public static String getAssignmentExpression(String s){
//		java.util.regex.Pattern p = java.util.regex.Pattern.compile("EVALUATE ([A-Za-z]*) ASSIGNING EACH([ .\r\n]*)");
//		Matcher m = p.matcher(s);
//		if(m.find()){
//			return m.group(2);
//		}else{
//			return null;
//		}
		if(s.indexOf("ASSIGNING EACH") != -1)
			return s.substring(s.indexOf("ASSIGNING EACH") + "ASSIGNING EACH".length() + 1);
		else
			return "";
		
	}
}
