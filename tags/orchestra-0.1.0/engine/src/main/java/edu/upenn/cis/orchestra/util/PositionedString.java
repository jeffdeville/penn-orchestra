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
package edu.upenn.cis.orchestra.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author tjgreen
 *
 * A string with an associated position, which can be changed.  Useful
 * in conjunction with parsing code.
 */
public class PositionedString {
	protected String m_string;
	protected int m_position;
	
	public PositionedString(String string) {
		m_string = string;
		m_position = 0;
	}
	
	public PositionedString(String string, int position) {
		m_string = string;
		m_position = position;
	}
	
	public PositionedString(PositionedString posStr) {
		m_string = posStr.m_string;
		m_position = posStr.m_position;
	}
	
	public int getPosition() {
		return m_position;
	}
	
	public void setPosition(int position) {
		m_position = position;
	}
	
	public void increment() {
		m_position++;
	}
	
	public void decrement() {
		m_position--;
	}
	
	public void offsetPosition(int offset) {
		m_position += offset;
	}
	
	public String getString() {
		return m_string;
	}
	
	public void setString(String string) {
		m_string = string;
	}
	
	public boolean inRange() {
		return m_position >= 0 && m_position < m_string.length();
	}

    public void skipWhitespace() {
    	for ( ; m_position < m_string.length() && Character.isWhitespace(m_string.charAt(m_position)); 
    			m_position++) {
    	}
    }
    
    public boolean skipString(String skip) {
    	int length = skip.length();
    	for (int i = 0; i < length; i++) {
    		if (m_position + i >= m_string.length() || m_string.charAt(m_position + i) != skip.charAt(i)) {
        		return false;
    		}
    	}
    	m_position += length;
    	return true;
    }
    
    public boolean isWhitespace() {
    	return Character.isWhitespace(m_string.charAt(m_position));
    }

    public boolean isLetterOrDigit() {
    	return Character.isLetterOrDigit(m_string.charAt(m_position));
    }
    
    public boolean isLetter() {
    	return Character.isLetter(m_string.charAt(m_position));
    }
    
    public boolean isDigit() {
    	return Character.isDigit(m_string.charAt(m_position));
    }
    
    public boolean isChar(char c) {
    	return m_string.charAt(m_position) == c;
    }
    
    public Matcher match(String regex) {
    	Pattern pat = Pattern.compile(regex);
    	Matcher mat = pat.matcher(m_string.substring(m_position));
    	if (mat.lookingAt()) {
    		String match = mat.group();
    		m_position += match.length();
    		return mat;
    	}
    	return null;
    }
    
    public String toString() {
    	if (inRange()) {
    		return m_string.substring(m_position);
    	}
    	return "<out of range>";
    }
}
