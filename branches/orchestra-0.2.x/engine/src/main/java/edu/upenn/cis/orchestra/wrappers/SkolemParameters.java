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
package edu.upenn.cis.orchestra.wrappers;


import java.util.ArrayList;

public class SkolemParameters {
	/**
	 * Simple container class for Skolem function parameters
	 * 
	 * @author zives
	 *
	 */
	public SkolemParameters() {
		setFnName("");
		setAttribNames(new ArrayList<String>());
		setTypeDefs(new ArrayList<String>());
		setJavaTypes(new ArrayList<String>());
	}
	public SkolemParameters(SkolemParameters other) {
		setFnName(other.getFnName());
		setAttribNames(new ArrayList<String>());
		setTypeDefs(new ArrayList<String>());
		setJavaTypes(new ArrayList<String>());
		
		getAttribNames().addAll(other.getAttribNames());
		getTypeDefs().addAll(other.getTypeDefs());
		getJavaTypes().addAll(other.getJavaTypes());
	}
	public void add(String aType, String attr, String typedef, String javaType) {
		setFnName(getFnName() + aType);
		getAttribNames().add(attr);
		getTypeDefs().add(typedef);
		getJavaTypes().add(javaType);
	}
	
	public int getNumAttribs() {
		return getAttribNames().size();
	}
	
	/**
	 * @param attribNames the attribNames to set
	 */
	public void setAttribNames(ArrayList<String> attribNames) {
		_attribNames = attribNames;
	}
	/**
	 * @return the attribNames
	 */
	public ArrayList<String> getAttribNames() {
		return _attribNames;
	}
	
	public String getAttribNameAt(int i) {
		return _attribNames.get(i);
	}
	
	/**
	 * @param typeDefs the typeDefs to set
	 */
	public void setTypeDefs(ArrayList<String> typeDefs) {
		_typeDefs = typeDefs;
	}
	/**
	 * @return the typeDefs
	 */
	public ArrayList<String> getTypeDefs() {
		return _typeDefs;
	}
	
	public String getTypeDefAt(int i) {
		return _typeDefs.get(i);
	}

	public String getTypeDefNNAt(int i) {
		return _typeDefs.get(i) + " NOT NULL";
	}

	/**
	 * @param fnName the fnName to set
	 */
	public void setFnName(String fnName) {
		_fnName = fnName;
	}
	/**
	 * @return the fnName
	 */
	public String getFnName() {
		return _fnName;
	}

	/**
	 * @param javaTypes the javaTypes to set
	 */
	public void setJavaTypes(ArrayList<String> javaTypes) {
		_javaTypes = javaTypes;
	}
	/**
	 * @return the javaTypes
	 */
	public ArrayList<String> getJavaTypes() {
		return _javaTypes;
	}
	
	public String getJavaTypeAt(int i) {
		return _javaTypes.get(i);
	}
	
	public String getJavaSetter(int i) {
		String s = _javaTypes.get(i);
		
		if (s.equals("int"))
			return "setInt";
		else
			return "set" + s;
	}

	private String _fnName;
	
	private ArrayList<String> _attribNames;
	private ArrayList<String> _typeDefs;
	private ArrayList<String> _javaTypes;

}
