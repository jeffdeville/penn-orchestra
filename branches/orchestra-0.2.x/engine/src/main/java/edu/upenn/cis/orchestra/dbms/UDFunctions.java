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
package edu.upenn.cis.orchestra.dbms;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;


public class UDFunctions{
	
	protected static Map<String, RelationMapping> _dependencies = new HashMap<String, RelationMapping>();
	private static Map<Relation, String> _depRelations = new HashMap<Relation, String>();
	
	public static class RelationMapping{
		private Map<Integer, RelationFieldContext> _mappings;
		
		protected RelationMapping(){
				_mappings = new HashMap<Integer, RelationFieldContext>();
		}
		protected void insertMapping(int pos, RelationFieldContext rfc){
			_mappings.put(pos, rfc);
		}
		public boolean equals(Object other){
			if(!(other instanceof RelationMapping))
				return false;
			
			RelationMapping otherMapping = (RelationMapping)other;
			return _mappings.equals(otherMapping._mappings);
		}
		protected RelationFieldContext getRelationFieldContext(Integer pos){
			return _mappings.get(pos);
		}
		
		protected List<Relation> getAllRelations(){
			List<Relation> relList = new ArrayList<Relation>();
			for(RelationFieldContext rf:_mappings.values()){
				if(!relList.contains(rf._rel))
					relList.add(rf._rel);
			}
			return relList;
		}
	}
	protected static class RelationFieldContext{
		private Relation _rel;
		private RelationField _relF;
		
		protected RelationFieldContext(Relation rel, RelationField rf){
			_rel = rel;
			_relF = rf;
		}
	}
	
	
	public static void registerFunction(Relation function){
		_dependencies.put(function.getName(), null);
	}
	
	public static void makeMappingFor(Relation function, RelationField functionField,
											Relation mappedRel, RelationField mappedRelField){
		
		RelationMapping rm = _dependencies.get(function.getName());
		RelationFieldContext rfc = new RelationFieldContext(mappedRel,mappedRelField);
		if(rm ==null){
			rm = new RelationMapping();
			_dependencies.put(function.getName(), rm);
		}
		int position = function.getFields().indexOf(functionField);
		
		if(position == -1)
			throw new RuntimeException("UDFunctions: Relation Field "+ functionField +" does not exist in function "+ function);
		rm.insertMapping(position, rfc);
		_depRelations.put(mappedRel, function.getName());
	}
	
	public static boolean isUDF(String function)
	{
		if(function==null)
			return false;
		return _dependencies.containsKey(function);
	}
	
	protected static RelationField argumentFor(String fn, Integer pos)
	{
		RelationMapping rm = _dependencies.get(fn);
		RelationFieldContext rfc = rm.getRelationFieldContext(pos);
		//indexForAttr = rfc._rel.getFields().indexOf(rfc._relF);
		
		return rfc._relF;
	}
	protected static Relation argumentFor(String fn, Integer pos, Relation rel)
	{
		RelationMapping rm = _dependencies.get(fn);
		RelationFieldContext rfc = rm.getRelationFieldContext(pos);
		//rel = rfc._rel;
		//indexForAttr = rfc._rel.getFields().indexOf(rfc._relF);
		
		return rfc._rel;
	}
	public static List<Relation> getDependenciesFor(String fn)
	{
		if(!_dependencies.containsKey(fn))
			throw new RuntimeException("Function "+fn + " was not registered as a UDF.");
		
		RelationMapping rm = _dependencies.get(fn);
		
		return rm.getAllRelations();
	}
	public static String isDependentRelation(Relation r){
		if(r==null || _depRelations==null)
			return null;
		return _depRelations.get(r);
	}
}