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
package edu.upenn.cis.orchestra.exchange;

import java.util.ArrayList;
import java.util.List;

import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.Relation;

/**
 * Preserve the relationship between outer union columns and the rules (and
 * body variables) for each mapping
 * 
 * @author zives
 *
 */
public class OuterUnionMapping {
	public OuterUnionMapping() {
		setRFMappings(new ArrayList<List<RuleFieldMapping>>());
		setMappings(new ArrayList<Mapping>()); 
		setColumns(new ArrayList<OuterUnionColumn>());
	}
	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}
	/**
	 * @param columns the columns to set
	 */
	public void setColumns(List<OuterUnionColumn> columns) {
		this.columns = columns;
	}
	/**
	 * @return the columns
	 */
	public List<OuterUnionColumn> getColumns() {
		return columns;
	}
	/**
	 * @param mappings the rule field mappings to set
	 */
	public void setRFMappings(List<List<RuleFieldMapping>> mappings) {
		this.rfmappings = mappings;
	}
	/**
	 * @return the rule field mappings
	 */
	public List<List<RuleFieldMapping>> getRFMappings() {
		return rfmappings;
	}
	
	/**
	 * @param mappings the mappings to set
	 */
	public void setMappings(List<Mapping> mappings) {
		this.mappings = mappings;
	}
	/**
	 * @return the mappings
	 */
	public List<Mapping> getMappings() {
		return mappings;
	}

	//	/**
//	 * @param relation the relation to set
//	 */
//	public void setRelation(Relation relation) {
//		this.relation = relation;
//	}
//	/**
//	 * @return the relation
//	 */
//	public Relation getRelation() {
//		return relation;
//	}

	/**
	 * Rule head
	 */
	private String name;

	/**
	 * All associated rule field mappings
	 */
	private List<List<RuleFieldMapping>> rfmappings;

	/**
	 * All associated rule field mappings
	 */
	private List<Mapping> mappings;
	
	/** Fields
	 */
	private List<OuterUnionColumn> columns;
	
//	private Relation relation;

}
