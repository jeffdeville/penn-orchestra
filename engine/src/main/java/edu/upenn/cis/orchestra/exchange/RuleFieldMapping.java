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

import java.util.List;

import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.RelationField;

/**
 * Class used to define the mapping from a field in the head of a rule
 * to its source columns.
 * 
 * @author zives
 *
 */
public class RuleFieldMapping {
	public boolean isIndex;
	public RelationField outputField;
	public Mapping rule;
//	public List<String> srcColumns;
	public List<RelationField> srcColumns;
	public List<RelationField> trgColumns;
	public AtomArgument srcArg;

//	public RuleFieldMapping(RelationField f, List<String> src, List<String> trg, 
	public RuleFieldMapping(RelationField f, List<RelationField> src, List<RelationField> trg,
			AtomArgument arg, boolean inx, Mapping r) {
		outputField = f;
		srcColumns = src;
		trgColumns = trg;
		srcArg = arg;
		isIndex = inx;
		rule = r;
	}

	@Override
	public String toString() {
		return new String(outputField.getName() + " :- " + srcColumns.toString());
	}
}

