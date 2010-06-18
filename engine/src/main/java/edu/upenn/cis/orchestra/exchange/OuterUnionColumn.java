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

import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datamodel.RelationField;

/**
 * 
 * @author zives, gkarvoun
 *
 */
public class OuterUnionColumn {
	public OuterUnionColumn(RelationField col, boolean inx, int previous, List<RelationField> src,
			List<RelationField> dist, AtomArgument srcVar) {
		setColumn(col);
		setIndex(inx);
		setSourceColumns(new ArrayList<List<RelationField>>());
		setDistinguishedColumns(new ArrayList<List<RelationField>>());
		setSourceVariables(new ArrayList<AtomArgument>());
		for (int i = 0; i < previous; i++) {
			getSourceColumns().add(null);
			getDistinguishedColumns().add(null);
			getSourceArgs().add(null);
		}
		
		getSourceColumns().add(src);
		getSourceArgs().add(srcVar);
		getDistinguishedColumns().add(dist);
	}
	/**
	 * @param sourceColumns the sourceColumns to set
	 */
	public void setSourceColumns(List<List<RelationField>> sourceColumns) {
		this.sourceColumns = sourceColumns;
	}
	/**
	 * @return the sourceColumns
	 */
	public List<List<RelationField>> getSourceColumns() {
		return sourceColumns;
	}
	/**
	 * @param sourceColumns the sourceColumns to set
	 */
	public void setDistinguishedColumns(List<List<RelationField>> sourceColumns) {
		this.distinguishedColumns = sourceColumns;
	}
	/**
	 * @return the sourceColumns
	 */
	public List<List<RelationField>> getDistinguishedColumns() {
		return distinguishedColumns;
	}
	/**
	 * @param column the column to set
	 */
	public void setColumn(RelationField column) {
		this.column = column;
	}
	/**
	 * @return the column
	 */
	public RelationField getColumn() {
		return column;
	}
	/**
	 * @param isIndex the isIndex to set
	 */
	public void setIndex(boolean isIndex) {
		this.isIndex = isIndex;
	}
	/**
	 * @return the isIndex
	 */
	public boolean isIndex() {
		return isIndex;
	}
	/**
	 * @param sourceVars the sourceVars to set
	 */
	public void setSourceVariables(List<AtomArgument> sourceArgs) {
		this.sourceArgs = sourceArgs;
	}
	/**
	 * @return the sourceVars
	 */
	public List<AtomArgument> getSourceArgs() {
		return sourceArgs;
	}
	public String toString() {
		return column.getName() + ((isIndex) ? "*" : "") + sourceColumns.toString() + "/" + sourceArgs.toString();
	}
	private RelationField column;
	private boolean isIndex;
	private List<List<RelationField>> sourceColumns;
	private List<List<RelationField>> distinguishedColumns;
	private List<AtomArgument> sourceArgs;

	public final static String ORIGINAL_NULL = "NULL";
}
