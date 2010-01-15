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
package edu.upenn.cis.orchestra.gui.graphs;

import edu.upenn.cis.orchestra.gui.graphs.LayoutHelperBuilder.LayoutAlgorithmType;


/**
 * The visualization of transactions, dependencies, and conflicts
 * 
 * @author zives
 *
 */
public class LegendGraph extends BasicGraph {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Object[] _roots;
	
	public LegendGraph() {
		super();
		setEditable(false);
		setSelectionEnabled(false);
	}
	
	public void setRoots(Object[] r) {
		_roots = r;
	}
	
	public Object[] getRoots() {
		return _roots;
	}
	
	/**
	 * Lays out this graph in a horizontal style.
	 * 
	 * @see edu.upenn.cis.orchestra.gui.graphs.BasicGraph#applyLayout()
	 */
	@Override
	protected void applyLayout ()
	{
		LayoutHelperBuilder builder = new LayoutHelperBuilder(this, LayoutAlgorithmType.HORIZONTAL);
		ILayoutHelper helper = builder.build();
		helper.applyLayout();
	}
}
