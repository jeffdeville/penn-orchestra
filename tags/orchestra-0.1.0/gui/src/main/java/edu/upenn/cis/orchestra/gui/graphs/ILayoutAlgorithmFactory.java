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

import org.eclipse.zest.layouts.LayoutAlgorithm;

/**
 * <p>
 * Implementations of this class are used when creating {@code
 * LayoutHelperBuilder}s. This factory will supply the {@code ILayoutHelper}
 * with the {@code LayoutAlgorithm} which will be used when laying out the
 * graph.
 * </p>
 * 
 * <p>
 * Implementors should be aware that Zest enforces the restriction that a
 * {@code LayoutAlgorithm} must be instantiated on the same thread that its
 * {@code LayoutAlgorithm.applyLayout(...)} method will be called. The latter is
 * always called on the Event Dispatch Thread.
 * </p>
 * 
 * <p>
 * The sole intended caller of {@code getLayoutAlgorithm()} is {@code
 * ILayoutHelper} which will only call it on the Event Dispatch Thread.
 * </p>
 * 
 * @author John Frommeyer
 * 
 */
public interface ILayoutAlgorithmFactory {

	/**
	 * Returns a preconfigured {@code LayoutAlgorithm}.
	 * 
	 * @return a {@code LayoutAlgorithm}.
	 */
	LayoutAlgorithm getLayoutAlgorithm();
}
