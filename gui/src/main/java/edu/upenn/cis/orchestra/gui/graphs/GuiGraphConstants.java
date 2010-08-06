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

import java.awt.geom.Dimension2D;
import java.util.Map;

import org.jgraph.graph.GraphConstants;
import org.jgraph.graph.Edge.Routing;

public class GuiGraphConstants extends GraphConstants {

	/**
	 * Key for the <code>vertexShape</code> attribute. This special attribute
	 * contains an Integer instance indicating which shape should be drawn by
	 * the renderer.
	 */
	public final static String VERTEXSHAPE = "vertexShape";

	/**
	 * Key for the <code>stretchImage</code> attribute. This special attribute
	 * contains a Boolean instance indicating whether the background image
	 * should be stretched.
	 */
	public final static String STRETCHIMAGE = "stretchImage";
	
	/**
	 * Key for an edge router to use only while applying the layout, will 
	 * work only if using the applyLayout methods from BasicGraph
	 */
	public final static String LAYOUTEDGEROUTING = "layoutEdgeRouting";
	
	
	/**
	 * Key for the preferred size of a vertex as determined by {@code GraphUI}.
	 * Used by the layout algorithms because in most cases the vertices are not
	 * at their final size when the layout is performed.
	 */
	public final static String VERTEXPREFERREDSIZE = "vertexPreferredSize";
	
	
	/**
	 * Returns true if stretchImage in this map is true. Default is false.
	 */
	public static final boolean isStretchImage(Map map) {
		Boolean boolObj = (Boolean) map.get(STRETCHIMAGE);
		if (boolObj != null)
			return boolObj.booleanValue();
		return false;
	}

	/**
	 * Sets stretchImage in the specified map to the specified value.
	 */
	@SuppressWarnings("unchecked")
	public static final void setStretchImage(Map map, boolean stretchImage) {
		map.put(STRETCHIMAGE, new Boolean(stretchImage));
	}
	
	
	/**
	 * Sets vertexShape in the specified map to the specified value.
	 */
	@SuppressWarnings("unchecked")
	public static final void setVertexShape(Map map, int shape) {
		map.put(VERTEXSHAPE, new Integer(shape));
	}

	/**
	 * Returns vertexShape from the specified map.
	 */
	public static final int getVertexShape(Map map) {
		Integer intObj = (Integer) map.get(VERTEXSHAPE);
		if (intObj != null)
			return intObj.intValue();
		return 0;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	public static void setLayoutEdgeRouting (Map map, Routing routing)
	{
		map.put(LAYOUTEDGEROUTING, routing);
	}
	
	public static Routing getLayoutEdgeRouting (Map map)
	{
		return (Routing) map.get(LAYOUTEDGEROUTING);
	}

	/**
	 * Set the preferred size of the vertex in the supplied map.
	 * 
	 * @param map
	 * @param size
	 */
	@SuppressWarnings("unchecked")
	public static void setVertexPreferredSize(Map map, Dimension2D size) {
		map.put(VERTEXPREFERREDSIZE, size);
	}
	
	/**
	 * Returns the {@code Dimension2D} representing the preferred size contained
	 * in the supplied map, or {@code null} if the map contains no preferred
	 * size.
	 * 
	 * <p>
	 * Currently, this is only used by the Zest layout algorithms. They use it
	 * to find out the size of the vertex only. They do not modify the value.
	 * </p>
	 * 
	 * @param map
	 * @return the preferred size from map.
	 */
	@SuppressWarnings("unchecked")
	public static Dimension2D getVertexPreferredSize(Map map) {
		return (Dimension2D)map.get(VERTEXPREFERREDSIZE); 
	}

}
