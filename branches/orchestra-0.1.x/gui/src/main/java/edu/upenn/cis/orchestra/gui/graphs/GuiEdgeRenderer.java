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

import java.awt.geom.Point2D;

import org.jgraph.graph.DefaultEdge;
import org.jgraph.graph.EdgeView;
import org.jgraph.graph.PortView;
import org.jgraph.util.ParallelEdgeRouter;
import org.jgraph.util.Spline2D;


public class GuiEdgeRenderer extends GuiBaseEdgeRenderer {

	public static final long serialVersionUID = 1L;
	
	
	public GuiEdgeRenderer (BasicGraph graph, GuiEdgeView view)
	{
		super(graph, view);
	}
	
    protected Point2D getLabelPosition(Point2D pos) {
        EdgeView view = getView ();
        if (view != null)
        {
        	
        	
	    	fontGraphics.setFont(GuiGraphConstants
	                            .getFont(view.getAllAttributes()));
        	
	    	if (getView().getSource()==getView().getTarget())
        		return new Point2D.Double(getView().getPoint(1).getX(), 
        									getView().getPoint(1).getY() - (metrics!=null?metrics.getHeight():5));

        	// For parallel edges we already set the right position        
	        if (getView().getPointCount()>2 
	        		&& (GuiGraphConstants.getRouting(view.getAllAttributes()) instanceof ParallelEdgeRouter))
	            return pos; 
	        else
	        {
	        	if (getView().getPointCount()>2)
	        	{
					if (GuiGraphConstants.getLineStyle(view.getAllAttributes())==GuiGraphConstants.STYLE_SPLINE)
					{
						Point2D[] points = new Point2D[view.getPointCount()];
						int i = 0;
						for (Object pt : view.getPoints())
						{
							if (pt instanceof Point2D)
								points[i++] = (Point2D) pt;
							else
								points[i++] = ((PortView) pt).getLocation();
						}
						Spline2D spline = new Spline2D(points);					
						double[] pt = spline.getPoint(0.5);
						Point2D ctrlPt = new Point2D.Double(pt[0],pt[1]);
/*						GraphConstants.setLabelPosition(view.getAttributes(), ctrlPt);
						GraphConstants.setLabelPosition(((DefaultEdge) view.getCell()).getAttributes(), ctrlPt);
						view.setLabelPosition(ctrlPt);*/
						return ctrlPt;
					}
	        	}
	        	else
	        	{
	        		if (view instanceof GuiEdgeView)
	        		{
	        			view.getAttributes().remove(GuiGraphConstants.LABELPOSITION);
	        			((DefaultEdge) view.getCell()).getAttributes().remove(GuiGraphConstants.LABELPOSITION);
	        			((GuiEdgeView) view).resetLabelPosition ();
	        			return super.getLabelPosition(view.getLabelPosition());
	        		}
	        	}
	        	
	            return super.getLabelPosition(pos);
	        }
        }
        return null;

    }        
    
     
}
