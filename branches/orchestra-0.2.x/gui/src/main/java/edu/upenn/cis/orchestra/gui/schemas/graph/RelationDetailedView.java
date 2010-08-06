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
package edu.upenn.cis.orchestra.gui.schemas.graph;

import java.awt.Color;
import java.awt.Component;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.io.Serializable;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSeparator;
import javax.swing.UIManager;

import org.jgraph.JGraph;
import org.jgraph.graph.CellView;
import org.jgraph.graph.CellViewRenderer;
import org.jgraph.graph.EdgeView;
import org.jgraph.graph.GraphConstants;
import org.jgraph.graph.VertexView;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.gui.graphs.GuiVertexView;

public class RelationDetailedView extends GuiVertexView 
{
	public final static long serialVersionUID=1L;
	
	protected RelationDetailedRenderer _renderer = null;
	

	public RelationDetailedView (RelationGraphCell cell)
	{
		super (cell);
	}
	


	@Override
	public CellViewRenderer getRenderer() 
	{
		if (_renderer == null)
			_renderer = new RelationDetailedRenderer (((RelationGraphCell) getCell()).getRelation());
		return _renderer;
	}
	
	@Override
	public Point2D getPerimeterPoint(EdgeView edge, Point2D source, Point2D p) {
		
		if (getRenderer() instanceof RelationDetailedRenderer)
			return ((RelationDetailedRenderer) getRenderer()).getPerimeterPoint(this, source, p);
		else
			return super.getPerimeterPoint(edge, source, p);
	}
	
	
	public static class RelationDetailedRenderer 
							extends JPanel 
							implements CellViewRenderer, Serializable {

		public final static long serialVersionUID=1L;
		
		private Color _bordercolor;
		private int _borderWidth;
		
		private transient final JPanel _fieldsPanel = new JPanel();
		
		private transient Color _defaultForeground; 
		private transient Color _defaultBackground;
		private transient boolean _selected;
		
		public RelationDetailedRenderer (AbstractRelation rel)
		{
			setLayout (new BoxLayout(this, BoxLayout.Y_AXIS));
			
			final JLabel relName = new JLabel (rel.getName());
			relName.setFont(UIManager.getFont("Relation.headerFont"));
			add(relName);
			JSeparator sep = new JSeparator(JSeparator.HORIZONTAL);
			sep.setForeground(UIManager.getColor("Relation.border"));
			sep.setBackground(UIManager.getColor("Relation.background"));
			add(sep);
			
			_fieldsPanel.setLayout (new BoxLayout(_fieldsPanel, BoxLayout.Y_AXIS));
			for (RelationField fld : rel.getFields()) {
				final JLabel lblField = new JLabel (fld.getName());
				if (rel.getPrimaryKey() != null) {
					if (rel.getPrimaryKey().getFields().contains(fld)) {
						lblField.setFont(UIManager.getFont("Relation.keyFont"));
					} else {
						lblField.setFont(UIManager.getFont("Relation.font"));
					}
				}
				_fieldsPanel.add (lblField);
			}
			add(_fieldsPanel);
			
			_defaultForeground = UIManager.getColor("Tree.textForeground");
			_defaultBackground = UIManager.getColor("Tree.textBackground");
		}
		
		
		/**
		 * Install the attributes of specified cell in this renderer instance. This
		 * means, retrieve every published key from the cells hashtable and set
		 * global variables or superclass properties accordingly.
		 * 
		 * @param view
		 *            the cell view to retrieve the attribute values from.
		 */
		protected void installAttributes(CellView view) {
			Map map = view.getAllAttributes();
			setOpaque(GraphConstants.isOpaque(map));
			_fieldsPanel.setOpaque(isOpaque());
			setBorder(GraphConstants.getBorder(map));
			_bordercolor = GraphConstants.getBorderColor(map);
			_borderWidth = Math.max(1, Math.round(GraphConstants.getLineWidth(map)));
			if (getBorder() == null && _bordercolor != null)
				setBorder(BorderFactory.createLineBorder(_bordercolor, _borderWidth));
			Color foreground = GraphConstants.getForeground(map);
			setForeground((foreground != null) ? foreground : _defaultForeground);
			_fieldsPanel.setForeground(getForeground());
			Color background = GraphConstants.getBackground(map);
			setBackground((background != null) ? background : _defaultBackground);
			_fieldsPanel.setBackground(getBackground());
			setFont(GraphConstants.getFont(map));
			_fieldsPanel.setFont(getFont());
		}		
		
		@Override
		public void paint(Graphics g) 
		{
		     
			super.paint(g);
			if (_selected)
			{
			     Graphics2D g2 = (Graphics2D) g;
		         g2.setStroke(GraphConstants.SELECTION_STROKE);
		         g2.setColor(Color.gray);
		         g2.drawRect(0, 0, getWidth(), getHeight());
			}
		}
	
		public Component getRendererComponent(JGraph graph, 
												CellView view, 
												boolean sel, 
												boolean focus, 
												boolean preview) 
		{
			if (view instanceof VertexView) {
				
				setComponentOrientation(graph.getComponentOrientation());
				_selected = sel;
				if (view.isLeaf()
						|| GraphConstants.isGroupOpaque(view.getAllAttributes()))
					installAttributes(view);
				else
					assert false : "This view doesn't support groups!";
				return this;
			}
			return null;
		}
		
		

		
	
		public Point2D getPerimeterPoint(VertexView view, 
											Point2D	source, 
											Point2D p) 
		{
			Rectangle2D bounds = view.getBounds();
			double x = bounds.getX();
			double y = bounds.getY();
			double width = bounds.getWidth();
			double height = bounds.getHeight();
			double xCenter = x + width / 2;
			double yCenter = y + height / 2;
			double dx = p.getX() - xCenter; // Compute Angle
			double dy = p.getY() - yCenter;
			double alpha = Math.atan2(dy, dx);
			double xout = 0, yout = 0;
			double pi = Math.PI;
			double pi2 = Math.PI / 2.0;
			double beta = pi2 - alpha;
			double t = Math.atan2(height, width);
			if (alpha < -pi + t || alpha > pi - t) { // Left edge
				xout = x;
				yout = yCenter - width * Math.tan(alpha) / 2;
			} else if (alpha < -t) { // Top Edge
				yout = y;
				xout = xCenter - height * Math.tan(beta) / 2;
			} else if (alpha < t) { // Right Edge
				xout = x + width;
				yout = yCenter + width * Math.tan(alpha) / 2;
			} else { // Bottom Edge
				yout = y + height;
				xout = xCenter + height * Math.tan(beta) / 2;
			}
			return new Point2D.Double(xout, yout);			
		}
	
	}	
}
