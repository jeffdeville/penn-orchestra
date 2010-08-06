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
import java.awt.geom.Rectangle2D;

import org.eclipse.zest.layouts.LayoutEntity;
import org.eclipse.zest.layouts.constraints.BasicEntityConstraint;
import org.eclipse.zest.layouts.constraints.LayoutConstraint;
import org.jgraph.graph.CellViewRenderer;
import org.jgraph.graph.VertexView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GuiVertexView extends VertexView implements LayoutEntity {

	public static final long serialVersionUID = 1L;
	
	private static final Logger logger = LoggerFactory.getLogger(GuiVertexView.class);
	
	private Object layoutInformation;
	
	/**
	 * Holds the static renderer for views of this kind.
	 */
	private GuiVertexRenderer renderer = new GuiVertexRenderer();

	/**
	 * Empty constructor for persistence.
	 */
	public GuiVertexView() {
		super();
	}

	/**
	 * Constructs a new vertex view for the specified cell.
	 * 
	 * @param cell
	 *            The cell to construct the vertex view for.
	 */
	public GuiVertexView(Object cell) {
		super(cell);
	}


	/**
	 * Returns the {@link #renderer}.
	 * 
	 * @return Returns the renderer for the cell view.
	 */
	public CellViewRenderer getRenderer() {
		return renderer;
	}

	/**
	 * @see org.eclipse.zest.layouts.LayoutEntity#getHeightInLayout()
	 */
	@Override
	public double getHeightInLayout() {
		Dimension2D size = GuiGraphConstants.getVertexPreferredSize(attributes);
		double height = (size == null) ? getBounds().getHeight() : size.getHeight();
		logger.trace("Reporting height: {}.", Double.valueOf(height));
		return height;
	}

	/**
	 * @see org.eclipse.zest.layouts.LayoutEntity#getLayoutInformation()
	 */
	@Override
	public Object getLayoutInformation() {

		return layoutInformation;
	}

	/**
	 * @see org.eclipse.zest.layouts.LayoutEntity#getWidthInLayout()
	 */
	@Override
	public double getWidthInLayout() {
		Dimension2D size = GuiGraphConstants.getVertexPreferredSize(attributes);
		double width = (size == null) ? getBounds().getWidth() : size.getWidth();
		logger.trace("Reporting width: {}.", Double.valueOf(width));
		return width;
	}

	/**
	 * @see org.eclipse.zest.layouts.LayoutEntity#getXInLayout()
	 */
	@Override
	public double getXInLayout() {

		return getBounds().getX();
	}

	/**
	 * @see org.eclipse.zest.layouts.LayoutEntity#getYInLayout()
	 */
	@Override
	public double getYInLayout() {

		return getBounds().getY();
	}

	/**
	 * @see org.eclipse.zest.layouts.LayoutEntity#populateLayoutConstraint(org.eclipse.zest.layouts.constraints.LayoutConstraint)
	 */
	@Override
	public void populateLayoutConstraint(LayoutConstraint constraint) {
			
			if (constraint instanceof BasicEntityConstraint) {
				BasicEntityConstraint c = (BasicEntityConstraint) constraint;
				Dimension2D preferredSize = GuiGraphConstants.getVertexPreferredSize(attributes);
				if (preferredSize != null) {
					c.hasPreferredSize = true;
					c.preferredHeight = preferredSize.getHeight();
					c.preferredWidth = preferredSize.getWidth();
				}
			}
		
		
	}

	/**
	 * @see org.eclipse.zest.layouts.LayoutEntity#setLayoutInformation(java.lang.Object)
	 */
	@Override
	public void setLayoutInformation(Object internalEntity) {
		
		layoutInformation = internalEntity;
	}

	/**
	 * @see org.eclipse.zest.layouts.LayoutEntity#setLocationInLayout(double,
	 *      double)
	 */
	@Override
	public void setLocationInLayout(double x, double y) {
		
		Rectangle2D oldBounds = getBounds();
		logger.trace("Location update. Old bounds: {}.", oldBounds);
		Rectangle2D newBounds = new Rectangle2D.Double(x, y, oldBounds
				.getWidth(), oldBounds.getHeight());
		logger.trace("Location update. New bounds: {}.", newBounds);
		setBounds(newBounds);
		logger.trace("Location update. Result: {}.", getBounds());
		
	}

	/**
	 * @see org.eclipse.zest.layouts.LayoutEntity#setSizeInLayout(double,
	 *      double)
	 */
	@Override
	public void setSizeInLayout(double width, double height) {
		// Zest cannot set the size since our vertices have autoSize (a JGraph property) set to true.
		//GuiGraphConstants.setVertexPreferredSize(attributes, new Dimension((int)width, (int)height));
		//Rectangle2D oldBounds = getBounds();
		//logger.trace("Size update. Old bounds: {}.", oldBounds);
		//Rectangle2D newBounds = new Rectangle2D.Double(oldBounds.getX(), oldBounds.getY(), width, height);
		//logger.trace("Size update. New bounds: {}.", newBounds);
		//setBounds(newBounds);
		//logger.trace("Size update. Result: {}.", getBounds());
	}

	/**
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Object o) {
		return 0;
	}

	/**
	 * @see org.eclipse.zest.layouts.LayoutItem#getGraphData()
	 */
	@Override
	public Object getGraphData() {
		return null;
	}

	/**
	 * @see org.eclipse.zest.layouts.LayoutItem#setGraphData(java.lang.Object)
	 */
	@Override
	public void setGraphData(Object o) {
	}
	
/*

	@Override
	public synchronized Map changeAttributes(Map arg0) {
		return super.changeAttributes(arg0);
	}
	
	@Override
	public synchronized  void childUpdated() {
		super.childUpdated();
	}

	@Override
	protected synchronized AttributeMap createAttributeMap() {
		return super.createAttributeMap();
	}
	
	@Override
	public synchronized  AttributeMap getAllAttributes() {
		return super.getAllAttributes();
	}

	@Override
	public synchronized AttributeMap getAttributes() {
		return super.getAttributes();
	}
	
	@Override
	public synchronized Rectangle2D getBounds() {
		return super.getBounds();
	}
	
	@Override
	public synchronized Rectangle2D getCachedBounds() {
		return super.getCachedBounds();
	}
	@Override
	public synchronized Object getCell() {
		return super.getCell();
	}
	
	@Override
	protected synchronized  AttributeMap getCellAttributes(GraphModel arg0) {
		return super.getCellAttributes(arg0);
	}
	
	@Override
	public synchronized  Point2D getCenterPoint() {
		return super.getCenterPoint();
	}
	
	@Override
	public synchronized CellView[] getChildViews() {
		return super.getChildViews();
	}
	
	@Override
	public synchronized  GraphCellEditor getEditor() {
		return super.getEditor();
	}
	
	@Override
	public synchronized CellHandle getHandle(GraphContext arg0) {
		return super.getHandle(arg0);
	}
	
	@Override
	public synchronized CellView getParentView() {
		return super.getParentView();
	}
	
	@Override
	public synchronized Point2D getPerimeterPoint(EdgeView arg0, Point2D arg1, Point2D arg2) {
		return super.getPerimeterPoint(arg0, arg1, arg2);
	}
	
	@Override
	public synchronized Point2D getPerimeterPoint(Point2D arg0, Point2D arg1) {
		return super.getPerimeterPoint(arg0, arg1);
	}
	
	@Override
	public synchronized Component getRendererComponent(JGraph arg0, boolean arg1, boolean arg2, boolean arg3) {
		return super.getRendererComponent(arg0, arg1, arg2, arg3);
	}
	
	@Override
	protected synchronized boolean includeInGroupBounds(CellView arg0) {
		return super.includeInGroupBounds(arg0);
	}
	
	@Override
	public synchronized boolean intersects(JGraph arg0, Rectangle2D arg1) {
		return super.intersects(arg0, arg1);
	}
	
	@Override
	public synchronized boolean isLeaf() {
		return super.isLeaf();
	}
	
	@Override
	protected synchronized void mergeAttributes() {
		super.mergeAttributes();
	}
	
	@Override
	public synchronized void refresh(GraphModel arg0, CellMapper arg1, boolean arg2) {
		super.refresh(arg0, arg1, arg2);
	}
	
	@Override
	public synchronized void removeFromParent() {
		super.removeFromParent();
	}
	
	@Override
	public synchronized void scale(double arg0, double arg1, Point2D arg2) {
		super.scale(arg0, arg1, arg2);
	}
	
	@Override
	public synchronized void setAttributes(AttributeMap arg0) {
		super.setAttributes(arg0);
	}
	
	@Override
	public synchronized void setBounds(Rectangle2D arg0) {
		super.setBounds(arg0);
	}
	
	@Override
	public synchronized void setCachedBounds(Rectangle2D arg0) {
		super.setCachedBounds(arg0);
	}
	
	@Override
	public synchronized void setCell(Object arg0) {
		super.setCell(arg0);
	}
	
	@Override
	public synchronized void translate(double arg0, double arg1) {
		super.translate(arg0, arg1);
	}
	
	@Override
	public synchronized void update() {
		super.update();
	}
	
	@Override
	protected synchronized void updateGroupBounds() {
		super.updateGroupBounds();
	}
	
		
	public static void setRenderer(GuiVertexRenderer renderer) {
		GuiVertexView.renderer = renderer;
	}
	*/
}

