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

import java.awt.Shape;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.List;
import java.util.Map;

import org.eclipse.zest.layouts.LayoutBendPoint;
import org.eclipse.zest.layouts.LayoutEntity;
import org.eclipse.zest.layouts.LayoutRelationship;
import org.eclipse.zest.layouts.constraints.LayoutConstraint;
import org.jgraph.JGraph;
import org.jgraph.graph.AttributeMap;
import org.jgraph.graph.CellHandle;
import org.jgraph.graph.CellMapper;
import org.jgraph.graph.CellView;
import org.jgraph.graph.CellViewRenderer;
import org.jgraph.graph.EdgeView;
import org.jgraph.graph.GraphCellEditor;
import org.jgraph.graph.GraphContext;
import org.jgraph.graph.GraphLayoutCache;
import org.jgraph.graph.GraphModel;

public class GuiEdgeView extends EdgeView implements LayoutRelationship {

	public static final long serialVersionUID = 1L;
	
	//Olivier: dirty fix for now: non static since JGraph is buggy with shared renderers
	public final GuiEdgeRenderer _renderer;
	
	public GuiEdgeView (BasicGraph graph)
	{
		this (graph, null);
	}
	
	public GuiEdgeView (BasicGraph graph, Object obj)
	{
		super (obj);
		_renderer = new GuiEdgeRenderer(graph, this); 
	}
	
	@Override
	public CellViewRenderer getRenderer() {
		return _renderer; 
	}
	
	
	protected synchronized Point2D getPointLocation(int index) {
		return super.getPointLocation(index);
	}	


	@Override
	protected synchronized void invalidate() {
		super.invalidate();
	}	
	
	@Override
	public synchronized void addExtraLabel(Point2D arg0, Object arg1) {
		super.addExtraLabel(arg0, arg1);
	}
	
	@Override
	public synchronized void addPoint(int arg0, Point2D arg1) {
		super.addPoint(arg0, arg1);
	}
	
	@Override
	public synchronized Map changeAttributes(GraphLayoutCache layoutCache, Map arg0) {
		return super.changeAttributes(layoutCache, arg0);
	}
	
	@Override
	protected synchronized void checkDefaultLabelPosition() {
		super.checkDefaultLabelPosition();
	}
	
	@Override
	public synchronized void childUpdated() {
		super.childUpdated();
	}
	
	@Override
	protected synchronized AttributeMap createAttributeMap() {
		return super.createAttributeMap();
	}
	
	@Override
	public synchronized AttributeMap getAllAttributes() {
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
	public synchronized Object getCell() {
		return super.getCell();
	}
	
	@Override
	protected synchronized AttributeMap getCellAttributes(GraphModel arg0) {
		return super.getCellAttributes(arg0);
	}	
	
	@Override
	public synchronized CellView[] getChildViews() {
		return super.getChildViews();
	}
	
	@Override
	public synchronized GraphCellEditor getEditor() {
		return super.getEditor();
	}
	
	
	@Override
	public synchronized Point2D getExtraLabelPosition(int arg0) {
		return super.getExtraLabelPosition(arg0);
	}
	
	@Override
	public synchronized CellHandle getHandle(GraphContext arg0) {
		return super.getHandle(arg0);
	}
	
	@Override
	public synchronized Point2D getLabelPosition() {
		return super.getLabelPosition();
	}
	
	@Override
	public synchronized Point2D getLabelVector() {
		return super.getLabelVector();
	}
	
	@Override
	protected synchronized Point2D getNearestPoint(boolean arg0) {
		return super.getNearestPoint(arg0);
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
	public synchronized Point2D getPoint(int arg0) {
		return super.getPoint(arg0);
	}
	
	@Override
	public synchronized int getPointCount() {
		return super.getPointCount();
	}
	
	@Override
	public synchronized List getPoints() {
		return super.getPoints();
	}
	/*
	@Override
	public synchronized Component getRendererComponent(JGraph arg0, boolean arg1, boolean arg2, boolean arg3) {
		return super.getRendererComponent(arg0, arg1, arg2, arg3);
	}
	*/
	@Override
	public Shape getShape() {
		return super.getShape();
	}
	
	@Override
	public synchronized CellView getSource() {
		return super.getSource();
	}
	
	@Override
	public synchronized CellView getSourceParentView() {
		return super.getSourceParentView();
	}
	
	@Override
	public synchronized CellView getTarget() {
		return super.getTarget();
	}
	
	@Override
	public synchronized CellView getTargetParentView() {
		return super.getTargetParentView();
	}
	
	@Override
	protected synchronized CellView getVisibleParent(GraphModel arg0, CellMapper arg1, Object arg2) {
		return super.getVisibleParent(arg0, arg1, arg2);
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
	public synchronized boolean isLoop() {
		return super.isLoop();
	}
	
	@Override
	protected synchronized void mergeAttributes() {
		super.mergeAttributes();
	}
	
	@Override
	public synchronized void refresh(GraphLayoutCache arg0, CellMapper arg1, boolean arg2) {
		super.refresh(arg0, arg1, arg2);
	}
	
	@Override
	public synchronized void removeExtraLabel(int arg0) {
		super.removeExtraLabel(arg0);
	}
	
	@Override
	public synchronized void removeFromParent() {
		super.removeFromParent();
	}
	
	@Override
	public synchronized void removePoint(int arg0) {
		super.removePoint(arg0);
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
	public synchronized void setCell(Object arg0) {
		super.setCell(arg0);
	}
	
	@Override
	public synchronized void setExtraLabelPosition(int arg0, Point2D arg1) {
		super.setExtraLabelPosition(arg0, arg1);
	}
	
	@Override
	public synchronized void setLabelPosition(Point2D arg0) {
		super.setLabelPosition(arg0);
	}
	
	@Override
	public synchronized void setPoint(int arg0, Point2D arg1) {
		super.setPoint(arg0, arg1);
	}
	
	@Override
	public synchronized void setSource(CellView arg0) {
		super.setSource(arg0);
	}
	
	@Override
	public synchronized void setTarget(CellView arg0) {
		super.setTarget(arg0);
	}
	
	@Override
	public synchronized void translate(double arg0, double arg1) {
		super.translate(arg0, arg1);
	}
	
	@Override
	public synchronized void update(GraphLayoutCache layoutCache) {
		super.update(layoutCache);
	}
	
	@Override
	protected synchronized void updateGroupBounds() {
		super.updateGroupBounds();
	}
	
	protected void resetLabelPosition ()
	{
		super.checkDefaultLabelPosition();
	}

	private Object _layoutInformation;
	/**
	 * @see org.eclipse.zest.layouts.LayoutRelationship#clearBendPoints()
	 */
	@Override
	public void clearBendPoints() {

	}

	/**
	 * @see org.eclipse.zest.layouts.LayoutRelationship#getDestinationInLayout()
	 */
	@Override
	public LayoutEntity getDestinationInLayout() {
		return (LayoutEntity) getTarget().getParentView();
	}

	/**
	 * @see org.eclipse.zest.layouts.LayoutRelationship#getLayoutInformation()
	 */
	@Override
	public Object getLayoutInformation() {
		return _layoutInformation;
	}

	/**
	 * @see org.eclipse.zest.layouts.LayoutRelationship#getSourceInLayout()
	 */
	@Override
	public LayoutEntity getSourceInLayout() {
		return (LayoutEntity) getSource().getParentView();
	}

	/**
	 * @see org.eclipse.zest.layouts.LayoutRelationship#populateLayoutConstraint(org.eclipse.zest.layouts.constraints.LayoutConstraint)
	 */
	@Override
	public void populateLayoutConstraint(LayoutConstraint constraint) {
	}

	/**
	 * @see org.eclipse.zest.layouts.LayoutRelationship#setBendPoints(org.eclipse.zest.layouts.LayoutBendPoint[])
	 */
	@Override
	public void setBendPoints(LayoutBendPoint[] bendPoints) {
	}

	/**
	 * @see org.eclipse.zest.layouts.LayoutRelationship#setLayoutInformation(java.lang.Object)
	 */
	@Override
	public void setLayoutInformation(Object layoutInformation) {
		_layoutInformation = layoutInformation;
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
	
	
}



