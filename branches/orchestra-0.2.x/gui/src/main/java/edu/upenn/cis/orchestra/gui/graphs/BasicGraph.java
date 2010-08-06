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

import java.awt.Container;
import java.awt.Dimension;
import java.awt.event.ComponentEvent;
import java.awt.event.ComponentListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.JViewport;
import javax.swing.ToolTipManager;

import org.jgraph.JGraph;
import org.jgraph.graph.CellView;
import org.jgraph.graph.DefaultEdge;
import org.jgraph.graph.DefaultGraphModel;
import org.jgraph.graph.EdgeView;
import org.jgraph.graph.GraphLayoutCache;
import org.jgraph.plaf.basic.BasicGraphUI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BasicGraph extends JGraph {

	public static final int DEFAULT_WIDTH = 800;
	public static final int DEFAULT_HEIGHT = 600;

	private boolean _enableRoutingWhenNoLayout = true;

	protected class BooleanWrap {
		private boolean _val = false;

		public synchronized boolean setVal(boolean val) {
			if (val = _val)
				return false;
			_val = val;
			return true;

		}

		public synchronized boolean getVal() {
			return _val;
		}

	}

	//TODO: Is this field necessary if we continue to enforce the rule that all gui code is called on the EDT?
	private BooleanWrap _currlayout = new BooleanWrap();

	/**
	 * Used by {@code FirstRevealListener} to keep track of the first showing of
	 * this graph. Allows us to delay layout until parent has been sized.
	 */
	private boolean _firstReveal = true;

	public BasicGraph() {
		this(false);

	}

	public BasicGraph(boolean useTooltips) {
		super(new DefaultGraphModel());

		getGraphLayoutCache().setSelectsAllInsertedCells(false);
		getGraphLayoutCache().setSelectsLocalInsertedCells(false);
		getGraphLayoutCache().setAutoSizeOnValueChange(true);

		setUI(new SpecGraphUI());

		setAntiAliased(true);

		getGraphLayoutCache().setFactory(new GuiCellViewFactory(this));

		// Set graph properties for user interaction
		setEditable(false);
		setDisconnectable(false);
		setAntiAliased(true);
		setDropEnabled(false);
		setEdgeLabelsMovable(false);

		if (useTooltips)
			ToolTipManager.sharedInstance().registerComponent(this);
	}

	@Override
	public String getToolTipText(MouseEvent event) {
		Object cell = getFirstCellForLocation(event.getX(), event.getY());
		if (cell instanceof GuiDefaultGraphObj) {
			return ((GuiDefaultGraphObj) cell).getTooltipText();
		}
		return null;
	}

	public SpecGraphUI getSpecializedGraphUI() {
		return (SpecGraphUI) getUI();
	}

	public void setUI(SpecGraphUI arg0) {
		super.setUI(arg0);
	}

	public boolean isCurrLayout() {
		return _currlayout.getVal();
	}

	public boolean setCurrLayout(boolean val) {
		return _currlayout.setVal(val);
	}

	/**
	 * Lays out the graph.
	 * 
	 * <p>
	 * {@code LayoutHelperBuilder} has been provided to make this job simpler for implementors.
	 * </p>
	 */
	protected abstract void applyLayout();

	void disableRouting() {
		Object[] edges = getGraphLayoutCache().getCells(false, false, false,
				true);
		for (Object edgeo : edges) {
			DefaultEdge edge = (DefaultEdge) edgeo;
			if (GuiGraphConstants.getLayoutEdgeRouting(edge.getAttributes()) != null) {
				GuiGraphConstants.setPoints(edge.getAttributes(),
						((EdgeView) getGraphLayoutCache()
								.getMapping(edge, true)).getPoints());
				// System.err.println("Before" + edge.getAttributes());
				edge.getAttributes().remove(GuiGraphConstants.ROUTING);
				// GuiGraphConstants.setMoveable(edge.getAttributes(), false);
				// System.err.println("After" + edge.getAttributes());
			}
			getGraphLayoutCache().editCell(edge, edge.getAttributes());
			// System.err.println("After edit" + edge.getAttributes());
		}
	}

	void enableRouting() {

		Object[] edges = getGraphLayoutCache().getCells(false, false, false,
				true);
		for (Object edgeo : edges) {
			DefaultEdge edge = (DefaultEdge) edgeo;
			if (GuiGraphConstants.getLayoutEdgeRouting(edge.getAttributes()) != null)
				GuiGraphConstants.setRouting(edge.getAttributes(),
						GuiGraphConstants.getLayoutEdgeRouting(edge
								.getAttributes()));
			getGraphLayoutCache().editCell(edge, edge.getAttributes());
		}

	}

	public void setEnableRoutingWhenNotLayout(boolean val) {
		if (_enableRoutingWhenNoLayout != val) {
			if (val)
				enableRouting();
			else
				disableRouting();
		}
		_enableRoutingWhenNoLayout = val;
	}

	public boolean isEnableRoutingWhenNotLayout() {
		return _enableRoutingWhenNoLayout;
	}

	@SuppressWarnings("unchecked")
	public void center() {

		if (getParent() != null) {
			Dimension viewBounds;
			if (getParent() instanceof JViewport)
				viewBounds = ((JViewport) getParent()).getExtentSize();
			else
				viewBounds = getParent().getSize();
			Rectangle2D graphBounds = GraphLayoutCache
					.getBounds(getGraphLayoutCache().getCellViews());

			double dx = -graphBounds.getX();
			dx += viewBounds.getWidth() > graphBounds.getWidth() ? (((viewBounds
					.getWidth() - graphBounds.getWidth()) / 2))
					: 0;
			double dy = -graphBounds.getY();
			dy += viewBounds.getHeight() > graphBounds.getHeight() ? (((viewBounds
					.getHeight() - graphBounds.getHeight()) / 2))
					: 0;

			Map nestedChanges = new HashMap();
			for (CellView view : getGraphLayoutCache().getCellViews()) {
				Rectangle2D cellBounds = view.getBounds();
				Map atts = new HashMap(2);
				GuiGraphConstants.setBounds(atts, new Rectangle2D.Double(
						cellBounds.getX() + dx, cellBounds.getY() + dy,
						cellBounds.getWidth(), cellBounds.getHeight()));

				nestedChanges.put(view.getCell(), atts);

				if (getModel().isEdge(view.getCell())) {
					for (Object obj : GuiGraphConstants.getPoints(view
							.getAllAttributes())) {
						if (obj instanceof Point2D) {
							Point2D pt = (Point2D) obj;
							pt.setLocation(pt.getX() + dx, pt.getY() + dy);
						}
					}
					/*
					 * 
					 * Point2D labPos =
					 * GuiGraphConstants.getLabelPosition(view.getAllAttributes
					 * ()); if (labPos!=null)
					 * GuiGraphConstants.setLabelPosition(atts, new
					 * Point2D.Double(labPos.getX()+dx,labPos.getY()+getY()));
					 */
				}
			}
			getGraphLayoutCache().edit(nestedChanges);
			getGraphLayoutCache().reload();
			revalidate();
			repaint();

		}

	}

	/**
	 * This method is intended to be called once, soon after this graph has been
	 * created and added to a parent {@code Container}. It will cause the graph
	 * to be laid out, but only after the parent has been resized for the first
	 * time after the graph was added. This way, the layout algorithm will know
	 * how much room it has to work with, i.e., the bounds of the parent. It is
	 * not necessary to use this method if the this graph is sized when created.
	 */
	public void applyLayoutOnFirstReveal() {
		Container parent = getParent();
		if (null == parent) {
			throw new IllegalStateException("Graph parent must be non-null.");
		} else if (parent instanceof JViewport) {
			// We get better bounds for layout algorithm on initial display when
			// watching the scroll pane itself.
			parent = parent.getParent();
		}
		setVisible(false);
		parent.addComponentListener(new FirstRevealListener(this));
	}

	/**
	 * A listener which uses {@code _firstShowing} to apply a layout to the
	 * enclosing graph the first time that its parent is resized. The idea is
	 * the first resizing of the parent is really the first time the parent has
	 * a non-zero size.
	 * 
	 * @author John Frommeyer
	 * 
	 */
	private class FirstRevealListener implements ComponentListener {

		/**
		 * The graph on behalf of which we are listening.
		 */
		final private BasicGraph _graph;

		/**
		 * The logger.
		 */
		final private Logger _logger = LoggerFactory
				.getLogger(FirstRevealListener.class);

		/**
		 * Creates a listener which will layout {@code graph} the first time the
		 * listened-to {@code Component} (presumably some close ancestor of
		 * {@code graph}) is resized. Once this first layout is done, this
		 * listener removes itself from the {@code Component}'s listeners.
		 * 
		 * @param graph
		 *            the graph to be laid out.
		 */
		private FirstRevealListener(BasicGraph graph) {
			_graph = graph;
		}

		/**
		 * @see java.awt.event.ComponentListener#componentHidden(java.awt.event.ComponentEvent)
		 */
		@Override
		public void componentHidden(ComponentEvent e) {
			debug(e);

		}

		/**
		 * @see java.awt.event.ComponentListener#componentMoved(java.awt.event.ComponentEvent)
		 */
		@Override
		public void componentMoved(ComponentEvent e) {
			debug(e);

		}

		/**
		 * @see java.awt.event.ComponentListener#componentResized(java.awt.event.ComponentEvent)
		 */
		@Override
		public void componentResized(ComponentEvent e) {
			if (_firstReveal) {
				applyLayout();
				setVisible(true);
				_firstReveal = false;
				_graph.getParent().removeComponentListener(this);
			}

			debug(e);
		}

		/**
		 * @see java.awt.event.ComponentListener#componentShown(java.awt.event.ComponentEvent)
		 */
		@Override
		public void componentShown(ComponentEvent e) {
			debug(e);
		}

		private void debug(ComponentEvent e) {
			_logger.debug("{}: Graph: [{}] in Component: [{}].", new Object[] {
					e.paramString(), _graph, e.getComponent() });

		}

	}

	/**
	 * Extend the default graph UI to add mouse listeners and not to have to
	 * extend graphUI at each time!
	 * 
	 * @author Olivier Biton
	 * 
	 */
	public class SpecGraphUI extends BasicGraphUI {
		public static final long serialVersionUID = 1L;

		private List<MouseListener> _listeners = new ArrayList<MouseListener>(2);

		/**
		 * Override the default listener to notify our own list of listeners
		 * 
		 * @return Mouse listener New mouse listener
		 */
		@Override
		protected MouseListener createMouseListener() {
			return new SkeletonUIMouseHandler();
		}

		public void addMouseListener(MouseListener mList) {
			_listeners.add(mList);
		}

		public void removeMouseListener(MouseListener mList) {
			_listeners.remove(new WeakReference<MouseListener>(mList));
		}

		/**
		 * This class is used to catch the double click on bio step classes
		 * 
		 * @author Olivier Biton
		 * 
		 */
		private class SkeletonUIMouseHandler extends BasicGraphUI.MouseHandler {
			public static final long serialVersionUID = 1L;

			/**
			 * Method called on mousePressed event.
			 * 
			 * @param e
			 *            Mouse pressed event...
			 */
			@Override
			public void mousePressed(MouseEvent e) {
				super.mousePressed(e);
				for (MouseListener list : _listeners)
					list.mousePressed(e);
			}

			@Override
			public void mouseClicked(MouseEvent e) {
				super.mouseClicked(e);
				for (MouseListener list : _listeners)
					list.mouseClicked(e);
			}

			@Override
			public void mouseEntered(MouseEvent e) {
				super.mouseEntered(e);
				for (MouseListener list : _listeners)
					list.mouseEntered(e);
			}

			@Override
			public void mouseExited(MouseEvent e) {
				super.mouseExited(e);
				for (MouseListener list : _listeners)
					list.mouseExited(e);
			}

			@Override
			public void mouseReleased(MouseEvent e) {
				super.mouseReleased(e);
				for (MouseListener list : _listeners)
					list.mouseReleased(e);
			}

		}
	}

}
