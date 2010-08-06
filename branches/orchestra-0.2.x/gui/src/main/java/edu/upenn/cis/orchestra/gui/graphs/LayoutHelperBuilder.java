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

import java.awt.Color;
import java.awt.Container;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Insets;
import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.geom.Dimension2D;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JViewport;

import org.eclipse.zest.layouts.InvalidLayoutConfiguration;
import org.eclipse.zest.layouts.LayoutAlgorithm;
import org.eclipse.zest.layouts.LayoutEntity;
import org.eclipse.zest.layouts.LayoutRelationship;
import org.eclipse.zest.layouts.LayoutStyles;
import org.eclipse.zest.layouts.algorithms.CompositeLayoutAlgorithm;
import org.eclipse.zest.layouts.algorithms.DirectedGraphLayoutAlgorithm;
import org.eclipse.zest.layouts.algorithms.HorizontalLayoutAlgorithm;
import org.eclipse.zest.layouts.algorithms.HorizontalTreeLayoutAlgorithm;
import org.eclipse.zest.layouts.algorithms.RadialLayoutAlgorithm;
import org.eclipse.zest.layouts.algorithms.SpringLayoutAlgorithm;
import org.eclipse.zest.layouts.algorithms.TreeLayoutAlgorithm;
import org.eclipse.zest.layouts.algorithms.HorizontalTreeLayoutAlgorithm.Orientation;
import org.jgraph.graph.AttributeMap;
import org.jgraph.graph.CellView;
import org.jgraph.graph.GraphLayoutCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds {@code ILayoutHelper}s which can be used in the implementation of
 * {@link #edu.upenn.cis.orchestra.gui.graphs.BasicGraph.applyLayout()}.
 * 
 * @author John Frommeyer
 * 
 */
public class LayoutHelperBuilder {

	/**
	 * This is a factory which returns simple {@code LayoutAlgorithm}'s with
	 * minimal configuration.
	 * 
	 * @author John Frommeyer
	 * 
	 */
	static public enum LayoutAlgorithmType implements ILayoutAlgorithmFactory {

		/**
		 * A force-based algorithm.
		 */
		SPRING {
			@Override
			public LayoutAlgorithm getLayoutAlgorithm() {
				return new SpringLayoutAlgorithm(LayoutStyles.ENFORCE_BOUNDS
						| LayoutStyles.NO_LAYOUT_NODE_RESIZING);
			}
		},

		/**
		 * An algorithm for trees.
		 */
		TREE {
			@Override
			public LayoutAlgorithm getLayoutAlgorithm() {
				return new TreeLayoutAlgorithm(LayoutStyles.ENFORCE_BOUNDS
						| LayoutStyles.NO_LAYOUT_NODE_RESIZING);
			}
		},

		/**
		 * An algorithm for horizontal trees.
		 */
		HORIZONTAL_TREE {
			@Override
			public LayoutAlgorithm getLayoutAlgorithm() {
				HorizontalTreeLayoutAlgorithm horizontalTree = new HorizontalTreeLayoutAlgorithm(
						LayoutStyles.ENFORCE_BOUNDS
								| LayoutStyles.NO_LAYOUT_NODE_RESIZING);
				horizontalTree.setOrientation(Orientation.EAST);
				return horizontalTree;
			}
		},

		/**
		 * A horizontal grid layout
		 */
		HORIZONTAL {
			@Override
			public LayoutAlgorithm getLayoutAlgorithm() {
				return new HorizontalLayoutAlgorithm(
						LayoutStyles.ENFORCE_BOUNDS
								| LayoutStyles.NO_LAYOUT_NODE_RESIZING);
			}
		},
		/**
		 * A directed graph layout.
		 */
		DIRECTED {
			@Override
			public LayoutAlgorithm getLayoutAlgorithm() {
				return new DirectedGraphLayoutAlgorithm(
						LayoutStyles.ENFORCE_BOUNDS
								| LayoutStyles.NO_LAYOUT_NODE_RESIZING);
			}
		},

		/**
		 * A directed graph layout composed with a forced-based layout.
		 */
		DIRECTED_SPRING_COMP {
			@Override
			public LayoutAlgorithm getLayoutAlgorithm() {
				SpringLayoutAlgorithm spring = new SpringLayoutAlgorithm(
						LayoutStyles.ENFORCE_BOUNDS
								| LayoutStyles.NO_LAYOUT_NODE_RESIZING);
				spring.setRandom(false);
				return new CompositeLayoutAlgorithm(
						LayoutStyles.ENFORCE_BOUNDS
								| LayoutStyles.NO_LAYOUT_NODE_RESIZING,
						new LayoutAlgorithm[] {
								new DirectedGraphLayoutAlgorithm(
										LayoutStyles.ENFORCE_BOUNDS
												| LayoutStyles.NO_LAYOUT_NODE_RESIZING),
								spring });
			}
		},

		/**
		 * A radial tree algorithm.
		 */
		RADIAL {
			@Override
			public LayoutAlgorithm getLayoutAlgorithm() {
				return new RadialLayoutAlgorithm(LayoutStyles.ENFORCE_BOUNDS
						| LayoutStyles.NO_LAYOUT_NODE_RESIZING);
			}
		};

	}

	/**
	 * A helper which uses Zest's algorithms to layout a graph.
	 * 
	 * @author John Frommeyer
	 * 
	 */
	public class ZestLayoutHelper implements ILayoutHelper {

		/**
		 * The logger.
		 */
		final private Logger _logger = LoggerFactory
				.getLogger(ZestLayoutHelper.class);

		/**
		 * This holds the Zest view of the graph's vertices.
		 */
		private ArrayList<LayoutEntity> _entities = new ArrayList<LayoutEntity>();

		/**
		 * This holds the Zest view of the graph's edges.
		 */
		private ArrayList<LayoutRelationship> _relationships = new ArrayList<LayoutRelationship>();

		/**
		 * Construct a layout helper which will use Zest layout algorithms.
		 */
		private ZestLayoutHelper() {
			CellView[] roots = _graph.getGraphLayoutCache().getRoots();
			for (CellView cell : roots) {
				if (cell instanceof GuiVertexView) {
					// We need to force the gui to calculate the size of the
					// vertex, since this will not have been done at this stage
					// in most cases.
					Dimension2D preferredSize = _graph.getUI()
							.getPreferredSize(_graph, cell);
					GuiGraphConstants.setVertexPreferredSize(cell
							.getAttributes(), preferredSize);
					_entities.add((GuiVertexView) cell);
				} else if (cell instanceof GuiEdgeView) {
					_relationships.add((GuiEdgeView) cell);
				} else {
					// Is this an error?
					_logger.debug("Found non-gui view: {}", cell);
				}
			}

		}

		/**
		 * This should only be called from the Event Dispatch Thread.
		 * 
		 * @see edu.upenn.cis.orchestra.gui.graphs.ILayoutHelper#applyLayout()
		 * 
		 */
		@Override
		public void applyLayout() {
			applyLayoutSynchronous();
			if (_centerAfterLayout) {
				_graph.center();
			}
		}

		/**
		 * Run the layout algorithm synchronously and non-continuously.
		 */

		public void applyLayoutSynchronous() {
			LayoutEntity[] entities = _entities.toArray(new LayoutEntity[0]);
			LayoutRelationship[] relationships = _relationships
					.toArray(new LayoutRelationship[0]);
			Container parent = _graph.getParent();
			_logger.debug("Laying out graph: {}", _graph);
			_logger.trace("Graph's Parent: {}", parent);
			_logger
					.trace("Graph Bounds before layoyut: {}", _graph
							.getBounds());

			double width = _graph.getWidth();
			double height = _graph.getHeight();
			if (null != parent && !_graph.isPreferredSizeSet()) {
				width = parent.getWidth();
				height = parent.getHeight();
				_logger.trace("Overriding graph bounds with parent bounds.");
				if (parent instanceof JViewport) {
					JViewport jvpParent = (JViewport) parent;
					Rectangle r = jvpParent.getVisibleRect();
					width = r.width;
					height = r.height;
					_logger
							.trace("Overriding parent bounds with visible rectangle bounds.");
				}
			}
			if (_logger.isDebugEnabled()) {
				_logger
						.debug(
								"Bounds given to layout algorithm: x = 0, y = 0, width = {}, height = {}",
								new Object[] { Double.valueOf(width),
										Double.valueOf(height) });
			}
			_graph.enableRouting();
			LayoutAlgorithm zestLayout = _algorithmFactory.getLayoutAlgorithm();
			try {
				zestLayout.applyLayout(entities, relationships, 0, 0, width,
						height, false, false);

			} catch (InvalidLayoutConfiguration e) {
				// This should not happen, since the only way to invalidate the
				// configuration is by choosing incompatible values for the
				// boolean arguments of applyLayout(...).
				String message = ("".equals(e.getMessage()) || e.getMessage() == null) ? "The layout algorithm for "
						+ _graph.getClass().getSimpleName()
						+ " has an invalid configuration."
						: e.getMessage();
				JOptionPane.showMessageDialog(_graph, message,
						"Unable to Layout Graph", JOptionPane.ERROR_MESSAGE);
				_logger.error("Invalid configuration.", e);
			} catch (Throwable t) {
				JOptionPane.showMessageDialog(_graph, t.getMessage(),
						"Unable to Layout Graph", JOptionPane.ERROR_MESSAGE);
				_logger
						.error(
								"Unchecked exception caught while running graph layout.",
								t);
			}

			Map<Object, AttributeMap> nested = new Hashtable<Object, AttributeMap>();
			for (LayoutEntity entity : entities) {
				GuiVertexView newView = (GuiVertexView) entity;
				nested.put(newView.getCell(), newView.getAllAttributes());
			}
			_graph.getGraphLayoutCache().edit(nested);

			if (!_graph.isEnableRoutingWhenNotLayout()) {
				_graph.disableRouting();
			}

			_logger.trace("Graph bounds after layout. From graph: {}", _graph
					.getBounds());
			_logger.trace(
					"Graph bounds after layout. From graph layout cache: {}",
					GraphLayoutCache.getBounds(_graph.getGraphLayoutCache()
							.getAllViews()));
			_logger.trace(
					"Graph bounds after layout. From graph cell bounds: {}",
					_graph.getCellBounds(_graph.getDescendants(_graph
							.getRoots())));

			// This appears to be necessary to avoid the occasional
			// problem of
			// previous renderings of a vertex sticking around when the
			// layout button is pushed.
			_graph.addOffscreenDirty(_graph.getBounds());

		}

		/**
		 * This method is only here for testing. It creates a simple version of
		 * the graph from Zest's point of view, using only the information
		 * available to the layout algorithms and none from JGraph.
		 * 
		 * @param entities
		 * @param width
		 * @param height
		 * @param layoutName
		 */
		private void checkLayout(LayoutEntity[] entities, double width,
				double height, String layoutName) {
			JFrame frame = new JFrame(_graph.getClass().getSimpleName() + " "
					+ layoutName + " " + entities.length);
			// frame.setSize((int) width, (int) height);
			frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

			class VertexPositions extends JComponent {
				private List<Rectangle2D> _entityBounds;
				private String _layoutName;

				VertexPositions(LayoutEntity[] entities, String layoutName) {
					super();
					_layoutName = layoutName;
					_entityBounds = new ArrayList<Rectangle2D>(entities.length);
					for (LayoutEntity ent : entities) {
						_entityBounds.add(new Rectangle2D.Double(ent
								.getXInLayout(), ent.getYInLayout(), ent
								.getWidthInLayout(), ent.getHeightInLayout()));

					}
				}

				/**
				 * @see javax.swing.JComponent#paintComponent(java.awt.Graphics)
				 */
				@Override
				protected void paintComponent(Graphics g) {
					Graphics2D g2 = (Graphics2D) g;
					for (Rectangle2D ent : _entityBounds) {
						drawVertex(g2, ent);

					}
				}

				/**
				 * Draws the bounding rectangle of a vertex.
				 * 
				 * @param g2
				 * @param ent
				 */
				private void drawVertex(Graphics2D g2, Rectangle2D ent) {
					double entX = ent.getX();
					double entY = ent.getY();
					double entWidth = ent.getWidth();
					double entHeight = ent.getHeight();
					_logger.debug("Layout: {} Drawing Vertex at: {} {} {} {}",
							new Object[] { _layoutName, Double.valueOf(entX),
									Double.valueOf(entY),
									Double.valueOf(entWidth),
									Double.valueOf(entHeight) });
					Shape c = new Rectangle2D.Double(entX, entY, entWidth,
							entHeight);
					g2.setPaint(Color.blue);
					g2.fill(c);

				}
			}
			frame.add(new VertexPositions(entities, layoutName));
			frame.setVisible(true);
			Insets insets = frame.getInsets();

			frame.setSize((int) width + insets.left + insets.right,
					(int) height + insets.top + insets.bottom);
		}

	}

	/**
	 * The graph we are laying out.
	 */
	final private BasicGraph _graph;

	/**
	 * The algorithm factory to use when laying out the graph.
	 */
	final private ILayoutAlgorithmFactory _algorithmFactory;

	/**
	 * If {@code true} we should center graph after applying the layout
	 * algorithm.
	 */
	private boolean _centerAfterLayout = false;

	/**
	 * Construct a helper to layout {@code graph}.
	 * 
	 * @param graph
	 * @param algorithmFactory
	 */
	public LayoutHelperBuilder(BasicGraph graph,
			ILayoutAlgorithmFactory algorithmFactory) {
		_graph = graph;
		_algorithmFactory = algorithmFactory;
	}

	/**
	 * Returns a layout helper for the supplied graph and configuration options.
	 * 
	 * @return a layout helper for the supplied graph and configuration options.
	 */
	public ILayoutHelper build() {
		ILayoutHelper helper = new ZestLayoutHelper();
		return helper;
	}

	/**
	 * If {@code center} is {@code true}, then the {@code ILayoutHelper} being
	 * built will center the graph after each layout. Otherwise, not. The
	 * default is to not center the graph.
	 * 
	 * @param center
	 * @return this {@code LayoutHelperBuilder}
	 */
	public LayoutHelperBuilder centerAfterLayout(boolean center) {
		_centerAfterLayout = center;
		return this;
	}
}
