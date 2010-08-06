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

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.event.AdjustmentEvent;
import java.awt.event.AdjustmentListener;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.ComponentListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;
import java.awt.geom.Point2D;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.lang.ref.WeakReference;

import javax.swing.BorderFactory;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JViewport;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;

import org.jgraph.JGraph;
import org.jgraph.event.GraphLayoutCacheEvent;
import org.jgraph.event.GraphLayoutCacheListener;
import org.jgraph.event.GraphModelEvent;
import org.jgraph.event.GraphModelListener;
import org.jgraph.graph.GraphLayoutCache;

/**
 * Birds-eye view on a graph. The displayed graph may be changed at runtime. The
 * class provides the {@link FactoryMethod} to be added to an editor factory.
 */
public class GraphBirdView extends JPanel implements GraphLayoutCacheListener,
		GraphModelListener, PropertyChangeListener, AdjustmentListener {

	public static final long serialVersionUID = 1L;

	/**
	 * Shared cursor objects to avoid expensive constructor calls.
	 */
	protected static final Cursor CURSOR_DEFAULT = new Cursor(
			Cursor.DEFAULT_CURSOR),
			CURSOR_HAND = new Cursor(Cursor.HAND_CURSOR);

	/**
	 * Component listener to udpate the scale.
	 */
	protected ComponentListener _componentListener = new ComponentAdapter() {
		public void componentResized(ComponentEvent e) {
			updateScale();
		}
	};

	/**
	 * References the inital layout cache of the backing graph.
	 */
	protected transient GraphLayoutCache _initialLayoutCache;

	/**
	 * Holds the backing graph and references the displayed (current) graph.
	 */
	protected JGraph _backingGraph;

	/**
	 * Weak reference to the current graph.
	 */
	protected WeakReference<BasicGraph> _currentGraph;

	/**
	 * Holds the navigator pane the displays the backing graph.
	 */
	protected NavigatorPane _navigatorPane;

	/**
	 * Specifies the maximum scale for the navigator view. Default is 0.5
	 */
	protected double _maximumScale = 0.5;

	/**
	 * Specifies the minimum scale for the navigator view. Default is 0.3
	 */
	protected double _minimumScale = 0.15;

	/**
	 * Constructs a new graph navigator using a <code>backingGraph</code> to
	 * display the graph in {@link #_currentGraph}.
	 * 
	 */
	public GraphBirdView() {
		super(new BorderLayout());

		setDoubleBuffered(true);
		JGraph backingGraph = new JGraph(new GraphLayoutCache());
		backingGraph.setEnabled(false);
		backingGraph.setFocusable(false);

		setBackingGraph(backingGraph);
		_initialLayoutCache = backingGraph.getGraphLayoutCache();
		backingGraph.setOpaque(false);
		backingGraph.setScale(_maximumScale);
		_navigatorPane = new NavigatorPane(backingGraph);
		backingGraph.addMouseListener(_navigatorPane);
		backingGraph.addMouseMotionListener(_navigatorPane);

		// Configures the navigator
		setBorder(BorderFactory.createEmptyBorder(4, 4, 4, 4));
		add(_navigatorPane, BorderLayout.CENTER);
		setFocusable(false);

		// Updates the size when the component is resized
		addComponentListener(_componentListener);
	}

	/**
	 * Returns the navigator pane that contains the backing graph.
	 * 
	 * @return Returns the navigator pane.
	 */
	public NavigatorPane getScrollPane() {
		return _navigatorPane;
	}

	/**
	 * Returns the maximum scale to be used for the backing graph.
	 * 
	 * @return Returns the maximumScale.
	 */
	public double getMaximumScale() {
		return _maximumScale;
	}

	/**
	 * Returns the minimum scale to be used for the backing graph
	 * 
	 * @return Minimum scale
	 */
	public double getMinimumScale() {
		return _minimumScale;
	}

	/**
	 * Sets the maximum scale to be used for the backing graph.
	 * 
	 * @param maximumScale
	 *            The maximumScale to set.
	 */
	public void setMaximumScale(double maximumScale) {
		this._maximumScale = maximumScale;
	}

	public void setMinimumScale(double minScale) {
		_minimumScale = minScale;
	}

	/**
	 * Returns the backing graph that is used to display {@link #_currentGraph}.
	 * 
	 * @return Returns the backing graph.
	 */
	public JGraph getBackingGraph() {
		return _backingGraph;
	}

	/**
	 * Sets the backing graph that is used to display {@link #_currentGraph}.
	 * 
	 * @param backingGraph
	 *            The backing graph to set.
	 */
	public void setBackingGraph(JGraph backingGraph) {
		this._backingGraph = backingGraph;
	}

	/**
	 * Returns the graph that is currently displayed.
	 * 
	 * @return Returns the backing graph.
	 */
	public BasicGraph getCurrentGraph() {
		return (BasicGraph) ((_currentGraph != null) ? _currentGraph.get()
				: null);
	}

	/**
	 * Sets the graph that is currently displayed.
	 * 
	 * @param sourceGraph
	 *            The current graph to set.
	 */
	public void setCurrentGraph(BasicGraph sourceGraph) {
		if (sourceGraph == null || getParentGraph(sourceGraph) == null) {
			if (sourceGraph != null) {
				JGraph oldValue = getCurrentGraph();

				// Removes listeners from the previous graph
				if (oldValue != null && sourceGraph != oldValue) {
					oldValue.getModel().removeGraphModelListener(this);
					oldValue.getGraphLayoutCache()
							.removeGraphLayoutCacheListener(this);
					oldValue.removePropertyChangeListener(this);
					JScrollPane scrollPane = getParentScrollPane(oldValue);
					if (scrollPane != null) {
						scrollPane.removeComponentListener(_componentListener);
						scrollPane.getVerticalScrollBar()
								.removeAdjustmentListener(this);
						scrollPane.getHorizontalScrollBar()
								.removeAdjustmentListener(this);
						scrollPane.removePropertyChangeListener(this);
					}

					// Restores the layout cache of the backing graph
					_backingGraph.setGraphLayoutCache(_initialLayoutCache);
				}
				this._currentGraph = new WeakReference<BasicGraph>(sourceGraph);

				// Installs change listeners to update the size
				if (sourceGraph != null) {
					sourceGraph.getModel().addGraphModelListener(this);
					sourceGraph.getGraphLayoutCache()
							.addGraphLayoutCacheListener(this);
					sourceGraph.addPropertyChangeListener(this);
					JScrollPane currentScrollPane = getParentScrollPane(sourceGraph);
					if (currentScrollPane != null) {
						currentScrollPane
								.addComponentListener(_componentListener);
						currentScrollPane.getVerticalScrollBar()
								.addAdjustmentListener(this);
						currentScrollPane.getHorizontalScrollBar()
								.addAdjustmentListener(this);
						currentScrollPane.addPropertyChangeListener(this);
					}
					_backingGraph.setGraphLayoutCache(sourceGraph
							.getGraphLayoutCache());
				}
				updateScale();
				_backingGraph.scrollPointToVisible(new Point2D.Double(0, 0));
			}
		}
	}

	/**
	 * Updates the scale of the backing graph.
	 */
	public void updateScale() {
		BasicGraph graph = getCurrentGraph();

		if (graph != null && !graph.isCurrLayout() && graph.isShowing()) {
			while (!graph.setCurrLayout(true)) {
				try {
					Thread.sleep(20);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			Dimension d = graph.getPreferredSize();
			Dimension b = graph.getBounds().getSize();
			d.width = Math.max(d.width, b.width);
			b.height = Math.max(d.height, b.height);
			double scale = graph.getScale();
			d.setSize(d.width * 1 / scale, d.height * 1 / scale);
			Dimension s = getScrollPane().getViewport().getSize();
			double sx = s.getWidth() / d.getWidth();
			double sy = s.getHeight() / d.getHeight();
			scale = Math.min(Math.min(sx, sy), getMaximumScale());
			scale = Math.max(scale, getMinimumScale());
			getBackingGraph().setScale(scale);

			graph.setCurrLayout(false);
		}
		revalidate();
	}

	/*
	 * (non-Javadoc)
	 */
	public void graphLayoutCacheChanged(GraphLayoutCacheEvent e) {
		updateScale();
	}

	/*
	 * (non-Javadoc)
	 */
	public void graphChanged(GraphModelEvent e) {
		updateScale();
	}

	/*
	 * (non-Javadoc)
	 */
	public void propertyChange(PropertyChangeEvent event) {
		updateScale();
	}

	/*
	 * (non-Javadoc)
	 */
	public void adjustmentValueChanged(AdjustmentEvent e) {
		_navigatorPane.repaint();
		_backingGraph.scrollRectToVisible(_navigatorPane._currentViewport);
	}

	/**
	 * Helper method that returns the parent scrollpane for the specified
	 * component in the component hierarchy. If the component is itself a
	 * scrollpane then it is returned.
	 * 
	 * @return Returns the parent scrollpane or component.
	 */
	public static JScrollPane getParentScrollPane(Component component) {
		while (component != null) {
			if (component instanceof JScrollPane)
				return (JScrollPane) component;
			component = component.getParent();
		}
		return null;
	}

	/**
	 * Helper method that returns the parent JGraph for the specified component
	 * in the component hierarchy. The component itself is never returned.
	 * 
	 * @return Returns the parent scrollpane or component.
	 */
	public static JGraph getParentGraph(Component component) {
		do {
			component = component.getParent();
			if (component instanceof JGraph)
				return (JGraph) component;
		} while (component != null);
		return null;
	}

	@Override
	public Dimension getPreferredSize() {
		return super.getPreferredSize();
	}

	/**
	 * Scrollpane that implements special painting used for the navigator
	 * preview.
	 */
	public class NavigatorPane extends JScrollPane implements MouseListener,
			MouseMotionListener {

		public static final long serialVersionUID = 1L;

		/**
		 * Holds the bounds of the finder (red box).
		 */
		protected Rectangle _currentViewport = new Rectangle();

		/**
		 * Holds the location of the last mouse event.
		 */
		protected Point _lastPoint = null;

		/**
		 * Constructs a new navigator pane using the specified backing graph to
		 * display the preview.
		 * 
		 * @param backingGraph
		 *            The backing graph to use for rendering.
		 */
		public NavigatorPane(JGraph backingGraph) {
			super(backingGraph);
			setOpaque(false);
			getViewport().setOpaque(false);
		}

		@Override
		public Dimension getPreferredSize() {
			return super.getPreferredSize();
		}

		/**
		 * Paints the navigator pane on the specified graphics.
		 * 
		 * @param g
		 *            The graphics to paint the navigator to.
		 */
		public void paint(Graphics g) {
			JGraph graph = getCurrentGraph();
			JScrollPane scrollPane = getParentScrollPane(graph);
			g.setColor(Color.lightGray);
			g.fillRect(0, 0, getWidth(), getHeight());
			if (scrollPane != null && graph != null) {
				JViewport viewport = scrollPane.getViewport();
				Rectangle rect = viewport.getViewRect();
				double scale = _backingGraph.getScale() / graph.getScale();
				Dimension pSize = graph.getPreferredSize();
				g.setColor(getBackground());
				g.fillRect(0, 0, (int) (pSize.width * scale),
						(int) (pSize.height * scale));
				g.setColor(Color.WHITE);
				_currentViewport.setFrame((int) (rect.getX() * scale),
						(int) (rect.getY() * scale),
						(int) (rect.getWidth() * scale), (int) (rect
								.getHeight() * scale));
				int deltaX = (int) getViewport().getViewPosition().getX();
				int deltaY = (int) getViewport().getViewPosition().getY();
				g.fillRect(_currentViewport.x - deltaX, _currentViewport.y
						- deltaY, _currentViewport.width,
						_currentViewport.height);

				super.paint(g);
				g.setColor(Color.RED);
				g.drawRect(_currentViewport.x - deltaX, _currentViewport.y
						- deltaY, _currentViewport.width,
						_currentViewport.height);
			}
		}

		/*
		 * (non-Javadoc)
		 */
		public void mouseClicked(MouseEvent e) {
			// empty
		}

		/*
		 * (non-Javadoc)
		 */
		public void mousePressed(MouseEvent e) {
			if (_currentViewport.contains(e.getX(), e.getY()))
				_lastPoint = e.getPoint();
		}

		/*
		 * (non-Javadoc)
		 */
		public void mouseReleased(MouseEvent e) {
			_lastPoint = null;
		}

		/*
		 * (non-Javadoc)
		 */
		public void mouseEntered(MouseEvent e) {
			// empty

		}

		/*
		 * (non-Javadoc)
		 */
		public void mouseExited(MouseEvent e) {
			// empty

		}

		/*
		 * (non-Javadoc)
		 */
		public void mouseDragged(MouseEvent e) {
			if (_lastPoint != null) {
				JGraph graph = getCurrentGraph();
				JScrollPane scrollPane = getParentScrollPane(graph);
				if (scrollPane != null && _currentGraph != null) {
					JViewport viewport = scrollPane.getViewport();
					Rectangle rect = viewport.getViewRect();
					double scale = _backingGraph.getScale() / graph.getScale();
					double x = (e.getX() - _lastPoint.getX()) / scale;
					double y = (e.getY() - _lastPoint.getY()) / scale;
					_lastPoint = e.getPoint();
					x = rect.getX() + ((x > 0) ? rect.getWidth() : 0) + x;
					y = rect.getY() + ((y > 0) ? rect.getHeight() : 0) + y;
					Point2D pt = new Point2D.Double(x, y);
					graph.scrollPointToVisible(pt);
					repaint();
				}
			}
		}

		/*
		 * (non-Javadoc)
		 */
		public void mouseMoved(MouseEvent e) {
			if (_currentViewport.contains(e.getPoint()))
				setCursor(CURSOR_HAND);
			else
				setCursor(CURSOR_DEFAULT);
		}

	}

}