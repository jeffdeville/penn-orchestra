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
package edu.upenn.cis.orchestra.gui.peers.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgraph.graph.DefaultPort;
import org.jgraph.graph.GraphConstants;

import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.gui.graphs.BasicGraph;
import edu.upenn.cis.orchestra.gui.graphs.GuiDefaultGraphCell;
import edu.upenn.cis.orchestra.gui.graphs.GuiDefaultGraphObj;
import edu.upenn.cis.orchestra.gui.graphs.ILayoutHelper;
import edu.upenn.cis.orchestra.gui.graphs.LayoutHelperBuilder;
import edu.upenn.cis.orchestra.gui.graphs.LayoutHelperBuilder.LayoutAlgorithmType;

/**
 * Graph representing the P2P "bird's eye view"
 * 
 * @author biton, zives
 * 
 */
public class PeerGraph extends BasicGraph {
	public static final long serialVersionUID = 1L;

	public static final int DEFAULT_WIDTH = 800;
	public static final int DEFAULT_HEIGHT = 600;

	private Set<PeerVertex> _allPeerVertices;
	
	// From mapping edge/node to the main edge/node in the mapping, i.e.,
	// the one with the label
	private HashMap<IPeerMapping, IPeerMapping> _allMappings;

	// private Rectangle2D _bounds;
	// private boolean _drawn = false;

	public static class Pair {
		public Pair(GuiDefaultGraphCell one, GuiDefaultGraphCell two) {
			first = one;
			second = two;
		}

		public GuiDefaultGraphCell first;
		public GuiDefaultGraphCell second;
	}

	/**
	 * 
	 * @param system
	 *            We need the whole/complete system to know how to relate the
	 *            peers together (using mappings)
	 */
	public PeerGraph(OrchestraSystem system) {
		super(true);
		
		//setPreferredSize(new Dimension(DEFAULT_WIDTH, DEFAULT_HEIGHT));

		_allPeerVertices = new HashSet<PeerVertex>();
		_allMappings = new HashMap<IPeerMapping, IPeerMapping>();

		// Create the peer network
		List<Peer> peersList = new ArrayList<Peer>(system.getPeers());
		addPeersAndMappings(peersList);

		// Apply an automatic layout
		// applyLayout ();
		
	}

	public void setSelectedPeer(PeerVertex pv) {
		for (PeerVertex p : _allPeerVertices) {
			if (p == pv) {
				PeerVertex.setHighlighted(p.getAttributes());
				getGraphLayoutCache().editCell(p, p.getAttributes());
			} else {
				PeerVertex.setNormal(p.getAttributes());
				getGraphLayoutCache().editCell(p, p.getAttributes());
			}
		}
		// getGraphLayoutCache().g

	}

	public void setSelectedMapping(IPeerMapping m) {
		IPeerMapping m2 = _allMappings.get(m);
		for (IPeerMapping map : _allMappings.keySet()) {
			if (map == m2) {
				map.setHighlighted(map.getAttributes());
				getGraphLayoutCache().editCell(map, map.getAttributes());
			} else {
				map.setNormal(map.getAttributes());
				getGraphLayoutCache().editCell(map, map.getAttributes());
			}
		}

	}

	/*
	 * ///TODO: Not really efficient, optimize memory... private void addPeers
	 * (List<Peer> peers) { List<DefaultGraphCell> cells = new
	 * ArrayList<DefaultGraphCell> ();
	 * 
	 * // Create all peers cells and prepare peer indices for next step
	 * Map<Peer,Map<Schema,DefaultGraphCell>> peerCellMap = new
	 * HashMap<Peer,Map<Schema,DefaultGraphCell>> (); // Let's suppose average
	 * schema/peer=2... List<Schema> allSchemas = new ArrayList<Schema>
	 * (peers.size()2); List<Peer> allSchemasPeer = new ArrayList<Peer>
	 * (peers.size()2); for (Peer p : peers) {
	 * allSchemas.addAll(p.getSchemas()); for (Schema s : p.getSchemas()) {
	 * allSchemasPeer.add(p); addPeer (p, s, cells, peerCellMap); } }
	 * 
	 * // Now we have to detect from the mappings what peers should be connected
	 * // TODO: Cache the associated mappings for GUI?? boolean[][]
	 * schemasMatrix = new boolean[allSchemas.size()][allSchemas.size()]; for
	 * (Peer p : peers) { for (ScMapping mapp : p.getMappings()) { // List the
	 * schemas (indexes) in the head Set<Integer> headSchemas = new
	 * HashSet<Integer>(mapp.getMappingHead().size()); for (ScMappingAtom atom :
	 * mapp.getMappingHead())
	 * headSchemas.add(allSchemas.indexOf(atom.getSchema())); // Complete the
	 * matrix with a link from each body schema to each head schema for
	 * (ScMappingAtom atom : mapp.getBody()) for (Integer headSchema :
	 * headSchemas)
	 * schemasMatrix[allSchemas.indexOf(atom.getSchema())][headSchema
	 * .intValue()]=true; } } // Once we have the matrix: create the edges
	 * addEdges (schemasMatrix, allSchemas, allSchemasPeer, peerCellMap, cells);
	 * 
	 * // We can now add all the cells to the graph model
	 * getGraphLayoutCache().insert(cells.toArray()); }
	 */

	/**
	 * Iterate through the peers and add edges between them according to the
	 * mappings. If m x n mapping appears, or if multiple 1-1 mappings between
	 * the same nodes, then add a mapping node rather than just a labeled edge.
	 * 
	 * @param peers
	 */
	private void addPeersAndMappings(List<Peer> peers) {
		List<GuiDefaultGraphObj> cells = new ArrayList<GuiDefaultGraphObj>();

		_allMappings.clear();
		_allPeerVertices.clear();

		// Create all peers cells and prepare peer indices for next step
		Map<Peer, Map<Schema, GuiDefaultGraphCell>> peerCellMap = new HashMap<Peer, Map<Schema, GuiDefaultGraphCell>>();
		// Let's suppose average schema/peer=2...
		List<Schema> allSchemas = new ArrayList<Schema>(peers.size() * 2);
		List<Peer> allSchemasPeer = new ArrayList<Peer>(peers.size() * 2);
		for (Peer p : peers) {
			allSchemas.addAll(p.getSchemas());
			for (Schema s : p.getSchemas()) {
				allSchemasPeer.add(p);
				addPeer(p, s, cells, peerCellMap);
			}
		}

		List<GuiDefaultGraphCell> mapCells = new ArrayList<GuiDefaultGraphCell>();

		List<Pair> oneToOnes = new ArrayList<Pair>();
		int i = 0;
		for (Peer p : peers) {
			for (Mapping mapp : p.getMappings()) {
				// List the schemas (indexes) in the head
				List<GuiDefaultGraphCell> headSchemas = new ArrayList<GuiDefaultGraphCell>(
						mapp.getMappingHead().size());
				List<GuiDefaultGraphCell> bodySchemas = new ArrayList<GuiDefaultGraphCell>(
						mapp.getBody().size());

				List<Schema> schemas = new ArrayList<Schema>();
				for (Atom atom : mapp.getMappingHead()) {
					if (!schemas.contains(atom.getSchema())) {
						GuiDefaultGraphCell c = peerCellMap.get(p).get(
								atom.getSchema());
						headSchemas.add(c);
						schemas.add(atom.getSchema());
					}
				}

				// Complete the matrix with a link from each body schema to each
				// head schema
				schemas.clear();
				for (Atom atom : mapp.getBody()) {
					if (!schemas.contains(atom.getSchema())) {
						Peer p2 = atom.getPeer();
						if (p2 != null) {
							GuiDefaultGraphCell c = peerCellMap.get(p2).get(
									atom.getSchema());
							bodySchemas.add(c);
						}
						schemas.add(atom.getSchema());
					}
				}

				// Add a node if the mapping is not 1-1 or if it's a cycle
				boolean needsNode = (headSchemas.size() != 1
						|| bodySchemas.size() != 1 || headSchemas.get(0) == bodySchemas
						.get(0));
				PeerMappingVertex cell = null;
				// See if we already have added an edge along this pair; if so
				// we add a node
				if (!needsNode)
					for (Pair pair : oneToOnes) {
						if (pair.first == headSchemas.get(0)
								&& pair.second == bodySchemas.get(0))
							needsNode = true;
					}
				if (!needsNode) {
					oneToOnes.add(new Pair(headSchemas.get(0), bodySchemas
							.get(0)));
					oneToOnes.add(new Pair(headSchemas.get(0), bodySchemas
							.get(0)));
				}

				// Create a node for the mapping
				if (needsNode) {
					cell = addMappingNode(p, mapp, i);
					// Add a default port for edges from/to this peer
					DefaultPort port = new DefaultPort();
					cell.add(port);
					port.setParent(cell);

					cells.add(cell);
				}

				addEdge(p, headSchemas, bodySchemas, cell, i, cells, needsNode,
						mapp);

				i++;
			}
		}
		cells.addAll(mapCells);

		// We can now add all the cells to the graph model
		getGraphLayoutCache().insert(cells.toArray());
	}

	private void addEdge(Peer p, List<GuiDefaultGraphCell> headCells,
			List<GuiDefaultGraphCell> bodyCells, PeerMappingVertex mappingCell,
			int mid, List<GuiDefaultGraphObj> cells, boolean needsNode,
			Mapping map) {
		if (!needsNode) {
			// Create the edge
			PeerMappingEdge edge = new PeerMappingEdge(p, map, /*
																 * map.getMappingHead
																 * (
																 * ).get(0).getPeer
																 * ().getId() +
																 */map.getId());// new
																				// String("m"
																				// +
																				// Integer.valueOf(mid)));

			// Connect to default ports
			edge.setSource(bodyCells.get(0).getChildAt(0));
			edge.setTarget(headCells.get(0).getChildAt(0));
			// Add arrow
			GraphConstants.setLineEnd(edge.getAttributes(),
					GraphConstants.ARROW_CLASSIC);
			GraphConstants.setLineStyle(edge.getAttributes(),
					GraphConstants.STYLE_SPLINE);
			GraphConstants.setBendable(edge.getAttributes(), true);
			GraphConstants.setLabelAlongEdge(edge.getAttributes(), true);

			_allMappings.put(edge, edge);

			// add the edge to the cells list
			cells.add(edge);
			return;
		} else {
			for (GuiDefaultGraphCell h : headCells) {
				// Create the edge
				PeerMappingEdge edge = new PeerMappingEdge(p, map);

				// Connect to default ports
				edge.setSource(mappingCell.getChildAt(0));
				edge.setTarget(h.getChildAt(0));
				// Add arrow
				GraphConstants.setLineEnd(edge.getAttributes(),
						GraphConstants.ARROW_CLASSIC);

				GraphConstants.setLineStyle(edge.getAttributes(),
						GraphConstants.STYLE_SPLINE);
				GraphConstants.setBendable(edge.getAttributes(), true);
				_allMappings.put(edge, mappingCell);

				// add the edge to the cells list
				cells.add(edge);
			}
			for (GuiDefaultGraphCell b : bodyCells) {
				// Create the edge
				PeerMappingEdge edge = new PeerMappingEdge(p, map);

				// Connect to default ports
				edge.setSource(b.getChildAt(0));
				edge.setTarget(mappingCell.getChildAt(0));
				GraphConstants.setLineStyle(edge.getAttributes(),
						GraphConstants.STYLE_SPLINE);
				GraphConstants.setBendable(edge.getAttributes(), true);
				// add the edge to the cells list
				_allMappings.put(edge, mappingCell);
				cells.add(edge);
			}
		}
	}

	/**
	 * Create a cell for peer p
	 * 
	 * @param p
	 *            Peer to represent with a cell
	 */
	private void addPeer(Peer p, Schema s, List<GuiDefaultGraphObj> cells,
			Map<Peer, Map<Schema, GuiDefaultGraphCell>> peerCellMap) {
		// Create the cell
		PeerVertex pCell = new PeerVertex(p, s);

		// Add a default port for edges from/to this peer
		DefaultPort port = new DefaultPort();
		pCell.add(port);
		port.setParent(pCell);

		// Add the new cell to the cells list
		cells.add(pCell);
		_allPeerVertices.add(pCell);
		Map<Schema, GuiDefaultGraphCell> internalMap;
		if (!peerCellMap.containsKey(p)) {
			internalMap = new HashMap<Schema, GuiDefaultGraphCell>();
			peerCellMap.put(p, internalMap);
		} else
			internalMap = peerCellMap.get(p);
		internalMap.put(s, pCell);
	}

	/**
	 * Create a cell for peer p
	 * 
	 * @param p
	 *            Peer to represent with a cell
	 */
	private PeerMappingVertex addMappingNode(Peer p, Mapping map, int mapId) {
		// Create the cell
		PeerMappingVertex pCell = new PeerMappingVertex(p, map, /*
																 * map.getMappingHead
																 * (
																 * ).get(0).getPeer
																 * ().getId() +
																 */map.getId());// "m"
																				// +
																				// String.valueOf(mapId));

		// Add a default port for edges from/to this peer
		DefaultPort port = new DefaultPort();
		pCell.add(port);
		port.setParent(pCell);
		_allMappings.put(pCell, pCell);

		return pCell;
	}

	/*
	 * private void addEdges (boolean[][] schemasMatrix, List<Schema>
	 * allSchemas, List<Peer> allSchemasPeers,
	 * Map<Peer,Map<Schema,DefaultGraphCell>> peerCellMap,
	 * List<DefaultGraphCell> cells) { for (int i = 0 ; i < schemasMatrix.length
	 * ; i++) for (int j = 0 ; j < schemasMatrix.length ; j++) if (i != j &&
	 * schemasMatrix[i][j]) { // Create the edge DefaultEdge edge = new
	 * DefaultEdge (); DefaultGraphCell src =
	 * peerCellMap.get(allSchemasPeers.get(i)).get(allSchemas.get(i));
	 * DefaultGraphCell target =
	 * peerCellMap.get(allSchemasPeers.get(j)).get(allSchemas.get(j)); //
	 * Connect to default ports edge.setSource(src.getChildAt(0));
	 * edge.setTarget(target.getChildAt(0)); // Add arrow
	 * GraphConstants.setLineEnd(edge.getAttributes(),
	 * GraphConstants.ARROW_CLASSIC);
	 * GraphConstants.setLineStyle(edge.getAttributes(),
	 * GraphConstants.STYLE_SPLINE);
	 * GraphConstants.setBendable(edge.getAttributes(), true); // add the edge
	 * to the cells list cells.add (edge); } }
	 */

	/**
	 * Applies a composite layout to this graph: first directed graph, then spring. The
	 * first layout is necessary to minimize edge crossings.
	 * 
	 * @see edu.upenn.cis.orchestra.gui.graphs.BasicGraph#applyLayout()
	 */
	@Override
	protected void applyLayout() {
		LayoutHelperBuilder builder = new LayoutHelperBuilder(this,
				LayoutAlgorithmType.DIRECTED_SPRING_COMP);

		final ILayoutHelper helper = builder.build();

		helper.applyLayout();
	}

}
