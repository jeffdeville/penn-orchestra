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
package edu.upenn.cis.orchestra.gui.provenance;

import java.awt.Color;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.JOptionPane;

import org.jgraph.graph.CellView;
import org.jgraph.graph.DefaultEdge;
import org.jgraph.graph.DefaultGraphCell;
import org.jgraph.graph.DefaultPort;
import org.jgraph.graph.GraphConstants;

import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datalog.atom.AtomConst;
import edu.upenn.cis.orchestra.datalog.atom.AtomVariable;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultSetIterator;
import edu.upenn.cis.orchestra.gui.graphs.BasicGraph;
import edu.upenn.cis.orchestra.gui.graphs.GuiGraphConstants;
import edu.upenn.cis.orchestra.gui.graphs.ILayoutHelper;
import edu.upenn.cis.orchestra.gui.graphs.LayoutHelperBuilder;
import edu.upenn.cis.orchestra.gui.graphs.LegendGraph;
import edu.upenn.cis.orchestra.gui.graphs.LayoutHelperBuilder.LayoutAlgorithmType;
import edu.upenn.cis.orchestra.mappings.Rule;

/**
 * The visualization of transactions, dependencies, and conflicts
 * 
 * @author zives
 *
 */
public class ProvenanceGraph extends BasicGraph {
	public class TupleContext {
		public TupleContext(TupleContext tp) {
			_context = tp._context;
			_tuple = tp._tuple;
		}
		
		public TupleContext(Tuple t, RelationContext context) {
			_context = context;
			_tuple = t;
		}
		
		public TupleContext(Tuple t) {
			_context = t.getOrigin();
			_tuple = t;
		}
		
		public RelationContext getContext() {
			return _context;
		}
		
		public Tuple getTuple() {
			return _tuple;
		}
		
		public int hashCode() {
			return _tuple.hashCode();
		}
		
		public boolean equals(Object o) {
			if (o == null)
				return false;
			
			if (!(o instanceof TupleContext))
				return super.equals(o);
			else {
				TupleContext other = (TupleContext)o;
				return ((_context == other._context || _context.equals(other._context))) && (_tuple == other._tuple || (_tuple.equals(other._tuple)));
			}
		}
		
		RelationContext _context;
		Tuple _tuple;
		
	}
	
	private static final int DEPTH = 4;

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Peer _curPeer;
	private Schema _curSchema;
	private List<DefaultGraphCell> _cells;
	private OrchestraSystem _system;
	private Tuple _root;
	private DefaultGraphCell _rootNode;
	
	private Map<TupleContext,DefaultGraphCell> _tupleNodes;
	private Map<TupleContext,DefaultGraphCell> _oldTupleNodes;

	public ProvenanceGraph(Peer p, Schema s, OrchestraSystem sys) {
		super(true);

		_system = sys;
		_curPeer = p;
		_root = null;
		_tupleNodes = new HashMap<TupleContext,DefaultGraphCell>();
		//_tupleMappings = new HashMap<SchemaTuple,Set<MappingVertex>>();

		_cells = new ArrayList<DefaultGraphCell>();
		//_tuplesVisited = new HashSet<TupleContext>();
		
		if (!_system.getMappingDb().isConnected())
			try {
				_system.getMappingDb().connect();
			} catch (Exception e) {
				JOptionPane.showMessageDialog(this, e.getMessage(), "Error connecting to DBMS", JOptionPane.ERROR_MESSAGE);
				throw new RuntimeException("Error connecting to DBMS", e);
			}
	}
	
	public void clearGraph() {
		// Cannot just use remove(Object[]), since this will not get rid of ports.
		getGraphLayoutCache().remove(_cells.toArray(), true, true);
		_root = null;
		_cells.clear();
		_oldTupleNodes = _tupleNodes;
		_tupleNodes = new HashMap<TupleContext,DefaultGraphCell>();
		
		//_tuplesVisited.clear();
	}
	
	public void setRoot(Tuple t) {
		//setVisible(false);
		clearGraph();
		
		_root = t;
		
		String nam = t.toString();
		if (nam.length() > 20)
			nam = nam.substring(0, 20) + "...)";
		//else
		//	nam = nam.substring(0, 20);
		
		if (nam.charAt(0) == ' ')
			nam = nam.substring(1);
		TupleVertex root = new TupleVertex(nam, t);
		root.setRoot();

		TupleContext cont = new TupleContext(_root, _root.getOrigin()); 
		//_tuplesVisited.add(cont);
		_cells.add(root);
		_tupleNodes.put(cont, root);
		_rootNode = root;
		
		try {
			setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			exploreNeighbors(cont, true, 1);
		} catch (Exception e) {
			e.printStackTrace();
			JOptionPane.showMessageDialog(this, e.getMessage(), "Error exploring provenance", JOptionPane.ERROR_MESSAGE);
		} finally {
			setCursor(Cursor.getDefaultCursor());
		}
		
		// We can now add all the cells to the graph model
		getGraphLayoutCache().insert(_cells.toArray());
		
				
		applyLayout();
		//setVisible(true);
		
	}

	/**
	 * Explore the frontier of neighbors, breadth-first-style
	 * 
	 * @param t
	 * @param mapping
	 * @return
	 * @throws Exception
	 */
	private void exploreNeighbors(TupleContext t, boolean mapping, int dist) throws Exception {
			if (dist > DEPTH)
				return;
			
			
			List<TupleContext> nextList = new ArrayList<TupleContext>();
			
			List<Rule> ruleList = getMappingRelationRulesFor(t.getTuple());
			int count = 0;
			for (Rule m : ruleList) {
				count++;
				//_tupleMappings.put(_root, new HashSet<MappingVertex>());
				
	//			System.out.println(dist + " " + m.getHead().toString());
				ResultSetIterator<Tuple> neighborList = null;
				try {
					Debug.println(m.toString());
					neighborList = _system.getMappingEngine().evalQueryRule(m);
				} catch (Exception e) {
					e.printStackTrace();
					JOptionPane.showMessageDialog(this, e.getMessage(), "Error exploring provenance", JOptionPane.ERROR_MESSAGE);
				}
				
				try {
				while (neighborList != null && neighborList.hasNext()) {
					Tuple tn = neighborList.next();
					tn.setOrigin(new RelationContext(m.getBody().get(0).getPeer(), m.getBody().get(0).getSchema(),
							m.getBody().get(0).getRelation()));
					
					TupleContext next = new TupleContext(tn);
					boolean visited = _tupleNodes.keySet().contains(next); 
					if (mapping) {
						MappingVertex map;
						
						if (!visited) {
							map = new MappingVertex(m.getHead().getRelation().getDescription(), tn.toString());
							_cells.add(map);
							
							//_tupleMappings.get(t).add(map);
							_tupleNodes.put(next, map);
						} else
							map = (MappingVertex)_tupleNodes.get(next);
						
						DefaultEdge e = new DefaultEdge(/*String.valueOf(dist)*/);
						setMappingTupleEdge(e, _tupleNodes.get(t), map);
						_cells.add(e);
					} else if (!next.getContext().getRelation().isInternalRelation()) {//.getName().endsWith("_L")) {
						TupleVertex tup;
						if (!visited) {
							String nam = tn.toString();
							if (nam.length() > 20)
								nam = nam.substring(0, 20) + "...)";
//							else
//								nam = nam.substring(0, 20);
							
							if (nam.charAt(0) == ' ')
								nam = nam.substring(1);
							
							tup = new TupleVertex(nam, tn);
							_cells.add(tup);
							
							if (next.getContext().getRelation().isInternalRelation())//.getName().endsWith("_L"))
								tup.setEdb();
						
							_tupleNodes.put(next, tup);
						} else {
							tup = (TupleVertex)_tupleNodes.get(next);
						}
						
						DefaultEdge e = new DefaultEdge(/*String.valueOf(dist)*/);
						setTupleMappingEdge(e, _tupleNodes.get(t), tup);
						_cells.add(e);						
					}
						
					if (!visited && !next.getContext().getRelation().isInternalRelation()) {//.getName().endsWith("_L")) {
						// Now recursively query the sources of the mapping
						nextList.add(next);
					}
				}
				} catch (StringIndexOutOfBoundsException e) {
					e.printStackTrace();
				}//} catch (IllegalStateException se) {
					// Should trap end-of-relation exception
					//System.err.println(se);
					//se.printStackTrace();
				//}
			}
			
			++dist;
	
			if (dist <= DEPTH) {
				for (TupleContext next : nextList)
					exploreNeighbors(next, !mapping, dist);
			} else {
				// Non-leaf node
				//if (count != 0) {
					//GraphConstants.setBackground(_tupleNodes.get(t).getAttributes(), Color.GRAY);
	
					for (TupleContext next : nextList) {
						if (!next.getContext().getRelation().getName().endsWith("_L")) {
							MappingVertex map = new MappingVertex("...", "...");
							map.setBorderless();
							_cells.add(map);
							DefaultEdge e = new DefaultEdge(/*String.valueOf(dist)*/);
							setMappingTupleEdge(e, _tupleNodes.get(next), map);
							_cells.add(e);
						}
					}
				//}
			}
	}
	
	/**
	 * Given a tuple and its schema, find the "neighboring" relations and
	 * create a set of rules to query for them.  Should bind the constants
	 * in the body to the values of t.
	 * 
	 * @param t
	 * @return
	 */
	private List<Rule> getMappingRelationRulesFor(Tuple t) {
		Map<Atom,Rule> atomsFor = _system.getMappingEngine().getMappingAtomsFor(t.getOrigin());
		
		List<Rule> rules = new ArrayList<Rule>();
		
		// Iterate through each "neighbor" atom and create an inverse rule
		int id = 0;
		for (Atom sourceAtom : atomsFor.keySet()) {
			
			if (!sourceAtom.isSkolem())
			{
				// Head should mirror the source atom, in terms of the schema
				List<AtomArgument> values = new ArrayList<AtomArgument>();
				
				String dispName = sourceAtom.getRelation().getDescription();
				
				Map<Atom,Rule> mappingDerivedFrom = _system.getMappingEngine().getMappingAtomsFor(sourceAtom.getRelationContext());
				if (mappingDerivedFrom.size() == 1 && mappingDerivedFrom.keySet().iterator().next().getRelation().getName().endsWith("_L"))
					dispName = "+";
				
				Relation outputRelation = new Relation(null, null, " " + sourceAtom.getRelation().getName(), " " + sourceAtom.getRelation().getName(), 
						dispName, false, false, sourceAtom.getRelation().getFields());
				outputRelation.setPrimaryKey(sourceAtom.getRelation().getPrimaryKey());
				outputRelation.setSchema(sourceAtom.getSchema(), Relation.NO_ID);
				outputRelation.markFinished();
	
				// Body should mirror the tuple, in terms of schema
				List<Atom> body = new ArrayList<Atom>();
				
				Atom originalHead = atomsFor.get(sourceAtom).getHead();
				
				// Iterate through the mapping relation, finding the vars in the head (original relation)
				// that are mapped into it.
				for (int i = 0; i < sourceAtom.getValues().size(); i++) {
					
					// See if the variable/value shows up in the original relation
					if(sourceAtom.getValues().get(i) instanceof AtomConst){
						AtomConst c = (AtomConst)sourceAtom.getValues().get(i);
						values.add(new AtomConst(c.getValue()));
					}else{
						int pos = originalHead.getValues().indexOf(sourceAtom.getValues().get(i));

						if (pos != -1) {
							Object obj = t.get(pos);

							if (obj != null){
								values.add(new AtomConst(t.get(pos)));
							}else{  // This doesn't work as it should for tuples with labeled nulls!!!
//								values.add(new AtomConst((Object)null));
								values.add(new AtomConst(t.getValueOrLabeledNull(pos).toString()));
//								values.add(new AtomConst("NULL(" + + ")");
							}
						} else {
							values.add(new AtomVariable("x" + i));//sourceAtom.getValues().get(i));
						}
					}
				}
				Atom bodyAtom = new Atom(sourceAtom.getRelationContext(), values);
				body.add(bodyAtom);
				
				Atom head = new Atom(t.getOriginatingPeer(), sourceAtom.getSchema(), 
						outputRelation, values);
				Rule inverseRule = new Rule(head, body, null, _system.getMappingDb().getBuiltInSchemas());
				
				//System.out.println(" " + sourceAtom.toString() + " " + atomsFor.get(sourceAtom).toString());
	//			System.out.println(inverseRule);
				rules.add(inverseRule);
				id++;
			}
		}

		return rules;
	}
	
	/**
	 * Makes the edge between a transaction and antecedent a dependency edge
	 * 
	 * @param edge
	 * @param ante
	 * @param trans
	 */
	private static void setMappingTupleEdge(DefaultEdge edge, DefaultGraphCell ante, DefaultGraphCell trans) {
		// Connect to default ports
		edge.setSource(ante.getChildAt(0));
		edge.setTarget(trans.getChildAt(0));
		
		// Add arrow
		GraphConstants.setLineColor(edge.getAttributes(), Color.BLACK);
		GraphConstants.setLineBegin(edge.getAttributes(), GraphConstants.ARROW_CLASSIC);
		GraphConstants.setLineStyle(edge.getAttributes(), GraphConstants.STYLE_SPLINE);
		GraphConstants.setLabelAlongEdge(edge.getAttributes(), true);
		
	}

	/**
	 * Makes the edge between a transaction and antecedent a conflict edge
	 * 
	 * @param edge
	 * @param ante
	 * @param trans
	 */
	private static void setTupleMappingEdge(DefaultEdge edge, DefaultGraphCell f, DefaultGraphCell s) {
		// Connect to default ports
		edge.setSource(f.getChildAt(0));
		edge.setTarget(s.getChildAt(0));
		
		GraphConstants.setLineColor(edge.getAttributes(), Color.BLACK);
		GraphConstants.setLineStyle(edge.getAttributes(), GraphConstants.STYLE_SPLINE);
		GraphConstants.setLabelAlongEdge(edge.getAttributes(), true);
	}

	/**
	 * Adds legend info to the JGraph
	 * 
	 * @param g
	 * @param width
	 * @param height
	 */
	public static LegendGraph createLegend(int width, int height, Color col) {
		LegendGraph g = new LegendGraph();
		g.setBackground(col);
		g.setSize(width, height);
		g.setPreferredSize(new Dimension(width, height));
		ArrayList<DefaultGraphCell> c = new ArrayList<DefaultGraphCell>();
		int i = 0;
		c.add(new TupleVertex("Tuple", null));
		c.get(i++).add(new DefaultPort());
		c.add(new MappingVertex("Mapping", "Mapping"));
		c.get(i++).add(new DefaultPort());
		c.add(new TupleVertex("Tuple", null));
		((TupleVertex)c.get(i)).setRoot();
		c.get(i++).add(new DefaultPort());

		c.add(new DefaultEdge("derives"));
		DefaultEdge edge = (DefaultEdge)c.get(i);
		//setMappingTupleEdge(edge, (DefaultGraphCell)c.get(1), (DefaultGraphCell)c.get(2));
		// Connect to default ports
		edge.setSource((DefaultGraphCell)c.get(1).getChildAt(0));
		edge.setTarget((DefaultGraphCell)c.get(2).getChildAt(0));
		
		// Add arrow
		GraphConstants.setLineColor(edge.getAttributes(), Color.BLACK);
		GraphConstants.setLineEnd(edge.getAttributes(), GraphConstants.ARROW_CLASSIC);
		GraphConstants.setLineStyle(edge.getAttributes(), GraphConstants.STYLE_SPLINE);
		GraphConstants.setLabelAlongEdge(edge.getAttributes(), true);
		c.add(new DefaultEdge("used-by"));
		setTupleMappingEdge((DefaultEdge)c.get(i+1), (DefaultGraphCell)c.get(0), (DefaultGraphCell)c.get(1));
		
		Object[] c2 = new Object[1];
		c2[0] = c.get(0);
		
		g.getGraphLayoutCache().insert(c.toArray());
		g.setRoots(c2);
		
		//g.applyLayout();

		// Let's fix the cells positions by hand to get a better result
		/*double deltaX = 30D;
		
		double maxHeight = 0D;
		for (int j = 0 ; j < 3 ; j++)
			maxHeight = Math.max (maxHeight, g.getGraphLayoutCache().getMapping(c.get(j), false).getBounds().getHeight());
		
		for (int j = 0 ; j < 3 ; j++)
			recenterVertically (g, c.get(j), deltaX, maxHeight, j);
		*/
		return g;
	}
	
	private static void recenterVertically (LegendGraph g, DefaultGraphCell cell, 
											double deltaX, double maxHeight,
											int indCell)
	{
		Rectangle2D bounds = getRectangle (g, cell, deltaX*indCell, maxHeight);
		GuiGraphConstants.setBounds(cell.getAttributes(), bounds);
		g.getGraphLayoutCache().editCell(cell, cell.getAttributes());		
	}
	
	private static Rectangle2D getRectangle (LegendGraph g, DefaultGraphCell cell,
										double deltaX, double maxHeight)
	{
		CellView view = g.getGraphLayoutCache().getMapping(cell, false);
		Rectangle2D res = new Rectangle2D.Double (
										view.getBounds().getX() + deltaX, 
										(maxHeight - view.getBounds().getHeight())/2,
										view.getBounds().getWidth(),
										view.getBounds().getHeight()
									);
		return res;
	}
	
	
/* No longer have access to JGraphFacade. May have to come up with a new implementation this.
	public void recenter(JGraphFacade facade) {
		Rectangle2D bounds = toScreen(facade.getCellBounds());
		
		int offsetX, offsetY;
		
		// ZI 7/18:  trap for no-bounds case, and at least make
		// sure the cells aren't pushed past the origin
		if (bounds == null) {
			offsetX = 10;
			offsetY = 10;
			facade.tilt(_cells, 100, 100);
		} else {
			offsetX = (getWidth() - (int)bounds.getWidth()) / 2;
			offsetY = (getHeight() - (int)bounds.getHeight()) / 2;
		}
		
		if (offsetX < 10)
			offsetX = 10;
		
		if (offsetY < 10)
			offsetY = 10;

		Map nested = facade.createNestedMap(true, new Point(offsetX,offsetY)); 
		getGraphLayoutCache().edit(nested);
	}
	*/
	
	/**
	 * Applies a horizontal tree layout to this graph.
	 * 
	 * @see edu.upenn.cis.orchestra.gui.graphs.BasicGraph#applyLayout()
	 */
	@Override
	protected void applyLayout ()
	{
		LayoutHelperBuilder builder = new LayoutHelperBuilder(this, LayoutAlgorithmType.HORIZONTAL_TREE);
		builder.centerAfterLayout(true);
		ILayoutHelper helper = builder.build();
		helper.applyLayout();
	}
}
