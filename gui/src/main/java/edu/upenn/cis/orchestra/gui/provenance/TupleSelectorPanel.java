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

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSlider;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableCellRenderer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.BadColumnName;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.gui.schemas.RelationDataEditorCellRenderer;
import edu.upenn.cis.orchestra.gui.schemas.RelationDataEditorFactory;
import edu.upenn.cis.orchestra.gui.schemas.RelationDataEditorIntf;
import edu.upenn.cis.orchestra.gui.schemas.RelationDataModel;

/**
 * Panel for choosing a peer, schema, relation, and tuple for
 * display in the provenance graph viewer
 *  
 * @author zives
 *
 */
public class TupleSelectorPanel extends JPanel {
	public static final long serialVersionUID = 1L;
	private final JLabel _labPeers = new JLabel ("Peer");
	private final JLabel _labSchemas = new JLabel ("Schema");
	private final JLabel _labRelations = new JLabel ("Relation");

	private final DefaultComboBoxModel _peers = new DefaultComboBoxModel();
	private final JComboBox _peerList = new JComboBox(_peers);
	private final DefaultComboBoxModel _schemas = new DefaultComboBoxModel();
	private final JComboBox _schemaList = new JComboBox(_schemas);
	private final DefaultComboBoxModel _relations = new DefaultComboBoxModel();
	private final JComboBox _relationList = new JComboBox(_relations);

	private OrchestraSystem _system;
	
	private Peer _currentPeer;
	private Schema _currentSchema;
	private edu.upenn.cis.orchestra.datamodel.Schema _workSchema;
	private Relation _currentRelation;
	private ProvenanceGraph _provGraph;
	
	private JTable _relationTable = null;
	private JScrollPane _relationScroller = null;
	private JPanel _relationPanel = null;
	private RelationDataModel _model = null;
	private RelationDataEditorIntf relDataEdit;
	private ProvenanceViewer _parentPane;
	
	private final RelationDataEditorFactory _dataEditFactory;
	
	private static final Logger logger = LoggerFactory.getLogger(TupleSelectorPanel.class);
	
	public TupleSelectorPanel(OrchestraSystem system, ProvenanceGraph g, ProvenanceViewer v, 
								RelationDataEditorFactory fact) {
		_workSchema = new edu.upenn.cis.orchestra.datamodel.Schema("");
		_parentPane = v;
		_system = system;
		_provGraph = g;
		_dataEditFactory = fact;
		
		long start = System.currentTimeMillis(); 
		init();
		long elapsed = System.currentTimeMillis() - start;
		logger.trace("init(): {} milliseconds", Long.valueOf(elapsed));

		start = System.currentTimeMillis();
		try {
			for (Schema s : system.getAllSchemas())
				for (AbstractRelation r : s.getRelations())
					_workSchema.getOrCreateRelationSchema(r);
			_workSchema.markFinished();
		} catch (BadColumnName bcn) {
			JOptionPane.showMessageDialog(this, bcn.getMessage(), "Error Initializing Schemas", JOptionPane.ERROR_MESSAGE);
		}
		elapsed = System.currentTimeMillis() - start;
		logger.trace("Creating _workSchema: {} milliseconds", Long.valueOf(elapsed));
		
		start = System.currentTimeMillis();
		populatePeers(system);
		elapsed = System.currentTimeMillis() - start;
		logger.trace("populatePeers(): {} milliseconds", Long.valueOf(elapsed));
	}

	public TupleSelectorPanel(OrchestraSystem system, ProvenanceGraph g, Peer p, Schema s, ProvenanceViewer v, RelationDataEditorFactory fact) {
		this(system, g, v, fact);
		
		long start = System.currentTimeMillis();
		setPeerAndSchema(p, s);
		long elapsed = System.currentTimeMillis() - start;
		logger.trace("setPeerAndSchema(): {} milliseconds", Long.valueOf(elapsed));
	}
	
	public void disableBoxes() {
		_peerList.setEnabled(false);
		_schemaList.setEnabled(false);
		_relationList.setEnabled(false);
		_relationTable.setEnabled(false);
	}
	
	public void enableBoxes() {
		_peerList.setEnabled(true);
		_schemaList.setEnabled(true);
		_relationList.setEnabled(true);
		_relationTable.setEnabled(true);
	}
	
	/**
	 * Set the focus on the current peer and schema
	 * 
	 * @param p
	 * @param s
	 */
	public void setPeerAndSchema(Peer p, Schema s) {
		
		if (p == _currentPeer && s == _currentSchema)
			return;
		
		_currentPeer = p;
		_currentSchema = s;
		
		// Match by name
		for (int i = 0; i < _peerList.getItemCount(); i++)
			if (((String)_peerList.getItemAt(i)).equals(p.getId()))
				_peerList.setSelectedIndex(i);
		
		for (int i = 0; i < _schemaList.getItemCount(); i++)
			if (((String)_schemaList.getItemAt(i)).equals(s.getSchemaId()))
					_schemaList.setSelectedIndex(i);
		
	}
	
	/**
	 * Set the focus to the current tuple, peer, schema, etc.
	 * 
	 * @param sc
	 * @param t
	 */
	public void setContext(RelationContext sc, Tuple t) {
		setPeerAndSchema(sc.getPeer(), sc.getSchema());

		_currentRelation = sc.getRelation();
		
		// Match by name
		for (int i = 0; i < _relationList.getItemCount(); i++)
			if (((String)_relationList.getItemAt(i)).equals(sc.getRelation().getName()))
				_relationList.setSelectedIndex(i);
		
		/*
		for (int i = 0; i < _tuples.getSize(); i++)
			if (_tuples.get(i).equals(t))
				_tupleList.setSelectedIndex(i);
				*/
		
		int i = _model.getSelectedIndex(t);
		
		if (i != -1) {
			_relationTable.setRowSelectionInterval(i, i);
		}
	}
	
	/**
	 * Populates the list of peers in the system
	 * 
	 * @param sys
	 */
	private void populatePeers(OrchestraSystem sys) {
			long start = System.currentTimeMillis();
			_peers.removeAllElements();
			_schemas.removeAllElements();
			_relations.removeAllElements();
			for (Peer p : sys.getPeers())
				_peers.addElement(p.getId());
			long elapsed = System.currentTimeMillis() - start;
			logger.trace("Added {} peers in {} ms.", _peers.getSize(), elapsed);
	}
	
	/**
	 * Populates the list of schemas for the current peer
	 * 
	 * @param p
	 */
	private void populateSchemas(Peer p) {
			long start = System.currentTimeMillis();
			_schemas.removeAllElements();
			_relations.removeAllElements();
	
			if (p != null)
				for (Schema s : p.getSchemas())
					_schemas.addElement(s.getSchemaId());
			long elapsed = System.currentTimeMillis() - start;
			logger.trace("Added {} schemas in {} ms.", _schemas.getSize(), elapsed);
	}

	/**
	 * Populates the list of relations in the current schema
	 * 
	 * @param s
	 */
	private void populateRelations(Schema s) {
			long start = System.currentTimeMillis();
			_relations.removeAllElements();
			
			if (s != null)
				for (Relation r : s.getRelations())
					//if (!r.getName().endsWith("_L") && !r.getName().endsWith("_R"))
					if (!r.isInternalRelation())
						_relations.addElement(r.getName());
			long elapsed = System.currentTimeMillis() - start;
			logger.trace("Added {} relations in {} ms.", _relations.getSize(), elapsed);
	}
	
	private void createRelationPanel(Peer p, Schema s, Relation r, RelationDataEditorFactory fact) {
		RelationContext relCtx = new RelationContext (r, s, p, false);
		
		relDataEdit = fact.getInstance(_system);

		_model = new RelationDataModel (relDataEdit, relCtx);
		_model.setEditable(false);
		
		_relationTable = new JTable(_model)
		{
			public static final long serialVersionUID = 1L;
			@Override
			public TableCellRenderer getCellRenderer(int row, int col) {
				return RelationDataEditorCellRenderer.getSharedInstance();
			}
		};
		final JTableHeader header = _relationTable.getTableHeader();
		header.setFont(header.getFont().deriveFont(Font.BOLD));
		final Font boldFont = header.getFont().deriveFont(Font.ITALIC+Font.BOLD);
		final TableCellRenderer headerRenderer = header.getDefaultRenderer();
		header.setDefaultRenderer( new TableCellRenderer() {
			public Component getTableCellRendererComponent( JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column ) {
					Component comp = headerRenderer.getTableCellRendererComponent( table, value, isSelected, hasFocus, row, column );
					if ( _model.isPrimaryKey(column) )
						comp.setFont( boldFont );
					return comp;
				}
		});		
		
		_relationTable.setFillsViewportHeight(true);
		_relationTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		_relationTable.setRowSelectionAllowed(true);
		_relationTable.setColumnSelectionAllowed(false);
		if (!Config.getGuiCacheRelEditData())
			_relationTable.setAutoCreateRowSorter(true);
		_relationTable.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
			public void valueChanged(ListSelectionEvent e) {
				    if (e.getValueIsAdjusting() == false) {
				        if (_relationTable.getSelectedRow() == -1) {
				            _provGraph.clearGraph();
		
				        } else {
				        		int row = _relationTable.getRowSorter().convertRowIndexToModel(_relationTable.getSelectedRow());
					        	Tuple cur = ((RelationDataModel)_relationTable.getModel()).getTupleAt(row);
								cur.setOrigin(new RelationContext(_currentRelation, _currentSchema, _currentPeer, false));
					            //_provGraph.setRoot(cur);
								_parentPane.loadProvenance(cur);
				        }
				    }
			}
		});

		_relationScroller = new JScrollPane(_relationTable, JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED,
				JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		_relationPanel.add(header, BorderLayout.PAGE_START);
		_relationPanel.add(_relationScroller, BorderLayout.CENTER);
	}
	
	/**
	 * Queries the selected peer/schema/relation and displays its
	 * elements in a list box
	 * 
	 * @param p selected peer
	 * @param s selected schema
	 * @param r selected relation
	 */
	private void populateTuples(Peer p, Schema s, Relation r, RelationDataEditorFactory dataEditFactory) {
			// Update the model for the table, so it shows the right relation
			if (_relationTable == null) {
				createRelationPanel(p, s, r, dataEditFactory);
			} else {
				RelationContext relCtx = new RelationContext (r, s, p, false);
				_model = new RelationDataModel (relDataEdit, relCtx);
				_model.setEditable(false);
	
				_relationTable.setModel(_model);
			}
	}
	
	/**
	 * Helper function to add a row and increment a counter
	 * 
	 * @param lastRow
	 * @param comp
	 * @return updated count
	 */
	private int addRow(int lastRow, JComponent comp) {
		GridBagConstraints cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = lastRow;
		cst.weightx = 0.1;
		cst.ipady = 5;
		cst.fill=GridBagConstraints.HORIZONTAL;
		cst.anchor = GridBagConstraints.PAGE_START;

		add(comp, cst);
		return lastRow + 1;
	}

	private void init() {
		//setBorder (BorderFactory.createTitledBorder("Tuple selector"));
		setLayout(new GridBagLayout());

		int r = 0;
		FontMetrics metrics = _peerList.getFontMetrics(_peerList.getFont());

		GridBagConstraints cst = new GridBagConstraints ();
		cst.gridx=0;
		cst.gridy=r++;
		cst.insets = new Insets (0, 0, 6, 0);
		cst.fill = GridBagConstraints.HORIZONTAL;
		final JSlider slider = new JSlider (50, 150, 100);
		slider.addChangeListener(new ChangeListener ()
				{
					public void stateChanged(ChangeEvent evt) {
						_provGraph.setScale((double)slider.getValue()/100D);
					}
				});
		add (slider, cst);
		
		r = addRow(r, _labPeers);
		_peerList.setPreferredSize(new Dimension(180, (metrics.getHeight() + metrics.getDescent() + metrics.getLeading()) * 1));

		// Listen for selected peer
		_peerList.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
					logger.trace("Peer list action: {}.", e.getActionCommand());
					long start = System.currentTimeMillis();

					JComboBox cb = (JComboBox)e.getSource();
					
					String sel = (String)cb.getSelectedItem();
					Peer p = _system.getPeer(sel);
					_currentPeer = p;
					
					populateSchemas(p);
					long elapsed = System.currentTimeMillis() - start;
					logger.trace("Handled peer list action in {} ms.", elapsed);
			}
		});
		r = addRow(r, _peerList);
				
		r = addRow(r, _labSchemas);
		_schemaList.setPreferredSize(new Dimension(180, (metrics.getHeight() + metrics.getDescent() + metrics.getLeading()) * 1));
		// Listen for selected schema
		_schemaList.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				logger.trace("Schema list action: {}.", e.getActionCommand());
				long start = System.currentTimeMillis();
				JComboBox cb = (JComboBox)e.getSource();
				
				String sel = (String)cb.getSelectedItem();
				if (_currentPeer != null && sel != null) {
					Schema s = _currentPeer.getSchema(sel);
					
					_currentSchema = s;
				
					populateRelations(s);
					long elapsed = System.currentTimeMillis() - start;
					logger.trace("Handled schema list action in {} ms.", elapsed);
				}
			}
		});
		
		r = addRow(r, _schemaList);
		r = addRow(r, _labRelations);
		_relationList.setPreferredSize(new Dimension(180, (metrics.getHeight() + metrics.getDescent() + metrics.getLeading()) * 1));
		// Listen for selected relation
		_relationList.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				logger.trace("Relation list action: {}.", e.getActionCommand());
				long start = System.currentTimeMillis();
					JComboBox cb = (JComboBox)e.getSource();
					
					String sel = (String)cb.getSelectedItem();
					if (_currentPeer != null && _currentSchema != null && sel != null) {
						try {
							Relation r = _currentSchema.getRelation(sel);
							_currentRelation = r;
						
							populateTuples(_currentPeer, _currentSchema, r, _dataEditFactory);
						} catch (RelationNotFoundException rnf) {
							JOptionPane.showMessageDialog((JComboBox)e.getSource(), rnf.getMessage(), "Relation not found", JOptionPane.ERROR_MESSAGE);
							rnf.printStackTrace();
						}
					}
					long elapsed = System.currentTimeMillis() - start;
					logger.trace("Handled relation list action in {} ms.", elapsed);
			}
		});
		//_peerList.setBorder(BorderFactory.createEmptyBorder(10,0,2,0));
		
		r = addRow(r, _relationList);
		//r = addRow(r, _labTuples);

		_relationPanel = new JPanel();
		_relationPanel.setLayout(new BorderLayout());
		_relationPanel.setPreferredSize(new Dimension(180, (metrics.getHeight() + metrics.getDescent() + metrics.getLeading()) * 20));
		
		cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = ++r;
		cst.weightx = 0.1;
		cst.weighty = 0.1;
		cst.insets = new Insets (10,0,0,0);
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		cst.fill = GridBagConstraints.BOTH;
		
		add (_relationPanel, cst);
	}
	public void setVisible(boolean s) {
		super.setVisible(s);
	}
	
	protected RelationDataModel getRelationDataModel ()
	{
		return _model;
	}
}
