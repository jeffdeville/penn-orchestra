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
package edu.upenn.cis.orchestra.gui.schemas;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableCellRenderer;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.RelationContext;


/**
 * A RelationDataPanel is a JPanel used to edit a relation's contents.
 * @author olivier
 *
 */
public class RelationDataPanel extends JPanel 
{
	public static final long serialVersionUID = 1L;

	// Used to communicate with the data store layer
	private final RelationDataEditorIntf _relDataEdit;
	// Relation to edit
	private final RelationContext _relCtx;
	// JTable model used for the relation
	private RelationDataModel _model;
	private JTable _dataTable;
	private RelationDataInsertionModel _insertModel;
	
	
	/**
	 * Create a new Relation editor panel
	 * @param relDataEdit Used to communicate with the data store layer
	 * @param relCtx Relation to edit
	 */
	public RelationDataPanel (final RelationDataEditorIntf relDataEdit, 
										final RelationContext relCtx)
	{
		setLayout(new BorderLayout ());
	
		// Copy local attributes
		_relDataEdit = relDataEdit;
		_relCtx = relCtx;
		
		// Create a new JTable model, uses a cache to avoid loading 
		// the whole relation data
		_model = new RelationDataModel (relDataEdit, relCtx);
		
		// Create the main panel which contains the table and the table 
		// options buttons
		JPanel mainTablePanel = createMainTablePanel();		
		add (mainTablePanel, BorderLayout.CENTER);
		
		// Create a panel which contains a one-row table for insertions.
		//final JPanel insertPanel = createInsertPanel(_model);
		//add (insertPanel, BorderLayout.SOUTH);
	}

	/**
	 * Create a panel which contains the data table and the related options buttons 
	 * (such as commit transaction, reload, delete...)
	 * @return Panel
	 */
	private JPanel createMainTablePanel ()
	{
		JPanel mainTablePanel = new JPanel (new GridBagLayout ());
				
		int currRow = 0;
		GridBagConstraints cst;
		
		// Create the JTable
		_dataTable = new JTable(_model)
		
		{
			public static final long serialVersionUID = 1L;
			@Override
			public TableCellRenderer getCellRenderer(int row, int col) {
				return RelationDataEditorCellRenderer.getSharedInstance();
			}
			

		};
		final JTableHeader header = _dataTable.getTableHeader();
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

		if (Config.getGuiCacheRelEditData())			
			// Do not use rows sorters since we don't load all data. 
			_dataTable.setAutoCreateRowSorter(false);
		else
			_dataTable.setAutoCreateRowSorter(true);

		_dataTable.putClientProperty("terminateEditOnFocusLost", Boolean.TRUE);
	

		// Add the JTable to the panel
		cst = new GridBagConstraints();
		cst.gridx = 0;
		cst.gridy = currRow++;
		cst.weightx = 1;
		cst.weighty = 1;
		cst.fill = GridBagConstraints.BOTH;
		cst.gridwidth = 4;
		cst.insets = new Insets (2, 1, 6, 1);
		mainTablePanel.add (new JScrollPane(_dataTable), cst);
		
		// The button delete is used to delete the current selection
//		JButton btnDelete = new JButton ("Delete");
//		btnDelete.setToolTipText("Delete the current selection");
//		btnDelete.addActionListener(new ActionListener ()
//					{
//						public void actionPerformed(ActionEvent arg0) {
//							int[] selectedRows = _dataTable.getSelectedRows();
//							for (int i = selectedRows.length-1 ; i >=0 ; i--)
//								_model.delete(selectedRows[i]);
//						}
//					});
//		cst = new GridBagConstraints();
//		cst.gridx = 0;
//		cst.gridy = currRow;
//		cst.anchor = GridBagConstraints.WEST;
//		cst.weightx = 1;
//		mainTablePanel.add (btnDelete, cst);

		// This button (not enabled) should show provenance
//		JButton btnProv = new JButton ("Provenance");
//		btnDelete.setToolTipText("Show provenance for the current selection");
//
//		cst = new GridBagConstraints();
//		cst.gridx = 1;
//		cst.gridy = currRow;
//		cst.anchor = GridBagConstraints.WEST;
//		cst.weightx = 1;
//		mainTablePanel.add(btnProv, cst);
//		btnProv.setEnabled(false);
		
		/*
		// The button Commit is used to validate the changes made to the data
		// and send all mappings as a  transaction to the data layer
		JButton btnValid = new JButton ("Commit");
		btnValid.setToolTipText("Commit the changes");
		btnValid.addActionListener(new ActionListener ()
					{
						public void actionPerformed(ActionEvent arg0) {
							_model.validateTransaction();
						}
					});
		cst = new GridBagConstraints();
		cst.gridx = 2;
		cst.gridy = currRow;
		cst.anchor = GridBagConstraints.EAST;
		mainTablePanel.add (btnValid, cst);

		// The button Commit is used to validate the changes made to the data
		// and send all mappings as a  transaction to the data layer
		JButton btnRoll = new JButton ("Rollback");
		btnRoll.setToolTipText("Rollback the changes");
		btnRoll.addActionListener(new ActionListener ()
					{
						public void actionPerformed(ActionEvent arg0) {
							_model.rollbackTransaction();
						}
					});
		cst = new GridBagConstraints();
		cst.gridx = 3;
		cst.gridy = currRow;
		cst.anchor = GridBagConstraints.EAST;
		mainTablePanel.add (btnRoll, cst);
		 */
		
		
		// The button reload is used to reload the data from the data source
		// instead of using the current cache and cursor
//		JButton btnReload = new JButton ("Reload");
//		btnReload.setToolTipText("Reload data from the data source, uses local cache otherwise");
//		btnReload.addActionListener(new ActionListener ()
//				{
//					public void actionPerformed(ActionEvent evt) {
//						_model = new RelationDataModel (_relDataEdit, _relCtx);
//						_dataTable.setModel(_model);
//						_insertModel.setRelationDataModel (_model);
//					}
//				});
//		cst = new GridBagConstraints();
//		cst.gridx = 3;
//		cst.gridy = currRow++;
//		cst.anchor = GridBagConstraints.EAST;
//		mainTablePanel.add(btnReload, cst);
		
		

		return mainTablePanel;
	}
	

	/**
	 * Create a panel that contains a one-row table for insertions, and 
	 * buttons to validate/cancel the current insertion.
	 * @param relDataModel Model used to show the relation content, will be 
	 * responsible for the actual insert.
	 * @return Panel
	 */
	private JPanel createInsertPanel (final RelationDataModel relDataModel)
	{
		JPanel insertPanel = new JPanel (new GridBagLayout());
		
		GridBagConstraints cst;
		int currRow = 0;
		
		// Add a separator for better screen separation 
		JSeparator sep = new JSeparator ();
		cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = currRow++;
		cst.fill = GridBagConstraints.HORIZONTAL;
		cst.weightx = 1;
		cst.gridwidth = 2;
		cst.insets = new Insets (7, 0, 4, 0);
		insertPanel.add (sep, cst);
		
		// Title for the zone
		JLabel labInsert = new JLabel ("Insert");
		labInsert.setFont(labInsert.getFont().deriveFont(Font.BOLD));
		cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = currRow++;
		cst.anchor = GridBagConstraints.WEST;
		cst.gridwidth = 2;
		insertPanel.add(labInsert, cst);
		
		// Create a relation model for insertions, basically a one row 
		// model that maintains a SchemaTuple for the current values entered by 
		// the user
		_insertModel = new RelationDataInsertionModel (_relDataEdit, _relCtx, relDataModel);
		
		// Create the table view for insertions
		final JTable insertTable = new JTable (_insertModel);
		// We don't want selections to appear in this table, let's hide using the 
		// default background and foreground colors.
		insertTable.getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		insertTable.setSelectionBackground(insertTable.getBackground());
		insertTable.setSelectionForeground(Color.BLACK);
		insertTable.putClientProperty("terminateEditOnFocusLost", Boolean.TRUE);

		cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = currRow++;
		cst.fill = GridBagConstraints.HORIZONTAL;
		cst.weightx = 1;
		cst.gridwidth = 2;
		insertPanel.add(insertTable, cst);
		
		
		// Add a button to validate the current insert
		final JButton okBtn = new JButton ("OK");
		okBtn.setToolTipText("Run the insert");
		okBtn.addActionListener(new ActionListener ()
					{
						public void actionPerformed(ActionEvent arg0) {
							try
							{
								_insertModel.validate();
							} catch (RelationDataConstraintViolationException ex)
							{
								JOptionPane.showMessageDialog(getParent(), "Primary key violated", "Warning", JOptionPane.WARNING_MESSAGE);
							} catch (RelationDataEditorException ex)
							{
								JOptionPane.showMessageDialog(getParent(), "Error while talking to the engine", "Warning", JOptionPane.WARNING_MESSAGE);
							}
							
						}
					});
		cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = currRow;		
		insertPanel.add(okBtn, cst);
		
		// Add a button to cancel (reset values) the current insert
		final JButton cancelBtn = new JButton ("Cancel");
		cancelBtn.setToolTipText("Cancel the insertion and reset values");
		cancelBtn.addActionListener(new ActionListener ()
						{
							public void actionPerformed(ActionEvent arg0) {
								_insertModel.reset();
							}
						});
		cst = new GridBagConstraints ();
		cst.gridx = 1;
		cst.gridy = currRow++;
		insertPanel.add(cancelBtn, cst);
		
		return insertPanel;
	}
	
	protected RelationDataModel getRelationDataModel ()
	{
		return _model;
	}
}
