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

import java.awt.Container;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.BoxLayout;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import org.jgraph.graph.DefaultGraphCell;
import org.jgraph.graph.DefaultPort;
import org.jgraph.graph.VertexView;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datalog.atom.AtomArgument;
import edu.upenn.cis.orchestra.datalog.atom.AtomVariable;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.DateType;
import edu.upenn.cis.orchestra.datamodel.ForeignKey;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateMappingIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicatePeerIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateRelationIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateSchemaIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.gui.graphs.BasicGraph;
import edu.upenn.cis.orchestra.gui.graphs.GuiCellViewFactory;
import edu.upenn.cis.orchestra.gui.graphs.ILayoutHelper;
import edu.upenn.cis.orchestra.gui.graphs.LayoutHelperBuilder;
import edu.upenn.cis.orchestra.gui.graphs.LayoutHelperBuilder.LayoutAlgorithmType;

public class SchemaGraph extends BasicGraph implements ActionListener 
{
	public static final long serialVersionUID = 1L;

	private List<SchemaGraphObserver> _observers = new ArrayList<SchemaGraphObserver>();
	
	//dinesh ++
	//Pop up menu for adding/removing attributes/relations
	JPopupMenu _popupAttribute;
	JPopupMenu _popupDropRelation;
	JPopupMenu _popupAddRelation;
	OrchestraSystem _sys;
	Schema _sc;
	String _peerId;
	String _AttributeAdd = "Add Attribute";
	String _AttributeDrop = "Drop Attribute";
	String _RelationDrop = "Drop Relation";
	String _RelationAdd = "Add relation";
	String _RelationSplit = "Split Relation";
	String _RelationMerge = "Merge Relation";
	//dinesh --

	public SchemaGraph (Schema sc,String peerId, OrchestraSystem _sys)
	{
		super ();
		//dinesh++
		//Setting the Orchestra System object
		this._sys = _sys;
		this._sc = sc;
		this._peerId = peerId;
		//dinesh--
		setAntiAliased(true);
		getGraphLayoutCache().setFactory(new GuiCellViewFactory(this) {

			/**
			 * @see edu.upenn.cis.orchestra.gui.graphs.GuiCellViewFactory#createVertexView(java.lang.Object)
			 */
			@Override
			protected VertexView createVertexView(Object cell) {
				if (cell instanceof RelationGraphCell) {
					return new RelationDetailedView ((RelationGraphCell) cell);
				}
				return super.createVertexView(cell);
			}});
		
		initGraph (sc);
		
		//dinesh ++
		//Add pop up menu
		System.out.println("calling function to add pop ups");
		_popupAttribute = addPopUpMenuAttribute();
		_popupAddRelation = addPopUpMenuAddRelation();
		System.out.println("Pop up created");
	    //dinesh--
	    
		addMouseListener(new MouseListener ()
		{
			public void mouseClicked(MouseEvent arg0) {
				
				if (getSelectionCell() instanceof RelationGraphCell)
				{
					RelationGraphCell cell = (RelationGraphCell) getSelectionCell();
					for (SchemaGraphObserver obs : _observers)
						obs.relationDbleClicked(cell.getRelation());
				}
			}
			public void mouseEntered(MouseEvent arg0) {}
			public void mouseExited(MouseEvent arg0) {}
			public void mousePressed(MouseEvent arg0) {
				//dinesh ++
		            if (arg0.isPopupTrigger()) {
		            	if (getSelectionCell() instanceof RelationGraphCell)
						{
		            		System.out.println("showPopup for Attribute");
			            	_popupAttribute.show(arg0.getComponent(),
			                		arg0.getX(), arg0.getY());
						}
		            	else
		            	{
			            	System.out.println("showPopup for Add Relation");
			            	_popupAddRelation.show(arg0.getComponent(),
			                		arg0.getX(), arg0.getY());

		            	}
		            }
				//dinesh --
			}
			public void mouseReleased(MouseEvent arg0) {
				//dinesh ++
				if (arg0.isPopupTrigger()) {
	            	if (getSelectionCell() instanceof RelationGraphCell)
					{
	            		System.out.println("showPopup for Attribute");
		            	_popupAttribute.show(arg0.getComponent(),
		                		arg0.getX(), arg0.getY());

					}
	            	else
	            	{
	            		System.out.println("showPopup for Add Relation");
		            	_popupAddRelation.show(arg0.getComponent(),
		                		arg0.getX(), arg0.getY());
	            	}
	            }
	            //dinesh --

			}
			
		}
		);
		//dinesh++
		
		//dinesh--
	}

	//dinesh++
	//Method to add pop up menu to the graph
	private JPopupMenu addPopUpMenuAttribute() 
	{
		final JPopupMenu popup = new JPopupMenu();
		JMenuItem menuItem = new JMenuItem(_AttributeAdd);
	    menuItem.addActionListener(this);
		popup.add(menuItem);
		
		menuItem = new JMenuItem(_AttributeDrop);
	    menuItem.addActionListener(this);
		popup.add(menuItem);
		
		menuItem = new JMenuItem(_RelationDrop);
	    menuItem.addActionListener(this);
		popup.add(menuItem);
		
		menuItem = new JMenuItem(_RelationSplit);
	    menuItem.addActionListener(this);
		popup.add(menuItem);
		
		/*menuItem = new JMenuItem(_RelationMerge);
	    menuItem.addActionListener(this);
		popup.add(menuItem);*/
		
		return popup;
	}
	//dinesh--
	
	//dinesh++
	//Method to add pop up menu to the graph
	private JPopupMenu addPopUpMenuAddRelation() 
	{
		final JPopupMenu popup = new JPopupMenu();
		JMenuItem menuItem = new JMenuItem(_RelationAdd);
	    menuItem.addActionListener(this);
		popup.add(menuItem);
		return popup;
	}
	//dinesh--


	private void initGraph (Schema sc)
	{	
		final List<DefaultGraphCell> cells = new ArrayList<DefaultGraphCell> (sc.getRelations().size()*2);

		final Map<AbstractRelation,RelationGraphCell> mapRelCells = new HashMap<AbstractRelation, RelationGraphCell> ();

		for (Relation rel : sc.getRelations())
		{
			if (!rel.getName().endsWith("_L") && !rel.getName().endsWith("_R"))
			{
				RelationGraphCell cell = new RelationGraphCell (rel);
				DefaultPort port = new DefaultPort ();
				cell.add (port);
				port.setParent(cell);

				cells.add (cell);
				mapRelCells.put(rel, cell);
			}
		}

		for (AbstractRelation rel : sc.getRelations())
		{
			if (!rel.getName().endsWith("_L") && !rel.getName().endsWith("_R"))
				for (ForeignKey fk : rel.getForeignKeys())
				{
					ForeignKeyEdge edge = new ForeignKeyEdge (fk);
					edge.setSource(mapRelCells.get(rel).getChildAt(0));
					edge.setTarget(mapRelCells.get(fk.getRefRelation()).getChildAt(0));

					cells.add (edge);
				}
		}

		getGraphLayoutCache().insert(cells.toArray());

	}


	/**
	 * Applies a spring layout to this graph.
	 * 
	 * @see edu.upenn.cis.orchestra.gui.graphs.BasicGraph#applyLayout()
	 */
	@Override
	protected void applyLayout()
	{
		LayoutHelperBuilder builder = new LayoutHelperBuilder(this, LayoutAlgorithmType.SPRING);
		ILayoutHelper helper = builder.build();
		helper.applyLayout();
	}

	public void addObserver (SchemaGraphObserver obs)
	{
		_observers.add (obs);		
	}

	public void removeObserver (SchemaGraphObserver obs)
	{
		_observers.remove(obs);
	}

	//dinesh ++
	public void actionPerformed(ActionEvent e) 
	{
		JMenuItem source = (JMenuItem)(e.getSource());
		String eventSource = source.getText();
		if (_AttributeAdd.equals(eventSource))
		{
			System.out.println(_AttributeAdd);
			new AddAttributeBox(null).setVisible (true);
		}
		else if (_AttributeDrop.equals(eventSource))
		{
			System.out.println(_AttributeDrop);
			new DropAttributeBox().setVisible (true);
		}
		else if (_RelationDrop.equals(eventSource))
		{
			System.out.println(_RelationDrop);
			dropRelation();
		}
		else if (_RelationAdd.equals(eventSource))
		{
			System.out.println(_RelationAdd);
			new AddRelationBox().setVisible (true);
		}
		else if (_RelationSplit.equals(eventSource))
		{
			System.out.println(_RelationSplit);
			new SplitRelationBox().setVisible (true);
		}
		/*else if (_RelationMerge.equals(eventSource))
		{
			System.out.println(_RelationMerge);
			new MergeRelationBox().setVisible (true);
		}*/
	}
	//dinesh --
	

	//dinesh ++
	//Pop Window to Add Attributes
	class AddAttributeBox extends JDialog {
		static final long serialVersionUID = 42;
		JTextField txtAttributeName;
		JTextField txtAttributeDesc;
		JComboBox listAttribute;
		Relation _rel;
		Relation _relAdd;
		ArrayList<RelationField> lstFields;
		Type[] arrAttributeTypes;

		public  AddAttributeBox(Relation relAdd) {
			super();
			System.out.println("******Inside AddAttributeBox*******");
			this._relAdd = relAdd;
			
			
			final JButton addexit = new JButton("Add & Exit");
			addexit.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					
					String attributeName = txtAttributeName.getText().toUpperCase();
					String attributeDescription = txtAttributeDesc.getText().toUpperCase();
					Type attributeType = (Type)listAttribute.getSelectedItem();
					RelationField field = new RelationField(attributeName, attributeDescription, attributeType);
					lstFields.add(field);
					if (_relAdd != null)
					{
						System.out.println("Adding attribute to new Relation");
						addAttributeToRel(_relAdd,lstFields,true);
					}
					else
					{
						System.out.println("Adding attribute to existing Relation");
						addAttributeToRel(_rel,lstFields,false);
					}
					dispose ();
					
				}
			});
			
			final JButton addcontinue = new JButton("Add & Continue");
			addcontinue.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					
					String attributeName = txtAttributeName.getText().toUpperCase();
					String attributeDescription = txtAttributeDesc.getText().toUpperCase();
					Type attributeType = (Type)listAttribute.getSelectedItem();
					RelationField field = new RelationField(attributeName, attributeDescription, attributeType);
					lstFields.add(field);
					txtAttributeName.setText("");
					txtAttributeDesc.setText("");
					listAttribute.setSelectedIndex(0);
				}
			});
			
			lstFields = new ArrayList<RelationField>();
			boolean blnLatestRevision = false;
			if (_relAdd == null)
			{
				RelationGraphCell cell = (RelationGraphCell) getSelectionCell();
				_rel = cell.getRelation();
				String latestRevisionName = _rel.getNextRevisionName();
				try 
				{
					_sc.getRelation(latestRevisionName);
				} 
				catch (RelationNotFoundException e1) 
				{
					blnLatestRevision = true;
				}
				
			}
			else
			{
				blnLatestRevision = true;
			}
			
			if(blnLatestRevision)
			{
				Container content = getContentPane();
				content.setLayout(new BoxLayout(content,BoxLayout.Y_AXIS));
				
				FlowLayout panelLayout = new FlowLayout();
				panelLayout.setAlignment(FlowLayout.LEFT);
				
				FlowLayout panelButtonLayout = new FlowLayout();
				panelButtonLayout.setAlignment(FlowLayout.CENTER);
				
				JPanel panelAttributeName = new JPanel();
				JLabel labelAttributeName = new JLabel("Attribute Name");
				txtAttributeName = new JTextField(10);
				panelAttributeName.add(labelAttributeName);
				panelAttributeName.add(txtAttributeName);
				panelAttributeName.setLayout(panelLayout);
				
				JPanel panelAttributeDesc = new JPanel();
				JLabel labelAttributeDesc = new JLabel("Attribute Description");
				txtAttributeDesc = new JTextField(10);
				panelAttributeDesc.add(labelAttributeDesc);
				panelAttributeDesc.add(txtAttributeDesc);
				panelAttributeDesc.setLayout(panelLayout);

				
				JPanel panelAttributeType = new JPanel();
				JLabel labelAttributeType = new JLabel("Attribute OptimizerType");
				
				//arrAttributeTypes = getAttributeTypes();
				//TODO Replace with call to getAttributeTypes() method
//				DateType dateType = new DateType(false,true);
//				IntType intType = new IntType(false,true);
				DateType dateType = new DateType(true,true);
				IntType intType = new IntType(true,true);
				arrAttributeTypes = new Type[2];
				arrAttributeTypes[0] = dateType;
				arrAttributeTypes[1] = intType;
				
				listAttribute = new JComboBox(arrAttributeTypes);
				panelAttributeType.add(labelAttributeType);
				panelAttributeType.add(listAttribute);
				panelAttributeType.setLayout(panelLayout);
				
					
				JPanel panelButtons = new JPanel();
				panelButtons.add(addexit);
				panelButtons.add(addcontinue);
				panelButtons.setLayout(panelButtonLayout);
				
				content.add(panelAttributeName);
				content.add(panelAttributeType);
				content.add(panelAttributeDesc);
				content.add(panelButtons);
				pack();	
			}
			else
			{
				JOptionPane.showMessageDialog(null, 
						"Please work with the latest version");
			}
		}
	}
	
	//Pop Window to Add Relation
	class AddRelationBox extends JDialog {
		static final long serialVersionUID = 42;
		JTextField txtRelationName;
		JTextField txtRelationDesc;

		public  AddRelationBox() {
			super();
			Container content = getContentPane();
			final JButton add = new JButton("Add");
			add.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					
					String relationName = txtRelationName.getText().toUpperCase();
					String relationDescription = txtRelationDesc.getText().toUpperCase();
					try 
					{
						addRelationtoSchema(relationName, relationDescription);
						dispose ();
					} 
					catch (DuplicateRelationIdException e1) 
					{
						txtRelationName.setText( "");
						JOptionPane.showMessageDialog(null, "Relation Name already exists");
						e1.printStackTrace();
					}
				}
			});
			final JButton cancel = new JButton("Cancel");
			cancel.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					dispose ();
				}
			});
			
			content.setLayout(new BoxLayout(content,BoxLayout.Y_AXIS));
			 
			FlowLayout panelLayout = new FlowLayout();
			panelLayout.setAlignment(FlowLayout.LEFT);
			
			FlowLayout panelButtonLayout = new FlowLayout();
			panelButtonLayout.setAlignment(FlowLayout.CENTER);
			
			JPanel panelRelationName = new JPanel();
			JLabel labelRelationName = new JLabel("Relation Name");
			txtRelationName = new JTextField(10);
			panelRelationName.add(labelRelationName);
			panelRelationName.add(txtRelationName);
			panelRelationName.setLayout(panelLayout);
			
			JPanel panelRelationDesc = new JPanel();
			JLabel labelRelationDesc = new JLabel("Relation Description");
			txtRelationDesc = new JTextField(10);
			panelRelationDesc.add(labelRelationDesc);
			panelRelationDesc.add(txtRelationDesc);
			panelRelationDesc.setLayout(panelLayout);

			JPanel panelButtons = new JPanel();
			panelButtons.add(add);
			panelButtons.add(cancel);
			panelButtons.setLayout(panelButtonLayout);
			
			content.add(panelRelationName);
			content.add(panelRelationDesc);
			content.add(panelButtons);
			pack();
			
		}

		
	}
	
	//Pop Window to Split Relation
	class SplitRelationBox extends JDialog {
		static final long serialVersionUID = 42;
		JTextField txtRelationOneName;
		JTextField txtRelationOneDesc;
		JTextField txtRelationTwoName;
		JTextField txtRelationTwoDesc;
		Relation _rel;
		JComboBox listJoinAttribute;

		public  SplitRelationBox() {
			super();
			
			final JButton split = new JButton("Split");
			split.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					
					String relationOneName = txtRelationOneName.getText().toUpperCase();
					String relationOneDescription = txtRelationOneDesc.getText().toUpperCase();
					String relationTwoName = txtRelationTwoName.getText().toUpperCase();
					String relationTwoDescription = txtRelationTwoDesc.getText().toUpperCase();
					String joinAttribute = (String)listJoinAttribute.getSelectedItem();
					try 
					{
						createSplitRelations(_rel,
												relationOneName, 
												relationOneDescription,
												relationTwoName,
												relationTwoDescription,
												joinAttribute);
						dispose ();
					} 
					catch (DuplicateRelationIdException e1) 
					{
						String relId = e1.getRelId();
						if(relId.equals(relationOneName))
						{
							txtRelationOneName.setText("");
						}
						else
						{
							txtRelationTwoName.setText("");
						}
						JOptionPane.showMessageDialog(null, "Relation Name already exists");
						e1.printStackTrace();
					}
					
				}
			});
			final JButton cancel = new JButton("Cancel");
			cancel.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					dispose ();
				}
			});
			
			RelationGraphCell cell = (RelationGraphCell) getSelectionCell();
			_rel = cell.getRelation();
			boolean blnLatestRevision = false;
			String latestRevisionName = _rel.getNextRevisionName();
			try 
			{
				_sc.getRelation(latestRevisionName);
			} 
			catch (RelationNotFoundException e1) 
			{
				blnLatestRevision = true;
			}
			
			if (blnLatestRevision)
			{
				Container content = getContentPane();
				content.setLayout(new BoxLayout(content,BoxLayout.Y_AXIS));
				 
				FlowLayout panelLayout = new FlowLayout();
				panelLayout.setAlignment(FlowLayout.LEFT);
				
				FlowLayout panelButtonLayout = new FlowLayout();
				panelButtonLayout.setAlignment(FlowLayout.CENTER);
				
				ArrayList<RelationField> lstFields = (ArrayList<RelationField>) _rel.getFields();
				String[] arrAttributeNames = new String[lstFields.size()];
				int attrCount = 0;
				for (RelationField field : lstFields)
				{
					arrAttributeNames[attrCount] = field.getName();
					attrCount++;
				}
				
				JPanel panelJoinAttribute = new JPanel();
				JLabel labelJoinAttribute = new JLabel("Join Attribute");
				
				listJoinAttribute = new JComboBox(arrAttributeNames);
				panelJoinAttribute.add(labelJoinAttribute);
				panelJoinAttribute.add(listJoinAttribute);
				panelJoinAttribute.setLayout(panelLayout);
				
				JPanel panelRelationOneName = new JPanel();
				JLabel labelRelationOneName = new JLabel("Relation 1 Name");
				txtRelationOneName = new JTextField(10);
				panelRelationOneName.add(labelRelationOneName);
				panelRelationOneName.add(txtRelationOneName);
				panelRelationOneName.setLayout(panelLayout);
				
				JPanel panelRelationOneDesc = new JPanel();
				JLabel labelRelationOneDesc = new JLabel("Relation 1 Description");
				txtRelationOneDesc = new JTextField(10);
				panelRelationOneDesc.add(labelRelationOneDesc);
				panelRelationOneDesc.add(txtRelationOneDesc);
				panelRelationOneDesc.setLayout(panelLayout);
				
				JPanel panelRelationTwoName = new JPanel();
				JLabel labelRelationTwoName = new JLabel("Relation 2 Name");
				txtRelationTwoName = new JTextField(10);
				panelRelationTwoName.add(labelRelationTwoName);
				panelRelationTwoName.add(txtRelationTwoName);
				panelRelationTwoName.setLayout(panelLayout);
				
				JPanel panelRelationTwoDesc = new JPanel();
				JLabel labelRelationTwoDesc = new JLabel("Relation 2 Description");
				txtRelationTwoDesc = new JTextField(10);
				panelRelationTwoDesc.add(labelRelationTwoDesc);
				panelRelationTwoDesc.add(txtRelationTwoDesc);
				panelRelationTwoDesc.setLayout(panelLayout);

				JPanel panelButtons = new JPanel();
				panelButtons.add(split);
				panelButtons.add(cancel);
				panelButtons.setLayout(panelButtonLayout);
				
				content.add(panelRelationOneName);
				content.add(panelRelationOneDesc);
				content.add(panelRelationTwoName);
				content.add(panelRelationTwoDesc);
				content.add(panelJoinAttribute);
				content.add(panelButtons);
				pack();
			}
			else
			{
				JOptionPane.showMessageDialog(null, 
				"Please work with the latest version");
			}
			
		}

		
	}
	
	//Pop Window to Merge Relations
	/*class MergeRelationBox extends JDialog {
		static final long serialVersionUID = 42;
		JComboBox listMergeRelation;
		JTextField txtRelationOneDesc;
		JTextField txtRelationTwoName;
		JTextField txtRelationTwoDesc;
		Relation _rel;
		JComboBox listMergeAttribute;

		public  MergeRelationBox() {
			super();
			try 
			{
				final JButton merge = new JButton("Merge");
				merge.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						
						String relationOneName = listMergeRelation.getText().toUpperCase();
						String relationOneDescription = txtRelationOneDesc.getText().toUpperCase();
						String relationTwoName = txtRelationTwoName.getText().toUpperCase();
						String relationTwoDescription = txtRelationTwoDesc.getText().toUpperCase();
						String joinAttribute = (String)listMergeAttribute.getSelectedItem();
						try 
						{
							createSplitRelations(_rel,
													relationOneName, 
													relationOneDescription,
													relationTwoName,
													relationTwoDescription,
													joinAttribute);
							dispose ();
						} 
						catch (DuplicateRelationIdException e1) 
						{
							String relId = e1.getRelId();
							if(relId.equals(relationOneName))
							{
								listMergeRelation.setText("");
							}
							else
							{
								txtRelationTwoName.setText("");
							}
							JOptionPane.showMessageDialog(null, "Relation Name already exists");
							e1.printStackTrace();
						}
						
					}
				});
				final JButton cancel = new JButton("Cancel");
				cancel.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						dispose ();
					}
				});
				
				RelationGraphCell cell = (RelationGraphCell) getSelectionCell();
				_rel = cell.getRelation();
				boolean blnLatestRevision = false;
				String latestRevisionName = _rel.getNextRevisionName();
				try 
				{
					_sc.getRelation(latestRevisionName);
				} 
				catch (RelationNotFoundException e1) 
				{
					blnLatestRevision = true;
				}
				
				if (blnLatestRevision)
				{
					Container content = getContentPane();
					content.setLayout(new BoxLayout(content,BoxLayout.Y_AXIS));
					 
					FlowLayout panelLayout = new FlowLayout();
					panelLayout.setAlignment(FlowLayout.LEFT);
					
					FlowLayout panelButtonLayout = new FlowLayout();
					panelButtonLayout.setAlignment(FlowLayout.CENTER);
					
					ArrayList<ScField> lstFields = (ArrayList<ScField>) _rel.getFields();
					String[] arrAttributeNames = new String[lstFields.size()];
					int attrCount = 0;
					for (ScField field : lstFields)
					{
						arrAttributeNames[attrCount] = field.getName();
						attrCount++;
					}
					
					JPanel panelMergeAttribute = new JPanel();
					JLabel labelMergeAttribute = new JLabel("Merge Attribute");
					
					listMergeAttribute = new JComboBox(arrAttributeNames);
					panelMergeAttribute.add(labelMergeAttribute);
					panelMergeAttribute.add(listMergeAttribute);
					panelMergeAttribute.setLayout(panelLayout);
					
					JPanel panelMergeRelation = new JPanel();
					JLabel labelMergeRelation = new JLabel("Relation to be Merged with");
					
					ArrayList<String> lstMergeRelations = _sc.getRelationNames();
					String[] arrRelationNames = new String[lstMergeRelations.size()];
					int relCount = 0;
					for (String relName : lstMergeRelations)
					{
						Relation rel = _sc.getRelation(relName);
				 		String relNextRevision = rel.getNextRevisionName();
						try
						{
							_sc.getRelation(relNextRevision);
						}
						catch(RelationNotFoundException e)
						{
							arrRelationNames[relCount] = relName;
							relCount++;
						}
					}
	
					listMergeRelation = new JComboBox(arrRelationNames);
					panelMergeRelation.add(labelMergeRelation);
					panelMergeRelation.add(listMergeRelation);
					panelMergeRelation.setLayout(panelLayout);
	
					JPanel panelButtons = new JPanel();
					panelButtons.add(merge);
					panelButtons.add(cancel);
					panelButtons.setLayout(panelButtonLayout);
					
					content.add(panelMergeRelation);
					content.add(panelMergeAttribute);
					content.add(panelButtons);
					pack();
				}
				else
				{
					JOptionPane.showMessageDialog(null, 
					"Please work with the latest version");
				}
		}
		catch (RelationNotFoundException e1) 
		{
			e1.printStackTrace();
		}
		}
	}*/
	
	//Pop Window to Add attributes to Split relation
	class AddAttributeToSplitRelationBox extends JDialog {
		static final long serialVersionUID = 42;
		Relation relOld;
		Relation relOneNew;
		Relation relTwoNew;
		private JList list;
	    private DefaultListModel listModel;
	    HashMap<String,RelationField> hmFields;

		public  AddAttributeToSplitRelationBox(Relation prel,
							Relation prelOneNew, Relation prelTwoNew) {
			super();
			this.relOld = prel;
			this.relOneNew = prelOneNew;
			this.relTwoNew = prelTwoNew;
			this.hmFields = new HashMap<String,RelationField>();
			Container content = getContentPane();
			final JButton add = new JButton("Add  Attributes to Relation");
			add.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					
					Object selectedRelationOne[] = list.getSelectedValues();
				
					ArrayList<String> lstAttrRelationOne = new ArrayList<String>();
					
					for (Object attrb: selectedRelationOne)
					{
						String attrName = (String)attrb;
						System.out.println("Attribute Selected for one-->"+attrName);
						lstAttrRelationOne.add(attrName);
					}
					
					ArrayList <RelationField> lstFieldsOne = new ArrayList<RelationField>();
					for (String attr : lstAttrRelationOne)
					{
						lstFieldsOne.add(hmFields.get(attr));
						System.out.println("Attributes final for one-->"+hmFields.get(attr));
					}
					addAttributeToRel(relOneNew, lstFieldsOne, true);
					
					ArrayList <RelationField> lstFieldsTwo = new ArrayList<RelationField>();
					for(String key : hmFields.keySet())
					{
					    RelationField field = hmFields.get(key);
						if (!lstFieldsOne.contains(field))
					    {
					    	lstFieldsTwo.add(field);
					    }
					}
					addAttributeToRel(relTwoNew, lstFieldsTwo, true);
					
					addMappingForSplitRelation(relOld.getDbRelName(),
									relOneNew.getDbRelName(),
									relTwoNew.getDbRelName());
					dispose ();
				}
			});
			
			
			content.setLayout(new BoxLayout(content,BoxLayout.Y_AXIS));
			 
			FlowLayout panelLayout = new FlowLayout();
			panelLayout.setAlignment(FlowLayout.LEFT);
			
			FlowLayout panelButtonLayout = new FlowLayout();
			panelButtonLayout.setAlignment(FlowLayout.CENTER);
			
	        listModel = new DefaultListModel();
	        ArrayList <RelationField> joinFields = (ArrayList<RelationField>) relOneNew.getFields();
	        for (RelationField field : relOld.getFields())
	        {
	        	if (!joinFields.contains(field))
	        	{
	        		listModel.addElement(field.getName());
		        	hmFields.put(field.getName(),field);
	        	}
	        	
	        }
	        
			JPanel panelAttributeRelOne = new JPanel();
			JLabel labelAttributeRelOne = new JLabel("Attributes for Relation 1");

	        //Create the list and put it in a scroll pane.
	        list = new JList(listModel);
	        list.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
	        list.setSelectedIndex(0);
	        list.addListSelectionListener(new ListSelectionListener()
	        {
	        	//This method is required by ListSelectionListener.
	            public void valueChanged(ListSelectionEvent e) {
	                if (e.getValueIsAdjusting() == false) {

	                    if (list.getSelectedIndex() == -1) {
	                    //No selection, disable add button.
	                    	add.setEnabled(false);

	                    } else {
	                    //Selection, enable the add button.
	                    	add.setEnabled(true);
	                    }
	                }
	            }
	        });
	        list.setVisibleRowCount(5);
	        JScrollPane listScrollPane = new JScrollPane(list);
	        
			panelAttributeRelOne.add(labelAttributeRelOne);
			panelAttributeRelOne.add(listScrollPane);
			panelAttributeRelOne.setLayout(panelLayout);
			
			JPanel panelButtons = new JPanel();
			panelButtons.add(add);
			panelButtons.setLayout(panelButtonLayout);
			
			content.add(panelAttributeRelOne);
			content.add(panelButtons);
			pack();
			
		}

		
	}

	
	/**
	 * Pop up Window to Drop Attributes
	 */
	class DropAttributeBox extends JDialog implements ItemListener{
		static final long serialVersionUID = 42;
		JCheckBox[] _attributeCheckB;
		Relation _rel;
		ArrayList<String> _removeAttributes;
		
		public  DropAttributeBox() {
			super();
			
			Container content = getContentPane();
			final JButton drop = new JButton("Drop");
			drop.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					removeAttributeFromRel(_rel,_removeAttributes);
					dispose ();
				}
			});

			RelationGraphCell cell = (RelationGraphCell) getSelectionCell();
			_rel = cell.getRelation();
			boolean blnLatestRevision = false;
			String latestRevisionName = _rel.getNextRevisionName();
			try 
			{
				_sc.getRelation(latestRevisionName);
			} 
			catch (RelationNotFoundException e1) 
			{
				blnLatestRevision = true;
			}
			
			if(blnLatestRevision)
			{
				String[] checkBoxLabels = new String[_rel.getNumCols()];
				int cntRelations = 0;
				for (RelationField field : _rel.getFields())
				{
					String fieldName = field.getName();
					checkBoxLabels[cntRelations] = fieldName;
					cntRelations++;
				}
				_removeAttributes = new ArrayList<String>(cntRelations);
				
				content.setLayout(new BoxLayout(content,BoxLayout.Y_AXIS));
				 
				FlowLayout panelButtonLayout = new FlowLayout();
				panelButtonLayout.setAlignment(FlowLayout.CENTER);
				
				_attributeCheckB = new JCheckBox[cntRelations];
				JPanel panelCheckBox = new JPanel();
				panelCheckBox.setLayout(new BoxLayout(panelCheckBox,BoxLayout.Y_AXIS));
				for(int i=0; i<checkBoxLabels.length ; i++)
			    {		      
					_attributeCheckB[i] = new JCheckBox(checkBoxLabels[i]);
					_attributeCheckB[i].addItemListener(this);
					panelCheckBox.add(_attributeCheckB[i]);
			    }
					
				JPanel panelButtons = new JPanel();
				panelButtons.add(drop);
				panelButtons.setLayout(panelButtonLayout);
				
				content.add(panelCheckBox);
				content.add(panelButtons);
				pack();
			}
			else
			{
				JOptionPane.showMessageDialog(null, 
				"Please work with the latest version");
			}
		}
		

		public void itemStateChanged(ItemEvent e) {
			 
			System.out.println("*****Item state changed****");
			if (e.getStateChange() == e.SELECTED)
			{
				System.out.println("Remove-->"+((JCheckBox)e.getSource()).getText());
				removeAttribute(((JCheckBox)e.getSource()).getText());
			}
			else
			{
				System.out.println("Undo Remove-->"+((JCheckBox)e.getSource()).getText());
				undoRemoveAttribute(((JCheckBox)e.getSource()).getText());
			}
		 
		}//ends itemStateChanged
		
		/**
		 * Method to select an Attribute from a Relation for removal
		 */
		public void removeAttribute(String field) 
		{
			_removeAttributes.add(field);
		}
		
		/**
		 * Method to undo selection of an Attribute
		 */
		public void undoRemoveAttribute(String field) 
		{
			_removeAttributes.remove(field);
		}

	}
	
	/**
	 * Method to add a relation  
	 * @throws DuplicateRelationIdException 
	 *   
	 */
	private void addRelationtoSchema(String relName, String relDescr) throws DuplicateRelationIdException 
	{
		try
		{
			System.out.println("******Inside addRelationtoSchema*******");
			Relation rel = new Relation(null, _sc.getSchemaId(), relName, relName, relDescr, true, true, new ArrayList<RelationField>());
			
			Peer _p = _sys.getPeer(_peerId);
			
			String schemaId = _sc.getSchemaId();
			String schemaDesc = _sc.getDescription();
			
			//Remove old version of schema and new version
			_p.removeSchema(schemaId);
			
			ArrayList <Relation> lstRelations = (ArrayList<Relation>)_sc.getRelations();
			Schema schemaNew = new Schema (schemaId,schemaDesc);
			
			for (Relation relOld : lstRelations)
			{
				schemaNew.addRelation(relOld);
			}
			
			//Add new relation to schema
			schemaNew.addRelation(rel);
			
			//Add new Schema to Peer
			_p.addSchema(schemaNew);
			this._sc = schemaNew;
			
			//Remove old version of Peer and add new version
			_sys.removePeer(_peerId);
			_sys.addPeer(_p);
			
			new AddAttributeBox(rel).setVisible (true);
			
			System.out.println("******Leaving addRelationtoSchema*******");
		}
		catch (DuplicateSchemaIdException e) 
		{
			e.printStackTrace();
		} 
		catch (DuplicatePeerIdException e) 
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * Method to remove an Attribute from a Relation
	 */
	private void removeAttributeFromRel(Relation rel,ArrayList<String> removeAttributes) 
	{
		try
		{
			Relation relNew;
			System.out.println("********Inside removeAttributeFromRel*******");
			
			String catName = rel.getDbCatalog();
			String schemaName = rel.getDbSchema();
			String tableName = rel.getNextRevisionName();
			String relName = rel.getNextRevisionName();
			String description = rel.getDescription();
			boolean mat = rel.isMaterialized();
			boolean hasLoc = rel.hasLocalData();
			
			System.out.println("Attributes to be removed");
			for (int i=0; i<removeAttributes.size();i++)
			{
				System.out.println(removeAttributes.get(i));
			}
			System.out.println("--------------");
			
			//Creation of Mapping for new relation  
			List<Atom> head = new ArrayList<Atom> ();
			List<Atom> body = new ArrayList<Atom> ();
			List<AtomArgument> valsHead = new ArrayList<AtomArgument> ();
			List<AtomArgument> valsBody = new ArrayList<AtomArgument> ();

			//Remove the fields that were dropped by the user
			ArrayList <RelationField> lstFieldsNew = new ArrayList<RelationField>();
			List <RelationField> lstFields = rel.getFields();
			int numberOfAttributes = lstFields.size();

			for (int cnt = 0 ; cnt < numberOfAttributes;cnt++)
			{
				RelationField  field = (RelationField)lstFields.get(cnt);
				String name  = field.getName();
				valsBody.add(new AtomVariable(name));
				if (!removeAttributes.contains(name))
				{
					System.out.println("Adding to new Relation-->"+name);
					lstFieldsNew.add(field);
					valsHead.add(new AtomVariable(name));
				}
			}
			
			//Create new Relation
			relNew = new Relation(catName,schemaName,tableName,
							relName,description,mat,hasLoc,lstFieldsNew);
			
			Peer _p = _sys.getPeer(_peerId);
			
			String schemaId = _sc.getSchemaId();
			String schemaDesc = _sc.getDescription();
			
			//Remove old version of schema and new version
			_p.removeSchema(schemaId);
			
			ArrayList <Relation> lstRelations = (ArrayList<Relation>)_sc.getRelations();
			Schema schemaNew = new Schema (schemaId,schemaDesc);
			
			for (Relation relOld : lstRelations)
			{
				schemaNew.addRelation(relOld);
			}
			
			//Add new relation to schema
			schemaNew.addRelation(relNew);
			
			//Add new Schema to Peer
			_p.addSchema(schemaNew);
			this._sc = schemaNew;
			
			//Add new mapping to peer
			Atom atomHead = new Atom (_p, schemaNew, relNew, valsHead);
			head.add (atomHead);
			
			Atom atomBody = new Atom (_p, schemaNew, rel, valsBody);
			body.add(atomBody);
			
			Mapping newMapping = new Mapping(relName+"DeletedMapping", "deleted mapping", true, 1, head, body);
			_p.addMapping(newMapping);
			
			//Remove old version of Peer and add new version
			_sys.removePeer(_peerId);
			_sys.addPeer(_p);
			
			System.out.println("Config.getTempSchemaFile()-->"+Config.getTempSchemaFile());
			File fileTest = new File(Config.getTempSchemaFile());
			_sys.serialize(new FileOutputStream(fileTest));
			
			System.out.println("**********Leaving removeAttributeFromRel*********");
			
		}
		catch (DuplicateRelationIdException e)
		{
			e.printStackTrace();
		} 
		catch (DuplicateSchemaIdException e) 
		{
			e.printStackTrace();
		} 
		catch (DuplicatePeerIdException e) 
		{
			e.printStackTrace();
		} 
		catch (DuplicateMappingIdException e) 
		{
			e.printStackTrace();
		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		} 
	}
	
	private void createSplitRelations(Relation rel, 
						String relationOneName, String relationOneDesc, 
						String relationTwoName, String relationTwoDesc,
						String joinAttribute) throws DuplicateRelationIdException 
	{
		try
		{
			Relation relOneNew;
			Relation relTwoNew;
			
			System.out.println("********Inside addSplitRelationsToSchema*******");
			
			String catName = rel.getDbCatalog();
			String schemaName = rel.getDbSchema();
			
			//Remove the fields that were dropped by the user
			ArrayList <RelationField> lstFieldsNew = new ArrayList<RelationField>();
			List <RelationField> fields = rel.getFields();
			
			int numberOfAttributes = fields.size();

			for (int cnt = 0 ; cnt < numberOfAttributes;cnt++)
			{
				RelationField  field = (RelationField)fields.get(cnt);
				String name  = field.getName();
				if (name.equals(joinAttribute))
				{
					System.out.println("Adding join attribute-->"+name);
					lstFieldsNew.add(field);
				}
			}
			
			//Create new Relations
			relOneNew = new Relation(catName,schemaName,relationOneName,
					relationOneName,relationOneDesc,true,false,lstFieldsNew);
			
			relTwoNew = new Relation(catName,schemaName,relationTwoName,
					relationTwoName,relationOneDesc,true,false,lstFieldsNew);
			
			Peer _p = _sys.getPeer(_peerId);
			
			String schemaId = _sc.getSchemaId();
			String schemaDesc = _sc.getDescription();
			
			//Remove old version of schema and new version
			_p.removeSchema(schemaId);
			
			ArrayList <Relation> lstRelations = (ArrayList<Relation>)_sc.getRelations();
			Schema schemaNew = new Schema (schemaId,schemaDesc);
			
			for (Relation relOld : lstRelations)
			{
				schemaNew.addRelation(relOld);
			}
			
			//Add new relations to schema
			schemaNew.addRelation(relOneNew);
			schemaNew.addRelation(relTwoNew);
			
			//Add new Schema to Peer
			_p.addSchema(schemaNew);
			this._sc = schemaNew;
			
			//Remove old version of Peer and add new version
			_sys.removePeer(_peerId);
			_sys.addPeer(_p);
			
			new AddAttributeToSplitRelationBox(rel,relOneNew,relTwoNew).setVisible (true);
			
			System.out.println("**********Leaving addSplitRelationsToSchema*********");
		}
		catch (DuplicatePeerIdException e) 
		{
			e.printStackTrace();
		} 
		catch (DuplicateSchemaIdException e) 
		{
			e.printStackTrace();
		} 

	}
	
	/*
	 * Add Mappings for a Split Relation
	 */
	private void addMappingForSplitRelation(String relOldName,
								String relOneNewName, String relTwoNewName) 
	{
		try 
		{
			
			System.out.println("**********Start addMappingSplitRelation*********");
			
			Peer _p = _sys.getPeer(_peerId);
			Relation relOld = _sc.getRelation(relOldName);
			Relation relOneNew = _sc.getRelation(relOneNewName);
			Relation relTwoNew = _sc.getRelation(relTwoNewName);
			

			//Creation of Mapping for split relation  
			List<Atom> head = new ArrayList<Atom> ();
			List<Atom> body = new ArrayList<Atom> ();
			List<AtomArgument> valsHead = new ArrayList<AtomArgument> ();
			List<AtomArgument> valsBodyOne = new ArrayList<AtomArgument> ();
			List<AtomArgument> valsBodyTwo = new ArrayList<AtomArgument> ();
			
			
			ArrayList<RelationField>lstFields = (ArrayList<RelationField>)relOld.getFields();
			int numberOfAttributes = 0;
			if (lstFields != null)
			{
				numberOfAttributes = lstFields.size();
			}
			
			for (int cnt = 0 ; cnt < numberOfAttributes;cnt++)
			{
				RelationField  field = (RelationField)lstFields.get(cnt);
				String name  = field.getName();
				valsHead.add(new AtomVariable(name));
			}
			
			lstFields = (ArrayList<RelationField>)relOneNew.getFields();
			numberOfAttributes = 0;
			if (lstFields != null)
			{
				numberOfAttributes = lstFields.size();
			}
			
			for (int cnt = 0 ; cnt < numberOfAttributes;cnt++)
			{
				RelationField  field = (RelationField)lstFields.get(cnt);
				String name  = field.getName();
				valsBodyOne.add(new AtomVariable(name));
			}
			
			lstFields = (ArrayList<RelationField>)relTwoNew.getFields();
			numberOfAttributes = 0;
			if (lstFields != null)
			{
				numberOfAttributes = lstFields.size();
			}
			
			for (int cnt = 0 ; cnt < numberOfAttributes;cnt++)
			{
				RelationField  field = (RelationField)lstFields.get(cnt);
				String name  = field.getName();
				valsBodyTwo.add(new AtomVariable(name));
			}
			
			//Add new mapping to peer
			Atom atomHead = new Atom (_p,_sc,relOld, valsHead);
			head.add (atomHead);
			Atom atomBodyOne = new Atom (_p,_sc, relOneNew, valsBodyOne);
			Atom atomBodyTwo = new Atom (_p,_sc,relTwoNew, valsBodyTwo);
			body.add(atomBodyOne);
			body.add(atomBodyTwo);
			
			Mapping newMapping = new Mapping(
					relOneNew.getDbRelName()+relTwoNew.getDbRelName()+"Split", "added mapping", true, 1, head, body);
			
			_p.addMapping(newMapping);
			
			//Remove old version of Peer and add new version
			_sys.removePeer(_peerId);
			_sys.addPeer(_p);
			System.out.println("Config.getTempSchemaFile()-->"+Config.getTempSchemaFile());
			File fileTest = new File(Config.getTempSchemaFile());
			_sys.serialize(new FileOutputStream(fileTest));
			
			System.out.println("**********Leaving addMappingSplitRelation*********");
		} 
		catch (DuplicateMappingIdException e) 
		{
			e.printStackTrace();
		} 
		catch (DuplicatePeerIdException e) 
		{
			e.printStackTrace();
		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		} 
		catch (RelationNotFoundException e)
		{
			e.printStackTrace();
		} 
	}
	
	
	/**
	 * 
	 * @param rel
	 * @param fields
	 * @param blnNewRelation
	 */
	
	private void addAttributeToRel(Relation rel, ArrayList <RelationField> fields, boolean blnNewRelation) 
	{
		try
		{
			Relation relNew;
			System.out.println("********Inside addAttributeToRel*******");
			
			String catName = rel.getDbCatalog();
			String schemaName = rel.getDbSchema();
			String tableName = "";
			String relName = "";
			if (!blnNewRelation)
			{
				tableName = rel.getNextRevisionName();
				relName = rel.getNextRevisionName();
			}
			else
			{
				tableName = rel.getDbRelName();
				relName = rel.getName();
			}
			
			String description = rel.getDescription();
			boolean mat = rel.isMaterialized();
			boolean hasLoc = rel.hasLocalData();
			
			//Creation of Mapping for new relation  
			List<Atom> head = new ArrayList<Atom> ();
			List<Atom> body = new ArrayList<Atom> ();
			List<AtomArgument> valsHead = new ArrayList<AtomArgument> ();
			List<AtomArgument> valsBody = new ArrayList<AtomArgument> ();

			//Remove the fields that were dropped by the user
			ArrayList <RelationField> lstFieldsNew = new ArrayList<RelationField>();
			List <RelationField> lstFields = rel.getFields();
			
			int numberOfAttributes = 0;
			if (lstFields != null)
			{
				numberOfAttributes = lstFields.size();
			}

			for (int cnt = 0 ; cnt < numberOfAttributes;cnt++)
			{
				RelationField  field = (RelationField)lstFields.get(cnt);
				String name  = field.getName();
				System.out.println("Existing Attribute Name-->"+name);
				valsBody.add(new AtomVariable(name));
				valsHead.add(new AtomVariable(name));
				lstFieldsNew.add(field);
			}
			
			int numberOfAttributesAdded = fields.size();

			for (int cnt = 0 ; cnt < numberOfAttributesAdded;cnt++)
			{
				RelationField  field = (RelationField)fields.get(cnt);
				String name  = field.getName();
				System.out.println("Adding new attribute-->"+name);
				valsHead.add(new AtomVariable(name));
				lstFieldsNew.add(field);
			}
			
			//Create new Relation
			relNew = new Relation(catName,schemaName,tableName,
							relName,description,mat,hasLoc,lstFieldsNew);
			
			Peer _p = _sys.getPeer(_peerId);
			
			String schemaId = _sc.getSchemaId();
			String schemaDesc = _sc.getDescription();
			
			//Remove old version of schema and new version
			_p.removeSchema(schemaId);
			
			ArrayList <Relation> lstRelations = (ArrayList<Relation>)_sc.getRelations();
			Schema schemaNew = new Schema (schemaId,schemaDesc);
			
			for (Relation relOld : lstRelations)
			{
				boolean blnAddRelation = false;
				if (relOld.equals(rel))
				{
					if (!blnNewRelation)
					{
						blnAddRelation = true;
					}
				}
				else
				{
					blnAddRelation = true;
				}
				if (blnAddRelation)
				{
					schemaNew.addRelation(relOld);
				}
			}
			
			//Add new relation to schema
			schemaNew.addRelation(relNew);
			
			//Add new Schema to Peer
			_p.addSchema(schemaNew);
			this._sc = schemaNew;
			
			if (!blnNewRelation)
			{
				//Add new mapping to peer
				Atom atomHead = new Atom (_p, schemaNew, relNew, valsHead);
				head.add (atomHead);
				
				Atom atomBody = new Atom (_p, schemaNew, rel, valsBody);
				body.add(atomBody);
				
				Mapping newMapping = new Mapping(relName+"AddedMapping", "added mapping", true, 1, head, body);
				_p.addMapping(newMapping);
			}
			
			//Remove old version of Peer and add new version
			_sys.removePeer(_peerId);
			_sys.addPeer(_p);
			System.out.println("Config.getTempSchemaFile()-->"+Config.getTempSchemaFile());
			File fileTest = new File(Config.getTempSchemaFile());
			_sys.serialize(new FileOutputStream(fileTest));
			
			System.out.println("**********Leaving addAttributeToRel*********");
			
		}
		catch (DuplicateRelationIdException e)
		{
			e.printStackTrace();
		} 
		catch (DuplicateSchemaIdException e) 
		{
			e.printStackTrace();
		} 
		catch (DuplicatePeerIdException e) 
		{
			e.printStackTrace();
		} 
		catch (DuplicateMappingIdException e) 
		{
			e.printStackTrace();
		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		}

	}

	/**
	 * Method to drop a relation from the schema
	 */
	private void dropRelation() 
	{
		try
		{
			System.out.println("**** Inside dropRelation****");
			//Get the relation to be dropped
			RelationGraphCell cell = (RelationGraphCell) getSelectionCell();
			Relation rel = cell.getRelation();
			
			boolean blnLatestRevision = false;
			String latestRevisionName = rel.getNextRevisionName();
			
			try 
			{
				_sc.getRelation(latestRevisionName);
			} 
			catch (RelationNotFoundException e1) 
			{
				blnLatestRevision = true;
			}
			
			if (blnLatestRevision)
			{
				String selectRelName = rel.getDbRelName();
				
				//Remove the Mappings related to the Relation
				removeRelatedMappingsForRel(rel);
				
				Peer _p = _sys.getPeer(_peerId); 
				
				String schemaId = _sc.getSchemaId();
				String schemaDesc = _sc.getDescription();
				
				//Remove old version of schema and new version
				_p.removeSchema(schemaId);
				
				ArrayList <Relation> lstRelations = (ArrayList<Relation>)_sc.getRelations();
				Schema schemaNew = new Schema (schemaId,schemaDesc);
				
				for (Relation relOld : lstRelations)
				{
					if (!selectRelName.equals(relOld.getDbRelName()))
					{
						schemaNew.addRelation(relOld);
					}
				}
				
				//Add new Schema to Peer
				_p.addSchema(schemaNew);
				this._sc = schemaNew;
				
				_sys.removePeer(_peerId);
				_sys.addPeer(_p);
				
				System.out.println("Config.getTempSchemaFile()-->"+Config.getTempSchemaFile());
				File fileTest = new File(Config.getTempSchemaFile());
				_sys.serialize(new FileOutputStream(fileTest));
			}
			else
			{
				JOptionPane.showMessageDialog(null, 
				"Please work with the latest version");
			}
			System.out.println("**** Leaving dropRelation****");

		}
		catch (DuplicateSchemaIdException e)
		{
			e.printStackTrace();
		} 
		catch (DuplicateRelationIdException e) 
		{
			e.printStackTrace();
		} 
		catch (DuplicatePeerIdException e) 
		{
			e.printStackTrace();
		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		}
	}


	/**
	 * Method to retrieve the mapping in which the relation occurs in the head
	 * @param rel
	 * @return
	 */
	private void removeRelatedMappingsForRel(Relation selectRel) 
	{
		try
		{
			System.out.println("*****Inside removeRelatedMappingsForRel******");
			String selectRelName = selectRel.getDbRelName();
			System.out.println("selectRelName-->"+selectRelName);
			List<Mapping>mappings =  _sys.getAllSystemMappings(true);
			boolean blnRemoveMapping;
			for (Mapping mapping : mappings)
			{
				blnRemoveMapping = false;
				Peer p = mapping.getMappingHead().get(0).getPeer();
				String peerId = p.getId();
				Relation rel = mapping.getMappingHead().get(0).getRelation();
				String relName = rel.getRelationName();
				String mappingId = mapping.getId();
				
				System.out.println("*************Mapping-->"+mappingId+"********");
				System.out.println("relName-->"+relName);
				
				
				if (relName.equals(selectRelName))
				{
					System.out.println("Mapping can be removed");
					blnRemoveMapping = true;
				}
				else
				{
					List<Atom> mappingsBody = mapping.getBody();
					for(Atom mappingBody : mappingsBody)
					{
						Relation relBody = mappingBody.getRelation();
						String relBodyName = relBody.getRelationName();
						System.out.println("relBodyName-->"+relBodyName);
						if (relBodyName.equals(selectRelName))
						{
							System.out.println("Mapping can be removed");
							blnRemoveMapping = true;
						}
					}

				}
				if (blnRemoveMapping)
				{
					p.removeMapping(mappingId);
					_sys.removePeer(peerId);
					_sys.addPeer(p);
				}
			}
			System.out.println("*****Leaving removeRelatedMappingsForRel******");
		}
		catch (DuplicatePeerIdException e)
		{
			e.printStackTrace();
		}
	}
	//dinesh --

	

}
