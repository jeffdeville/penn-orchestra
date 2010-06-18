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
package edu.upenn.cis.orchestra.gui.peers;

import java.awt.Dimension;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.ListSelectionModel;
import javax.swing.UIManager;

import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Schema;

public class SchemaInfoSidePanel extends JPanel {
	public static final long serialVersionUID = 1L;
	private final JLabel _labSchemaId = new JLabel ();
	private final JLabel _labSchemaNbRelations = new JLabel ();
	private final JTextArea _txtAreaSchemaDesc = new JTextArea ();
	
	private final DefaultListModel _relations = new DefaultListModel();
	private final JList _relList = new JList(_relations);
	private final JScrollPane _scrollRelns = new JScrollPane (_relList);

	public SchemaInfoSidePanel() {
		init();
	}

	private void init() {
		setBorder (BorderFactory.createTitledBorder("Schema details"));
		setLayout(new GridBagLayout());
		
		GridBagConstraints cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = 0;
		cst.weightx = 0.1;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		//_labSchemaId.setFont(new Font("SERIF", Font.BOLD, 14));
		add(_labSchemaId, cst);
		
/*		cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = 1;
		cst.weightx = 0.1;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		add(_labSchemaNbRelations, cst);*/		
		
		_txtAreaSchemaDesc.setEditable(false);
		_txtAreaSchemaDesc.setBackground(getBackground());
		_txtAreaSchemaDesc.setLineWrap(true);
		_txtAreaSchemaDesc.setWrapStyleWord(true);
		Font txtFont = UIManager.getFont("SchemaDescription.font");
		//txtFont = txtFont.deriveFont((float)txtFont.getSize()-2F);
		//txtFont = new Font ("Serif", Font.PLAIN, 12);
		_txtAreaSchemaDesc.setFont(txtFont);
		
		cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = 1;
		cst.weightx = 0.1;
		cst.weighty = 0.1;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		cst.fill = GridBagConstraints.BOTH;
		_scrollRelns.setVisible(false);
		_relList.setSelectionMode(ListSelectionModel.SINGLE_INTERVAL_SELECTION);
		_relList.setLayoutOrientation(JList.HORIZONTAL_WRAP);
		_relList.setVisibleRowCount(-1);
		_relList.setFont(txtFont);
		FontMetrics metrics = _relList.getFontMetrics(_relList.getFont());
		_scrollRelns.setMinimumSize(new Dimension(180, (metrics.getHeight() + metrics.getDescent() + metrics.getLeading()) * 4));
		_scrollRelns.setPreferredSize(new Dimension(180, (metrics.getHeight() + metrics.getDescent() + metrics.getLeading()) * 10));
		add (_scrollRelns, cst);
		
		final JScrollPane scrollTxtScDesc = new JScrollPane (_txtAreaSchemaDesc);
		scrollTxtScDesc.setBorder(BorderFactory.createEmptyBorder());
		cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = 2;
		cst.weightx = 0.1;
		cst.weighty = 0.1;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		cst.fill = GridBagConstraints.BOTH;
		add (scrollTxtScDesc, cst);
		
		setVisible(false);
	}
	
	public void setSchema(Schema s) {
		_labSchemaId.setText("Schema " + s.getSchemaId());
		_labSchemaNbRelations.setText(s.getRelations().size() + " relation" 
										+ (s.getRelations().size()>1?"s":""));
		_txtAreaSchemaDesc.setText(s.getDescription());		

		_relations.clear();
		for (Relation r : s.getRelations()) {
			if (r.isInternalRelation())//.getName().endsWith("_L") || r.getName().endsWith("_R"))
				continue;
			String nam = r.getName() + "(";
			
			boolean again = false;
			for (RelationField f : r.getFields()) {
				if (again)
					nam = nam + ",";
				nam = nam + f.getName();
				again = true;
			}
			_relations.addElement(nam + ")");
		}
		_scrollRelns.setVisible(true);
	}
	
	public void setVisible(boolean s) {
		super.setVisible(s);
	}

	public JLabel getLabelSchemaId() {
		return _labSchemaId;
	}

	public JLabel getLabelSchemaNbRelations() {
		return _labSchemaNbRelations;
	}

	public JTextArea getTxtAreaSchemaDesc() {
		return _txtAreaSchemaDesc;
	}
}
