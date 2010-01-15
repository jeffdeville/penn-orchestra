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

import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datamodel.Mapping;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.UIManager;

public class MappingInfoSidePanel extends JPanel {
	public static final long serialVersionUID = 1L;
	private final JLabel _labMappingId = new JLabel ();
	//private final JLabel _labSchemaNbRelations = new JLabel ();
	private final JTextArea _txtAreaMapping = new JTextArea ();
	
	public MappingInfoSidePanel() {
		init();
	}

	private void init() {
		setBorder (BorderFactory.createTitledBorder("Mapping details"));
		setLayout(new GridBagLayout());
		
		GridBagConstraints cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = 0;
		cst.weightx = 0.1;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		add(_labMappingId, cst);
		_labMappingId.setFont(UIManager.getFont("MappingLabel.font"));
		//_labMappingId.setFont(new Font("Serif", Font.BOLD, 14));
	
		_txtAreaMapping.setEditable(false);
		_txtAreaMapping.setBackground(getBackground());
		_txtAreaMapping.setLineWrap(true);
		_txtAreaMapping.setWrapStyleWord(true);		
		_txtAreaMapping.setFont(UIManager.getFont("MappingDescription.font"));
		//_txtAreaMapping.setFont(new Font("Serif", Font.PLAIN, 12));
		
		cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = 1;
		cst.weightx = 0.1;
		cst.weighty = 0.1;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		cst.fill = GridBagConstraints.BOTH;
		add (_txtAreaMapping, cst);
		
		setVisible(false);
	}
	
	public void setMapping(Mapping m) {
		_labMappingId.setText("Mapping " + m.getMappingHead().get(0).getPeer().getId() + m.getId() + ":");
		//_labSchemaNbRelations.setText(s.getRelations().size() + " relation" 
		//								+ (s.getRelations().size()>1?"s":""));
		StringBuffer head = new StringBuffer();
		StringBuffer body = new StringBuffer();
		
		for (Atom a : m.getMappingHead()) {
			if (head.length() > 0)
				head.append(", ");
			
			head.append(cleanedMapping(a));
		}
		for (Atom a : m.getBody()) {
			if (body.length() > 0)
				body.append(", ");

			body.append(cleanedMapping(a));
		}
		_txtAreaMapping.setText(head + "\n \u2190 \n" + body);		
		_txtAreaMapping.setToolTipText(head + " \u2190 " + body);
	}

	/**
	 * Simple function to return a reasonably formatted atom
	 * 
	 * @param a Atom to string-ify
	 * @return
	 */
	public static String cleanedMapping(Atom a) {
		String g = a.toString2();
		
		// Strip trailing underscores
		while (g.contains("_,"))
			g = g.replaceAll("_,", ",");
		while (g.contains("_)"))
			g = g.replace("_)", ")");
		
		// Strip "a:v0" symbols 
		while (g.contains("a:v")) {
			int pos = g.indexOf("a:v");
			
			int pos2 = pos + 3;
			while (pos2 < g.length() && g.charAt(pos2) >= '0' && g.charAt(pos2) <= '9')
				pos2++;
			
			g = g.substring(0, pos) + "-" + g.substring(pos2);
		}
		
		return g;
	}
	
	public void setVisible(boolean s) {
		super.setVisible(s);
	}

	public JLabel getLabelMappingId() {
		return _labMappingId;
	}

	public JTextArea getTxtAreaMapping() {
		return _txtAreaMapping;
	}
}
