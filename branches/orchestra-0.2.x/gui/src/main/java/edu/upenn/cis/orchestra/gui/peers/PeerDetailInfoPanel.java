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

import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Schema;

public class PeerDetailInfoPanel extends JPanel 
{
	public static final long serialVersionUID = 1L;
	
	private List<PeerDetailInfoPanelObserver> _observers = new ArrayList<PeerDetailInfoPanelObserver> ();
	
	private final Peer _p;
	private final OrchestraSystem _sys;
	private final PeerTransactionsIntf _transIntf;
	private final IPeerBrowsingContext _context;
	
	public PeerDetailInfoPanel(final OrchestraSystem sys, final Peer p, 
								final PeerTransactionsIntf transIntf,
								final IPeerBrowsingContext context) 
	{
		_sys = sys;
		_p = p;
		_transIntf = transIntf;
		_context = context;
		
		setLayout (new GridBagLayout ());
		
		GridBagConstraints cst;
		int currRow = 0;
		
		final JLabel lblPeer = new JLabel ("Peer id: " + p.getId());
//		lblPeer.setFont(lblPeer.getFont().deriveFont(Font.BOLD).deriveFont((float)lblPeer.getFont().getSize()+3F));
		cst = new GridBagConstraints();
		cst.insets = new Insets (0, 2, 0, 2);
		cst.gridx = 0;
		cst.gridy = currRow++;
		add(lblPeer, cst);
		
		final JLabel lblDescriptionTitle = new JLabel ("Description:");
//		Font titleFont = lblDescriptionTitle.getFont().deriveFont(Font.BOLD).deriveFont((float)lblDescriptionTitle.getFont().getSize()+1F);
//		lblDescriptionTitle.setFont(titleFont);
		cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = currRow++;
		cst.insets = new Insets (12, 2, 0, 2);
		cst.anchor = GridBagConstraints.WEST;
		add (lblDescriptionTitle, cst);
		
		final JTextArea txtPeerDesc = new JTextArea(p.getDescription());
		txtPeerDesc.setEditable(false);
		txtPeerDesc.setLineWrap(true);
		txtPeerDesc.setWrapStyleWord(true);
		//txtPeerDesc.setBackground(getBackground());
		final JScrollPane scrollPeerDesc = new JScrollPane (txtPeerDesc);		
		//scrollPeerDesc.setBorder(BorderFactory.createEmptyBorder());
		cst = new GridBagConstraints();
		cst.gridx = 0;
		cst.gridy = currRow++;
		cst.fill = GridBagConstraints.BOTH;
		cst.weightx = 1;
		cst.weighty = 2;
		cst.insets = new Insets (0, 2, 0, 2);
		add(scrollPeerDesc, cst);
		

		// Publish & Reconcile
//		if (_sys.getRecMode())
		{
			final JLabel lblMappingsTitle = new JLabel ("Operations:");
//			lblMappingsTitle.setFont(titleFont);
			cst = new GridBagConstraints ();
			cst.gridx = 0;
			cst.gridy = currRow++;
			cst.anchor = GridBagConstraints.WEST;
			cst.insets = new Insets (12, 2, 0, 2);		
			add (lblMappingsTitle, cst);
	
			
			final JPanel pubRecPanel = new JPanel (); 
			final JButton btnPublish = new JButton ("Publish and Reconcile");
			if (_sys.isLocalPeer(p)) {
			btnPublish.addActionListener(new ActionListener ()
							{
									public void actionPerformed(ActionEvent arg0) {
										_context.setRefreshTransactions();
										PeerCommands.publishAndReconcile(getParent(), _sys, _transIntf);
									}
							});
			} else {
				btnPublish.setEnabled(false);
			}
			pubRecPanel.add (btnPublish);
	
			cst = new GridBagConstraints();
			cst.gridx = 0;
			cst.gridy = currRow++;
			cst.anchor = GridBagConstraints.WEST;
			cst.insets = new Insets (0, 8, 0, 2);		
			add (pubRecPanel, cst);
		}
		
		
		// Mappings
		final JLabel lblMappingsTitle = new JLabel ("Mappings:");
//		lblMappingsTitle.setFont(titleFont);
		cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = currRow++;
		cst.anchor = GridBagConstraints.WEST;
		cst.insets = new Insets (12, 2, 0, 2);		
		add (lblMappingsTitle, cst);

		
		final JButton btnEditMappings = new JButton ("View peer mappings");
		btnEditMappings.addActionListener(new ActionListener ()
						{
								public void actionPerformed(ActionEvent arg0) {
									mappingsBtnClicked();
								}
						});
		cst = new GridBagConstraints();
		cst.gridx = 0;
		cst.gridy = currRow++;
		cst.anchor = GridBagConstraints.WEST;
		cst.insets = new Insets (0, 8, 0, 2);		
		add (btnEditMappings, cst);
		
		
		
		// Schemas
		final JLabel lblSchemasTitle = new JLabel ("Schemas:");
//		lblSchemasTitle.setFont(titleFont);
		cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = currRow++;
		cst.anchor = GridBagConstraints.WEST;
		cst.insets = new Insets (12, 2, 4, 2);		
		add (lblSchemasTitle, cst);

		
		for (Schema sc : p.getSchemas())
		{
			final JLabel schemaLabel = new JLabel ("Schema " + sc.getSchemaId());
			schemaLabel.setFont(schemaLabel.getFont().deriveFont(Font.BOLD));
			cst = new GridBagConstraints();
			cst.gridx = 0;
			cst.gridy = currRow++;
			cst.anchor = GridBagConstraints.WEST;
			cst.insets = new Insets (3, 6, 0, 2);
			add (schemaLabel, cst);

			final JTextArea txtSchemaDesc = new JTextArea(sc.getDescription());
			txtSchemaDesc.setEditable(false);
			txtSchemaDesc.setLineWrap(true);
			txtSchemaDesc.setWrapStyleWord(true);
			//txtSchemaDesc.setBackground(getBackground());
			final JScrollPane scrollSchemaDesc = new JScrollPane (txtSchemaDesc);		
			//scrollSchemaDesc.setBorder(BorderFactory.createEmptyBorder());
			cst = new GridBagConstraints();
			cst.gridx = 0;
			cst.gridy = currRow++;
			cst.fill = GridBagConstraints.BOTH;
			cst.weightx = 1;
			cst.weighty = 1;
			cst.insets = new Insets (0, 10, 0, 2);
			add(scrollSchemaDesc, cst);
			
			final JButton schemaEditBtn = new JButton ("View details");
			final Schema locSc = sc;
			schemaEditBtn.addActionListener(new ActionListener ()
					{
						public void actionPerformed(ActionEvent evt) 
						{
							schemaDetailsSelected (locSc);
						};
					}); 
			cst = new GridBagConstraints();
			cst.gridx = 0;
			cst.gridy = currRow++;
			cst.anchor = GridBagConstraints.WEST;
			cst.insets = new Insets (0, 10, 3, 2);
			add (schemaEditBtn, cst);			
		}
		
		
	}
	

	
	private void mappingsBtnClicked ()
	{
		for (PeerDetailInfoPanelObserver obs : _observers)
			obs.mappingsBtnClicked(_p);
	}
	
	private void schemaDetailsSelected (Schema sc)
	{
		for (PeerDetailInfoPanelObserver obs : _observers)
			obs.schemaDetailsBtnClicked(_p, sc);
		
	}
	
	public void addObserver (PeerDetailInfoPanelObserver obs)
	{
		_observers.add (obs);
	}
	
	public void removeObserver (PeerDetailInfoPanelObserver obs)
	{
		_observers.remove(obs);
	}
}
