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
package edu.upenn.cis.orchestra.gui.transactions;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Dimension;
import java.awt.FontMetrics;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JList;
import javax.swing.ListSelectionModel;
import javax.swing.DefaultListModel;

import edu.upenn.cis.orchestra.reconciliation.Db;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;

public class TransactionInfoPanel extends JPanel {
	public static final long serialVersionUID = 1L;
	
	private final DefaultListModel _updates = new DefaultListModel();
	private final JList _updateList = new JList(_updates);
	private final JScrollPane _scrollUpdates = new JScrollPane (_updateList);
	
	
	private final JLabel _labTxnId = new JLabel ();
	private final JLabel _labNbUpdates = new JLabel ();

	private Db _db;
	//private TxnPeerID _curTxn;

	public TransactionInfoPanel(Db db) {
		_db = db;
		init();
	}

	private void init() {
		setBorder (BorderFactory.createTitledBorder("Transaction details"));
		setLayout(new GridBagLayout());
		
		GridBagConstraints cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = 0;
		cst.weightx = 0.1;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		add(_labTxnId, cst);
		
		cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = 1;
		cst.weightx = 0.1;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		add(_labNbUpdates, cst);
		
		cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = 2;
		cst.weightx = 0.1;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		cst.fill = GridBagConstraints.BOTH;

		_updateList.setSelectionMode(ListSelectionModel.SINGLE_INTERVAL_SELECTION);
		_updateList.setLayoutOrientation(JList.HORIZONTAL_WRAP);
		_updateList.setVisibleRowCount(-1);
		FontMetrics metrics = _updateList.getFontMetrics(_updateList.getFont());
		_scrollUpdates.setMinimumSize(new Dimension(180, (metrics.getHeight() + metrics.getDescent() + metrics.getLeading()) * 3));
		_scrollUpdates.setPreferredSize(new Dimension(180, (metrics.getHeight() + metrics.getDescent() + metrics.getLeading()) * 10));
		add (_scrollUpdates, cst);
		
		clearUpdates();
	}
	
	public void clearUpdates() {
		_labTxnId.setText("No transaction selected");
		_labNbUpdates.setText("Priority N/A");

		_updates.clear();
	}
	
	public void setTransaction(TxnPeerID txn, int prio) {
		try {
			//_curTxn = txn;
			List<Update> updates = _db.getTransaction(txn);
			
			_labTxnId.setText("Transaction " + txn.toString());
			
			Map<Integer,List<List<Set<TxnPeerID>>>> conf = _db.getConflicts();
			Set<TxnPeerID> conflicting = new HashSet<TxnPeerID>();

			for (Integer i : conf.keySet())
				for (List<Set<TxnPeerID>> ls : conf.get(i))
					for (Set<TxnPeerID> txns : ls)
						conflicting.addAll(txns);
			
			String status;
			if (conflicting.contains(txn))
				status = " (CONFLICTING)";
			else if (_db.hasAcceptedTxn(txn))
				status = " (accepted)";
			else if (_db.hasRejectedTxn(txn))
				status = " (rejected)";
			else
				status = " (unreconciled)";
			_labNbUpdates.setText("Priority " + prio + status);
	
			_updates.clear();
			for (Update u : updates) {
				_updates.addElement(u.toString());
			}
		} catch (Exception e) {
			
		}
	}
	
	public void setVisible(boolean s) {
		super.setVisible(s);
	}

	public JLabel getLabelTransactionId() {
		return _labTxnId;
	}

	public JLabel getLabelTransactionNbUpdates() {
		return _labNbUpdates;
	}
}
