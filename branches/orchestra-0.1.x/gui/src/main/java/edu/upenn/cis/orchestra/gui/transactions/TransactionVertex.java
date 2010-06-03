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

import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.UIManager;

import org.jgraph.graph.DefaultGraphCell;
import org.jgraph.graph.GraphConstants;

import edu.upenn.cis.orchestra.datamodel.TxnPeerID;

public class TransactionVertex extends DefaultGraphCell 
{
	public static final long serialVersionUID = 1L;
	
	private TxnPeerID _curTxn;
	private int _curPrio;
	private boolean _isAcc;
	private boolean _isRej;
	private String _label;
	private String _updates;
	
	TransactionVertex (String label, int prio, boolean isAccepted, boolean isRejected)
	{
		super(label);
		
		_curPrio = prio;	
		_curTxn = null;
		_label = label;
		_isAcc = isAccepted;

		_isRej = isRejected;
		setProperties(prio, isAccepted, isRejected, getAttributes());
	}
	
	TransactionVertex (TxnPeerID txn, String updates, int prio, boolean isAccepted, boolean isRejected)
	{
		super (txn);

		_updates = updates;
		_curPrio = prio;	
		_curTxn = txn;
		_isAcc = isAccepted;
		_isRej = isRejected;

		setProperties(prio, isAccepted, isRejected, getAttributes());
	}
	
	/**
	 * Make the node "dimmed"
	 * 
	 * @param attrs
	 */
	public void setDimmed() {
		setDimmed(getAttributes(), _isAcc);
	}
	
	public static void setDimmed(Map attr, boolean accepted) {
		GraphConstants.setBackground(attr, UIManager.getColor("TransactionVertex.dimmed.background"));//Color.WHITE);
		GraphConstants.setForeground(attr, UIManager.getColor("TransactionVertex.dimmed.foreground"));//Color.LIGHT_GRAY);
		if (!accepted) {
			GraphConstants.setBorder(attr, BorderFactory.createLineBorder(UIManager.getColor("TransactionVertex.dimmed.border"), 1));
		} else {
			GraphConstants.setBorder(attr, BorderFactory.createLineBorder(UIManager.getColor("TransactionVertex.dimmed.border"), 4));
		}
			
		GraphConstants.setInset(attr, 2);
	}
	/**
	 * Make the node "highlighted"
	 * 
	 * @param attrs
	 */
	public void setHighlighted() {
		setHighlighted(getAttributes(), _isAcc);
	}
	
	public static void setHighlighted(Map attr, boolean accepted){
		GraphConstants.setBackground(attr, UIManager.getColor("TransactionVertex.highlighted.background"));//Color.PINK);
		GraphConstants.setForeground(attr, UIManager.getColor("TransactionVertex.highlighted.foreground"));//Color.BLACK);
		if (!accepted) {
			GraphConstants.setBorder(attr, BorderFactory.createLineBorder(UIManager.getColor("TransactionVertex.normal.border"), 1));
		} else {
			GraphConstants.setBorder(attr, BorderFactory.createLineBorder(UIManager.getColor("TransactionVertex.accepted.border"), 4));
		}
			
		GraphConstants.setInset(attr, 2);
	}
	
	/**
	 * Make the node invisible and borderless
	 * 
	 * @param attrs
	 */
	public static void setInvisible(Map attrs) {
		GraphConstants.setOpaque(attrs, false);
		GraphConstants.setLabelEnabled(attrs, false);
		GraphConstants.setBorder(attrs, BorderFactory.createEmptyBorder());
		//GraphConstants.setBackground(attrs, UIManager.getColor("TransactionVertex.invisible.background"));//Color.WHITE);
		//GraphConstants.setForeground(attrs, UIManager.getColor("TransactionVertex.invisible.foreground"));//Color.WHITE);
		//GraphConstants.setBorder(attrs, BorderFactory.createLineBorder(UIManager.getColor("TransactionVertex.invisible.border"), 1));//Color.WHITE, 1));
	}

	/**
	 * Make the node visible
	 * 
	 * @param attrs
	 */
	public void setVisible(Map attrs) {
		setProperties(_curPrio, _isAcc, _isRej, attrs);
	}

	public void setProperties (int prio, boolean isAccepted, boolean isRejected, Map attrs) {
		// Set the cell graphic properties
		GraphConstants.setOpaque(attrs, true);
		GraphConstants.setAutoSize(attrs, true);

		if (prio < 1) {
			GraphConstants.setBackground(attrs, UIManager.getColor("TransactionVertex.untrusted.background"));//Color.RED);
			GraphConstants.setForeground(attrs, UIManager.getColor("TransactionVertex.untrusted.foreground"));//Color.WHITE);
		} else if (prio < edu.upenn.cis.orchestra.datamodel.TrustConditions.OWN_TXN_PRIORITY){
			GraphConstants.setBackground(attrs, UIManager.getColor("TransactionVertex.trusted.background"));//Color.YELLOW);
			GraphConstants.setForeground(attrs, UIManager.getColor("TransactionVertex.trusted.foreground"));//Color.BLACK);
		} else {//if (prio == 2){
			GraphConstants.setBackground(attrs, UIManager.getColor("TransactionVertex.local.background"));//Color.GREEN);
			GraphConstants.setForeground(attrs, UIManager.getColor("TransactionVertex.local.foreground"));//Color.BLACK);
		}
		if (isRejected) {
			GraphConstants.setBorder(attrs, BorderFactory.createLineBorder(UIManager.getColor("TransactionVertex.rejected.border"), 1));//Color.RED, 1));
		} else if (!isAccepted) {
			GraphConstants.setBorder(attrs, BorderFactory.createLineBorder(UIManager.getColor("TransactionVertex.normal.border"), 1));
		} else {
			GraphConstants.setBorder(attrs, BorderFactory.createLineBorder(UIManager.getColor("TransactionVertex.accepted.border"), 4));
		}
			
		GraphConstants.setInset(attrs, 2);
	}
	
	public String toString ()
	{
		if (_curTxn != null) {
			//return _curTxn.getTid() + _curTxn.getPeerID().toString();
			String prefix = _curTxn.getPeerID().toString().substring(1) + ":";
			if (_isAcc || _isRej)
				prefix = "";
			if (_updates.length() < 15)
				return prefix + _updates;
			else
				return prefix + _updates.substring(0, 12) + "...";
		} else
			return _label;
	}
	
	public TxnPeerID getTxn ()
	{
		return _curTxn;
	}
	
	public int getPriority() {
		return _curPrio;
	}


	public String getToolTipString() {
		return "Peer " + _curTxn.getPeerID().toString().substring(1) + " Txn " + _curTxn.getTid() + ": " + _updates;
	}

}
