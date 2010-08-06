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

import javax.swing.JInternalFrame;
import javax.swing.JOptionPane;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.reconciliation.DbException;

public class TransactionIFrame extends JInternalFrame {
	public static final long serialVersionUID = 1L;
	TransactionViewer _transView;

	public TransactionIFrame (Peer p, OrchestraSystem sys)
	{
		super ("Transaction view: " + p.getId(), true, true, true, true);
		try {
			_transView = new TransactionViewer(p, sys);
			add (_transView);
			//_transView.setVisible(true);
		} catch (DbException e) {
			JOptionPane.showMessageDialog(this, e.getMessage(), "Error accessing DB", JOptionPane.ERROR_MESSAGE);
			e.printStackTrace();
		} 
	}
	
	public void close() {
		/*
		try {
			_transView.tearDown();
		} catch (Exception e) {
			e.printStackTrace();
		}*/
	}
}
