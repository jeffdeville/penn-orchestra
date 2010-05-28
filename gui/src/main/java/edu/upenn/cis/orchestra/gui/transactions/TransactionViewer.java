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

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSlider;
import javax.swing.JSplitPane;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.jgraph.event.GraphSelectionEvent;
import org.jgraph.event.GraphSelectionListener;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.gui.graphs.LegendGraph;
import edu.upenn.cis.orchestra.reconciliation.Db;
import edu.upenn.cis.orchestra.reconciliation.DbException;

public class TransactionViewer extends JPanel {

	public static final long serialVersionUID = 1L;

	private JSplitPane _splitPane;
	private TransactionInfoPanel _transInfoPanel;
	private int _currentEpoch;
	private TransactionGraph _tg;
	private JSlider _slider = new JSlider(JSlider.HORIZONTAL, 0, 0, 0);

	public TransactionViewer(Peer p, OrchestraSystem sys) throws DbException {

		// dbs = new ArrayList<Db>(numPeers);
		// dbs.add(sys.getRecDb(p.getId()));
		// Iterator<Peer> peers = sys.getPeers().iterator();

		/*
		 * while (dbs.size() < numPeers) { if (! peers.hasNext()) { throw new
		 * RuntimeException("System must have at least " + numPeers + " peers");
		 * } String id = peers.next().getId(); if (id.equals(p.getId())) {
		 * continue; } dbs.add(sys.getRecDb(id)); }
		 */

		initFrame(p, _currentEpoch /* setUpTrans() */, sys.getRecDb(p.getId()));
		/*
		 * try { tearDown(); } catch (Exception e) {
		 * 
		 * }
		 */
	}

	private void initFrame(Peer p, int epoch, Db db) {
		setLayout(new BorderLayout());

		// Prepare the split pane
		_splitPane = new JSplitPane();
		_splitPane.setResizeWeight(1.0);

		/*
		 * try { setUpTrans(db); } catch (ClassNotFoundException e) {
		 * e.printStackTrace(); return; }
		 */

		// Db db = dbs.get(dbI);
		// Create the transaction graph
		_tg = new TransactionGraph(p, /* epoch, trans, */db, _slider);
		JPanel pan = new JPanel();
		pan.setLayout(new BorderLayout());

		// Create the graph scroll pane
		JScrollPane scrollPGraph = new JScrollPane(_tg);
		pan.add(scrollPGraph, BorderLayout.CENTER);

		// Create the legend
		JPanel subPan = new JPanel();
		subPan.setBorder(BorderFactory.createTitledBorder("Legend"));
		subPan.setLayout(new BorderLayout());
		// JLabel lab = new JLabel("Legend:");
		// lab.setSize(80, 15);
		// subPan.add(lab, BorderLayout.PAGE_START);
		LegendGraph g = TransactionGraph.createLegend(600, 40, subPan
				.getBackground());
		subPan.add(g, BorderLayout.CENTER);
		g.applyLayoutOnFirstReveal();
		pan.add(subPan, BorderLayout.PAGE_END);

		_splitPane.setLeftComponent(pan);

		_tg.getSelectionModel().addGraphSelectionListener(
				new GraphSelectionListener() {
					public void valueChanged(GraphSelectionEvent evt) {
						if (_tg.getSelectionCount() == 1
								&& _tg.getSelectionCell() instanceof TransactionVertex) {
							TxnPeerID t = ((TransactionVertex) _tg
									.getSelectionCell()).getTxn();
							int prio = ((TransactionVertex) _tg
									.getSelectionCell()).getPriority();
							_transInfoPanel.setTransaction(t, prio);
						} else {
							_transInfoPanel.clearUpdates();

						}
					}
				});

		// Right panel
		JPanel rightPanel = new JPanel();
		rightPanel.setLayout(new GridBagLayout());

		GridBagConstraints cst = new GridBagConstraints();
		cst.gridx = 0;
		cst.gridy = 0;
		cst.gridheight = 1;
		cst.weightx = 0.4;
		cst.weighty = 1;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		cst.fill = GridBagConstraints.HORIZONTAL;

		int num = 0;
		try {
			num = db.getCurrentRecno();
		} catch (DbException d) {
			num = 0;
		}
		// _slider = new JSlider(JSlider.HORIZONTAL, 0, num, _currentEpoch);
		_slider.setMinimum(0);
		_slider.setMaximum(num);
		_slider.setValue(_currentEpoch);
		_slider.setBorder(BorderFactory.createTitledBorder(p.getId()
				+ " Reconciliation Epoch"));
		if (num > 3)
			_slider.setMajorTickSpacing(num / 4);
		else if (num == 0)
			_slider.setEnabled(false);
		else
			_slider.setMajorTickSpacing(1);
		_slider.setMinorTickSpacing(1);

		_slider.setPaintLabels(true);
		_slider.setPaintTicks(true);
		_slider.setSnapToTicks(true);
		_slider.putClientProperty("JSlider.isFilled", Boolean.TRUE);

		_slider.addChangeListener(new ChangeListener() {
			public void stateChanged(ChangeEvent e) {
				try {
					synchronized (this) {
						JSlider source = (JSlider) e.getSource();
						// if (!source.getValueIsAdjusting()) {
						_tg.setEpoch(source.getValue());
						// }
					}
				} catch (DbException d) {

				}
			}
		});

		rightPanel.add(_slider, cst);

		cst = new GridBagConstraints();
		cst.gridx = 0;
		cst.gridy = 1;
		cst.weightx = 0.4;
		cst.weighty = 1;
		cst.anchor = GridBagConstraints.CENTER;
		cst.fill = GridBagConstraints.NONE;
		JButton reload = new JButton("Refresh View");
		reload.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				try {
					computeEpochs();
				} catch (DbException ex) {
					JOptionPane.showMessageDialog(null, ex.getMessage(),
							"Error accessing DB", JOptionPane.ERROR_MESSAGE);
				}
			}
		});
		rightPanel.add(reload, cst);

		// Add the panel used to show selected /schemas properties
		initTransInfoPanel(db);
		cst = new GridBagConstraints();
		cst.gridx = 0;
		cst.gridy = 2;
		cst.weightx = 0.4;
		cst.weighty = 1;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		cst.fill = GridBagConstraints.HORIZONTAL;
		rightPanel.add(_transInfoPanel, cst);

		_splitPane.setRightComponent(rightPanel);

		add(_splitPane, BorderLayout.CENTER);

		// add(_tg, BorderLayout.CENTER);
		// _tg.setVisible(true);
	}

	private void initTransInfoPanel(Db db) {
		_transInfoPanel = new TransactionInfoPanel(db);
	}

	/**
	 * Change the current epoch
	 * 
	 * @param epoch
	 */
	public void setEpoch(int epoch) throws DbException {
		synchronized (this) {
			_currentEpoch = epoch;
			_tg.setEpoch(epoch);
			if (epoch > _slider.getMaximum()) {
				_slider.setValue(_slider.getMaximum());
			}
		}
	}

	public void computeEpochs() throws DbException {
		synchronized (this) {
			int max = _tg.computeAllEpochs();
			_slider.setMinimum(0);
			_slider.setMaximum(max);
			_slider.setEnabled(true);
			if (max > 3)
				_slider.setMajorTickSpacing(max / 4);
			else if (max == 0)
				_slider.setEnabled(false);
			else
				_slider.setMajorTickSpacing(1);
			_slider.setValue(_currentEpoch);
		}
	}

	// ////////////////////////////////////////////////////////////////////////////
	// This is code to set up the test case
	/*
	 * private List<TxnPeerID> setUpTrans(Db peerDB) throws
	 * ClassNotFoundException { ArrayList<TxnPeerID> trans = new
	 * ArrayList<TxnPeerID>();
	 * 
	 * TxnPeerID tid0; TxnPeerID tid1; TxnPeerID tid2; TxnPeerID tid3; try {
	 * setUp(peerDB);
	 * 
	 * ArrayList<Update> peer0 = new ArrayList<Update>(); peer0.add(insN1);
	 * ArrayList<Update> peer1 = new ArrayList<Update>(); peer1.add(modN1N2);
	 * ArrayList<Update> peer2 = new ArrayList<Update>(); peer2.add(modN1N3);
	 * ArrayList<Update> peer3 = new ArrayList<Update>(); peer3.add(modN2N5);
	 * tid0 = dbs.get(0).addTransaction(peer0); dbs.get(0).publish();
	 * dbs.get(0).reconcile(); dbs.get(1).reconcile(); dbs.get(2).reconcile();
	 * tid1 = dbs.get(1).addTransaction(peer1); tid2 =
	 * dbs.get(2).addTransaction(peer2); dbs.get(1).publish();
	 * dbs.get(3).reconcile(); dbs.get(2).publish(); dbs.get(3).reconcile();
	 * tid3 = dbs.get(3).addTransaction(peer3); dbs.get(3).publish();
	 * dbs.get(0).reconcile();
	 * 
	 * trans.add(tid0); trans.add(tid1); trans.add(tid2); trans.add(tid3); }
	 * catch (Exception e) { e.printStackTrace(); }
	 * 
	 * return trans; }
	 */
	/*
	 * protected ArrayList<Db> dbs; Schema s; RelationSchema rs; SchemaTuple
	 * tN1, tN2, tN3, tN4, tN5, tM4, tM5, tNull; Update insN1, insN2, insN3,
	 * modN1N3, modN1N2, modN1N4, modN2N5, insM4, insM5, modM4M5, modN1M5,
	 * delN1, delM5, insNull;
	 * 
	 * static protected final int numPeers = 4;
	 * 
	 * protected void setUp(Db curDb) throws Exception { s = curDb.getSchema();
	 * rs = s.getRelationSchema("R");
	 * 
	 * tN1 = new SchemaTuple(rs); tN1.set("name", "Nick"); tN1.set("val", 1);
	 * tN2 = new SchemaTuple(rs); tN2.set("name", "Nick"); tN2.set("val", 2);
	 * tN3 = new SchemaTuple(rs); tN3.set("name", "Nick"); tN3.set("val", 3);
	 * tN4 = new SchemaTuple(rs); tN4.set("name", "Nick"); tN4.set("val", 4);
	 * tN5 = new SchemaTuple(rs); tN5.set("name", "Nick"); tN5.set("val", 5);
	 * tM4 = new SchemaTuple(rs); tM4.set("name", "Mark"); tM4.set("val", 4);
	 * tM5 = new SchemaTuple(rs); tM5.set("name", "Mark"); tM5.set("val", 5);
	 * tNull = new SchemaTuple(rs); tNull.set("name", "Fred");
	 * tNull.setLabeledNull("val", 999);
	 * 
	 * insN1 = new Update(null, tN1); insN2 = new Update(null, tN2); insN3 = new
	 * Update(null, tN3); modN1N3 = new Update(tN1,tN3); modN1N2 = new
	 * Update(tN1,tN2); modN1N4 = new Update(tN1, tN4); modN2N5 = new
	 * Update(tN2, tN5); insM4 = new Update(null, tM4); insM5 = new Update(null,
	 * tM5); modM4M5 = new Update(tM4, tM5); modN1M5 = new Update(tN1, tM5);
	 * delN1 = new Update(tN1, null); delM5 = new Update(tM5, null); insNull =
	 * new Update(null, tNull);
	 * 
	 * }
	 */

}
