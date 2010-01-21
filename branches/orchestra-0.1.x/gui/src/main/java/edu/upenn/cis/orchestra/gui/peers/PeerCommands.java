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

import java.awt.Component;
import java.awt.Cursor;

import javax.swing.JOptionPane;
import javax.swing.SwingWorker;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.reconciliation.DbException;
import edu.upenn.cis.orchestra.reconciliation.StateStore.SSException;

public class PeerCommands {

	/**
	 * The message which appears in the {@code JOptionPane} when Reconciliation
	 * was successful.
	 */
	public static final String RECONCILE_SUCCESS_MESSAGE = "Reconciliation was successful";
	/**
	 * The message which appears in the {@code JOptionPane} when Publication was
	 * successful.
	 */
	public static final String PUBLISH_SUCCESS_MESSAGE = "Publication was successful";

	public static void reconcile(final Component parentComp,
			final OrchestraSystem sys, final Peer p,
			final PeerTransactionsIntf peerTransIntf) {
		if (!sys.isLocalPeer(p)) {
			JOptionPane.showMessageDialog(parentComp,
					"You cannot reconcile the non-local peer " + p.getId()
							+ ".", "Reconciliation",
					JOptionPane.WARNING_MESSAGE);
			return;
		}

		if ((peerTransIntf == null)
				|| (peerTransIntf != null && !peerTransIntf
						.hasCurrentTransaction())) {
			if (sys.getRecMode()) {
				changeCursor(parentComp, true);
				new SwingWorker<Void, Void>() {
					@Override
					protected Void doInBackground() throws SSException,
							DbException {
						sys.reconcile();
						return null;
					}

					@Override
					protected void done() {
						peerTransIntf.setRefreshDataViews(false);
						changeCursor(parentComp, false);
						try {
							get();
							JOptionPane.showMessageDialog(parentComp,
									RECONCILE_SUCCESS_MESSAGE,
									"Reconciliation",
									JOptionPane.INFORMATION_MESSAGE);
						} catch (Exception ex) {
							ex.printStackTrace();
							JOptionPane.showMessageDialog(parentComp,
									"Error while reconciling: "
											+ ex.getMessage(), "Error",
									JOptionPane.ERROR_MESSAGE);
						}
					}
				}.execute();
			} else {
				changeCursor(parentComp, true);
				new SwingWorker<Void, Void>() {
					@Override
					protected Void doInBackground() throws Exception {
						if (!sys.getMappingDb().isConnected())
							sys.getMappingDb().connect();

						// Now run the Exchange
						sys.translate();
						return null;
					}

					@Override
					protected void done() {
						if (peerTransIntf != null)
							peerTransIntf.setRefreshDataViews(false);
						changeCursor(parentComp, false);
						try {
							get();
							JOptionPane.showMessageDialog(parentComp,
									RECONCILE_SUCCESS_MESSAGE,
									"Reconciliation",
									JOptionPane.INFORMATION_MESSAGE);
						} catch (Exception ex) {
							JOptionPane.showMessageDialog(parentComp, ex
									.getMessage(), "Error reconciling",
									JOptionPane.ERROR_MESSAGE);
							ex.printStackTrace();
						}
					}
				}.execute();
			}
		} else {
			JOptionPane
					.showMessageDialog(
							parentComp,
							"You cannot reconcile unless you commit or rollback your changes first.",
							"Current transaction", JOptionPane.WARNING_MESSAGE);
		}
	}

	public static void publish(Component parentComp, OrchestraSystem sys,
			PeerTransactionsIntf peerTransIntf) {
		if (peerTransIntf != null && !peerTransIntf.hasCurrentTransaction())
			try {
				sys.fetch();
				JOptionPane.showMessageDialog(parentComp,
						"Publication was successful", "Publication",
						JOptionPane.INFORMATION_MESSAGE);
			} catch (SSException ex) {
				ex.printStackTrace();
				JOptionPane.showMessageDialog(parentComp,
						"Error while publishing: " + ex.getMessage(), "Error",
						JOptionPane.ERROR_MESSAGE);
			} catch (Exception ex) {
				ex.printStackTrace();
				JOptionPane.showMessageDialog(parentComp,
						"Error while publishing: " + ex.getMessage(), "Error",
						JOptionPane.ERROR_MESSAGE);
			}
		else
			JOptionPane
					.showMessageDialog(
							parentComp,
							"You cannot publish unless you commit or rollback your changes first.",
							"Current transaction", JOptionPane.WARNING_MESSAGE);
	}

	public static void publishAndReconcile(final Component parentComp,
			final OrchestraSystem sys, final PeerTransactionsIntf peerTransIntf) {
		if (peerTransIntf != null && !peerTransIntf.hasCurrentTransaction()) {
			try {
				sys.fetch();
			} catch (SSException ex) {
				ex.printStackTrace();
				JOptionPane.showMessageDialog(parentComp,
						"Error while publishing: " + ex.getMessage(), "Error",
						JOptionPane.ERROR_MESSAGE);
				return;
			} catch (Exception ex) {
				ex.printStackTrace();
				JOptionPane.showMessageDialog(parentComp,
						"Error while publishing: " + ex.getMessage(), "Error",
						JOptionPane.ERROR_MESSAGE);
				return;
			}
		} else {
			JOptionPane
					.showMessageDialog(
							parentComp,
							"You cannot publish or reconcile unless you commit or rollback your changes first.",
							"Current transaction", JOptionPane.WARNING_MESSAGE);
			return;
		}

		if (sys.getRecMode()) {
			changeCursor(parentComp, true);
			new SwingWorker<Void, Void>() {
				@Override
				protected Void doInBackground() throws SSException, DbException {
					sys.reconcile();
					return null;
				}

				@Override
				protected void done() {
					peerTransIntf.setRefreshDataViews(false);
					changeCursor(parentComp, false);
					try {
						get();
						JOptionPane.showMessageDialog(parentComp,
								RECONCILE_SUCCESS_MESSAGE, "Reconciliation",
								JOptionPane.INFORMATION_MESSAGE);
					} catch (Exception ex) {
						ex.printStackTrace();
						JOptionPane.showMessageDialog(parentComp,
								"Error while reconciling: " + ex.getMessage(),
								"Error", JOptionPane.ERROR_MESSAGE);
					}
				}
			}.execute();
		} else {
			changeCursor(parentComp, true);
			new SwingWorker<Void, Void>() {
				@Override
				protected Void doInBackground() throws Exception {
					if (!sys.getMappingDb().isConnected())
						sys.getMappingDb().connect();

					// Now run the Exchange
					sys.translate();
					return null;
				}

				@Override
				protected void done() {
					peerTransIntf.setRefreshDataViews(false);
					changeCursor(parentComp, false);
					try {
						get();
						JOptionPane.showMessageDialog(parentComp,
								RECONCILE_SUCCESS_MESSAGE, "Reconciliation",
								JOptionPane.INFORMATION_MESSAGE);
					} catch (Exception ex) {
						JOptionPane.showMessageDialog(parentComp, ex
								.getMessage(), "Error reconciling",
								JOptionPane.ERROR_MESSAGE);
						ex.printStackTrace();
					}
				}
			}.execute();
		}

	}

	public static void changeCursor(Component parentComp, boolean isWait) {
		parentComp.setCursor(Cursor
				.getPredefinedCursor(isWait ? Cursor.WAIT_CURSOR
						: Cursor.DEFAULT_CURSOR));
	}

}
