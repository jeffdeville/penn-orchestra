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
package edu.upenn.cis.orchestra.localupdates.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.localupdates.ILocalUpdater;
import edu.upenn.cis.orchestra.localupdates.ILocalUpdates;
import edu.upenn.cis.orchestra.localupdates.apply.IApplier;
import edu.upenn.cis.orchestra.localupdates.apply.IApplierFactory;
import edu.upenn.cis.orchestra.localupdates.apply.sql.ApplierFactoryJdbc;
import edu.upenn.cis.orchestra.localupdates.apply.sql.IDerivabilityCheck;
import edu.upenn.cis.orchestra.localupdates.exceptions.LocalUpdatesException;
import edu.upenn.cis.orchestra.localupdates.extract.IExtractor;
import edu.upenn.cis.orchestra.localupdates.extract.IExtractorFactory;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.DBConnectionError;
import edu.upenn.cis.orchestra.localupdates.extract.exceptions.NoExtractorClassException;
import edu.upenn.cis.orchestra.localupdates.extract.sql.ExtractorFactoryJdbc;

/**
 * Extracts and applies updates to the local instance using a JDBC connection.
 * 
 * @author John Frommeyer
 * 
 */
public class LocalUpdaterJdbc implements ILocalUpdater {

	private final String server;
	private final String user;
	private final String password;
	private static final Logger logger = LoggerFactory
			.getLogger(LocalUpdaterJdbc.class);

	private final IExtractor<Connection> extractor;
	private final IApplier<Connection> applier;

	/**
	 * Constructor.
	 * 
	 * @param user
	 * @param password
	 * @param server
	 * @param derivabilityChecker 
	 * 
	 * @throws NoExtractorClassException
	 */
	public LocalUpdaterJdbc(@SuppressWarnings("hiding") final String user,
			@SuppressWarnings("hiding") final String password,
			@SuppressWarnings("hiding") final String server, IDerivabilityCheck derivabilityChecker)
			throws NoExtractorClassException {
		this.user = user;
		this.password = password;
		this.server = server;
		IExtractorFactory<Connection> factory = new ExtractorFactoryJdbc();
		this.extractor = factory.getExtractUpdateInst();
		IApplierFactory<Connection> appFactory = new ApplierFactoryJdbc();
		this.applier = appFactory.getApplyUpdateInst(derivabilityChecker);
	}

	private void rollbackConnection(Exception exception, Connection connection)
			throws LocalUpdatesException {
		try {
			connection.rollback();
		} catch (SQLException rollbackException) {
			logger.error("Failed to rollback after an local update error.",
					rollbackException);
			throw new LocalUpdatesException(
					"There was an error while handling local updates. In addition, the attempt to rollback  also resulted in an exception.",
					exception);
		}
		throw new LocalUpdatesException(exception);
	}

	private void closeConnection(Exception exception, Connection connection)
			throws LocalUpdatesException {
		try {
			connection.close();
		} catch (SQLException closeException) {
			logger.error("Failed close connection after an exception.",
					closeException);
			throw new LocalUpdatesException(
					"There was an error while handling local updates. In addition, the attempt to close the connection also resulted in an exception.",
					exception);
		}
		throw new LocalUpdatesException(exception);
	}

	private void closeConnection(Connection connection)
			throws LocalUpdatesException {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException closeException) {
				throw new LocalUpdatesException("Failed to close connection.",
						closeException);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeedu.upenn.cis.orchestra.localupdates.ILocalUpdater#
	 * extractAndApplyLocalUpdates(edu.upenn.cis.orchestra.datamodel.Peer)
	 */
	// @Override
	public void extractAndApplyLocalUpdates(Peer peer)
			throws DBConnectionError, LocalUpdatesException {

		ILocalUpdates updates = null;
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(server, user, password);
			connection.setAutoCommit(false);
		} catch (SQLException e) {
			if (connection != null) {
				closeConnection(e, connection);
			}
			throw new DBConnectionError(e);
		}

		try {
			updates = extractor.extractTransactions(peer, connection);
			applier.applyUpdates(updates, connection);
			connection.commit();
		} catch (Exception e) {
			rollbackConnection(e, connection);
		} finally {
			closeConnection(connection);
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.upenn.cis.orchestra.localupdates.ILocalUpdater#prepare(IDb)
	 */
	@Override
	public void prepare(IDb db, List<? extends Relation> relations) {
		extractor.prepare(db, relations);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see edu.upenn.cis.orchestra.localupdates.ILocalUpdater#postReconcileHook(edu.upenn.cis.orchestra.dbms.IDb,
	 *      java.util.List)
	 */
	@Override
	public void postReconcileHook(IDb db, List<? extends Relation> relations) {
		extractor.postReconcileHook(db, relations);
	}

}
