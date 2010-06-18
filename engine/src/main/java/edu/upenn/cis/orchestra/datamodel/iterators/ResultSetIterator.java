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
package edu.upenn.cis.orchestra.datamodel.iterators;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;


public abstract class ResultSetIterator<T> implements ResultIterator<T> {
	protected boolean backwards = false;
	protected boolean empty = false;
	protected T current = null;
	protected ResultSet rs;

	public ResultSetIterator(ResultSet rs) throws SQLException {
		this.rs = rs;
		this.empty = !rs.first();
		if (!this.empty) {
			rs.previous();
		}
	}

	public void close() throws IteratorException {
		try {
			rs.close();
		} catch (SQLException e) {
			throw new IteratorException("Error closing ResultSet", e);
		}
	}

	public boolean hasNext() throws IteratorException {
		if (empty) {
			return false;
		}
		try {
			/*
			// Disable this for now since it's a Java6 method
			if (rs.isClosed()) {
				throw new IllegalStateException("Cannot invoke a hasNext on a closed ResultSet");
			}
			*/
			if (backwards) {
				return current != null;
			}
			
			boolean hasNext = rs.next();
			// Back up to where we were. We never want to
			// be at the row after the last row since when
			// the resultset is pointing at the last row
			// our iterator has already read it and is
			// off the end of the set
			rs.previous();
			return hasNext;
		} catch (SQLException e) {
			throw new IteratorException(e);
		}
	}

	public boolean hasPrev() throws IteratorException {
		if (empty) {
			return false;
		}
		try {
			/*
			// Disable this for now since it's a Java6 method
			if (rs.isClosed()) {
				throw new IllegalStateException("Cannot invoke a hasPrev on a closed ResultSet");
			}
			*/
			if (! backwards) {
				return current != null;
			}
			boolean hasPrev = rs.previous();
			rs.next();
			return hasPrev;
		} catch (SQLException e) {
			throw new IteratorException(e);
		}
	}

	public T next() throws IteratorException, NoSuchElementException {
		if (backwards) {
			if (current != null) {
				backwards = false;
				return current;
			} else {
				throw new NoSuchElementException();
			}
		}
		try {
			boolean hasNext = rs.next();
			if (! hasNext) {
				throw new NoSuchElementException();
			}
			current = readCurrent();
			return current;
		} catch (SQLException e) {
			throw new IteratorException(e);
		}
	}

	public T prev() throws IteratorException, NoSuchElementException {
		if (! backwards) {
			if (current != null) {
				backwards = true;
				return current;
			} else {
				throw new NoSuchElementException();
			}
		}
		try {
			boolean hasPrev = rs.previous();
			if (! hasPrev) {
				throw new NoSuchElementException();
			}
			current = readCurrent();
			return current;
		} catch (SQLException e) {
			throw new IteratorException(e);
		}
	}

	public abstract T readCurrent() throws IteratorException;
}

