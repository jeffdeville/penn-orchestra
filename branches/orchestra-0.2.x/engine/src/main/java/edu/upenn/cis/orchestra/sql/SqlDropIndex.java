package edu.upenn.cis.orchestra.sql;

import edu.upenn.cis.orchestra.Config;

class SqlDropIndex implements ISqlDropIndex {
	/** The name of the schema we're dropping. */
	private final String _index;

	/**
	 * Create a {@code DROP SCHEMA} statement on a given schema.
	 * 
	 * @param schema the schema name
	 */
	SqlDropIndex(final String index) {
		_index = index;
	}

	/** {@inheritDoc} */
	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer("DROP INDEX ");
		buf.append(_index);
		//if (Config.isDB2()) {
		//	buf.append(" RESTRICT");
		//}
		return buf.toString();
	}

}
