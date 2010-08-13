package edu.upenn.cis.orchestra.datamodel.exceptions;

import schemacrawler.schema.Table;

public class IncompatibleRelationException extends Exception {

	public static final long serialVersionUID=1L;
	
	public Table _table;

	public IncompatibleRelationException ()
	{
		super ();
	}

	public IncompatibleRelationException (String msg, Table t)
	{
		super (msg);
		_table = t;
	}
	
	public Table getTable() {
		return _table;
	}
}
