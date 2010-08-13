/*
 * Copyright (C) 2009 Trustees of the University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.upenn.cis.orchestra.sql.dtp;

import static edu.upenn.cis.orchestra.sql.dtp.SqlDtpUtil.getSQLQueryParserFactory;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.datatools.modelbase.sql.query.OrderBySpecification;
import org.eclipse.datatools.modelbase.sql.query.QueryValueExpression;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParserFactory;

import edu.upenn.cis.orchestra.sql.ISqlExp;
import edu.upenn.cis.orchestra.sql.ISqlOrderByItem;

/**
 * A DTP backed {@code ISqlOrderByItem}.
 * 
 * @author John Frommeyer
 */
class DtpSqlOrderByItem extends AbstractSQLQueryObject<OrderBySpecification>
		implements ISqlOrderByItem {

	private final OrderBySpecification _orderBySpecification;

	DtpSqlOrderByItem(QueryValueExpression valueExpression) {
		this(valueExpression, OrderType.NONE, NullOrderType.NONE);
	}
	
	DtpSqlOrderByItem(QueryValueExpression valueExpression,
			OrderType orderType, NullOrderType nullOrderType) {
		int orderByInt;
		int nullOrderInt;
		
		switch (orderType) {
		case ASC:
			orderByInt = SQLQueryParserFactory.ORDERING_SPEC_TYPE_ASC;
			break;
		case DESC:
			orderByInt = SQLQueryParserFactory.ORDERING_SPEC_TYPE_DESC;
			break;
		case NONE:
			orderByInt = SQLQueryParserFactory.ORDERING_SPEC_TYPE_NONE;
			break;
		default:
			throw new IllegalArgumentException("Illegal order type: " + orderType);
		}
		
		switch (nullOrderType) {
		case NULLS_FIRST:
			nullOrderInt = SQLQueryParserFactory.NULL_ORDERING_TYPE_NULLS_FIRST;
			break;
		case NULLS_LAST:
			nullOrderInt = SQLQueryParserFactory.NULL_ORDERING_TYPE_NULLS_LAST;
			break;
		case NONE:
			nullOrderInt = SQLQueryParserFactory.NULL_ORDERING_TYPE_NONE;
			break;
		default:
			throw new IllegalArgumentException("Illegal null order type: " + orderType);
		}
		
		_orderBySpecification = getSQLQueryParserFactory()
				.createOrderByExpression(valueExpression, orderByInt,
						nullOrderInt);
	}

    /** {@inheritDoc} */
	@Override
	public OrderBySpecification getSQLQueryObject() {
		return _orderBySpecification;
	}

	public boolean isBoolean() { return false; }

	public ISqlExp addOperand(ISqlExp e) {
		return null;
	}
	
	public ISqlExp getOperand(int i) {
		return null;
	}
	
	public List<ISqlExp> getOperands() {
		return new ArrayList<ISqlExp>();
	}
}
