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
package edu.upenn.cis.orchestra.deltaRules;

import java.util.Calendar;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datalog.DatalogEngine;
import edu.upenn.cis.orchestra.datalog.DatalogSequence;
import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.exchange.sql.SqlEngine;

/**
 * Rules for incremental insertion during update exchange.
 * 
 * @author zives, gkarvoun, John Frommeyer
 * 
 */
class InsertionDeltaRules extends DeltaRules {

	/**
	 * Creates executable insertion rules from {@code code}.
	 * 
	 * @param code
	 */
	protected InsertionDeltaRules(List<DatalogSequence> code) {
		super(code);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.upenn.cis.orchestra.deltaRules.DeltaRules#execute(edu.upenn.cis.orchestra
	 * .datalog.DatalogEngine)
	 */
	@Override
	public long execute(DatalogEngine de) throws Exception {
		// dbCn.commit();+
		// _provenancePrep.activateNotLoggedInitDB2(dbCn, system);

		// dbCn.commit();
		// _provenancePrep.collectStatistics(dbCn, system);

		// Map to each field it's database datatype. This is necessary
		// because DB2 needs to cast null values!!!
		try {
			Calendar before;
			Calendar after;
			long time;
			long retTime;

			// if(!"yes".equals(System.getProperty("skipins"))){
			System.out
					.println("=====================================================");
			System.out.println("INSERTIONS");
			System.out
					.println("=====================================================");

			de.commitAndReset();

			if (de._sql instanceof SqlDb)
				((SqlDb) de._sql).activateRuleBasedOptimizer();

			List<DatalogSequence> insProg = getCode();

			before = Calendar.getInstance();
			de.evaluatePrograms(insProg.get(0));
			after = Calendar.getInstance();
			time = after.getTimeInMillis() - before.getTimeInMillis();
			System.out.println("INSERTION PREP TIME: " + time + " msec");

			de.commitAndReset();

			before = Calendar.getInstance();
			de.evaluatePrograms(insProg.get(1));
			after = Calendar.getInstance();
			time = after.getTimeInMillis() - before.getTimeInMillis();
			System.out.println("INCREMENTAL INSERTION ALG TIME (INCL COMMIT): "
					+ time + " msec");
			System.out
					.println("EXP: TIME SPENT FOR COMMIT AND LOGGING DEACTIVATION: "
							+ de.logTime() + " msec");
			System.out.println("EXP: TIME SPENT FOR EMPTY CHECKING: "
					+ de.emptyTime() + " msec");
			System.out.println("EXP: NET INSERTION TIME: "
					+ (time - de.logTime()) + " msec");

			SqlEngine.insTimes.add(new Long(time - de.logTime()));
			retTime = time - de.logTime();

			de.commitAndReset();

			before = Calendar.getInstance();
			de.evaluatePrograms(insProg.get(2));
			after = Calendar.getInstance();
			time = after.getTimeInMillis() - before.getTimeInMillis();
			System.out.println("POST INSERTION TIME: " + time + " msec");

			de.commitAndReset();

			return retTime;

		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
			// return -1;
		}
	}

	@Override
	public Document serialize() {
		Document doc = super.serialize();
		Element root = (Element) doc.getFirstChild();
		root.setAttribute("type", "Insertion");
		return doc;
	}

}
