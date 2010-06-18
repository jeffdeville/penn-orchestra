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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datalog.DatalogEngine;
import edu.upenn.cis.orchestra.datalog.DatalogSequence;
import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.exchange.sql.SqlEngine;

/**
 * The rules for incremental deletion during update exchange.
 * 
 * @author zives, gkarvoun, John Frommeyer
 * 
 */
class DeletionDeltaRules extends DeltaRules {

	private boolean bidirectional;
	private static final Logger logger = LoggerFactory
			.getLogger(DeletionDeltaRules.class);

	/**
	 * Create executable deletion rules from {@code code}.
	 * 
	 * @param code
	 */
	protected DeletionDeltaRules(List<DatalogSequence> code,
			boolean containsBidirectionalMappings) {
		super(code);
		bidirectional = containsBidirectionalMappings;
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
		// Map to each field it's database datatype. This is necessary
		// because DB2 needs to cast null values!!!
		try {
			Calendar before = Calendar.getInstance();
			Calendar after;
			// long insertionTime = 0;
			long time;
			long retTime;

			logger.debug("DELETIONS START");

			de.commitAndReset();

			if (de._sql instanceof SqlDb)
				((SqlDb) de._sql).activateRuleBasedOptimizer();

			List<DatalogSequence> delProg = getCode();

			DatalogSequence delPrep;
			DatalogSequence delMaint;
			DatalogSequence delPost;

			if (!bidirectional) {
				delPrep = delProg.get(0);
				delMaint = delProg.get(1);
				delPost = delProg.get(2);
			} else {
				if (Config.getAllowSideEffects()) {
					DatalogSequence updPolPrep = delProg.get(0);
					DatalogSequence updPolicy = delProg.get(1);
					DatalogSequence updPolPost = delProg.get(2);

					before = Calendar.getInstance();
					de.evaluatePrograms(updPolPrep);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					logger.debug("UPDATE POLICY PREP TIME: {} msec", Long
							.valueOf(time));

					before = Calendar.getInstance();
					de.evaluatePrograms(updPolicy);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					logger.debug("EXP: UPDATE POLICY COMP TIME: {} msec", Long
							.valueOf(time));

					before = Calendar.getInstance();
					de.evaluatePrograms(updPolPost);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					logger.debug("UPDATE POLICY POST TIME: {} msec", Long
							.valueOf(time));

					de.commitAndReset();
				} else {
					long totalTime = 0;
					long totalHackTime = 0;
					DatalogSequence prep = delProg.get(0);
					DatalogSequence upd = delProg.get(1);
					DatalogSequence post1 = delProg.get(2);
					DatalogSequence seDetectPrep = delProg.get(3);
					DatalogSequence seDetectMaint = delProg.get(4);
					DatalogSequence seDetectPost = delProg.get(5);
					DatalogSequence subtract1 = delProg.get(6);
					DatalogSequence lineagePrep = delProg.get(7);
					DatalogSequence lineage = delProg.get(8);
					DatalogSequence lineagePost = delProg.get(9);
					DatalogSequence subtract2 = delProg.get(10);
					DatalogSequence post2 = delProg.get(11);

					before = Calendar.getInstance();
					de.evaluatePrograms(prep);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					logger.debug("PART 1 TIME: {} msec", Long.valueOf(time));
					totalHackTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(upd);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					logger.debug("EXP: UPDATE POLICY COMP TIME: {} msec", Long
							.valueOf(time));
					totalTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(post1);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					logger.debug("PART 3 TIME: {} msec", Long.valueOf(time));
					totalHackTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(seDetectPrep);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					logger.debug("PART 4 TIME: {} msec", Long.valueOf(time));
					totalHackTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(seDetectMaint);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					logger.debug("EXP: SE DETECT MAINTENANCE TIME: {} msec",
							Long.valueOf(time));
					totalTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(seDetectPost);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					logger.debug("PART 6 TIME: {} msec", Long.valueOf(time));
					totalHackTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(subtract1);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					logger.debug("EXP: SUBTRACT1 TIME: {} msec", Long
							.valueOf(time));
					totalTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(lineagePrep);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					logger.debug("PART 8 TIME: {} msec", Long.valueOf(time));
					totalHackTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(lineage);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					logger.debug("EXP: LINEAGE TIME: {} msec", Long
							.valueOf(time));
					totalTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(lineagePost);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					logger.debug("PART 10 TIME: {} msec", Long.valueOf(time));
					totalHackTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(subtract2);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					logger.debug("EXP: SUBTRACT2 TIME: {} msec", Long
							.valueOf(time));
					totalTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(post2);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					logger.debug("PART 12 TIME: {} msec", Long.valueOf(time));
					totalHackTime += time;

					logger.debug(
							"EXP: TOTAL UPD POL + SE DETECT TIME: {} msec",
							Long.valueOf(totalTime));
					logger.debug("EXP: TOTAL HACK TIME: {} msec", Long
							.valueOf(totalHackTime));
				}
				delPrep = delProg.get(delProg.size() - 3);
				delMaint = delProg.get(delProg.size() - 2);
				delPost = delProg.get(delProg.size() - 1);
			}

			before = Calendar.getInstance();
			de.evaluatePrograms(delPrep);
			after = Calendar.getInstance();
			time = after.getTimeInMillis() - before.getTimeInMillis();
			logger.debug("DELETION PREP TIME: {} msec", Long.valueOf(time));

			de.commitAndReset();

			boolean recomputeQueries = !Config.getPrepare()
					&& Config.getStratified();

			before = Calendar.getInstance();
			de.evaluatePrograms(delMaint, recomputeQueries);
			after = Calendar.getInstance();
			time = after.getTimeInMillis() - before.getTimeInMillis();
			logger.debug(
					"INCREMENTAL DELETION ALG TIME  (INCL COMMIT): {} msec",
					Long.valueOf(time));
			logger
					.debug(
							"EXP: TIME SPENT FOR COMMIT AND LOGGING DEACTIVATION: {} msec",
							Long.valueOf(de.logTime()));
			logger.debug("EXP: TIME SPENT FOR EMPTY CHECKING: "
					+ de.emptyTime() + " msec");
			logger.debug("EXP: NET DELETION TIME: {} msec", Long.valueOf(time
					- de.logTime()));

			SqlEngine.delTimes.add(new Long(time - de.logTime()));
			retTime = time - de.logTime();

			de.commitAndReset();

			before = Calendar.getInstance();
			de.evaluatePrograms(delPost);
			after = Calendar.getInstance();
			time = after.getTimeInMillis() - before.getTimeInMillis();
			logger.debug("POST DELETION TIME: {} msec", Long.valueOf(time));

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
		root.setAttribute("type", "Deletion");
		return doc;
	}
}
