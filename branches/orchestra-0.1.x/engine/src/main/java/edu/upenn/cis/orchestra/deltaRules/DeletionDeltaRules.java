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
	
	/**
	 * Create executable deletion rules from {@code code}.
	 * 
	 * @param code
	 */
	protected DeletionDeltaRules(List<DatalogSequence> code, boolean containsBidirectionalMappings) {
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

			System.out
					.println("\n=====================================================");
			System.out.println("DELETIONS");
			System.out
					.println("=====================================================");

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
					System.out.println("UPDATE POLICY PREP TIME: " + time
							+ " msec");

					before = Calendar.getInstance();
					de.evaluatePrograms(updPolicy);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					System.out.println("EXP: UPDATE POLICY COMP TIME: " + time
							+ " msec");

					before = Calendar.getInstance();
					de.evaluatePrograms(updPolPost);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					System.out.println("UPDATE POLICY POST TIME: " + time
							+ " msec");

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
					System.out.println("PART 1 TIME: " + time + " msec");
					totalHackTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(upd);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					System.out.println("EXP: UPDATE POLICY COMP TIME: " + time
							+ " msec");
					totalTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(post1);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					System.out.println("PART 3 TIME: " + time + " msec");
					totalHackTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(seDetectPrep);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					System.out.println("PART 4 TIME: " + time + " msec");
					totalHackTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(seDetectMaint);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					System.out.println("EXP: SE DETECT MAINTENANCE TIME: "
							+ time + " msec");
					totalTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(seDetectPost);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					System.out.println("PART 6 TIME: " + time + " msec");
					totalHackTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(subtract1);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					System.out
							.println("EXP: SUBTRACT1 TIME: " + time + " msec");
					totalTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(lineagePrep);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					System.out.println("PART 8 TIME: " + time + " msec");
					totalHackTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(lineage);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					System.out.println("EXP: LINEAGE TIME: " + time + " msec");
					totalTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(lineagePost);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					System.out.println("PART 10 TIME: " + time + " msec");
					totalHackTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(subtract2);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					System.out
							.println("EXP: SUBTRACT2 TIME: " + time + " msec");
					totalTime += time;

					before = Calendar.getInstance();
					de.evaluatePrograms(post2);
					after = Calendar.getInstance();
					time = after.getTimeInMillis() - before.getTimeInMillis();
					System.out.println("PART 12 TIME: " + time + " msec");
					totalHackTime += time;

					System.out.println("EXP: TOTAL UPD POL + SE DETECT TIME: "
							+ totalTime + " msec");
					System.out.println("EXP: TOTAL HACK TIME: " + totalHackTime
							+ " msec");
				}
				delPrep = delProg.get(delProg.size() - 3);
				delMaint = delProg.get(delProg.size() - 2);
				delPost = delProg.get(delProg.size() - 1);
			}

			before = Calendar.getInstance();
			de.evaluatePrograms(delPrep);
			after = Calendar.getInstance();
			time = after.getTimeInMillis() - before.getTimeInMillis();
			System.out.println("DELETION PREP TIME: " + time + " msec");

			de.commitAndReset();

			boolean recomputeQueries = !Config.getPrepare() && Config.getStratified();

			before = Calendar.getInstance();
			de.evaluatePrograms(delMaint, recomputeQueries);
			after = Calendar.getInstance();
			time = after.getTimeInMillis() - before.getTimeInMillis();
			System.out.println("INCREMENTAL DELETION ALG TIME  (INCL COMMIT): "
					+ time + " msec");
			System.out
					.println("EXP: TIME SPENT FOR COMMIT AND LOGGING DEACTIVATION: "
							+ de.logTime() + " msec");
			System.out.println("EXP: TIME SPENT FOR EMPTY CHECKING: "
					+ de.emptyTime() + " msec");
			System.out.println("EXP: NET DELETION TIME: "
					+ (time - de.logTime()) + " msec");

			SqlEngine.delTimes.add(new Long(time - de.logTime()));
			retTime = time - de.logTime();


			de.commitAndReset();

			before = Calendar.getInstance();
			de.evaluatePrograms(delPost);
			after = Calendar.getInstance();
			time = after.getTimeInMillis() - before.getTimeInMillis();
			System.out.println("POST DELETION TIME: " + time + " msec");

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
