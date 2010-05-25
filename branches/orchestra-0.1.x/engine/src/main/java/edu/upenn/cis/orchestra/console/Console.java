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
package edu.upenn.cis.orchestra.console;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xml.sax.SAXException;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datalog.DatalogSequence;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.StringPeerID;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.exceptions.RelationNotFoundException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.datamodel.iterators.IteratorException;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultSetIterator;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.exchange.BasicEngine;
import edu.upenn.cis.orchestra.exchange.flatfile.FileDb;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.ProvenanceRelation.ProvRelType;
import edu.upenn.cis.orchestra.reconciliation.Db;
import edu.upenn.cis.orchestra.reconciliation.DbException;
import edu.upenn.cis.orchestra.reconciliation.Decision;
import edu.upenn.cis.orchestra.reconciliation.USDump;
import edu.upenn.cis.orchestra.repository.dao.RepositorySchemaDAO;
import edu.upenn.cis.orchestra.repository.dao.flatfile.FlatFileRepositoryDAO;

public class Console {
	protected OrchestraSystem m_catalog;
	protected BufferedReader m_in;
	protected PrintStream m_out;
	protected PrintStream m_err;
	protected Formatter m_formatter; // Formatter attached to m_out
	protected static final String VERSION = "ORCHESTRA Interactive Console v0.1 (beta)";

	protected static String repeat(char c, int number) {
		char[] str = new char[number];
		for (int i = 0; i < number; i++) {
			str[i] = c;
		}
		return new String(str);
	}

	protected void exec(String command, File dir) throws CommandException {
		try {
			Process proc = Runtime.getRuntime().exec(command, null, dir);

			// create threads to read the process stdout and stderr streams 
			IOThread outThread = new IOThread(proc.getInputStream(), m_out);
			IOThread errThread = new IOThread(proc.getInputStream(), m_err);

			// start both threads
			outThread.start();
			errThread.start();

			// wait for process to end
			proc.waitFor();

			// finish reading whatever's left in the buffers
			outThread.join();
			errThread.join();

		} catch (Exception e) {
			throw new CommandException(e);
		}
	}

	protected void update(String rule, boolean deletion) throws CommandException {
		try {
			Rule r = Rule.parse(getCatalog(), rule);
			r.getHead().setNeg(deletion);
			BasicEngine eng = getCatalog().getMappingEngine();
			int count = eng.evalUpdateRule(r);
			m_out.println(count + " tuples updated");
		} catch (java.text.ParseException e) {
			throw new CommandException(e);
		} catch (RelationNotFoundException e) {
			throw new CommandException(e);
		} catch (SQLException e) {
			throw new CommandException(e);
		} catch (Exception e) {
			throw new CommandException(e);
		}
	}

	protected void query(String rule) throws CommandException {
		try {
			Rule r = Rule.parse(getCatalog(), rule);

			// See if we are outputting to a relation in the schema
			try {
				for (edu.upenn.cis.orchestra.datamodel.Schema s : getCatalog().getAllSchemas())
					if (s.getRelation(r.getHead().getRelation().getName()) != null) {
						break;
					}
			} catch (RelationNotFoundException rnf) {
				r.getHead().getRelation().setLabeledNulls(false);
			}


			BasicEngine eng = getCatalog().getMappingEngine();
			ResultSetIterator<Tuple> result = eng.evalQueryRule(r);
			int count = 0;
			while (result != null && result.hasNext()) {
				Tuple tuple = result.next();
				m_out.println(tuple.toString());
				count++;
			}
			m_out.println(count + " results total");
		} catch (java.text.ParseException e) {
			throw new CommandException(e);
		} catch (IteratorException e) {
			throw new CommandException(e);
		} catch (RelationNotFoundException e) {
			throw new CommandException(e);
		} catch (Exception e) {
			throw new CommandException(e);
		}
	}

	protected void runQueryProgram(String filename) throws CommandException {
		try {
			getCatalog().runMaterializedQuery(filename);
		} catch (java.text.ParseException e) {
			throw new CommandException(e);
		} catch (RelationNotFoundException e) {
			throw new CommandException(e);
		} catch (FileNotFoundException f) {
			File cur = new File(".");
			try {
				throw new CommandException("Unable to find file " + cur.getCanonicalPath() + File.separator + filename);
			} catch (IOException ie) {
				throw new CommandException(f);
			}
		} catch (Exception e) {
			throw new CommandException(e);
		}
	}

	protected ConsoleCommand[] _cmds = new ConsoleCommand[] {
			new BaseCommand("help", "", "Print this help message") {
				public void myExecute(Map<String,String> params) {
					int nmax = 0;
					int pmax = 0;
					for (ConsoleCommand cmd : _cmds) {
						nmax = Math.max(nmax, cmd.name().length());
						pmax = Math.max(pmax, cmd.params().length());
					}
					String format = "%-" + nmax + "s  %-" + pmax + "s  %s%n";
					for (ConsoleCommand cmd : _cmds) {
						m_formatter.format(format, cmd.name(), cmd.params(), cmd.help());
					}
				}
			},
			new BaseCommand("catalog", "", "Dump the system catalog") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem catalog = getCatalog();
					catalog.serialize(m_out);
				}
			},
			new BaseCommand("reset", "", "Clear all tables and reset update store") {
				public void myExecute(Map<String,String> params) throws CommandException {
					try {
						getCatalog().reset();
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("clear", "", "Clear all tables") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem catalog = getCatalog();
					BasicEngine engine = catalog.getMappingEngine();
					try {
						engine.softReset();
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("subtractLInsDel", "", "Subtract local deletions from local insertions and clear local deletions") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem catalog = getCatalog();
					BasicEngine engine = catalog.getMappingEngine();
					try {
						engine.subtractLInsDel();
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("create-noscript", "", "Initially create tables") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem catalog = getCatalog();
					BasicEngine engine = catalog.getMappingEngine();
					try {
						engine.createBaseSchemaRelations();
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("drop", "", "Reset and drop all tables") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem catalog = getCatalog();
					BasicEngine engine = catalog.getMappingEngine();
					try {
						getCatalog().reset();
						engine.clean();
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("startUSS", "", "Start update store server") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem catalog = getCatalog();
					try {
						catalog.startStoreServer();
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("stopUSS", "", "Start update store server") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem catalog = getCatalog();
					try {
						catalog.stopStoreServer();
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("copy", "", "Copy base tables to nameOLD") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem catalog = getCatalog();
					BasicEngine engine = catalog.getMappingEngine();
					try {
						engine.copyBaseTables();
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("compare", "", "Compare base tables to nameOLD (stored copies of past run)") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem catalog = getCatalog();
					BasicEngine engine = catalog.getMappingEngine();
					try {
						engine.compareBaseTablesWithCopies();
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("exec", "workdir command...", "Execute a system command") {
				public void myExecute(Map<String,String> params) throws CommandException {
					File dir = new File(params.get("workdir"));
					String command = params.get("command");
					exec(command, dir);
				}
			},
			new BaseCommand("examine", "(peer schema)? table", "Examine the contents of a table") {
				protected void appendVars(int count, StringBuffer buf) {
					for (int i = 0; i < count; i++) {
						if (i != 0) {
							buf.append(", ");
						}
						buf.append("x" + i);
					}
				}

				protected RelationContext getRelation(String peer, String schema, String table) throws CommandException, RelationNotFoundException {
					OrchestraSystem catalog = getCatalog();
					if (peer != null) {
						assert(schema != null);
						return catalog.getRelationByName(peer, schema, table);
					} else {
						assert(schema == null);
						List<RelationContext> list = catalog.getRelationsByName(table);
						if (list.size() == 1) {
							return list.get(0);
						} else {
							throw new RelationNotFoundException(table);
						}
					}
				}

				public void myExecute(Map<String,String> params) throws CommandException {
					String peer = params.get("peer");
					String schema = params.get("schema");
					String table = params.get("table");
					OrchestraSystem catalog = getCatalog();
					if (catalog.getRecMode()) {
						// reconciliation relation?
						try {
							Db db = catalog.getRecDb(peer);
							ResultIterator<Tuple> result = db.getRelationContents(table);
							while (result.hasNext()) {
								Tuple tuple = result.next();
								m_out.println(tuple.toString());
							}
						} catch (DbException e) {
							throw new CommandException(e);
						} catch (IteratorException e) {
							throw new CommandException(e);
						}
					} else {
						try {
							// update exchange relation?
							String prefix = Atom.getPrefix(table);
							RelationContext rc = getRelation(peer, schema, prefix);
							StringBuffer buf = new StringBuffer();
							int count = rc.getRelation().getFields().size();
							buf.append(rc.toString());
							buf.append("(");
							appendVars(count, buf);
							buf.append(") :- ");
							buf.append(rc.getPeer().getId() + "." + rc.getSchema().getSchemaId() + "." + table);
							buf.append("(");
							appendVars(count, buf);
							buf.append(")");
							query(buf.toString());
						} catch (RelationNotFoundException re) {
							throw new CommandException(re);
						}
					}
				}
			},
			new BaseCommand("create", "schema?", "Create initial translation database") {
				public void myExecute(Map<String,String> params) throws CommandException {
					String schema = params.get("schema");
					if (schema == null) {
						schema = Config.getTestSchemaName() + ".create";
					} else {
						schema = schema + ".create";
					}
					File dir = new File(Config.getWorkDir());
					String cmdline = "db2cmd /c /w /i db2 -tf " + schema;
					m_out.println(cmdline);
					exec(cmdline, dir);
				}
			},
			new BaseCommand("db2cmd", "params...", "Execute db2cmd /c /w /i db2 -tf params...") {
				public void myExecute(Map<String,String> params) throws CommandException {
					String command = params.get("params");
					File dir = new File(Config.getWorkDir());
					String cmdline = "db2cmd /c /w /i db2 -tf " + command;
					exec(cmdline, dir);
				}
			},
			new BaseCommand("cmd", "params...", "Execute cmd /c params...") {
				public void myExecute(Map<String,String> params) throws CommandException {
					String command = params.get("params");
					File dir = new File(Config.getWorkDir());
					String cmdline = "cmd /c " + command;
					exec(cmdline, dir);
				}
			},
			new BaseCommand("import-bulk", "workload?", "Import updates from bulk load workload files") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem system = getCatalog();
					BasicEngine engine = system.getMappingEngine();
					String prefix = params.get("workload");
					if (prefix == null) {
						prefix = Config.getWorkloadPrefix();
					}
					try {
						engine.importUpdates(new FileDb(prefix, Config.getImportExtension()));
					} catch (Exception e) {
						throw new CommandException(e);
					}

				}
			},
			new BaseCommand("import-noscript", "path", "Import updates from bulk load workload files at path") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem system = getCatalog();
					BasicEngine engine = system.getMappingEngine();
					String path = params.get("path");
					ArrayList<String> succ = new ArrayList<String>();
					ArrayList<String> fail = new ArrayList<String>();
					try {
						engine.importUpdates(null, path, succ, fail);
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("commit", "", "Commit changes") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem system = getCatalog();
					BasicEngine engine = system.getMappingEngine();
					try {
						engine.commit();
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("runStats", "", "Run statistics on all tables") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem system = getCatalog();
					BasicEngine engine = system.getMappingEngine();
					try {
						boolean runstats = Config.getRunStatistics();
						Config.setRunStatistics(true);
						engine.commit();
						engine.finalize();
						Config.setRunStatistics(runstats);
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("recompute", "", "Recompute delta rules") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem system = getCatalog();
					BasicEngine engine = system.getMappingEngine();
					try {
						engine.cleanupPreparedStmts();
						engine.computeDeltaRules();
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("migrate", "", "Migrate tables to expanded schema") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem system = getCatalog();
					BasicEngine engine = system.getMappingEngine();
					try {
						engine.migrate();
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("translate", "peer", "Translate and exchange pending updates") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem system = getCatalog();
					String pName = params.get("peer");
					try {
						system.translate();
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("generateDeltaRules", "", "Recompute update exchange rules") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem system = getCatalog();
					BasicEngine engine = system.getMappingEngine();
					try {
						engine.cleanupPreparedStmts();
						engine.computeDeltaRules();
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("join", "mappings...", "Create (inner) join of mapping tables for given mappings in specified order") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem system = getCatalog();
					BasicEngine engine = system.getMappingEngine();
					String mappings = params.get("mappings");

					String[] mappingList = mappings.split(" ");
					try {
						engine.joinMappingRelations(mappingList, ProvRelType.INNER_JOIN);
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("ojoin", "mappings...", "Create outer join of mapping tables for given mappings in specified order") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem system = getCatalog();
					BasicEngine engine = system.getMappingEngine();
					String mappings = params.get("mappings");

					String[] mappingList = mappings.split(" ");
					try {
						engine.joinMappingRelations(mappingList, ProvRelType.OUTER_JOIN);
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("lojoin", "mappings...", "Create left outer join of mapping tables for given mappings in specified order") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem system = getCatalog();
					BasicEngine engine = system.getMappingEngine();
					String mappings = params.get("mappings");

					String[] mappingList = mappings.split(" ");
					try {
						engine.joinMappingRelations(mappingList, ProvRelType.LEFT_OUTER_JOIN);
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("rojoin", "mappings...", "Create right outer join of mapping tables for given mappings in specified order") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem system = getCatalog();
					BasicEngine engine = system.getMappingEngine();
					String mappings = params.get("mappings");

					String[] mappingList = mappings.split(" ");
					try {
						engine.joinMappingRelations(mappingList, ProvRelType.RIGHT_OUTER_JOIN);
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("sojoin", "mappings...", "Create simulated outer join of mapping tables for given mappings in specified order") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem system = getCatalog();
					BasicEngine engine = system.getMappingEngine();
					String mappings = params.get("mappings");

					String[] mappingList = mappings.split(" ");
					try {
						engine.joinMappingRelations(mappingList, ProvRelType.SIMULATED_OUTER_JOIN);
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("slojoin", "mappings...", "Create simulated left outer join of mapping tables for given mappings in specified order") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem system = getCatalog();
					BasicEngine engine = system.getMappingEngine();
					String mappings = params.get("mappings");

					String[] mappingList = mappings.split(" ");
					try {
						engine.joinMappingRelations(mappingList, ProvRelType.SIMULATED_LEFT_OUTER_JOIN);
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("srojoin", "mappings...", "Create simulated right outer join of mapping tables for given mappings in specified order") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem system = getCatalog();
					BasicEngine engine = system.getMappingEngine();
					String mappings = params.get("mappings");

					String[] mappingList = mappings.split(" ");
					try {
						engine.joinMappingRelations(mappingList, ProvRelType.SIMULATED_RIGHT_OUTER_JOIN);
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("union", "mappings...", "Create outer union of mapping tables for given mappings") {
				public void myExecute(Map<String,String> params) throws CommandException {
					OrchestraSystem system = getCatalog();
					BasicEngine engine = system.getMappingEngine();
					String mappings = params.get("mappings");

					String[] mappingList = mappings.split(" ");
					try {
						engine.unionMappingRelations(mappingList);
					} catch (Exception e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("mappings", "type?", "Print mappings (optional type can be inv, ins, or del)") {
				public void myExecute(Map<String,String> params) throws CommandException {
					String type = params.get("type");
					OrchestraSystem system = getCatalog();
					BasicEngine engine = system.getMappingEngine();
					if (type == null) {
						List<Mapping> mappings = system.getAllSystemMappings(true);
						for (Mapping s: mappings) {
							m_out.println(s.toString());
						}
					} else if (type.equals("inv")) {
						List<Rule> ruleSet = engine.getTranslationRules();
						for (Rule r: ruleSet) {
							m_out.println(r.toString());
						}
					} else if (type.equals("ins")) {
						List<DatalogSequence> insRules = engine.getIncrementalInsertionProgram();
						for (DatalogSequence seq : insRules) {
							m_out.println(seq.toString());
						}
					} else if (type.equals("del")) {
						List<DatalogSequence> delRules = engine.getIncrementalDeletionProgram();
						for (DatalogSequence seq : delRules) {
							m_out.println(seq.toString());
						}
					} else {
						throw new CommandException(shortUsage());
					}
				}
			},
			new BaseCommand("version", "", "Print version information") {
				public void myExecute(Map<String,String> params) {
					m_out.println(VERSION);
				}
			},
			new BaseCommand("batch", "filename", "Execute commands from a file") {
				public void myExecute(Map<String,String> params) throws CommandException {
					File f = new File(params.get("filename"));
					BufferedReader in = m_in;
					try {
						FileInputStream str = new FileInputStream(f);
						m_in = new BufferedReader(new InputStreamReader(str));
						processInput(false);
						m_in = in;
					} catch (FileNotFoundException e) {
						m_in = in;
						throw new CommandException(e);
					} catch (IOException e) {
						m_in = in;
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("get", "key?", "Print the value of a configuration variable (or all variables)") {
				public void myExecute(Map<String,String> params) {
					String key = params.get("key");
					if (key != null) {
						String value = Config.getProperty(key);
						m_out.println(key + "=" + value);
					} else {
						Config.dumpParams(m_out);
					}
				}
			},
			new BaseCommand("query", "rule...", "Execute a query (specified by datalog rule)") {
				public void myExecute(Map<String,String> params) throws CommandException {
					String rule = params.get("rule");
					query(rule);
				}
			},
			new BaseCommand("run", "file...", "Load a recursive datalog program from a file (period indicates end of each program in sequence)") {
				public void myExecute(Map<String,String> params) throws CommandException {
					String file = params.get("file");
					runQueryProgram(file);
				}
			},
			new BaseCommand("insert", "rule...", "Perform an insertion update (specified by datalog rule)") {
				public void myExecute(Map<String,String> params) throws CommandException {
					String rule = params.get("rule");
					update(rule, false);
				}
			},
			new BaseCommand("delete", "rule...", "Perform a deletion update (specified by datalog rule)") {
				public void myExecute(Map<String,String> params) throws CommandException {
					String rule = params.get("rule");
					update(rule, true);
				}
			},
			new BaseCommand("quit", "", "Exit this application") {
				public void myExecute(Map<String,String> params) {
					System.exit(0);
				}
			},
			new BaseCommand("connect", "", "Connect to the translation database") {
				public void myExecute(Map<String,String> params) throws CommandException {
					IDb db = getCatalog().getMappingDb();
					//					IDb updateDb = getCatalog().getUpdateDb();
					try {
						db.connect();
						m_out.println("Connected to " + db.getServer() + " as " + db.getUsername());
						/*						if (updateDb != null) {
							updateDb.connect();
							m_out.println("Connected to " + updateDb.getServer() + " as " + updateDb.getUsername());
						}
						 */					} catch (Exception e) {
							 throw new CommandException(e);
						 }
				}
			},
			new BaseCommand("disconnect", "", "Disconnect from the translation and reconciliation databases") {
				public void myExecute(Map<String,String> params) throws CommandException {
					//					IDb updateDb = getCatalog().getUpdateDb();
					try {
						getCatalog().disconnect();
						m_out.println("Disconnected from " + getCatalog().getMappingDb().getServer());
						/*						if (updateDb != null) {
							updateDb.disconnect();
							m_out.println("Disconnected from " + updateDb.getServer());
						}
						 */					} catch (Exception e) {
							 throw new CommandException(e);
						 }
				}
			},
			new BaseCommand("set", "key=value...", "Set a configuration variable") {
				public void myExecute(Map<String,String> params) throws CommandException {
					String key = params.get("key");
					String value = params.get("value").trim();
					Config.setProperty(key, value);
					System.out.println("EXP: =========================================");
					System.out.println("EXP:" + key + "=" + value);
					System.out.println("EXP: =========================================");
				}
			},
			new BaseCommand("conflicts", "peer", "View the conflicts for a particular peer") {
				@Override
				protected void myExecute(Map<String, String> args) throws CommandException {
					String peer = args.get("peer");
					String header = "%-10s %-10s %-10s %s%n";
					m_formatter.format(header, "Recno", "Conflict", "Option", "Transactions");
					m_formatter.format(header, repeat('-',10), repeat('-',10), repeat('-',10), repeat('-', 15));
					try {
						Db db = getCatalog().getRecDb(peer);
						Map<Integer,List<List<Set<TxnPeerID>>>> conflicts = db.getConflicts();
						for (Map.Entry<Integer,List<List<Set<TxnPeerID>>>> me : conflicts.entrySet()) {
							final int recno = me.getKey();
							List<List<Set<TxnPeerID>>> conflictsForRecno = me.getValue();
							final int numConflicts = conflictsForRecno.size();
							for (int conflictId = 0; conflictId < numConflicts; ++conflictId) {
								List<Set<TxnPeerID>> options = conflictsForRecno.get(conflictId);
								final int numOptions = options.size();
								for (int option = 0; option < numOptions; ++option) {
									m_formatter.format("%-10d %-10d %-10d %s%n", recno, conflictId, option, options.get(option));
								}
							}
						}
					} catch (DbException e) {
						throw new CommandException(e);
					}
				}

			},
			new BaseCommand("decisions", "peer", "View the decisions for a particular peer") {

				@Override
				protected void myExecute(Map<String, String> args) throws CommandException {
					String peer = args.get("peer");
					String header = "%-10s %-15s %s%n";
					m_formatter.format(header, "Recno", "Transaction", "Decision");
					m_formatter.format(header, repeat('-',10), repeat('-',15), repeat('-', 10));
					try {
						Db db = getCatalog().getRecDb(peer);
						ResultIterator<Decision> decisions = db.getDecisions();
						while (decisions.hasNext()) {
							Decision d = decisions.next();
							m_formatter.format("%-10d %-15s %s%n", d.recno, d.tpi, d.accepted ? "ACCEPTED" : "REJECTED");
						}
						decisions.close();
					} catch (DbException e) {
						throw new CommandException(e);
					} catch (IteratorException e) {
						throw new CommandException(e);
					}
				}
			},
			new BaseCommand("transaction", "tid peer", "View the contents of a published transaction") {

				@Override
				protected void myExecute(Map<String, String> args) throws CommandException {
					try {
						int tid;
						String peer;
						String tpi = args.get("tpi");
						if (tpi != null) {
							int atIndex = tpi.indexOf('@');
							if (atIndex == -1) {
								throw new CommandException("tpi must be of the form tid@peer");
							}
							tid = Integer.parseInt(tpi.substring(0,atIndex));
							peer = tpi.substring(atIndex + 1);
						} else {
							tid = Integer.parseInt(args.get("tid"));
							peer = args.get("peer");
						}
						AbstractPeerID pid = new StringPeerID(peer);
						Db db = getCatalog().getRecDb(peer);
						TxnPeerID tidObj = new TxnPeerID(tid, pid);
						List<Update> txn = db.getTransaction(tidObj);
						if (txn == null) {
							m_out.println("Transaction " + tidObj + " not found");
						} else {
							for (Update u : txn) {
								m_out.println(u);
							}
						}
					} catch (NumberFormatException nfe) {
						throw new CommandException("Invalid transaction ID",nfe);
					} catch (DbException e) {
						throw new CommandException(e);
					}
				}
			}, new ConsoleCommand() {

				public void execute(String[] args) throws CommandException {
					if (args.length < 5 || (args.length % 3) != 2) {
						throw new CommandException("Input does not match (recno conflict option)+");
					}

					String peer = args[1];

					Map<Integer,Map<Integer,Integer>> resolutions = new HashMap<Integer,Map<Integer,Integer>>();
					try {
						for (int i = 2; i < args.length; i += 3) {
							int recno = Integer.parseInt(args[i]);
							int conflict = Integer.parseInt(args[i+1]);
							int option = Integer.parseInt(args[i+2]);

							if (recno < 0) {
								throw new CommandException("Invalid recno, " + recno);
							}

							if (conflict < 0) {
								throw new CommandException("Invalid conflict number, " + conflict);
							}

							Map<Integer,Integer> resForRecno = resolutions.get(recno);
							if (resForRecno == null) {
								resForRecno = new HashMap<Integer,Integer>();
								resolutions.put(recno, resForRecno);
							}
							if (option >= 0) {
								resForRecno.put(conflict, option);
							} else {
								resForRecno.put(conflict, null);
							}
						}
					} catch (NumberFormatException nfe) {
						throw new CommandException("Invalid recno, conflict, or option", nfe);
					}

					Db db;
					try {
						db = getCatalog().getRecDb(peer);
						db.resolveConflicts(resolutions);
					} catch (DbException e) {
						throw new CommandException(e);
					}
				}

				public String help() {
					return "Resolve one or more conflicts";
				}

				public String name() {
					return "resolve";
				}

				public String params() {
					return "(recno conflict option)+";
				}

			}, new BaseCommand("reconcile", "peer", "Perform reconciliation for the specified peer") {
				@Override
				protected void myExecute(Map<String, String> args) throws CommandException {
					String peer = args.get("peer");

					try {
						getCatalog().reconcile();
					} catch (DbException e) {
						throw new CommandException(e);
					}
				}
			}, new BaseCommand("fetch", "peer", "Publish a peer's _INS/_DEL updates") {
				@Override
				protected void myExecute(Map<String, String> args) throws CommandException {
					//Peer is unused because fetch() now only publishes for the OrchestraSystem's local peer.
					String peer = args.get("peer");

					try {
						int count = getCatalog().fetch();
						m_out.println("Added " + count + " update transactions.");
					} catch (Exception x) {
						throw new CommandException(x);
					}
				}
			}, new BaseCommand("publish", "peer", "Publish a peer's pending updates") {
				@Override
				protected void myExecute(Map<String, String> args) throws CommandException {
					String peer = args.get("peer");

					try {
						Db db = getCatalog().getRecDb(peer);
						db.publish();
					} catch (DbException e) {
						throw new CommandException(e);
					}
				}
			}, new BaseCommand("conditions", "peer", "Show peer trust conditions") {
				@Override
				protected void myExecute(Map<String, String> args) throws CommandException {
					String peer = args.get("peer");
					try {
						m_out.println(getCatalog().getRecDb(peer).getTrustConditions());
					} catch (DbException e) {
						throw new CommandException(e);
					}
				}
			}, new BaseCommand("dump", "peer file", "Dump update store state using peer's schema") {
				@Override
				protected void myExecute(Map<String, String> args) throws CommandException {
					String peer = args.get("peer");
					Peer p = getCatalog().getPeer(peer);
					String file = args.get("file");

					try {
						USDump dump = getCatalog().dump(p);
						FileOutputStream fos = new FileOutputStream(file);
						ObjectOutputStream oos = new ObjectOutputStream(fos);
						oos.writeObject(dump);
						oos.close();
						fos.close();
					} catch (DbException de) {
						throw new CommandException(de);
					} catch (IOException e) {
						throw new CommandException(e);
					}
				}

			}, new BaseCommand("restore", "peer file", "Restore update store state from file") {
				@Override
				protected void myExecute(Map<String, String> args) throws CommandException {
					String peer = args.get("peer");
					Peer p = getCatalog().getPeer(peer);
					String file = args.get("file");
					try {
						FileInputStream fis = new FileInputStream(file);
						ObjectInputStream ois = new ObjectInputStream(fis);
						USDump dump = (USDump) ois.readObject();
						ois.close();
						fis.close();
						getCatalog().restore(dump);
					} catch (IOException e) {
						throw new CommandException(e);
					} catch (DbException de) {
						throw new CommandException(de);
					} catch (ClassNotFoundException e) {
						throw new CommandException(e);
					}
				}
			}
	};

	protected Comparator<ConsoleCommand> _comparator = new Comparator<ConsoleCommand>() {
		public int compare(ConsoleCommand c1, ConsoleCommand c2) {
			return c1.name().compareTo(c2.name());
		}
	};

	public OrchestraSystem getCatalog() throws CommandException {
		if (m_catalog == null) {
			try {
				RepositorySchemaDAO dao = new FlatFileRepositoryDAO(Config.getSchemaFile());
				m_catalog = dao.loadAllPeers();
			} catch (UnsupportedTypeException ute) {
				throw new CommandException(ute);
			} catch (FileNotFoundException e) {
				throw new CommandException(e);
			} catch (IOException e) {
				throw new CommandException(e);
			} catch (SAXException e) {
				throw new CommandException(e);
			} catch (edu.upenn.cis.orchestra.util.XMLParseException e) {
				throw new CommandException(e);
			} catch (Exception e) {
				throw new CommandException(e);
			}
		}
		return m_catalog;
	}

	public void setCatalog(OrchestraSystem catalog) {
		m_catalog = catalog;
	}

	public Console() throws IOException {
		this(System.in, System.out, System.err);
	}

	public Console(InputStream in, OutputStream out, OutputStream err) {
		this(in, new PrintStream(out), new PrintStream(err));
	}

	public Console(InputStream in, PrintStream out, PrintStream err) {
		m_out = out;
		m_err = err;
		m_in = new BufferedReader(new InputStreamReader(in));
		Arrays.sort(_cmds, _comparator);
		m_formatter = new Formatter(m_out);
	}

	protected Vector<ConsoleCommand> findCommand(String name) {
		Vector<ConsoleCommand> matches = new Vector<ConsoleCommand>();
		for (ConsoleCommand cmd : _cmds) {
			if (cmd.name().startsWith(name)) {
				matches.add(cmd);
			}
		}
		return matches;
	}

	protected Pattern m_pat = Pattern.compile("\\$(\\w+)");

	protected String[] expand(String[] args) {
		String[] exp = new String[args.length];
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < args.length; i++) {
			String str = args[i];
			buf.setLength(0);
			Matcher m = m_pat.matcher(str);
			int last = 0;
			while (m.find()) {
				buf.append(str.substring(last, m.start()));
				buf.append(Config.getProperty(m.group(1)));
				last = m.end();
			}
			buf.append(str.substring(last, str.length()));
			exp[i] = buf.toString();
		}
		return exp;
	}

	public void processLine(String[] args) throws CommandException {
		if (args[0].equals("?")) {
			findCommand("help").get(0).execute(args);
		} else {
			Vector<ConsoleCommand> matches = findCommand(args[0]);
			switch (matches.size()) {
			case 0:
				throw new CommandException("Unrecognized command \'" + args[0] + "\'.  Use \'?\' for help.");
			case 1:
				args = expand(args);
				matches.get(0).execute(args);
				break;
			default:
				StringBuffer buf = new StringBuffer();
			for (ConsoleCommand cmd : matches) {
				buf.append(cmd.name());
				buf.append(", ");
			}
			buf.setLength(buf.length()-2);
			throw new CommandException("Ambiguous command \'" + args[0] + "\' (" + buf.toString() + ").");
			}
		}
	}

	public void prompt(boolean showversion) {
		if (showversion) {
			m_out.println(VERSION);
		}
		try {
			m_out.print(getCatalog().getName());
		} catch (CommandException e) {
			m_out.println(e.toString());
		}
		m_out.print("> ");
		m_out.flush();
		m_err.flush();
	}

	public void processInput(boolean interactive) throws IOException {
		if (interactive) {
			prompt(true);
		}
		while (true) {
			String line = m_in.readLine();
			if (line == null) {
				break;
			} else if (!interactive) {
				m_out.println("> " + line);
			}
			line = line.trim();
			String[] args = line.split("\\s+");
			if (args.length > 0 && args[0].length() > 0 && !args[0].startsWith("#")) {
				try {
					processLine(args);
				} catch (CommandException e) {
					if (Config.getDebug()) {
						e.printStackTrace(m_out);
					}
					m_out.println(e.getMessage());
				}
			}
			if (interactive) {
				prompt(false);
			}
		}
	}

	public static class IOThread extends Thread {
		protected PrintStream m_out;
		protected InputStream m_in;

		public IOThread(InputStream in, PrintStream out) {
			m_out = out;
			m_in = in;
		}

		public void run() {
			try {
				int ch;
				while (-1 != (ch = m_in.read())) {
					m_out.write(ch);
				}
			} catch (IOException e) {
				m_out.println("Read error: " + e.getMessage());
			}
		}	
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		//		String jdbcDriver = Config.getUSJDBCDriver();
		String jdbcDriver = Config.getJDBCDriver();
		Class.forName(jdbcDriver);
		if (args.length > 0 && !args[0].startsWith("-")) {
			System.err.println("Usage: java " + Class.class.getName() + " [-command args] ...");
			System.exit(-1);
		} else {
			Console cons = new Console();
			for (int i = 0; i < args.length; ) {
				int j;
				for (j = i+1; j < args.length; j++) {
					if (args[j].startsWith("-")) {
						break;
					}
				}
				String[] subargs = new String[j-i];
				subargs[0] = args[i].substring(1);
				System.arraycopy(args, i+1, subargs, 1, j-i-1);
				try {
					cons.processLine(subargs);
				} catch (CommandException e) {
					System.err.println(e.getMessage());
					System.exit(-1);
				}
				i = j;
			}
			cons.processInput(true);
		}
	}
}
