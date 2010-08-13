package edu.upenn.cis.orchestra.gui.proql;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.filechooser.FileNameExtensionFilter;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.DatalogSequence;
import edu.upenn.cis.orchestra.datalog.DatalogViewUnfolder;
import edu.upenn.cis.orchestra.datalog.NonRecursiveDatalogProgram;
import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datalog.atom.Atom.AtomType;
import edu.upenn.cis.orchestra.deltaRules.DeltaRuleGen;
import edu.upenn.cis.orchestra.exchange.sql.SqlEngine;
import edu.upenn.cis.orchestra.gui.schemas.RelationDataEditorIntf;
import edu.upenn.cis.orchestra.gui.schemas.RelationDataModel;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.mappings.exceptions.RecursionException;
import edu.upenn.cis.orchestra.proql.MatchPatterns;
import edu.upenn.cis.orchestra.proql.Pattern;
import edu.upenn.cis.orchestra.proql.ProQL;
import edu.upenn.cis.orchestra.proql.QueryParser;
import edu.upenn.cis.orchestra.proql.SchemaGraph;
import edu.upenn.cis.orchestra.proql.SchemaSubgraph;
import edu.upenn.cis.orchestra.provenance.OuterJoinUnfolder;
import edu.upenn.cis.orchestra.provenance.ProvenanceNode;

public class ProQLEditorPanel extends JPanel {
	JTextArea _query;
	JList _results;
	DefaultListModel _data;
	OrchestraSystem _system;
	private JFileChooser _queryFileChooser = null;
	private RelationDataModel _model = null;
	private RelationDataEditorIntf relDataEdit;

	private JTable _relationTable = null;
	private JPanel _relationPanel = null;
	private JScrollPane _relationScroller = null;

	public ProQLEditorPanel(OrchestraSystem system) {
		_system = system;

		setLayout(new BorderLayout());

		init();
	}

	private void init() {
		/*
		 * Query sub-pane includes label, text area, buttons
		 */
		final JPanel queryPart = new JPanel();
		queryPart.setLayout(new BorderLayout());
		_query = new JTextArea();

		queryPart.setBorder(BorderFactory.createTitledBorder("ProQL query"));

		JScrollPane editorScrollPane = new JScrollPane(_query);
		editorScrollPane.setVerticalScrollBarPolicy(
				JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);
		editorScrollPane.setHorizontalScrollBarPolicy(
				JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		editorScrollPane.setPreferredSize(new Dimension(250, 145));
		editorScrollPane.setMinimumSize(new Dimension(10, 10));

		queryPart.add(editorScrollPane, BorderLayout.CENTER);

		JPanel buttons = new JPanel();
		buttons.setLayout(new GridLayout(3, 1));
		final JButton btnLoad = new JButton ("Load");
		final JButton btnSave = new JButton ("Save");
		final JButton btnRun = new JButton ("Run");
		btnLoad.addActionListener(new ActionListener ()
		{
			public void actionPerformed(ActionEvent arg0) {
				ProQLEditorPanel.this.openQueryFile();
			}
		});
		btnSave.addActionListener(new ActionListener ()
		{
			public void actionPerformed(ActionEvent arg0) {
				ProQLEditorPanel.this.saveQueryFile();
			}
		});
		btnRun.addActionListener(new ActionListener ()
		{
			public void actionPerformed(ActionEvent arg0) {
				try {
					ProQLEditorPanel.this.runQuery(_query.getText(), true);
				} catch (Exception e) {
					JOptionPane.showMessageDialog(ProQLEditorPanel.this, e.getMessage(), "Error executing query", JOptionPane.ERROR_MESSAGE);
					e.printStackTrace();
				}
			}
		});
		buttons.add (btnLoad);
		buttons.add (btnSave);
		buttons.add (btnRun);
		queryPart.add(buttons, BorderLayout.EAST);

		/*
		 * Results sub-area includes label + answers
		 */
		final JPanel resultPart = new JPanel();
		resultPart.setLayout(new BorderLayout());
		_data = new DefaultListModel();
		_results = new JList(_data);
		_results.setCellRenderer(new edu.upenn.cis.orchestra.gui.query.TupleRenderer());

		//JLabel lab2 = new JLabel("Query results:");
		resultPart.setBorder(BorderFactory.createTitledBorder("Query results"));
		JScrollPane resultsScrollPane = new JScrollPane(_results);
		resultsScrollPane.setVerticalScrollBarPolicy(
				JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);
		resultsScrollPane.setHorizontalScrollBarPolicy(
				JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		resultsScrollPane.setPreferredSize(new Dimension(250, 145));
		resultsScrollPane.setMinimumSize(new Dimension(10, 10));

		resultPart.add(resultsScrollPane, BorderLayout.CENTER);

		/*
		 * Assemble the main pane
		 */
		add(queryPart, BorderLayout.NORTH);
		add(resultPart, BorderLayout.CENTER);
	}

	/**
	 * Select a file to open for a query
	 */
	private void openQueryFile()
	{
		if (_queryFileChooser == null) {
			_queryFileChooser = new JFileChooser(Config.getWorkDir());

			FileNameExtensionFilter filter = new FileNameExtensionFilter(
					"Datalog queries", "datalog");
			_queryFileChooser.setFileFilter(filter);
		}
		_queryFileChooser.setDialogTitle("Select a datalog file");
		int returnVal = _queryFileChooser.showOpenDialog(this);

		try {
			if (returnVal == JFileChooser.APPROVE_OPTION) {
				File file = _queryFileChooser.getSelectedFile();

				String fName = file.getAbsolutePath();

				loadQuery(fName);
			}
		} catch (IOException e) {
			JOptionPane.showMessageDialog(this, e.getMessage(), "I/O error loading datalog file", JOptionPane.ERROR_MESSAGE);
			e.printStackTrace();

		} catch (Exception e) {
			JOptionPane.showMessageDialog(this, e.getMessage(), "Error loading datalog", JOptionPane.ERROR_MESSAGE);
			e.printStackTrace();

		}
	}

	/**
	 * Select a filename to save to
	 */
	private void saveQueryFile()
	{
		if (_queryFileChooser == null) {
			_queryFileChooser = new JFileChooser(Config.getWorkDir());
			_queryFileChooser.setDialogTitle("Select a datalog file");

			FileNameExtensionFilter filter = new FileNameExtensionFilter(
					"Datalog queries", "datalog");
			_queryFileChooser.setFileFilter(filter);
		}
		_queryFileChooser.setDialogTitle("Enter a filename for the datalog file");
		int returnVal = _queryFileChooser.showSaveDialog(this);

		try {
			if (returnVal == JFileChooser.APPROVE_OPTION) {
				File file = _queryFileChooser.getSelectedFile();

				String fName = file.getAbsolutePath();//file.getName();

				saveQuery(fName);
			}
		} catch (IOException e) {
			JOptionPane.showMessageDialog(this, e.getMessage(), "I/O error loading datalog file", JOptionPane.ERROR_MESSAGE);
			e.printStackTrace();

		} catch (Exception e) {
			JOptionPane.showMessageDialog(this, e.getMessage(), "Error loading datalog", JOptionPane.ERROR_MESSAGE);
			e.printStackTrace();

		}
	}

	/**
	 * Loads a query from the named file, into the text area
	 * 
	 * @param filename
	 * @throws IOException
	 */
	private void loadQuery(String filename) throws IOException {

		BufferedReader source = new BufferedReader(new FileReader(filename));

		_query.selectAll();

		String next = source.readLine();
		if (next != null)
			_query.replaceSelection(next + "\n");
		next = source.readLine();
		while (next != null) {
			_query.append(next + "\n");
			next = source.readLine();
		}
		source.close();
	}

	/**
	 * Saves the query text area to the designated file
	 * 
	 * @param filename
	 * @throws IOException
	 */
	private void saveQuery(String filename) throws IOException {

		BufferedWriter f = new BufferedWriter(new FileWriter(filename));

		f.write(_query.getText());
		f.close();
	}

	/**
	 * Tries to execute the query in the current text area
	 * 
	 * @throws Exception
	 */
	private void runQuery(String q, boolean printResults) throws Exception {
		List<Tuple> queryResults = ProQL.runProvenanceQuery(q, printResults, true, _system);
		//			List<Tuple> queryResults = _system.runUnfoldedQuery(programWithASRs, false);

		if(printResults){
			for (Tuple tuple : queryResults) {
				//_results.append(tuple.toString() + "\n");
				_data.addElement(tuple);
			}
		}
		System.out.println("DONE");
		//			DatalogSequence ds = new DatalogSequence(false, false);
		//			ds.add(new NonRecursiveDatalogProgram(programWithASRs));
		//
		//			engine.evaluateProvenanceQuery(ds);
	}
}
