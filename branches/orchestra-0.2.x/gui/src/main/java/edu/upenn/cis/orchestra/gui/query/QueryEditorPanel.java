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
package edu.upenn.cis.orchestra.gui.query;

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
import java.util.List;

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
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.gui.schemas.RelationDataEditorIntf;
import edu.upenn.cis.orchestra.gui.schemas.RelationDataModel;
import edu.upenn.cis.orchestra.mappings.exceptions.RecursionException;

public class QueryEditorPanel extends JPanel {
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
	
	public QueryEditorPanel(OrchestraSystem system) {
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
		
		queryPart.setBorder(BorderFactory.createTitledBorder("Datalog query"));
		
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
					QueryEditorPanel.this.openQueryFile();
				}
			});
		btnSave.addActionListener(new ActionListener ()
		{
			public void actionPerformed(ActionEvent arg0) {
				QueryEditorPanel.this.saveQueryFile();
			}
		});
		btnRun.addActionListener(new ActionListener ()
		{
			public void actionPerformed(ActionEvent arg0) {
				try {
					QueryEditorPanel.this.runQuery();
				} catch (Exception e) {
					JOptionPane.showMessageDialog(QueryEditorPanel.this, e.getMessage(), "Error executing query", JOptionPane.ERROR_MESSAGE);
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
		_results.setCellRenderer(new TupleRenderer());
		
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
	private void runQuery() throws Exception {
		StringReader sr = new StringReader(_query.getText());
		
		List<Tuple> results = new ArrayList<Tuple>();
		
		// Try to run it as non-recursive: more efficient
		try {
			results = _system.runUnfoldedQuery(new BufferedReader(sr), false);
			
		// If the program had recursion, we need to run it differently
		} catch (RecursionException re) {
			sr = new StringReader(_query.getText());
			results = _system.runMaterializedQuery(new BufferedReader(sr));
		}
		
		_data.clear();
		
//		// TODO:  get the results!!!
		for (Tuple tuple : results) {
			//_results.append(tuple.toString() + "\n");
			_data.addElement(tuple);
		}
	}
}
