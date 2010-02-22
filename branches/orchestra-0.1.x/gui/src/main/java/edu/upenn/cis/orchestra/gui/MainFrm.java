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
package edu.upenn.cis.orchestra.gui;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.HeadlessException;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.beans.PropertyVetoException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.BorderFactory;
import javax.swing.DesktopManager;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JDesktopPane;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JInternalFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTextPane;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.UIDefaults;
import javax.swing.UIManager;
import javax.swing.event.InternalFrameEvent;
import javax.swing.event.InternalFrameListener;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.plaf.ColorUIResource;
import javax.swing.plaf.FontUIResource;
import javax.swing.plaf.IconUIResource;
import javax.swing.text.DefaultStyledDocument;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyledDocument;

import org.xml.sax.SAXException;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.exchange.BasicEngine;
import edu.upenn.cis.orchestra.gui.peers.PeerCommands;
import edu.upenn.cis.orchestra.gui.peers.PeerTransactionsIntf;
import edu.upenn.cis.orchestra.gui.peers.PeersMgtPanel;
import edu.upenn.cis.orchestra.gui.peers.PeersMgtPanelObserver;
import edu.upenn.cis.orchestra.gui.utils.JWindowsMenu;
import edu.upenn.cis.orchestra.reconciliation.DbException;
import edu.upenn.cis.orchestra.reconciliation.USDump;
import edu.upenn.cis.orchestra.repository.dao.RepositorySchemaDAO;
import edu.upenn.cis.orchestra.repository.dao.flatfile.FlatFileRepositoryDAO;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class MainFrm extends JFrame 
			implements PeersMgtPanelObserver, InternalFrameListener
{
	public static final long serialVersionUID = 1L;

//	private static SkolemServer _skolemServer = null;
	private final JDesktopPane _desktop = new JDesktopPane ();

	/** File dialog for schema files */ 
	private JFileChooser _schemaFileChooser = null;//
	
	/** File dialog for directory import */
	private JFileChooser _importDirectoryChooser = null;//
	
	/** File dialog for opening dump files */
	private JFileChooser _openDumpChooser = null;
	
	/** File dialog for saving dump files */
	private JFileChooser _saveDumpChooser = null;

	private JMenu _menuPeer = null;
	private JPopupMenu _cxMenuPeer = null;
	private ImageIcon _icon = null;

	private List<JMenuItem> _exchangeModeOnly = new ArrayList<JMenuItem>();
	private List<JMenuItem> _recModeOnly = new ArrayList<JMenuItem>();
	private JMenuItem _mnuItmFileClose;
	
	private JMenuItem _startUS = null, _stopUS = null, _clearUS = null;

	/**
	 * Orchestra about box
	 * @author biton
	 *
	 */
	class AboutBox extends JDialog {
		static final long serialVersionUID = 42;

		public AboutBox (JFrame frame, String centerMode) {
			super(frame, "About ORCHESTRA Collaborative Data Sharing System", true /* modal */);
			ImageIcon logo = null;
			URL url = getClass().getClassLoader().getResource("images/OrchestraLogo.png");
			if (url != null) {
				logo = new ImageIcon(url, "Orchestra");
			}
			Container content = getContentPane();
			JLabel label = new JLabel();
			if (logo != null)
				label.setIcon(logo);
			final JButton ok = new JButton("OK");
			ok.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					dispose ();
				}
			});
			StyledDocument doc = new DefaultStyledDocument();
			SimpleAttributeSet set=new SimpleAttributeSet();
			StyleConstants.setAlignment(set,StyleConstants.ALIGN_CENTER);
			doc.setParagraphAttributes(12, 0, set, false);
			JTextPane text = new JTextPane(doc);
			text.setEditable(false);
			//text.setWrapStyleWord(true);
			//text.setLineWrap(true);
			text.setFont(label.getFont());
			text.setBackground(getBackground());
			String str = "\n\nCopyright (c) 2009-2010 Trustees of the University of Pennsylvania. All rights reserved.\n\n" +
				"A project of the Penn Database Group and Center for Networked Information.\n\n" +
				"Sponsored by NSF IIS-0477972, IIS-0513778, and DARPA HR0011-06-1-0016.\n\n" +
				"http://db.cis.upenn.edu/\n\n";
			text.setText(str);

			JScrollPane scroll = new JScrollPane(text);
			scroll.setBorder(BorderFactory.createEmptyBorder());
			setLayout(new BorderLayout());
			JPanel logopanel = new JPanel();
			logopanel.add(label);
			content.add(logopanel, BorderLayout.NORTH);
			content.add(scroll, BorderLayout.CENTER);
			//content.add(panel);
			JPanel panel = new JPanel();
			panel.add(ok);
			content.add(panel, BorderLayout.SOUTH);
			pack();

			Dimension dim = getSize();
			Dimension txt = text.getPreferredSize();
			Dimension txtCurr = text.getSize();
			dim.width = (int) (dim.getWidth() + txt.getWidth()-txtCurr.getWidth());
			dim.height = (int) (dim.getHeight() + txt.getHeight()-txtCurr.getHeight());
			setSize(dim);

			setLocationRelativeTo(centerMode.equals("W") ? frame : null);

		}
	}

	/**
	 * File import status box
	 * 
	 * @author zives
	 *
	 */
	class ImportBox extends JDialog {
		static final long serialVersionUID = 42;

		public ImportBox (JFrame frame, List<String> succ, List<String> fail) {
			super(frame, "CDSS Data Import Complete", true /* modal */);

			final JButton ok = new JButton("OK");
			ok.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					dispose ();
				}
			});
			StyledDocument doc = new DefaultStyledDocument();
			SimpleAttributeSet set=new SimpleAttributeSet();
			StyleConstants.setAlignment(set,StyleConstants.ALIGN_LEFT);
			doc.setParagraphAttributes(12, 0, set, false);
			JTextPane text = new JTextPane(doc);
			text.setEditable(false);
			text.setFont(frame.getFont());
			text.setBackground(getBackground());
			StringBuffer str = new StringBuffer();
			
			if (succ.size() > 0) {
				str.append("Successfully imported:\n");
				
				for (String s : succ)
					str.append("  " + s + "\n");
			}
			
			if (succ.size() > 0 && fail.size() > 0)
				str.append("\n\n");
			
			if (fail.size() > 0) {
				str.append("Failed to import:\n");
				for (String f : fail)
					str.append("  " + f + "\n");
			}
			
			text.setText(str.toString());

			JScrollPane scroll = new JScrollPane(text);
			scroll.setBorder(BorderFactory.createEmptyBorder());
			setLayout(new BorderLayout());
			JPanel panel = new JPanel();
			panel.add(ok);
			Container content = getContentPane();
			content.add(scroll, BorderLayout.NORTH);
			content.add(panel, BorderLayout.SOUTH);
			pack();

			Dimension dim = getSize();
			Dimension txt = text.getPreferredSize();
			Dimension txtCurr = text.getSize();
			dim.width = (int) (dim.getWidth() + txt.getWidth()-txtCurr.getWidth());
			dim.height = (int) (dim.getHeight() + txt.getHeight()-txtCurr.getHeight());
			setSize(dim);

			setLocationRelativeTo(frame);

		}
	}

	public MainFrm() throws HeadlessException
	{
		super ("ORCHESTRA Collaborative Data Sharing System");
		setName(Config.getTestSchemaName());
		if ("Ajax".equals(Config.getProperty("gui.mode"))){
			setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		} else {
			setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		}
		URL url = getClass().getClassLoader().getResource("images/OrchestraIcon.png");
		_icon = new ImageIcon(url, "Orchestra");
		setIconImage(_icon.getImage());
	}


	public void startup() throws HeadlessException {
		setLayout(new BorderLayout());
		createMainMenu();
		// dinesh ++
		String currSchemaName = Config.getTestSchemaName();
		Config.setCurrSchemaName(currSchemaName);
		// dinesh --
		add(_desktop, BorderLayout.CENTER);

		try {
			setSize(1024, 768);
			final MainIFrame mif = createAndMaximizePeersNetwork();
			setVisible(true);
			try {
				mif.setSelected(true);
				mif.requestFocusInWindow();
			} catch (PropertyVetoException e) {
				assert (false);
			}
			
		} catch (Exception e) {
			JOptionPane.showMessageDialog(this, e.getMessage(),
					"Unable to Open CDSS", JOptionPane.ERROR_MESSAGE);
			e.printStackTrace();
			setVisible(true);
		}
	}
	
	private MainIFrame createAndMaximizePeersNetwork() throws Exception
	{
		return loadNewP2PNetwork(Config.getSchemaFile(), Config.getTestSchemaName());
	}

	/**
	 * Load the Orchestra schema from a file
	 * 
	 * @param schemaFile
	 * @param schema
	 * @return
	 * @throws Exception
	 */
	private MainIFrame loadNewP2PNetwork(String schemaFile, String schema) throws Exception
	{
		OrchestraSystem system = null;
		try {
			RepositorySchemaDAO dao = new FlatFileRepositoryDAO(schemaFile);
			system = dao.loadAllPeers();
			//createRules(_system);
		} catch (SAXException se) {
			throw new XMLParseException("SAX exception: " + se.getMessage());
		} catch (FileNotFoundException fnf) {
			throw new XMLParseException("File not found exception:" + fnf.getMessage());
		} catch (IOException ioe) {
			throw new XMLParseException("I/O error exception:" + ioe.getMessage());
		}
		
		JInternalFrame f = _desktop.getSelectedFrame();
		
		MainIFrame iframe = new MainIFrame(system, schema, schemaFile);
		iframe.getPanel().getPeersMgtPanel().addObserver(this);
		iframe.setSize(1000,750);
		iframe.setVisible(true);
		iframe.setFrameIcon(_icon);
		iframe.addInternalFrameListener(this);
		iframe.setClosable(true);
		_desktop.add(iframe);
		try {
			iframe.setMaximum(true);
		} catch (PropertyVetoException ex) {
			assert(false); // Won't happen, maximizable is set in the constructor
		}

//		if (system.getRecMode()) {
			for (JMenuItem m : _recModeOnly) {
				m.setEnabled(true);
			}
//			for (JMenuItem m : _exchangeModeOnly) {
//				m.setEnabled(false);
//			}

//		} else {
			for (JMenuItem m : _exchangeModeOnly) {
				m.setEnabled(true);
			}
//			for (JMenuItem m : _recModeOnly) {
//				m.setEnabled(false);
//			}
//		}
//		_startUS.setEnabled(true);
//		_stopUS.setEnabled(false);
//		_clearUS.setEnabled(true);

		if (f != null && f instanceof MainIFrame) {
		
		} else {
			iframe.getPanel().startStoreServer();
		}
		_startUS.setEnabled(false);
		_stopUS.setEnabled(true);
		_clearUS.setEnabled(true);
		return iframe;
	}


	/**
	 * For each schema + relation, import contents from a text file
	 * 
	 * @param allPeers
	 */
	private void importPeerData(boolean allPeers)
	{
		if (_importDirectoryChooser == null) {
			_importDirectoryChooser = new JFileChooser(Config.getWorkDir());
			_importDirectoryChooser.setDialogTitle("Select an import directory with delimited files");
			
			_importDirectoryChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
			_importDirectoryChooser.setAcceptAllFileFilterUsed(false);
		}

		int returnVal = _importDirectoryChooser.showOpenDialog(this);

		try {
			if (returnVal == JFileChooser.APPROVE_OPTION) {
				File file = _importDirectoryChooser.getSelectedFile();

				String dir = file.getCanonicalPath();

				//System.out.println(dir);
				
				ArrayList<String> succ = new ArrayList<String>();
				ArrayList<String> fail = new ArrayList<String>();

				getSelectedCatalog().getMappingEngine().importUpdates((allPeers) ? null : getSelectedPeer(),
						dir, succ, fail);
				
				ImportBox i = new ImportBox(this, succ, fail);
				
				i.setVisible(true);
			}
		} catch (IOException e) {
			JOptionPane.showMessageDialog(this, e.getMessage(), "I/O error importing data", JOptionPane.ERROR_MESSAGE);

		} catch (Exception e) {
			JOptionPane.showMessageDialog(this, e.getMessage(), "Error importing data", JOptionPane.ERROR_MESSAGE);

		}
	}
	
	private void openGlobalProperties() {
		try {
			openPropertiesFile("Modify Global Configuration", "global.properties");
		} catch (IOException e) {
			JOptionPane.showMessageDialog(this, "Unable to process global.properties", "Error opening file", JOptionPane.ERROR_MESSAGE);
		} catch (URISyntaxException u) {
			JOptionPane.showMessageDialog(this, "Ill-formed path to global.properties", "Error opening file", JOptionPane.ERROR_MESSAGE);
		}
	}
	
	private void openLocalProperties() {
		try {
			openPropertiesFile("Modify Local Configuration", "local.properties");
		} catch (IOException e) {
			JOptionPane.showMessageDialog(this, "Unable to process local.properties", "Error opening file", JOptionPane.ERROR_MESSAGE);
		} catch (URISyntaxException u) {
			JOptionPane.showMessageDialog(this, "Ill-formed path to local.properties", "Error opening file", JOptionPane.ERROR_MESSAGE);
		}
	}
	
	private void openPropertiesFile(String title, String filename) throws IOException, URISyntaxException {
		URL url = Config.class.getResource(filename);
		
		File file = null;
		
		if (url != null) {
			file = new File(url.toURI());
		} else {
			file = new File(filename);
		}

		ConfigDialog cf = new ConfigDialog(file, title);
		
		cf.setVisible(true);
	}
	
	private void openSchemaFile()
	{
		if (_schemaFileChooser == null) {
			_schemaFileChooser = new JFileChooser(Config.getWorkDir());
			_schemaFileChooser.setDialogTitle("Select a CDSS schema definition file");

			FileNameExtensionFilter filter = new FileNameExtensionFilter(
					"CDSS definitions", "schema");
			_schemaFileChooser.setFileFilter(filter);
		}

		int returnVal = _schemaFileChooser.showOpenDialog(this);

		try {
			if (returnVal == JFileChooser.APPROVE_OPTION) {
				File file = _schemaFileChooser.getSelectedFile();

				String fPath = file.getCanonicalPath();

				String fName = file.getName();
				//dinesh ++
				//Set the current Schema Name
				Config.setCurrSchemaName(fName);	
				//dinesh --
				loadNewP2PNetwork(fPath, fName);
			}
		} catch (IOException e) {
			JOptionPane.showMessageDialog(this, e.getMessage(), "I/O error loading CDSS", JOptionPane.ERROR_MESSAGE);
			e.printStackTrace();
		} catch (XMLParseException e) {
			JOptionPane.showMessageDialog(this, e.getMessage(), "XML parse error loading CDSS", JOptionPane.ERROR_MESSAGE);
			e.printStackTrace();

		} catch (Exception e) {
			JOptionPane.showMessageDialog(this, e.getMessage(), "Error loading CDSS", JOptionPane.ERROR_MESSAGE);
			e.printStackTrace();

		}
	}

	private void openDumpFile() {
		if (_openDumpChooser == null) {
			_openDumpChooser = new JFileChooser(Config.getWorkDir());
			_openDumpChooser.setDialogTitle("Select a data dump file");

			FileNameExtensionFilter filter = new FileNameExtensionFilter(
					"Update store dumps", "dump");
			_openDumpChooser.setFileFilter(filter);
		}

		int returnVal = _openDumpChooser.showOpenDialog(this);

		if (returnVal == JFileChooser.APPROVE_OPTION) {
			final File file = _openDumpChooser.getSelectedFile();

			PeerCommands.changeCursor(this, true);
			new SwingWorker<Object,Object>() {
				final OrchestraSystem _catalog = getSelectedCatalog();
				@Override
				protected Object doInBackground() throws Exception {
					FileInputStream fis = new FileInputStream(file);
					ObjectInputStream ois = new ObjectInputStream(fis);
					final USDump dump = (USDump) ois.readObject();
					ois.close();
					fis.close();

					_catalog.restore(getSelectedPeer(), dump);
					return null;
				}

				@Override
				protected void done() {
					PeerCommands.changeCursor(MainFrm.this, false);
					Throwable t = null;;
					try {
						// If doInBackground failed with an exception,
						// retrieve it
						get();
					} catch (InterruptedException ie) {
						t = ie;
					} catch (ExecutionException ee) {
						t = ee.getCause();
					}
					try {
						reset();
					} catch (Exception e) {
						t = e;
					}
					if (t == null) {
						JOptionPane.showMessageDialog(MainFrm.this, "Loading from " + file + " succeeded", "Load succeeded", JOptionPane.INFORMATION_MESSAGE);
					} else {
						JOptionPane.showMessageDialog(MainFrm.this, "Loading from " + file + " failed: " + t.getMessage(), "Load failed", JOptionPane.ERROR_MESSAGE);
					}
				}

			}.execute();
		}
	}

	private void saveDumpFile() {
		if (_saveDumpChooser == null) {
			_saveDumpChooser = new JFileChooser(Config.getWorkDir());
			_saveDumpChooser.setDialogTitle("Specify a data dump filename and path");
		}

		int returnVal = _saveDumpChooser.showSaveDialog(this);

		if (returnVal == JFileChooser.APPROVE_OPTION) {
			File suggestedFile = _saveDumpChooser.getSelectedFile();
			String filename = suggestedFile.getName();
			final File file;
			if (filename.endsWith(".dump")) {
				file  = suggestedFile;
			} else {
				File newFile = new File(suggestedFile.getParent(), suggestedFile.getName() + ".dump");
				file = newFile;
			}

			PeerCommands.changeCursor(this, true);
			new SwingWorker<Object,Object>() {

				final OrchestraSystem _system = getSelectedCatalog();
				@Override
				protected Object doInBackground() throws Exception {
					USDump dump = _system.dump(getSelectedPeer());
					FileOutputStream fos = new FileOutputStream(file);
					ObjectOutputStream oos = new ObjectOutputStream(fos);
					oos.writeObject(dump);
					oos.close();
					fos.close();
					return null;
				}

				@Override
				protected void done() {
					PeerCommands.changeCursor(MainFrm.this, false);
					Throwable t = null;
					try {
						get();
					} catch (ExecutionException ee) {
						t = ee.getCause();
					} catch (InterruptedException e) {
						t = e;
					}
					if (t == null) {
						JOptionPane.showMessageDialog(MainFrm.this, "Saving to file " + file + " succeeded", "Saved update store state", JOptionPane.INFORMATION_MESSAGE);
					} else {
						String title = "Error";
						if (t instanceof DbException) {
							title = "Orchestra error saving update store state";
						} else if (t instanceof FileNotFoundException) {
							title = "File not found trying to save update store state";
						} else if (t instanceof IOException) {
							title = "I/O Error saving update store state";
						}
						JOptionPane.showMessageDialog(MainFrm.this, t.getMessage(), title, JOptionPane.ERROR_MESSAGE);
					}
				}

			}.execute();
		}
	}

	private void createMainMenu() 
	{
		JMenuBar mainMenu = new JMenuBar ();
		createFileMenu(mainMenu);
		createPeerMenu(mainMenu);
		createServicesMenu(mainMenu);
		//createOrchReposMenu(mainMenu);
		createWindowsMenu(mainMenu);
		createHelpMenu(mainMenu);
		setJMenuBar(mainMenu);
		createPeerContextMenu();
	}

	private void createFileMenu (JMenuBar mainMenu)
	{
		JMenu mnuFile = new JMenu ("File");
		mnuFile.setMnemonic(KeyEvent.VK_F);

		JMenuItem mnuItmFileNew = new JMenuItem("New Window");
		mnuItmFileNew.setMnemonic(KeyEvent.VK_N);
		ActionListener listener = new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				MainIFrame mif = getSelectedMIF();
				if (mif != null) {
					String cdss = mif.getCDSSName();
					String file = mif.getFilename();
					try {
						MainIFrame mifNew = loadNewP2PNetwork(file, cdss + "'");
						PeersMgtPanel pan = mif.getPanel().getPeersMgtPanel();
						PeersMgtPanel panNew = mifNew.getPanel().getPeersMgtPanel();
						panNew.mimic(pan);
					} catch (Exception ex) {
						JOptionPane.showMessageDialog(MainFrm.this, ex.getMessage(), "Error loading CDSS", JOptionPane.ERROR_MESSAGE);
					}
				}
			}
		};
		mnuItmFileNew.addActionListener(listener);
		mnuFile.add(mnuItmFileNew);

		JMenuItem mnuItmFileOpen = new JMenuItem ("Open Schema...");
		mnuItmFileOpen.setMnemonic(KeyEvent.VK_O);
		listener = new ActionListener() {
			public void actionPerformed(@SuppressWarnings("unused")
					ActionEvent e) {
				openSchemaFile();
			}
		};
		mnuItmFileOpen.addActionListener(listener);
		mnuFile.add(mnuItmFileOpen);

		JMenuItem mnuItmPeerImport = new JMenuItem ("Import CDSS Data...");
		mnuItmPeerImport.setMnemonic(KeyEvent.VK_I);
		listener = new ActionListener() {
			public void actionPerformed(@SuppressWarnings("unused")
					ActionEvent e) {
				importPeerData(false);
			}
		};
		mnuItmPeerImport.addActionListener(listener);
		mnuFile.add(mnuItmPeerImport);

		mnuFile.addSeparator();
		
		_mnuItmFileClose = new JMenuItem("Close");
		_mnuItmFileClose.setMnemonic(KeyEvent.VK_C);
		listener = new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				JInternalFrame f = _desktop.getSelectedFrame();
				if (f != null) {
					DesktopManager dm = _desktop.getDesktopManager();
					dm.closeFrame(f);
				}
			}
		};
		_mnuItmFileClose.addActionListener(listener);
		mnuFile.add(_mnuItmFileClose);
		
		mnuFile.addSeparator();
		JMenuItem mnuItmFileLocal = new JMenuItem ("Edit Local Config...");
		mnuItmFileLocal.setMnemonic(KeyEvent.VK_L);
		listener = new ActionListener() {
			public void actionPerformed(@SuppressWarnings("unused")
					ActionEvent e) {
				openLocalProperties();
			}
		};
		mnuItmFileLocal.addActionListener(listener);
		mnuFile.add(mnuItmFileLocal);
		
		JMenuItem mnuItmFileGlobal = new JMenuItem ("Edit Global Config...");
		mnuItmFileGlobal.setMnemonic(KeyEvent.VK_G);
		listener = new ActionListener() {
			public void actionPerformed(@SuppressWarnings("unused")
					ActionEvent e) {
				openGlobalProperties();
			}
		};
		mnuItmFileGlobal.addActionListener(listener);
		mnuFile.add(mnuItmFileGlobal);
		
		mnuFile.addSeparator();
		

		JMenuItem mnuItmFileExit = new JMenuItem ("Exit");
		mnuItmFileExit.setMnemonic(KeyEvent.VK_X);
		listener = new ActionListener() {
			public void actionPerformed(@SuppressWarnings("unused")
					ActionEvent e) {
				closeDown();
			}
		};
		mnuItmFileExit.addActionListener(listener);
		mnuFile.add(mnuItmFileExit);

		mainMenu.add(mnuFile);
	}

	private void closeDown() {
//		SkolemServer.quitServer();
		try {
			getSelectedMainPanel().stopStoreServer();
//			Add call to commit without activating "not logged initially" before exiting
			for(JInternalFrame frame :_desktop.getAllFrames()){
				if (frame instanceof MainIFrame) {
					MainPanel panel = ((MainIFrame)frame).getPanel();
					panel.getSystem().getMappingDb().finalize();
				}
			}
			
			

		} catch (Exception e) {
			
		}
		if (!"Ajax".equals(Config.getProperty("gui.mode"))){
			System.exit(0);
		}
	}

//	private void createOrchReposMenu(JMenuBar mainMenu) 
//	{
//	//TODO
//	JMenu mnuRepos = new JMenu ("Repository");

//	mainMenu.add (mnuRepos);

//	}

	private MainIFrame getSelectedMIF() {
		JInternalFrame frame = _desktop.getSelectedFrame();
		
		if (frame instanceof MainIFrame) {
			return (MainIFrame)frame;
		}
		return null;
	}
	
	private MainPanel getSelectedMainPanel() {
		JInternalFrame frame = _desktop.getSelectedFrame();
		if (frame instanceof MainIFrame) {
			return ((MainIFrame)frame).getPanel();
		}
		return null;
	}
	
	private Peer getSelectedPeer() {
		MainPanel mp = getSelectedMainPanel();
		if (mp != null) {
			return mp.getPeersMgtPanel().getCurrentPeer();
		}
		return null;
	}
	
	private OrchestraSystem getSelectedCatalog() {
		MainPanel mp = getSelectedMainPanel();
		if (mp != null) {
			return mp.getSystem();
		}
		return null;
	}
	
	private PeerTransactionsIntf getSelectedTIF() {
		MainPanel mp = getSelectedMainPanel();
		if (mp != null) {
			return mp.getPeersMgtPanel().getPeerTransactionsIntf();
		}
		return null;
	}
	
	/**
	 * Right-click context menu
	 * 
	 */
	private void createPeerContextMenu()
	{
		_cxMenuPeer = new JPopupMenu ("Peer");

		_cxMenuPeer.setLabel("Peer");

		JMenuItem recon = new JMenuItem("Publish and Reconcile");
		recon.setMnemonic(KeyEvent.VK_R);
		//recon.setEnabled(false);
		_cxMenuPeer.add(recon);

		final MainFrm frm = this;

		ActionListener listener = new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				PeerCommands.publishAndReconcile(frm, getSelectedCatalog(), getSelectedTIF());
			}
		};
		recon.addActionListener(listener);
		
		_cxMenuPeer.addSeparator();

		JMenuItem mnuItmPeerImport = new JMenuItem ("Import Data...");
		mnuItmPeerImport.setMnemonic(KeyEvent.VK_I);
		listener = new ActionListener() {
			public void actionPerformed(@SuppressWarnings("unused")
					ActionEvent e) {
				importPeerData(false);
			}
		};
		mnuItmPeerImport.addActionListener(listener);
		_cxMenuPeer.add(mnuItmPeerImport);

		_cxMenuPeer.addSeparator();
		// Open transaction history for the peer
		JMenuItem trans = new JMenuItem("View Transaction History...");
		trans.setMnemonic(KeyEvent.VK_T);
		listener = new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				Peer peer = getSelectedPeer();
				//OrchestraSystem catalog = getSelectedCatalog();
				if (peer != null) {
					
					/*
					if (_transViewer == null || _transViewer.isClosed()) {
						if (_transViewer != null && !_transViewer.isClosed())
							_transViewer.close();

						_transViewer = new TransactionIFrame(peer, catalog);
						_desktop.add(_transViewer);
					}
					_transViewer.setSize(800,600);
					_transViewer.setVisible(true);
					try {
						_transViewer.setMaximum(true);
					} catch (PropertyVetoException ex) {
						assert(false); // Won't happen, maximizable is set in the constructor
					}*/
					getSelectedMIF().getPanel().showTransactionViewer(peer);
				}
			}
		};
		trans.addActionListener(listener);
		_cxMenuPeer.add(trans);
		_recModeOnly.add(trans);

		// Open provenance viewer for the peer
		JMenuItem prov = new JMenuItem("View Provenance...");
		prov.setMnemonic(KeyEvent.VK_P);
		listener = new ActionListener() {
            public void actionPerformed(ActionEvent e) {
            	Peer peer = getSelectedPeer();
            	if (peer != null) 
            	{
            		getSelectedMIF().getPanel().showProvenanceViewer(peer);
	            }
            }
        };
        prov.addActionListener(listener);
        _cxMenuPeer.add(prov);
        _exchangeModeOnly.add(prov);
	}

	private void createPeerMenu(JMenuBar mainMenu) 
	{
		_menuPeer = new JMenu ("Peer");
		_menuPeer.setMnemonic(KeyEvent.VK_P);
		
		JMenuItem recon = new JMenuItem("Publish and Reconcile");
		recon.setMnemonic(KeyEvent.VK_R);
		//recon.setEnabled(false);
		_menuPeer.add(recon);

		final MainFrm frm = this;

		ActionListener listener = new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				PeerCommands.publishAndReconcile(frm, getSelectedCatalog(), getSelectedTIF());
			}
		};
		recon.addActionListener(listener);

		JMenuItem mnuItmPeerImport = new JMenuItem ("Import Data...");
		mnuItmPeerImport.setMnemonic(KeyEvent.VK_I);
		listener = new ActionListener() {
			public void actionPerformed(@SuppressWarnings("unused")
					ActionEvent e) {
				importPeerData(false);
			}
		};
		mnuItmPeerImport.addActionListener(listener);
		_menuPeer.add(mnuItmPeerImport);

		// Open transaction history through main panel
		JMenuItem trans = new JMenuItem("View Transaction History...");
		trans.setMnemonic(KeyEvent.VK_T);
		listener = new ActionListener() {
            public void actionPerformed(ActionEvent e) {
            	Peer peer = getSelectedPeer();
            	if (peer != null) {
            		getSelectedMIF().getPanel().showTransactionViewer(peer);
	            }
            }
        };
        trans.addActionListener(listener);
        _menuPeer.add(trans);
        _recModeOnly.add(trans);


		// Open provenance viewer through main panel
		JMenuItem prov = new JMenuItem("View Provenance...");
		prov.setMnemonic(KeyEvent.VK_P);
		listener = new ActionListener() {
            public void actionPerformed(ActionEvent e) {
            	Peer peer = getSelectedPeer();
            	if (peer != null) {
            		getSelectedMIF().getPanel().showProvenanceViewer(peer);
	            }
            }
        };
        prov.addActionListener(listener);
        _menuPeer.add(prov);
        _exchangeModeOnly.add(prov);
        
        // Default to disabled, since no peer is active
        _menuPeer.setEnabled(false);
        
        mainMenu.add (_menuPeer);
	}

	private void createServicesMenu(JMenuBar mainMenu) 
	{
		JMenu mnuServices = new JMenu ("Services");
		mnuServices.setMnemonic(KeyEvent.VK_S);
		
		/*
		JMenuItem mnuItmServicesSkolems = new JMenuItem ("Skolem server");
		mnuItmServicesSkolems.setMnemonic(KeyEvent.VK_S);
        ActionListener listener = new ActionListener() {
            public void actionPerformed(@SuppressWarnings("unused")
            ActionEvent e) {
            	if (_skolemServer == null || !_skolemServer.isEnabled()) {
            		_skolemServer = new SkolemServer();

            		_skolemServer.start();
            	}
            }
        };
        mnuItmServicesSkolems.addActionListener(listener);
        mnuServices.add(mnuItmServicesSkolems);

		JMenuItem mnuItmServicesShutdown = new JMenuItem ("Shutdown Skolems");
		mnuItmServicesShutdown.setMnemonic(KeyEvent.VK_D);
        listener = new ActionListener() {
            public void actionPerformed(@SuppressWarnings("unused")
            ActionEvent e) {
            	if (_skolemServer != null)
            		SkolemServer.quitServer();
            }
        };
        mnuItmServicesShutdown.addActionListener(listener);
        mnuServices.add(mnuItmServicesShutdown);

        mnuServices.addSeparator();*/

		JMenuItem mnuItmServicesCreate = new JMenuItem ("Create DB from Schema");
		ActionListener listener = new ActionListener() {
			public void actionPerformed(ActionEvent ev) {
				BasicEngine engine = getSelectedMIF().getPanel().getSystem().getMappingEngine();
				try {
					engine.createBaseSchemaRelations();
				} catch (Exception e) {
					JOptionPane.showMessageDialog(null, e.getMessage(), "Error creating DB", JOptionPane.ERROR_MESSAGE);
				}
			}
		};
		mnuItmServicesCreate.addActionListener(listener);
		mnuServices.add(mnuItmServicesCreate);

		JMenuItem mnuItmServicesMigrate = new JMenuItem ("Migrate Existing DB");
		listener = new ActionListener() {
			public void actionPerformed(ActionEvent ev) {
				BasicEngine engine = getSelectedMIF().getPanel().getSystem().getMappingEngine();
				try {
					engine.migrate();
				} catch (Exception e) {
					e.printStackTrace();
					JOptionPane.showMessageDialog(null, e.getMessage(), "Error migrating DB", JOptionPane.ERROR_MESSAGE);
				}
			}
		};
		mnuItmServicesMigrate.addActionListener(listener);
		mnuServices.add(mnuItmServicesMigrate);
		_exchangeModeOnly.add(mnuItmServicesMigrate);
		
		mnuServices.addSeparator();

		JMenuItem mnuItmServicesReset = new JMenuItem ("Clear DB Contents");
		listener = new ActionListener() {
			public void actionPerformed(ActionEvent ev) {
				OrchestraSystem system = getSelectedCatalog(); 
				if (system != null) {
					try {
						PeerCommands.changeCursor(MainFrm.this, true);
						system.reset();
						reset();
					} catch (Exception e) {
						JOptionPane.showMessageDialog(null, e.getMessage(), "Error resetting state", JOptionPane.ERROR_MESSAGE);
					} finally {
						PeerCommands.changeCursor(MainFrm.this, false);
					}
				}
			}
		};
		mnuItmServicesReset.addActionListener(listener);
		mnuServices.add(mnuItmServicesReset);
	
		mnuServices.addSeparator();

		JMenuItem load = new JMenuItem("Load Update Store");
		load.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				openDumpFile();
			}
		});
		mnuServices.add(load);

		JMenuItem dump = new JMenuItem("Save Update Store");
		dump.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				saveDumpFile();
			}
		});
		mnuServices.add(dump);
		
		
		_startUS = new JMenuItem("Start Update Store");
		_startUS.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				startUpdateStore();
			}
			
		});
		_startUS.setEnabled(false);
		_stopUS = new JMenuItem("Stop Update Store");
		_stopUS.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				try {
					PeerCommands.changeCursor(MainFrm.this, true);
					getSelectedMainPanel().stopStoreServer();
					_stopUS.setEnabled(false);
					_startUS.setEnabled(true);
					_clearUS.setEnabled(true);
				} catch (Exception e) {
					JOptionPane.showMessageDialog(null, e.getMessage(), "Error stopping update store", JOptionPane.ERROR_MESSAGE);
				} finally {
					PeerCommands.changeCursor(MainFrm.this, false);
				}
			}
		});
		_stopUS.setEnabled(false);
		
		_clearUS = new JMenuItem("Clear Update Store");
		_clearUS.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				try {
					PeerCommands.changeCursor(MainFrm.this, true);
					getSelectedMainPanel().clearStoreServer();
				} catch (Exception e) {
					JOptionPane.showMessageDialog(null, e.getMessage(), "Error stopping update store", JOptionPane.ERROR_MESSAGE);
				} finally {
					PeerCommands.changeCursor(MainFrm.this, false);
				}
			}
		});
		
		//dinesh ++
		//Menu to Commit Changes made to the Schema
		JMenuItem mnuItmCommitChanges = new JMenuItem ("Commit Changes");
		listener = new ActionListener() {
			public void actionPerformed(ActionEvent ev) {
				try 
				{
					//Overwrite existing Schema file with new Schema file that has been created
					File fileTestSchema = new File (Config.getTempSchemaFile());
					String currSchemaLoc = Config.getCurrSchemaFile().substring(Config.getCurrSchemaFile().indexOf("\\"));
					File fileCurrSchema = new File (currSchemaLoc);
					copyFile(fileTestSchema, fileCurrSchema);
					
					//Load the new Schema
					loadNewP2PNetwork(Config.getCurrSchemaFile(), Config.getCurrSchemaName());
					
					//Commit changes to the DB
					BasicEngine engine = getSelectedMIF().getPanel().getSystem().getMappingEngine();
					engine.createBaseSchemaRelations();
				} 
				catch (Exception e) 
				{
					JOptionPane.showMessageDialog(null, e.getMessage(), "Error creating DB", JOptionPane.ERROR_MESSAGE);
				}
			}
		};
		mnuItmCommitChanges.addActionListener(listener);
		mnuServices.add(mnuItmCommitChanges);
		//dinesh --
		
		mnuServices.add(_startUS);
		mnuServices.add(_stopUS);
		mnuServices.add(_clearUS);
		_recModeOnly.add(load);
		_recModeOnly.add(dump);
		_recModeOnly.add(_startUS);
		_recModeOnly.add(_stopUS);
		_recModeOnly.add(_clearUS);
		
		
		mainMenu.add (mnuServices);
	}

	private void startUpdateStore() {
		try {
			PeerCommands.changeCursor(MainFrm.this, true);
			getSelectedMainPanel().startStoreServer();
			_startUS.setEnabled(false);
			_stopUS.setEnabled(true);
			_clearUS.setEnabled(false);
		} catch (Exception e) {
			JOptionPane.showMessageDialog(null, e.getMessage(), "Error starting update store", JOptionPane.ERROR_MESSAGE);
		} finally {
			PeerCommands.changeCursor(MainFrm.this, false);
		}
	}
	
	
	private void createWindowsMenu(JMenuBar mainMenu) 
	{
		JWindowsMenu mnuWindows = new JWindowsMenu (_desktop);
		mnuWindows.setMnemonic(KeyEvent.VK_W);
		mainMenu.add (mnuWindows);
	}

	private void createHelpMenu(JMenuBar mainMenu) {
		JMenu help = new JMenu ("Help");
		help.setMnemonic(KeyEvent.VK_H);
		JMenuItem about = new JMenuItem("About ORCHESTRA...");
		about.setMnemonic(KeyEvent.VK_A);
		ActionListener listener = new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				new AboutBox(MainFrm.this, "W").setVisible (true);
			}
		};
		about.addActionListener(listener);
		help.add(about);
		mainMenu.add(help);
	}

	protected static FontUIResource newFont(Map<String,String> params) {
		String name = params.get("name");
		String style = params.get("style");
		int code;
		if (style.equals("bold")) {
			code = FontUIResource.BOLD;
		} else if (style.equals("italic")) {
			code = FontUIResource.ITALIC;
		} else {
			code = FontUIResource.PLAIN;
		}
		int size = Integer.parseInt(params.get("size"));
		return new FontUIResource(name, code, size);
	}

	protected static ColorUIResource newColor(Map<String,String> params) {
		int r = Integer.parseInt(params.get("r"));
		int g = Integer.parseInt(params.get("g"));
		int b = Integer.parseInt(params.get("b"));
		return new ColorUIResource(r,g,b);
	}

	private final static Empty e = new Empty ();
	protected static IconUIResource newIcon (Map<String,String> params)
	{
		IconUIResource res = new IconUIResource(new ImageIcon(e.getClass().getResource(params.get("file"))));
		return res;
	}

	static final Pattern m_pat = Pattern.compile("\\s*(\\w+(\\.\\w+)*)\\[(\\w+=(\\w|\\.|\\/|\\-)+(,\\s*\\w+=(\\w|\\.|\\/|\\-)+)*)\\]\\s*");
	protected static Object newResource(String resource) throws Exception, ClassNotFoundException { 
		// Examples:
		// Label.font=javax.swing.plaf.FontUIResource[family\=Dialog,name\=Dialog,style\=plain,size\=40]
		// Table.font=javax.swing.plaf.FontUIResource[family\=Dialog,name\=Dialog,style\=plain,size\=20]
		// Table.color=javax.swing.plaf.ColorUIResource[r\=255,g\=0,b\=0]
		// Button.background=javax.swing.plaf.ColorUIResource[r\=255,g\=0,b\=0]
		Matcher mat = m_pat.matcher(resource);
		if (mat.matches()) {
			String clazz = mat.group(1);
			String rest = mat.group(3);
			String[] list = rest.split(",\\s*");
			HashMap<String,String> params = new HashMap<String,String>();
			for (String str : list) {
				int eq = str.indexOf("=");
				params.put(str.substring(0, eq), str.substring(eq+1));
			}
			if (clazz.equals("javax.swing.plaf.FontUIResource") || 
					clazz.equals("FontUIResource")) {
				return newFont(params);
			} else if (clazz.equals("javax.swing.plaf.ColorUIResource") ||
					clazz.equals("ColorUIResource")) {
				return newColor(params);
			} else if (clazz.equals("javax.swing.plaf.ImageIconUIResource")) {
				return newIcon(params);
			}
			else{
				throw new ClassNotFoundException(clazz);
			}
		}
		throw new Exception("Malformed resource description \'" + resource + "\'");
	}


	public static void main (String args[]) throws Exception
	{
		// Switch off D3D because of Sun XOR painting bug
        // See http://www.jgraph.com/forum/viewtopic.php?t=4066
		// and http://bugs.sun.com/view_bug.do?bug_id=6635462
		// for updates.
		System.setProperty("sun.java2d.d3d", "false");
		Config.parseCommandLine(args);
		// Set System L&F
		if (!"Ajax".equals(Config.getProperty("gui.mode"))){
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		}
		InputStream props = MainFrm.class.getResourceAsStream("theme.properties");
		Properties theme = new Properties();
		theme.load(props);
		// First do the "direct" definitions
		for (Object obj : theme.keySet()) {
			String key = (String)obj;
			String value = theme.getProperty(key);
			if (!value.startsWith("$")) {
				Object resource = newResource(value);
				UIManager.put(key, resource);
			}
		}
		// Then do the "indirect" definitions 
		for (Object obj : theme.keySet()) {
			String key = (String)obj;
			String value = theme.getProperty(key);
			if (value.startsWith("$")) {
				value = value.substring(1).trim();
				Object resource = UIManager.get(value);
				UIManager.put(key, resource);
			}
		}
		SwingUtilities.invokeLater(new Runnable() {
			@Override
			public void run() {
				JButton button = new JButton();
				Font font1 = button.getFont();
				UIDefaults defs = UIManager.getDefaults();
				Font font2 = defs.getFont("Button.font");
				assert (font1 == font2);

				MainFrm frm = new MainFrm();
				frm.startup();
			}
		});
	}

	public void mappingWasSelected(PeersMgtPanel panel, Mapping m) 
	{
		selectionIsEmpty(panel);
	}

	public void peerWasSelected(PeersMgtPanel panel, Peer p, Schema s, PeerTransactionsIntf peerTransIntf) 
	{
		_menuPeer.setEnabled(true);		
	}

	public void peerContextMenu(PeersMgtPanel panel, Peer p, Schema s, PeerTransactionsIntf peerTransIntf, JComponent parent, int x, int y) 
	{
		peerWasSelected(panel, p, s, peerTransIntf);
		_cxMenuPeer.show(parent, x, y);
	}

	public void selectionIsEmpty(PeersMgtPanel panel) {
		_menuPeer.setEnabled(false);
	}

	private void reset() throws Exception {
		MainIFrame mif = getSelectedMIF();
		boolean startStore = mif.getPanel().storeServerRunning();
		if (startStore) {
			mif.getPanel().stopStoreServer();
		}
		if (mif != null) {
			_desktop.remove(mif);
			MainIFrame mifNew = loadNewP2PNetwork(mif.getFilename(), mif.getCDSSName());
			PeersMgtPanel pan = mif.getPanel().getPeersMgtPanel();
			PeersMgtPanel panNew = mifNew.getPanel().getPeersMgtPanel();
			panNew.mimic(pan);
			if (startStore) {
				startUpdateStore();
			}
		}
	}

	public void internalFrameActivated(InternalFrameEvent e) {
		JInternalFrame f = e.getInternalFrame();
		if (f instanceof MainIFrame) {
			MainIFrame mif = (MainIFrame)f;
			Peer p = mif.getPanel().getPeersMgtPanel().getCurrentPeer();
			if (p != null) {
				_menuPeer.setEnabled(true);
			}
			if (mif.getPanel().storeServerRunning()) {
				_startUS.setEnabled(false);
				_stopUS.setEnabled(true);
			} else {
				_startUS.setEnabled(true);
				_stopUS.setEnabled(false);
			}
		}
	}

	public void internalFrameClosed(InternalFrameEvent e) {
	}

	public void internalFrameClosing(InternalFrameEvent e) {
	}

	public void internalFrameDeactivated(InternalFrameEvent e) {
		_menuPeer.setEnabled(false);
	}

	public void internalFrameDeiconified(InternalFrameEvent e) {
	}

	public void internalFrameIconified(InternalFrameEvent e) {
	}

	public void internalFrameOpened(InternalFrameEvent e) {
	}
	
	//dinesh ++
	public static void copyFile(File in, File out) throws Exception {
	    FileInputStream fis  = new FileInputStream(in);
	    FileOutputStream fos = new FileOutputStream(out);
	    try {
	        byte[] buf = new byte[1024];
	        int i = 0;
	        while ((i = fis.read(buf)) != -1) {
	            fos.write(buf, 0, i);
	        }
	    } 
	    catch (Exception e) {
	        throw e;
	    }
	    finally {
	        if (fis != null) fis.close();
	        if (fos != null) fos.close();
	    }
	  }
	//dinesh --
}
