/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Apr 12, 2004
 */
package com.persistit.test;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Container;
import java.awt.Font;
import java.awt.Rectangle;
import java.awt.event.ActionEvent;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;

import javax.swing.AbstractAction;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JToggleButton;
import javax.swing.ListSelectionModel;
import javax.swing.ScrollPaneConstants;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.WindowConstants;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.plaf.FontUIResource;
import javax.swing.table.AbstractTableModel;
import javax.swing.text.BadLocationException;
import javax.swing.text.Document;
import javax.swing.text.PlainDocument;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;

/**
 * 
 * Framework for running tests on Persistit.
 * 
 * Runs various tests, records progress and results in a log file. This is
 * intended to permit long-term unattended testing.
 * 
 */
public class TestRunnerGui extends JFrame {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final static String[] COLUMN_NAMES = { "Index", "Unit", "Test",
            "Progress", "Time", "Status", };

    Container _exchangeGui;
    TestTableModel _testTableModel;
    DisplayPanel _displayPanel = new DisplayPanel();

    public TestRunnerGui() {
        setSize(1000, 800);
        setLocation(100, 100);
        setTitle("TestRunnerGui");

        _testTableModel = new TestTableModel();
        getContentPane().add(createStatusPanel());

        setVisible(true);
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
    }

    JPanel createStatusPanel() {
        final JPanel panel = new JPanel(new BorderLayout());
        final JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        final JTable table = new JTable(_testTableModel);
        splitPane.add(new JScrollPane(table,
                ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS,
                ScrollPaneConstants.HORIZONTAL_SCROLLBAR_ALWAYS));
        splitPane.add(_displayPanel);
        panel.add(splitPane, BorderLayout.CENTER);
        table.setSelectionMode(ListSelectionModel.SINGLE_INTERVAL_SELECTION);
        final ListSelectionModel lsm = table.getSelectionModel();

        table.getSelectionModel().addListSelectionListener(
                new ListSelectionListener() {
                    public void valueChanged(final ListSelectionEvent lse) {
                        if (!lsm.getValueIsAdjusting()) {
                            final int index = lsm.getMinSelectionIndex();
                            if (index != -1) {
                                final AbstractTestRunnerItem test = _testTableModel
                                        .getTest(index);
                                _displayPanel.setTest(test);
                            }
                        }
                    }
                });
        return panel;
    }

    static class ConsoleOutputStream extends OutputStream {
        private final PlainDocument _consoleDocument;
        private final StringBuilder _sb = new StringBuilder(100);
        private final SimpleAttributeSet _as = new SimpleAttributeSet();

        public ConsoleOutputStream(final PlainDocument doc,
                final boolean errorStream) {
            _consoleDocument = doc;
            _as.addAttribute(PlainDocument.lineLimitAttribute, new Integer(120));
            _as.addAttribute(PlainDocument.tabSizeAttribute, new Integer(8));
            StyleConstants.setFontFamily(_as, "Monospaced");
            StyleConstants.setFontSize(_as, 12);
            StyleConstants.setForeground(_as, errorStream ? Color.red
                    : Color.black);
            StyleConstants.setBackground(_as, Color.white);
        }

        public Document getDocument() {
            return _consoleDocument;
        }

        @Override
        public synchronized void write(final int c) {
            _sb.append((char) c);
        }

        @Override
        public synchronized void flush() {
            if (_sb.length() > 0) {
                final String s = _sb.toString();
                _sb.setLength(0);
                try {
                    final int offset = _consoleDocument.getLength();
                    _consoleDocument.insertString(offset, s, _as);
                } catch (final BadLocationException ble) {
                    ble.printStackTrace();
                }
            }
        }
    }

    static class DisplayPanel extends JPanel {
        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        AbstractTestRunnerItem _test;

        JTextArea _textArea = new JTextArea();

        JScrollPane _scrollPane = new JScrollPane(_textArea,
                ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS,
                ScrollPaneConstants.HORIZONTAL_SCROLLBAR_ALWAYS);

        JLabel _caption = new JLabel("No Test Selected");

        JToggleButton _autoscroll = new JToggleButton(new AbstractAction(
                "Scroll Lock") {
            /**
                 * 
                 */
            private static final long serialVersionUID = 1L;

            public void actionPerformed(ActionEvent ae) {
                if (!((JToggleButton) ae.getSource()).isSelected()) {
                    scrollToEnd();
                }
            }
        });

        DocumentListener _listener = new DocumentListener() {
            public void changedUpdate(DocumentEvent ev) {
            }

            public void insertUpdate(DocumentEvent ev) {
                if (!_autoscroll.isSelected()) {
                    scrollToEnd();
                }
            }

            public void removeUpdate(DocumentEvent ev) {
                if (!_autoscroll.isSelected()) {
                    scrollToEnd();
                }
            }

        };

        Runnable _scroller = new Runnable() {
            public void run() {
                int h = _textArea.getHeight();
                _scrollPane.getViewport().scrollRectToVisible(
                        new Rectangle(0, h - 1, 1, 1));
            }
        };

        private void setTest(final AbstractTestRunnerItem test) {
            if ((_test != null) && (_test.getDocument() != null)) {
                _test.getDocument().removeDocumentListener(_listener);
            }
            _test = test;
            _textArea.setDocument(_test.getDocument());
            _textArea.repaint();

            _test.getDocument().addDocumentListener(_listener);
            _caption.setText(_test.getUnitName() + ": " + _test.toString()
                    + " (" + _test.getName() + ")");
        }

        private DisplayPanel() {
            setLayout(new BorderLayout());
            add(_scrollPane, BorderLayout.CENTER);
            final JPanel southPanel = new JPanel(new BorderLayout());
            southPanel.add(_autoscroll, BorderLayout.EAST);
            southPanel.add(_caption, BorderLayout.WEST);
            add(southPanel, BorderLayout.SOUTH);
        }

        void scrollToEnd() {
            SwingUtilities.invokeLater(_scroller);
        }
    }

    public void addTest(final AbstractTestRunnerItem test) {

        final PlainDocument doc = new PlainDocument();
        final PrintStream ps = new PrintStream(new ConsoleOutputStream(doc,
                false));
        final PrintStream es = new PrintStream(new ConsoleOutputStream(doc,
                true));
        test.setDocument(doc);
        test.setOutputStream(ps);
        test.setErrorStream(es);

        _testTableModel.addTest(test);
    }

    private class TestTableModel extends AbstractTableModel {
        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        private final ArrayList<AbstractTestRunnerItem> _list = new ArrayList<AbstractTestRunnerItem>();

        private final Thread _flusher = new Thread(new Runnable() {
            public void run() {
                for (;;) {
                    try {
                        Thread.sleep(2000);
                        SwingUtilities.invokeLater(new Runnable() {
                            public void run() {
                                fireTableDataChanged();
                            }
                        });
                    } catch (InterruptedException ie) {
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        private TestTableModel() {
            _flusher.setDaemon(true);
            _flusher.setName("Status_Flusher");
            _flusher.start();
        }

        public void addTest(final AbstractTestRunnerItem test) {
            _list.add(test);
            SwingUtilities.invokeLater(new Runnable() {
                public void run() {
                    fireTableRowsInserted(_list.size() - 1, _list.size() - 1);
                }
            });
        }

        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        public int getRowCount() {
            return _list.size();
        }

        public AbstractTestRunnerItem getTest(final int row) {
            return _list.get(row);
        }

        public Object getValueAt(final int row, final int column) {
            final AbstractTestRunnerItem test = (AbstractTestRunnerItem) _list
                    .get(row);
            if (test == null) {
                return null;
            }
            switch (column) {
            case 0:
                return Integer.toString(row);

            case 1:
                return test.getUnitName();

            case 2:
                return test.toString();

            case 3:
                return test.getProgressString();

            case 4:
                if (test.isFinished()) {
                    return Long.toString(test._finishTime - test._startTime);
                } else {
                    return "";
                }

            case 5:
                return test.status();

            default:
                return "???";
            }
        }

        @Override
        public String getColumnName(final int column) {
            return COLUMN_NAMES[column];
        }
    }

    /**
     * Sets the default font for all Swing components.
     * 
     * @param font
     *            The FontUIResource to be made the default
     */
    public static void setUIFont(final FontUIResource font) {
        final java.util.Enumeration keys = UIManager.getDefaults().keys();
        while (keys.hasMoreElements()) {
            final Object key = keys.nextElement();
            final Object value = UIManager.get(key);
            if (value instanceof javax.swing.plaf.FontUIResource) {
                UIManager.put(key, font);
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        setUIFont(new FontUIResource("SanSerif", Font.PLAIN, 13));

        final TestRunner runner = new TestRunner();
        final TestRunnerGui gui = new TestRunnerGui();

        runner.setGui(gui);
        runner.parseScript(args);
        runner.execute();
    }

    public void setupConsoleOutput(final AbstractTestRunnerItem test) {
        final PlainDocument doc = new PlainDocument();
        final PrintStream ps = new PrintStream(new ConsoleOutputStream(doc,
                false));
        final PrintStream es = new PrintStream(new ConsoleOutputStream(doc,
                true));
        test.setOutputStream(ps);
        test.setErrorStream(es);
    }
}
