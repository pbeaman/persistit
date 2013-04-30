/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.awt.BorderLayout;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

import com.persistit.Exchange;
import com.persistit.KeyFilter;
import com.persistit.Persistit;
import com.persistit.exception.PersistitException;

/**
 * <p>
 * Demonstrates KeyFilter, Exchange pooling and use of Persistit within a
 * JFC/Swing application. This application presents a simple UI that lets you
 * load a list of file names from a text file or directory path, and then find
 * subsets of that list by typing partial matches with wildcards. For example,
 * you can find all instances of files named "read.me" by searching for
 * "*read.me". The search is made fast by indexing each name by its regular
 * spelling, and by its reversed spelling. Thus all "*read.me" files can be
 * found by traversing the keys of the reversed-spelling index from "em.daer" to
 * "em.daer". This program uses a KeyFilter to simplify this step.
 * </p>
 * <p>
 * This program can either read a list of file names from a file or traverse
 * directories. Specify either a text file name or a directory name in the Load
 * File text box.
 * </p>
 * 
 * @version 1.0
 */
public class FindFile extends JPanel {
    private DefaultListModel listModel = new DefaultListModel();
    private JList list = new JList(listModel);
    private JTextField searchField = new JTextField(30);
    private JTextField loadField = new JTextField(30);
    private JButton searchButton = new JButton("Find");
    private JButton loadButton = new JButton("Load");
    private JButton clearButton = new JButton("Clear");
    private JProgressBar progressBar = new JProgressBar();

    private int currentCount;
    private int estimatedTotalCount;
    private String currentDirectory;

    private Persistit persistit;

    /**
     * Construct a Swing JPanel with a simple layout for loading, clearing and
     * searching for lists of files.
     * 
     * @param defaultFileName
     *            Name of a text file containing files name list. May be null;
     *            this value is only used to populate a JTextField with a
     *            default value.
     */
    public FindFile(String defaultFileName, Persistit persistit) {
        setLayout(new BorderLayout());
        JPanel northPanel = new JPanel();
        northPanel.add(new JLabel("Search for"));
        northPanel.add(searchField);
        northPanel.add(searchButton);
        northPanel.add(progressBar);

        JPanel southPanel = new JPanel();
        southPanel.add(new JLabel("Load file"));
        southPanel.add(loadField);
        southPanel.add(loadButton);
        southPanel.add(clearButton);

        add(northPanel, BorderLayout.NORTH);
        add(new JScrollPane(list), BorderLayout.CENTER);
        add(southPanel, BorderLayout.SOUTH);

        if (defaultFileName != null)
            loadField.setText(defaultFileName);

        this.persistit = persistit;

        searchButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ae) {
                doSearch();
            }
        });

        loadButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ae) {
                doLoad();
            }
        });

        clearButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ae) {
                doClear();
            }
        });
    }

    /**
     * <p>
     * Search for all file names that match a specified wildcard expression. The
     * wildcard expression may contain "*" to represent zero or more arbitry
     * characters and "?" to represent exactly one arbitrary character. For
     * example, the expression "/opt/*java" finds files in subdirectories of
     * /opt that end with the four letters "java". To test for matches, this
     * method converts the wildcard expression to a regular expression.
     * </p>
     * <p>
     */
    private void doSearch() {
        //
        // First clear the old content.
        //
        listModel.clear();
        //
        // * and ? are treated as traditional wildcards.
        //
        String expr = searchField.getText();
        StringBuilder sb = new StringBuilder();
        for (int index = expr.length(); --index >= 0;) {
            int c = expr.charAt(index);
            if (c == '*' || c == '?')
                break;
            sb.append((char) c);
        }
        //
        // Substring the follows the final wildcard character. For example,
        // if the input is "/opt/*java", the suffix is "java".
        //
        final String suffix = sb.toString();

        sb.setLength(0);
        int lastPrefixIndex = -1;
        boolean wildcardFound = false;

        sb.append("^");
        for (int index = 0; index < expr.length(); index++) {
            int c = expr.charAt(index);
            switch (c) {
            // Characters that need to be quoted in the regular expression
            case '\\':
            case '.':
            case '^':
            case '$':
            case '[':
            case ']': {
                sb.append('\\');
                sb.append((char) c);
                break;
            }
                // translaction of "*"
            case '*': {
                wildcardFound = true;
                sb.append(".*");
                break;
            }
                // translation of "?"
            case '?': {
                wildcardFound = true;
                sb.append(".");
            }
            default: {
                sb.append((char) c);
            }
                if (!wildcardFound)
                    lastPrefixIndex = index;
            }
        }
        sb.append("$");
        final Pattern pattern = Pattern.compile(sb.toString());

        final DefaultListModel model = (DefaultListModel) list.getModel();
        //
        // Substring that precedes the first wildcard character. For example,
        // if the input is "/opt/*java", the prefix is "/opt/".
        //
        final String prefix = expr.substring(0, lastPrefixIndex + 1);
        //
        // Since the traversal may take some time, it needs to be
        // performed in a separate thread.
        //
        Thread workerThread = new Thread() {
            public void run() {
                if (suffix.length() > prefix.length()) {
                    //
                    // Traverse the index formed from reversing the spelling
                    // of each file name.
                    //
                    traverseFileNames(suffix, pattern, false);
                } else {
                    //
                    // Traverse the index based on regular file name.
                    //
                    traverseFileNames(prefix, pattern, true);
                }
                adjustProgressBar(0, 0);
            }
        };
        workerThread.start();
    }

    /**
     * Load a list of files supplied in a text file. The work is done in a
     * separate thread; the worker thread periodically updates a progress bar.
     */
    private void doLoad() {
        final String fileName = loadField.getText();
        //
        // So we don't launch two competing threads (although nothing very
        // bad would happen if we did.)
        //
        loadButton.setEnabled(false);

        Thread workerThread = new Thread(new Runnable() {
            public void run() {
                loadFileNames(fileName);
            }
        });
        workerThread.start();
    }

    /**
     * Remove all the file names previously loaded into the index. This is
     * performed in a separate thread.
     * 
     */
    private void doClear() {
        Thread workerThread = new Thread(new Runnable() {
            public void run() {
                resetFileNames();
            }
        });
        workerThread.start();
    }

    /**
     * Populate the DefaultListModel with file names that match the specified
     * pattern and update a progress bar while searching.
     * 
     * @param fixed
     * @param pattern
     * @param forward
     */
    private void traverseFileNames(String fixed, Pattern pattern, boolean forward) {
        currentCount = 0;
        estimatedTotalCount = 1000;
        final ArrayList selectedFileNames = new ArrayList();
        Exchange ex = null;
        StringBuilder sb = new StringBuilder();
        try {
            ex = persistit.getExchange("ffdemo", "filenames", true);
            ex.clear().append(forward ? "L2R" : "R2L");
            //
            // Construct a KeyFilter that accepts all keys within the
            // designated subtree.
            //
            KeyFilter filter = new KeyFilter(ex.getKey());
            if (fixed.length() != 0) {
                String end = fixed.substring(0, fixed.length() - 1)
                        + new Character((char) (fixed.charAt(fixed.length() - 1) + 1));
                //
                // append a Term that selects only the range accepted by the
                // fixed portion of the name.
                //
                filter = filter.append(KeyFilter.rangeTerm(fixed, end));
            }
            while (ex.next(filter)) {
                String fileName = ex.getKey().indexTo(1).decodeString();
                if (!forward) {
                    sb.setLength(0);
                    sb.append(fileName);
                    fileName = sb.reverse().toString();
                }
                //
                // Apply the Regex pattern, and if the name matches,
                // add it to the list model.
                //
                if (pattern.matcher(fileName).matches()) {
                    selectedFileNames.add(fileName);
                    currentCount++;
                    if (currentCount + 200 > estimatedTotalCount) {
                        estimatedTotalCount += 500;
                    }
                    if (currentCount % 100 == 0) {
                        adjustProgressBar(currentCount, estimatedTotalCount);
                    }
                }
            }
            estimatedTotalCount = currentCount;
            adjustProgressBar(currentCount, currentCount);
            setEnabled(searchField, true);

            SwingUtilities.invokeLater(new Runnable() {
                public void run() {
                    int size = selectedFileNames.size();
                    for (int index = 0; index < size; index++) {
                        listModel.addElement(selectedFileNames.get(index));
                    }
                }
            });
        } catch (PersistitException pe) {
            pe.printStackTrace();
        } finally {
            persistit.releaseExchange(ex);
        }
    }

    /**
     * Load and index a list of path names into a Persistit database. This
     * process takes a few seconds, and it updates a JProgressBar to indicate
     * progress.
     * 
     * @param fromFileName
     *            Name of the file to read from.
     */
    private void loadFileNames(String fromFileName) {
        currentCount = 0;
        estimatedTotalCount = 1000;
        resetFileNames();
        try {
            StringBuilder sb = new StringBuilder();
            long time = System.currentTimeMillis();
            File file = new File(fromFileName);
            if (file.isDirectory()) {
                loadFromDirectory(file, sb, 100);
            } else {
                loadFromTextFile(file, sb);
            }
            time = System.currentTimeMillis() - time;
            System.out.println("Took " + time + "ms to load " + currentCount + " file names");
            adjustProgressBar(0, 0);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            setEnabled(loadButton, true);
        }
    }

    private void loadFromDirectory(File dir, StringBuilder sb, int depth) throws IOException {
        if (dir.exists()) {
            loadOneLine(dir.getPath(), sb);
            File[] files = dir.listFiles();
            if (files == null || depth == 0) {
                System.out.println("Can't traverse directory " + dir);
            } else {
                for (File file : dir.listFiles()) {
                    if (file.isDirectory()) {
                        loadFromDirectory(file, sb, depth - 1);
                    } else {
                        loadOneLine(file.getPath(), sb);
                    }
                }
            }
        }
    }

    private void loadFromTextFile(File file, StringBuilder sb) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        for (;;) {
            String line = reader.readLine();
            boolean done = line == null;

            if (done) {
                break;
            }
            loadOneLine(line, sb);
        }
    }

    private void resetFileNames() {
        Exchange ex = null;
        try {
            // Obtain an Exchange from the Exchange pool. The arguments are
            // the same as Exchange's constructor.
            //
            ex = persistit.getExchange("ffdemo", "filenames", true);
            //
            // Delete all keys in the filenames tree.
            ex.removeAll();

        } catch (PersistitException pe) {
            pe.printStackTrace();
        } finally {
            //
            // Relinquish the Exchange back to the pool. Note that if
            // ex is null due to an exception, the releaseExchange method
            // does nothing.
            //
            persistit.releaseExchange(ex);
        }

    }

    /**
     * Parse and index a file name from one line of the source text file. For
     * Linix/Unix ls command, this involves recognizing lines that introduce
     * subdirectories.
     * 
     * @param line
     *            The line
     * @param sb
     *            A StringBuilder to reuse for concatenation.
     */
    private void loadOneLine(String line, StringBuilder sb) {
        //
        // handles output from Windows/DOS command
        // DIR /B /S and
        // 'nix command
        // ls -1 -p -R
        //
        if (line.length() == 0 || line.endsWith("/"))
            return;
        if (line.endsWith(":")) {
            currentDirectory = line.substring(0, line.length() - 1);
            return;
        }
        sb.setLength(0);
        if (currentDirectory != null) {
            sb.append(currentDirectory);
            sb.append('/');
        }
        sb.append(line);
        //

        // fileName contains the fully formed file name (with path).
        // reversedFileName contains the name spelled in reverse.
        //
        String fileName = sb.toString();
        String reversedFileName = sb.reverse().toString();

        Exchange ex = null;
        try {
            // Obtain an Exchange from the Exchange pool. The arguments are
            // the same as Exchange's constructor.
            //
            ex = persistit.getExchange("ffdemo", "filenames", true);
            ex.getValue().put(null); // No value to store, just the keys
            ex.clear().append("L2R").append(fileName).store();
            ex.clear().append("R2L").append(reversedFileName).store();
        } catch (PersistitException pe) {
            pe.printStackTrace();
        } finally {
            //
            // Relinquish the Exchange back to the pool. Note that if
            // ex is null due to an exception, the releaseExchange method
            // does nothing.
            //
            persistit.releaseExchange(ex);
        }

        if (currentCount % 1000 == 0) {
            adjustProgressBar(currentCount, estimatedTotalCount);

            if (currentCount % 10000 == 0) {
                System.out.println(Thread.currentThread() + " has loaded " + currentCount + " file names");
            }
        }
        currentCount++;
        if (currentCount >= estimatedTotalCount) {
            estimatedTotalCount = estimatedTotalCount *= 2;
        }

    }

    /**
     * Adjust the JProgressBar. This is invoked by worker threads, so it must
     * call SwingUtilities.invokeLater to enqueue the action to run on the Swing
     * event dispatch thread.
     * 
     * @param value
     *            Current progress value
     * 
     * @param maximum
     *            Estimated total value
     */
    private void adjustProgressBar(final int value, final int maximum) {
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                progressBar.setMaximum(maximum);
                progressBar.setValue(value);
            }
        });
    }

    /**
     * Enable or disable a component. This is invoked by worker threads so it
     * must call SwingUtilities.invokeLater to enqueue the action to run on the
     * Swing event dispatch thread.
     * 
     * @param component
     *            The component to disable or enable
     * 
     * @param enabled
     *            <tt>true</tt> to enable the component,
     *            <tt>false,/tt> to disable it.
     */
    private void setEnabled(final JComponent component, final boolean enabled) {
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                component.setEnabled(enabled);
            }
        });
    }

    /**
     * Main program entry point. Sets up a JFrame, adds a WindowListener to
     * close Persistit when the JFrame closes, and creates the FindFileDemo
     * panel within the JFrame.
     * 
     * @param args
     */
    public static void main(String[] args) {
        final Persistit persistit = new Persistit();
        try {
            persistit.initialize();
        } catch (PersistitException pe) {
            System.err.println("PersistitException during initialization");
            pe.printStackTrace();
        }

        JFrame frame = new JFrame("FileFindDemo");

        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

        //
        // Persistit should always be closed on normal program exit. For
        // a Swing application, do this on the windowClosed event of
        // the containing JFrame.
        //
        frame.addWindowListener(new WindowAdapter() {
            public void windowClosed(WindowEvent we) {
                //
                // Persistit.close may take several seconds to complete. Best
                // not to do it on the Swing event dispatch thread.
                //
                Thread workerThread = new Thread(new Runnable() {
                    public void run() {
                        try {
                            System.out.println("Closing Persistit");
                            persistit.close();
                        } catch (PersistitException pe) {
                            System.err.println("PersistitException during close");
                            pe.printStackTrace();
                        }
                    }
                });
                workerThread.start();
            }
        });

        String defaultFileName = args.length == 0 ? null : args[0];
        //
        // Create the UI panel.
        //
        frame.getContentPane().add(new FindFile(defaultFileName, persistit));
        //
        // Force layout.
        //
        frame.pack();
        //
        // Make it visible.
        //
        frame.setVisible(true);
    }
}
