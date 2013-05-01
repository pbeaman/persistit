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

package com.persistit.ui;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.GridBagConstraints;
import java.awt.Image;
import java.awt.MediaTracker;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.InputEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.FileInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URL;
import java.rmi.Naming;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;

import javax.swing.AbstractAction;
import javax.swing.AbstractButton;
import javax.swing.Action;
import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.ButtonModel;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JCheckBoxMenuItem;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JRadioButtonMenuItem;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JWindow;
import javax.swing.KeyStroke;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.WindowConstants;
import javax.swing.border.TitledBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.plaf.FontUIResource;

import com.persistit.Management;
import com.persistit.Task;
import com.persistit.util.UtilControl;

public class AdminUI implements UtilControl, Runnable, AdminCommand {
    final static String BUNDLE_NAME = "com.persistit.ui.AdminUI";
    final static String SPLASH_FILE_NAME = "persistit_splash.png";
    final static String DEFAULT_RMI_HOST = "localhost:1099";
    final static boolean ENABLE_SPLASH = false;
    final static String DEFAULT_CONFIG_FILE = "adminui.properties";
    final static String CONFIG_FILE_PROPERTY = "com.persistit.ui.properties";
    final static String HELP_SET_NAME = "help/PersistitHelp.hs";

    private DecimalFormat _percentageFormat;
    private SimpleDateFormat _dateFormat;
    private DecimalFormat _timeFormat;
    private DecimalFormat _longFormat;
    private DecimalFormat _integerFormat;
    private String _fileLocationFormat;

    private ResourceBundle _bundle;
    private Properties _properties;
    private JFrame _frame = null;
    private JTabbedPane _tabbedPane = null;
    private Management _management;
    private final Map _actionMap = new HashMap();
    private final List _textComponentList = new ArrayList();
    private String _rmiHost = DEFAULT_RMI_HOST;
    private SplashWindow _splashWindow;
    private String[] _taskStates;

    int _selectedTab = -1;

    private Timer _refreshTimer = new Timer();
    private TimerTask _refreshTimerTask;
    private int _refreshInterval;
    private AbstractButton _refreshOnceButton;
    private boolean _refreshing;

    private FontUIResource _defaultFont;
    private FontUIResource _boldFont;
    private FontUIResource _fixedFont;
    private Color _persistitAccentColor;
    private String _waitingMessage;
    private String _nullMessage;

    private boolean _fixedFontMode = false;
    private boolean _wrapMode = false;
    private boolean _wrapWordMode = false;

    private String _myHostName = "unknown";

    private JavaHelpAdapter _javaHelpAdapter;

    /**
     * Implements the Closeable interface
     */
    @Override
    public boolean isAlive() {
        return _frame != null;
    }

    /**
     * Implements the Closeable interface
     */
    @Override
    public void close() {
        final Timer timer = _refreshTimer;
        if (timer != null) {
            timer.cancel();
            _refreshTimer = null;
        }

        final JFrame frame = _frame;
        final JavaHelpAdapter adapter = _javaHelpAdapter;
        _frame = null;
        _javaHelpAdapter = null;

        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                if (adapter != null) {
                    adapter.dispose();
                }
                if (frame != null) {
                    frame.dispose();
                }
            }
        });
    }

    /**
     * Implements the Runnable interface
     */
    @Override
    public void run() {
        resetRefreshTimer();
        refreshMenuEnableState();
        refresh(true);
    }

    /**
     * Construct an AdminUI on the local Persistit instance.
     */
    public AdminUI(final Management management) {
        this();
        setManagement(management);
    }

    public AdminUI(final String rmiHost) {
        this();
        _rmiHost = rmiHost;
        if (rmiHost != null)
            connect(rmiHost);
    }

    public AdminUI() {
        //
        // This is a workaround for JVM bug 4030718 in JDK1.3.
        // (See http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4030718).
        // Without this, the app won't shut down once the GUI has been
        // launched. You have to use Control-C. This workaround is
        // suggested by contributor nethi near the bottom of the comment
        // thread. The goal is to make the AWT event dispatch thread
        // a daemon thread so that the helper threads it launches are also
        // daemon threads. Then they will go away when no non-daemon
        // threads are left.
        //
        final Thread daemonThread = new Thread() {
            @Override
            public void run() {
                _frame = new JFrame();
                if (ENABLE_SPLASH) {
                    _splashWindow = new SplashWindow(_frame);
                    _splashWindow.display();
                }
                setupFrame();
            }
        };

        daemonThread.setDaemon(true);
        daemonThread.start();

        final Thread hostNameThread = new Thread() {
            @Override
            public void run() {
                try {
                    final InetAddress inetAddr = InetAddress.getLocalHost();
                    _myHostName = inetAddr.getHostName();
                } catch (final Exception e) {
                }
            }
        };
        hostNameThread.setDaemon(true);
        hostNameThread.start();
        try {
            daemonThread.join();
            if (ENABLE_SPLASH) {
                Thread.sleep(2000);
                final SplashWindow sp = _splashWindow;
                if (sp != null)
                    sp.dispose();
                _splashWindow = null;
            }
        } catch (final InterruptedException ie) {
        }
    }

    public void refresh(final boolean reset) {
        synchronized (this) {
            if (_refreshing)
                return;
            _refreshing = true;
        }
        try {
            Management management = _management;
            if (management != null) {
                try {
                    management.isInitialized();
                } catch (final RemoteException re) {
                    disconnect();
                    management = null;
                }
            }

            if (_tabbedPane != null) {
                try {
                    final AdminPanel mp = (AdminPanel) _tabbedPane.getSelectedComponent();
                    mp.refresh(reset || management == null);
                } catch (final Exception e) {
                    postException(e);
                }
            }

            // Reset toggle buttons to represent current state.
            for (final Iterator iter = _actionMap.values().iterator(); iter.hasNext();) {
                final AdminAction action = (AdminAction) iter.next();
                if (action.isToggle()) {
                    action.stateChanged(getManagementState(action));
                }
            }
        } finally {
            _refreshing = false;
        }
    }

    public Management getManagement() {
        return _management;
    }

    @Override
    public void setManagement(final Management newManagement) {
        final Management oldManagement = _management;
        if (oldManagement != null) {
            unfreeze(oldManagement);
        }
        _management = newManagement;
        SwingUtilities.invokeLater(this);
    }

    private void unfreeze(final Management management) {
        try {
            management.setShutdownSuspended(false);
            management.setUpdateSuspended(false);
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    public String getProperty(final String propertyName) {
        try {
            if (_bundle == null) {
                _bundle = ResourceBundle.getBundle(AdminUI.BUNDLE_NAME);
                String propFileName = null;
                try {
                    propFileName = System.getProperty(CONFIG_FILE_PROPERTY);
                } catch (final Exception e) {
                }
                if (propFileName == null)
                    propFileName = DEFAULT_CONFIG_FILE;
                try {
                    final FileInputStream fis = new FileInputStream(propFileName);
                    _properties = new Properties();
                    _properties.load(fis);
                } catch (final Exception e) {
                }
            }
            String value = null;
            if (_properties != null)
                value = _properties.getProperty(propertyName);
            if (value == null)
                value = _bundle.getString(propertyName);
            return value;
        } catch (final MissingResourceException mre) {
            return null;
        }
    }

    public int getRefreshInterval() {
        return _refreshInterval;
    }

    public String getHostName() {
        return _myHostName;
    }

    public FontUIResource getBoldFont() {
        return _boldFont;
    }

    public FontUIResource getDefaultFont() {
        return _defaultFont;
    }

    public FontUIResource getFixedFont() {
        return _fixedFont;
    }

    public Color getPersistitAccentColor() {
        return _persistitAccentColor;
    }

    public String getWaitingMessage() {
        return _waitingMessage;
    }

    public String getNullMessage() {
        return _nullMessage;
    }

    public String getTaskStateString(final int state) {
        if (state >= 0 && state < _taskStates.length) {
            return _taskStates[state];
        }
        return "?";
    }

    public String formatDate(final long ts) {
        if (ts == 0 || ts == Long.MAX_VALUE || ts == Long.MIN_VALUE)
            return "";
        return _dateFormat.format(new Date(ts));
    }

    public String formatTime(final long ts) {
        return _timeFormat.format(ts / 1000.0);
    }

    public String formatInteger(final int v) {
        return _integerFormat.format(v);
    }

    public String formatLong(final long v) {
        return _longFormat.format(v);
    }

    public String formatPercent(final double v) {
        return _percentageFormat.format(v);
    }

    public String formatFileLocation(final String path, final long address) {
        return path == null ? "" : String.format(_fileLocationFormat, path, address);
    }

    private void setFrameTitle(final String hostName) {
        String title = getProperty("title");
        if (title == null)
            title = "Persistit Admin Client";
        if (title.indexOf("Persistit") < 0)
            title = "Persistit - " + title;
        {
            if (hostName != null && hostName.length() > 0) {
                title += " - " + hostName;
            }
        }
        _frame.setTitle(title);
    }

    private void setupFrame() {
        _defaultFont = new FontUIResource("Dialog", Font.PLAIN, 12);
        _boldFont = new FontUIResource("Dialog", Font.BOLD, 12);
        _fixedFont = new FontUIResource("Monospaced", Font.PLAIN, 12);

        _persistitAccentColor = new Color(119, 17, 34);

        final String lnfClassName = getProperty("lnf");
        boolean lafLoaded = false;
        if (lnfClassName != null && lnfClassName.length() > 0) {
            try {
                final Class lnfClass = Class.forName(lnfClassName);

                Method setPropertyMethod = null;

                final Enumeration props = _bundle.getKeys();
                while (props.hasMoreElements()) {
                    final String propName = (String) props.nextElement();
                    if (propName.startsWith("lnf.")) {
                        final String propValue = _bundle.getString(propName);
                        if (setPropertyMethod == null) {
                            setPropertyMethod = lnfClass.getMethod("setProperty", new Class[] { String.class,
                                    String.class });
                        }

                        setPropertyMethod.invoke(null, new Object[] { propName, propValue });
                    }
                }
                final javax.swing.LookAndFeel lnf = (javax.swing.LookAndFeel) lnfClass.newInstance();

                javax.swing.UIManager.setLookAndFeel(lnf);
                lafLoaded = true;
            } catch (final Exception ex) {
                System.err.println("Could not load LnF class " + lnfClassName);
                ex.printStackTrace();
            }
        }

        if (!lafLoaded) {
            try {
                UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
            } catch (final Exception e) {
                // Ignore exception
            }
        }

        setUIFont(_defaultFont);
        _percentageFormat = new DecimalFormat(getProperty("PERCENTAGE_FORMAT"));
        _dateFormat = new SimpleDateFormat(getProperty("DATE_FORMAT"));
        _timeFormat = new DecimalFormat(getProperty("TIME_FORMAT"));
        _longFormat = new DecimalFormat(getProperty("LONG_FORMAT"));
        _integerFormat = new DecimalFormat(getProperty("INTEGER_FORMAT"));
        _fileLocationFormat = getProperty("FILE_LOCATION_FORMAT");
        _waitingMessage = getProperty("WaitingMessage");
        _nullMessage = getProperty("NullMessage");
        _taskStates = new String[7];
        for (int state = 0; state < 7; state++) {
            _taskStates[state] = getProperty("TaskState." + state);
        }

        _tabbedPane = new JTabbedPane();
        _frame.getContentPane().add(_tabbedPane);

        _frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        _frame.addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosed(final WindowEvent we) {
                SwingUtilities.invokeLater(new Runnable() {
                    @Override
                    public void run() {
                        // if (_frame != null)
                        // {
                        //
                        // Will restore normal operation to
                        // target Persistit instance
                        //
                        setManagement(null);
                        close();
                        // }
                    }
                });
            }
        });
        setupMenu();
        setupTabbedPanes();
        _tabbedPane.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent ce) {
                handleTabChanged();
            }
        });
        handleTabChanged();
        refreshMenuEnableState();
        final Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        setFrameTitle(null);
        _frame.pack();
        _frame.setLocation((screenSize.width - _frame.getWidth()) / 2, (screenSize.height - _frame.getHeight()) / 2);
        _frame.setVisible(true);
    }

    void setupMenu() {
        final JMenuBar bar = new JMenuBar();
        _frame.setJMenuBar(bar);

        final String menuBarItems = getProperty("MainMenu");
        final StringTokenizer st1 = new StringTokenizer(menuBarItems, ",");
        while (st1.hasMoreTokens()) {
            final String s = st1.nextToken();
            final AdminAction menuAction = createAction(this, s);
            final JMenu menu = new JMenu(menuAction);
            menuAction.addButton(menu);
            bar.add(menu);
            final StringTokenizer st2 = new StringTokenizer(s, ":");
            final String actionName = st2.nextToken();
            final JComponent[] items = createMenuArray(this, "MainMenu", actionName);
            for (int index = 0; index < items.length; index++) {
                menu.add(items[index]);
            }
        }
    }

    JComponent[] createMenuArray(final AdminCommand command, final String basePropertyName, final String actionName) {
        final ArrayList list = new ArrayList();
        for (int index = 0;; index++) {
            final String propName = basePropertyName + "." + actionName + "." + index;
            final String t = getProperty(propName);
            if (t == null || t.startsWith("."))
                break;
            if (t.startsWith("-")) {
                list.add(new JSeparator());
            } else {
                final AdminAction action = createAction(command, t);
                final AbstractButton item = action.menuItem(command, propName);
                action.addButton(item);
                list.add(item);
            }
        }
        return (JComponent[]) (list.toArray(new JComponent[list.size()]));
    }

    private void setupTabbedPanes() {
        for (int index = 0;; index++) {
            final String paneSpecification = getProperty("TabbedPane." + index);
            if (paneSpecification == null || paneSpecification.startsWith(".")) {
                break;
            }
            final StringTokenizer st = new StringTokenizer(paneSpecification, ":");
            final String className = st.nextToken();
            final String caption = st.nextToken();
            String iconName = null;
            if (st.hasMoreTokens()) {
                iconName = st.nextToken();
            }
            try {
                final Class cl = Class.forName(className);
                final AdminPanel panel = (AdminPanel) cl.newInstance();
                panel.setup(this);
                _tabbedPane.addTab(caption, panel);
            } catch (final Exception e) {
                showMessage(e, getProperty("SetupFailedMessage"), JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    private void handleTabChanged() {
        final int oldTab = _selectedTab;
        final int newTab = _tabbedPane.getSelectedIndex();
        if (oldTab == newTab)
            return;
        _selectedTab = newTab;
        final AdminPanel oldPanel = oldTab == -1 ? null : (AdminPanel) _tabbedPane.getComponent(oldTab);
        final AdminPanel newPanel = newTab == -1 ? null : (AdminPanel) _tabbedPane.getComponent(newTab);
        if (oldPanel != null) {
            oldPanel.setIsShowing(false);
            changeMenuMap(oldPanel.getMenuMap(), false);
        }
        if (newPanel != null) {
            newPanel.setIsShowing(true);
            changeMenuMap(newPanel.getMenuMap(), true);
            scheduleRefresh(-1);
        }
        newPanel.setDefaultButton();
    }

    void changeMenuMap(final Map menuMap, final boolean add) {
        for (final Iterator iter = menuMap.entrySet().iterator(); iter.hasNext();) {
            final Map.Entry entry = (Map.Entry) iter.next();
            String menuName = (String) entry.getKey();
            if (menuName.indexOf(".") >= 0) {
                menuName = menuName.substring(0, menuName.indexOf("."));
            }
            final JComponent[] items = (JComponent[]) entry.getValue();
            final Action menuAction = (Action) _actionMap.get(menuName);
            final JMenuBar bar = _frame.getJMenuBar();
            if (menuAction != null) {
                for (int index = 0; index < bar.getMenuCount(); index++) {
                    final JMenu menu = bar.getMenu(index);
                    if (menu.getAction() == menuAction) {
                        for (int k = 0; k < items.length; k++) {
                            final JComponent item = items[k];
                            if (add) {
                                menu.add(item);
                            } else {
                                menu.remove(item);
                            }
                        }
                    }
                }
            }
        }
    }

    void refreshMenuEnableState() {
        for (final Iterator iter = _actionMap.values().iterator(); iter.hasNext();) {
            final AdminAction action = (AdminAction) iter.next();
            if (action.isDisableSensitive()) {
                action.setEnabled(_management != null);
            }
        }
    }

    /**
     * Sets the default font for all Swing components.
     * 
     * @param font
     *            The FontUIResource to be made the default
     */
    private static void setUIFont(final FontUIResource font) {
        final java.util.Enumeration keys = UIManager.getDefaults().keys();
        while (keys.hasMoreElements()) {
            final Object key = keys.nextElement();
            final Object value = UIManager.get(key);
            if (value instanceof javax.swing.plaf.FontUIResource) {
                UIManager.put(key, font);
            }
        }
    }

    JComponent addLabeledField(final JPanel panel, final GridBagConstraints gbc, final JComponent component,
            final String propertyName, final boolean newLine) {
        final String prop = getProperty(propertyName);
        final StringTokenizer st = new StringTokenizer(prop, ":");
        final String caption = st.nextToken();
        String widthStr = st.nextToken();
        final String alignment = st.nextToken();
        final String gridwidth = st.nextToken();
        final String tooltip = st.nextToken();
        String heightStr = "1";
        final int p = widthStr.indexOf(',');
        if (p >= 0) {
            heightStr = widthStr.substring(p + 1);
            widthStr = widthStr.substring(0, p);
        }

        final JLabel label = new JLabel(caption);
        label.setHorizontalAlignment(JLabel.TRAILING);
        gbc.gridwidth = 1;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 0.0;
        panel.add(label, gbc);
        gbc.gridx++;

        try {
            if ("REMAINDER".equals(gridwidth)) {
                gbc.gridwidth = GridBagConstraints.REMAINDER;
            } else if ("RELATIVE".equals(gridwidth)) {
                gbc.gridwidth = GridBagConstraints.RELATIVE;
            } else {
                gbc.gridwidth = Integer.parseInt(gridwidth);
            }
        } catch (final NumberFormatException nfe) {
            // ignore
        }

        component.setToolTipText(tooltip);
        JComponent wrappedComponent = component;
        if (component instanceof JTextField) {
            final JTextField textField = (JTextField) component;
            textField.setColumns(Integer.parseInt(widthStr));
            textField.setHorizontalAlignment(alignment.equals("R") ? SwingConstants.TRAILING
                    : alignment.equals("C") ? SwingConstants.CENTER : SwingConstants.LEADING);
            textField.setEditable(false);
            textField.setEnabled(true);
            textField.setBackground(Color.white);
        } else if (component instanceof JTextArea) {
            final JTextArea textArea = (JTextArea) component;
            textArea.setColumns(Integer.parseInt(widthStr));
            textArea.setRows(Integer.parseInt(heightStr));
            textArea.setEditable(false);
            textArea.setEnabled(true);
            textArea.setBackground(Color.white);
            wrappedComponent = new JScrollPane(textArea);
            wrappedComponent.setMinimumSize(textArea.getPreferredScrollableViewportSize());
            registerTextComponent(textArea);
        }
        gbc.weightx = 1.0;
        panel.add(wrappedComponent, gbc);

        if (newLine || gbc.gridwidth == GridBagConstraints.REMAINDER || gbc.gridwidth == GridBagConstraints.RELATIVE) {
            gbc.gridy++;
            gbc.gridx = 0;
        } else {
            gbc.gridx += gbc.gridwidth;
        }

        component.setMinimumSize(component.getPreferredSize());
        return component;
    }

    AdminAction createAction(final AdminCommand command, final String specification) {
        final StringTokenizer st = new StringTokenizer(specification, ":");
        String actionName = st.nextToken();
        String caption = null;
        String iconName = null;
        if (st.hasMoreTokens())
            caption = st.nextToken();
        if (st.hasMoreTokens())
            iconName = st.nextToken();

        final boolean isDisableSensitive = actionName.startsWith("?");
        if (isDisableSensitive)
            actionName = actionName.substring(1);

        final boolean isToggle = actionName.startsWith("*");
        if (isToggle)
            actionName = actionName.substring(1);

        final boolean isRadio = actionName.startsWith("!");
        if (isRadio)
            actionName = actionName.substring(1);

        final boolean hasSubActions = actionName.startsWith("+");
        if (hasSubActions)
            actionName = actionName.substring(1);

        int mnemonicIndex = -1;
        int acceleratorChar = 0;
        if (caption != null) {
            mnemonicIndex = caption.indexOf('&');
            if (mnemonicIndex >= 0) {
                caption = caption.substring(0, mnemonicIndex) + caption.substring(mnemonicIndex + 1);
            }

            final int acceleratorIndex = caption.indexOf('^');
            if (acceleratorIndex >= 0 && acceleratorIndex + 1 < caption.length()) {
                acceleratorChar = caption.charAt(acceleratorIndex + 1);
                caption = caption.substring(0, acceleratorIndex) + caption.substring(acceleratorIndex + 2);
            }
        }

        AdminAction action = null;
        if (caption == null) {
            action = new AdminAction(actionName);
        } else if (iconName == null || iconName.length() == 0) {
            action = new AdminAction(command, actionName, caption);
        } else {
            action = new AdminAction(command, actionName, caption, new ImageIcon(iconName));
        }
        action._isToggle = isToggle;
        action._isRadio = isRadio;
        action._hasSubActions = hasSubActions;
        action._isDisableSensitive = isDisableSensitive;
        action._mnemonicIndex = mnemonicIndex;
        action._acceleratorChar = acceleratorChar;
        _actionMap.put(actionName, action);
        return action;
    }

    public JTabbedPane getTabbedPane() {
        return _tabbedPane;
    }

    public AdminAction getAction(final String actionName) {
        return (AdminAction) _actionMap.get(actionName);
    }

    public TitledBorder createTitledBorder(final String captionProperty) {
        String caption = getProperty(captionProperty);
        if (caption == null || caption.length() == 0)
            caption = " ";

        return BorderFactory.createTitledBorder(BorderFactory.createEmptyBorder(10, 2, 2, 2), caption,
                TitledBorder.LEADING, TitledBorder.DEFAULT_POSITION, _boldFont, _persistitAccentColor);
    }

    private void connectDialog(final String defaultHost) {
        final Object value = JOptionPane.showInputDialog(_frame, "RMI Registry", "Connection Specification",
                JOptionPane.QUESTION_MESSAGE, null, null, defaultHost);
        if (value instanceof String) {
            final Thread thread = new Thread() {
                @Override
                public void run() {
                    connect((String) value);
                }
            };
            thread.setDaemon(true);
            thread.start();
        }
    }

    private boolean connect(final String rmiHost) {
        try {
            _rmiHost = rmiHost;
            final Management management = (Management) Naming.lookup("//" + rmiHost + "/PersistitManagementServer");
            setManagement(management);
            setFrameTitle(rmiHost);
            return true;
        } catch (final Exception e) {
            setManagement(null);
            showMessage(e, getProperty("ConnectionFailedMessage"), JOptionPane.ERROR_MESSAGE);
            return false;
        }
    }

    private void disconnect() {
        setManagement(null);
    }

    void showMessage(final Object message, final String title, final int type) {
        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                JOptionPane.showMessageDialog(_frame, message, title, type);
            }
        });
    }

    public static void main(final String[] args) {
        new AdminUI(args.length > 0 ? args[0] : null);
    }

    /**
     * Actions for this UI
     */
    class AdminAction extends AbstractAction {
        private String _name = "";
        private String _caption = "";
        private AdminCommand _command;
        private boolean _isToggle;
        private boolean _isRadio;
        private boolean _hasSubActions;
        boolean _isDisableSensitive;
        private ArrayList _buttonList;
        private int _mnemonicIndex;
        private int _acceleratorChar;

        AdminAction(final String actionName) {
            super();
            _name = actionName;
        }

        AdminAction(final AdminCommand command, final String actionName, final String caption) {
            super(caption);
            _name = actionName;
            _caption = caption;
            _command = command;
        }

        AdminAction(final AdminCommand command, final String actionName, final String caption, final Icon icon) {
            super(caption, icon);
            _name = actionName;
            _caption = caption;
            _command = command;
        }

        @Override
        public void actionPerformed(final ActionEvent ae) {
            if (!_refreshing) {
                _command.actionPerformed(this, ae);
            }
        }

        public String getName() {
            return _name;
        }

        public AdminCommand getActionCommand() {
            return _command;
        }

        @Override
        public String toString() {
            return _caption;
        }

        public boolean isToggle() {
            return _isToggle;
        }

        public boolean isRadio() {
            return _isRadio;
        }

        public boolean hasSubActions() {
            return _hasSubActions;
        }

        public boolean isDisableSensitive() {
            return _isDisableSensitive;
        }

        public ArrayList getButtonList() {

            return _buttonList;
        }

        public void addButton(final AbstractButton button) {
            if (_buttonList == null)
                _buttonList = new ArrayList();
            _buttonList.add(button);
            if (_mnemonicIndex >= 0) {
                final char mnemonicChar = _caption.charAt(_mnemonicIndex);
                button.setMnemonic((int) mnemonicChar);
            }
            if (_acceleratorChar > 0 && button instanceof JMenuItem && !(button instanceof JMenu)) {
                final KeyStroke accelerator = KeyStroke.getKeyStroke(_acceleratorChar, InputEvent.CTRL_MASK);
                ((JMenuItem) button).setAccelerator(accelerator);
            }
        }

        public void removeButton(final AbstractButton button) {
            if (_buttonList != null) {
                _buttonList.remove(button);
            }
            if (_buttonList.size() == 0)
                _buttonList = null;
        }

        public void stateChanged(final boolean selected) {
            if (_buttonList != null) {
                for (int i = 0; i < _buttonList.size(); i++) {
                    final AbstractButton button = (AbstractButton) _buttonList.get(i);
                    if (button.isSelected() != selected) {
                        button.setSelected(selected);
                    }
                }
            }
        }

        public AbstractButton menuItem(final AdminCommand command, final String propName) {
            AbstractButton item;
            if (isToggle()) {
                item = new JCheckBoxMenuItem(this);
                addButton(item);
            } else if (isRadio()) {
                item = new JRadioButtonMenuItem(this);
                addButton(item);
            } else {
                if (hasSubActions()) {
                    item = new JMenu(this);
                    ButtonGroup bg = null;
                    for (int subIndex = 0;; subIndex++) {
                        final String subName = propName + "." + subIndex;
                        final String u = getProperty(subName);
                        if (u == null)
                            break;
                        final AdminAction action = createAction(command, u);
                        final AbstractButton subItem = action.menuItem(command, subName);
                        item.add(subItem);
                        if (subItem instanceof JRadioButtonMenuItem) {
                            final boolean first = bg == null;
                            if (first)
                                bg = new ButtonGroup();
                            bg.add(subItem);
                            if (first)
                                subItem.setSelected(true);
                        }
                    }
                } else {
                    item = new JMenuItem(this);
                }
            }
            if ("REFRESH_0".equals(_name)) {
                _refreshOnceButton = item;
            }
            return item;
        }
    }

    ButtonModel getMenuItemModel(final String actionName) {
        final AdminAction action = (AdminAction) _actionMap.get(actionName);
        final ArrayList buttons = action.getButtonList();
        if (buttons != null) {
            for (final Iterator iter = buttons.iterator(); iter.hasNext();) {
                final AbstractButton button = (AbstractButton) iter.next();
                if (button instanceof JMenuItem) {
                    return button.getModel();
                }
            }
        }
        return null;
    }

    private void resetRefreshTimer() {
        if (_refreshOnceButton != null) {
            _refreshOnceButton.setSelected(true);
        }
    }

    @Override
    public void actionPerformed(final AdminAction action, final ActionEvent ae) {
        try {
            final String name = action._name;
            boolean textModeChanged = false;
            // System.out.println("AdminUI actionPerformed: " + name);
            final Management management = _management;

            if (name.startsWith("REFRESH")) {
                int interval = 0;
                if (name.startsWith("REFRESH_")) {
                    interval = Integer.parseInt(name.substring("REFRESH_".length()));
                }
                scheduleRefresh(interval);
            } else if ("EXIT".equals(name)) {
                setManagement(null);
                close();
            } else if ("CONNECT".equals(name)) {
                connectDialog(_rmiHost);
            } else if ("DISCONNECT".equals(name)) {
                disconnect();
            } else if ("ABOUT".equals(name)) {
                if (_splashWindow == null) {
                    _splashWindow = new SplashWindow(_frame);
                    _splashWindow.display();
                }
            } else if ("HELP".equals(name)) {
                showHelp(ae);
            } else if ("SSUSP".equals(name)) {
                boolean state = ((AbstractButton) ae.getSource()).isSelected();
                if (management != null && management.isInitialized()) {
                    if (state) {
                        final int confirm = JOptionPane.showConfirmDialog(_frame, getProperty("ssusp.confirm"));

                        if (confirm != JOptionPane.YES_OPTION) {
                            state = false;
                        }
                    }

                    management.setShutdownSuspended(state);
                    scheduleRefresh(-1);
                } else {
                    ((AbstractButton) ae.getSource()).setSelected(false);
                }
            } else if ("USUSP".equals(name)) {
                boolean state = ((AbstractButton) ae.getSource()).isSelected();
                if (management != null && management.isInitialized()) {
                    if (state) {

                        final int confirm = JOptionPane.showConfirmDialog(_frame, getProperty("ususp.confirm"));

                        if (confirm != JOptionPane.YES_OPTION) {
                            state = false;
                        }
                    }
                    management.setUpdateSuspended(state);
                    scheduleRefresh(-1);
                } else {
                    ((AbstractButton) ae.getSource()).setSelected(false);
                }
            } else if ("AONLY".equals(name)) {
                boolean state = ((AbstractButton) ae.getSource()).isSelected();
                if (management != null && management.isInitialized()) {
                    if (state) {

                        final int confirm = JOptionPane.showConfirmDialog(_frame, getProperty("aonly.confirm"));

                        if (confirm != JOptionPane.YES_OPTION) {
                            state = false;
                        }
                    }
                    management.setAppendOnly(state);
                    scheduleRefresh(-1);
                } else {
                    ((AbstractButton) ae.getSource()).setSelected(false);
                }
            } else if ("JCOPY".equals(name)) {
                boolean state = ((AbstractButton) ae.getSource()).isSelected();
                if (management != null && management.isInitialized()) {
                    if (state) {

                        final int confirm = JOptionPane.showConfirmDialog(_frame, getProperty("jcopy.confirm"));

                        if (confirm != JOptionPane.YES_OPTION) {
                            state = false;
                        }
                    }
                    management.setJournalCopyingFast(state);
                    scheduleRefresh(-1);
                } else {
                    ((AbstractButton) ae.getSource()).setSelected(false);
                }
            } else if ("FLUSH".equals(name)) {
                if (management != null && management.isInitialized()) {
                    management.flushAndForce();
                }
                // } else if ("SHUTDOWN".equals(name)) {
                // if (management != null && management.isInitialized()) {
                // int confirm = JOptionPane.showConfirmDialog(_frame,
                // getProperty("shutdown.confirm"));
                //
                // if (confirm == JOptionPane.YES_OPTION) {
                // management.close();
                // }
                // }
            } else if ("WRAP_MODE_NONE".equals(name)) {
                _wrapMode = false;
                textModeChanged = true;
            } else if ("WRAP_MODE_CHARACTER".equals(name)) {
                _wrapMode = true;
                _wrapWordMode = false;
                textModeChanged = true;
            } else if ("WRAP_MODE_WORD".equals(name)) {
                _wrapMode = true;
                _wrapWordMode = true;
                textModeChanged = true;
            } else if ("FONT_MODE_NORMAL".equals(name)) {
                _fixedFontMode = false;
                textModeChanged = true;
            } else if ("FONT_MODE_FIXED".equals(name)) {
                _fixedFontMode = true;
                textModeChanged = true;
            } else if (name.startsWith("TASK.")) {
                final String taskDescriptorPropName = "TaskDescriptor." + name.substring(5);
                final String taskDescriptorString = getProperty(taskDescriptorPropName);
                final TaskSetupPanel tsp = new TaskSetupPanel(this, taskDescriptorString);

                final JOptionPane optionPane = new JOptionPane(tsp, JOptionPane.QUESTION_MESSAGE,
                        JOptionPane.OK_CANCEL_OPTION);

                final JDialog dialog = optionPane.createDialog(_frame, tsp.getTaskName());
                dialog.setResizable(true);

                tsp.refresh(false);

                dialog.pack();
                dialog.setVisible(true);

                final Object value = optionPane.getValue();
                if (value != null && value instanceof Integer && ((Integer) value).intValue() == 0) {
                    doTask(tsp);
                }
            } else if ("START_NEW_TASK".equals(name)) {
                final JPopupMenu popup = new JPopupMenu(getProperty("SelectNewTaskMessage"));
                for (int index = 0;; index++) {
                    final AdminAction taskAction = getAction("TASK." + index);
                    if (taskAction == null)
                        break;
                    popup.add(new JMenuItem(taskAction));
                }
                final JComponent source = (JComponent) ae.getSource();
                popup.show(source, source.getWidth(), 0);
            } else {
                System.out.println("Undefined ACTION name " + name);
            }

            if (textModeChanged)
                handleTextModeChanged();
        } catch (final NoSuchObjectException ex) {
            setManagement(null);
            scheduleRefresh(0);
        } catch (final Exception ex) {
            postException(ex);
        }
    }

    public boolean getManagementState(final AdminAction action) {
        final String name = action.getName();
        try {
            if (_management == null)
                return false;
            if ("SSUSP".equals(name)) {
                return _management.isShutdownSuspended();
            }
            if ("USUSP".equals(name)) {
                return _management.isUpdateSuspended();
            }
            if ("AONLY".equals(name)) {
                return _management.getJournalInfo().isAppendOnly();
            }
            if ("USUSP".equals(name)) {
                return _management.getJournalInfo().isFastCopying();
            }
        } catch (final RemoteException re) {
        }
        return false;
    }

    void handleTextModeChanged() {
        for (final Iterator iter = _textComponentList.iterator(); iter.hasNext();) {
            final JComponent component = (JComponent) iter.next();
            component.setFont(_fixedFontMode ? _fixedFont : _defaultFont);
            if (component instanceof JTextArea) {
                final JTextArea tc = (JTextArea) component;
                tc.setLineWrap(_wrapMode);
                if (_wrapMode)
                    tc.setWrapStyleWord(_wrapWordMode);
            }
        }
    }

    void registerTextComponent(final JComponent component) {
        _textComponentList.add(component);
    }

    public void postException(Throwable throwable) {
        try {
            Throwable cause = null;
            if (throwable instanceof RemoteException) {
                try {
                    // Use reflection so this will compile and run (but not
                    // report the cause correctly) in JDK 1.3
                    final Method method = throwable.getClass().getMethod("getCause", new Class[0]);
                    if (method != null) {
                        cause = (Throwable) method.invoke(throwable, new Object[0]);
                    }
                } catch (final NoSuchMethodException e) {
                    // ignore for JDK 1.3
                }
            }
            if (cause != null)
                throwable = cause;
            if (throwable instanceof Management.WrappedRemoteException) {
                throwable = ((Management.WrappedRemoteException) throwable).getCause();
            }
            showMessage(throwable, getProperty("ExceptionMessage"), JOptionPane.ERROR_MESSAGE);
        } catch (final Exception e) {
            System.out.println("Exception while reporting throwable:");
            e.printStackTrace();
        }
    }

    public synchronized void scheduleRefresh(final int interval) {
        if (_refreshTimer == null)
            return;

        if (_refreshTimerTask != null) {
            _refreshTimerTask.cancel();
        }

        _refreshTimerTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    SwingUtilities.invokeAndWait(new Runnable() {
                        @Override
                        public void run() {
                            refresh(false);
                        }
                    });
                } catch (final InterruptedException e) {
                } catch (final InvocationTargetException e) {
                }
            }
        };

        if (interval >= 0)
            _refreshInterval = interval;
        if (_refreshInterval > 0) {
            _refreshTimer.schedule(_refreshTimerTask, 0, _refreshInterval * 1000);
        } else {
            _refreshTimer.schedule(_refreshTimerTask, 0);
        }
    }

    protected void doTask(final TaskSetupPanel tsp) throws RemoteException {
        final Management management = getManagement();
        if (management != null) {
            // TODO - reinstate task interface
            management.startTask(tsp.getDescriptionString(), tsp.getOwnerString(), tsp.getCommandLine(),
                    tsp.getExpirationTime(), tsp.isVerboseEnabled() ? Task.LOG_VERBOSE : Task.LOG_NORMAL);
        }
        scheduleRefresh(1000);
    }

    private synchronized void showHelp(final ActionEvent ae) {
        // disabled and removed for now
        //
        // if (_javaHelpAdapter == null) {
        // try {
        // JavaHelpAdapter adapter = new JavaHelpAdapterImpl();
        // adapter.create(HELP_SET_NAME, ae);
        // } catch (Exception e) {
        // showMessage(e, "Exception while launching Help",
        // JOptionPane.ERROR_MESSAGE);
        // }
        // } else {
        // _javaHelpAdapter.showHelp(ae);
        // }
    }

    private class SplashWindow extends JWindow {
        Image _image;

        private SplashWindow(final JFrame frame) {
            super(frame);
        }

        private void display() {
            final URL url = AdminUI.class.getResource(SPLASH_FILE_NAME);
            _image = Toolkit.getDefaultToolkit().createImage(url);
            // Load the image
            final MediaTracker mt = new MediaTracker(this);
            mt.addImage(_image, 0);
            try {
                mt.waitForID(0);
            } catch (final InterruptedException ie) {
            }

            // Center the window on the screen.
            final int width = _image.getWidth(this);
            final int height = _image.getHeight(this);

            setSize(width, height);
            final Dimension screenDim = Toolkit.getDefaultToolkit().getScreenSize();
            setLocation((screenDim.width - width) / 2, (screenDim.height - height) / 2);
            addMouseListener(new MouseAdapter() {
                @Override
                public void mouseClicked(final MouseEvent evt) {
                    setVisible(false);
                    dispose();
                    _splashWindow = null;
                }
            });
            setVisible(true);
        }

        @Override
        public void update(final Graphics g) {
        }

        @Override
        public void paint(final Graphics g) {
            g.drawImage(_image, 0, 0, this);
        }
    }
}
