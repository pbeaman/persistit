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
 * Created on Mar 21, 2005
 */
package com.persistit.ui;

import java.awt.BorderLayout;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import javax.swing.JComponent;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import javax.swing.SwingUtilities;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import com.persistit.Key;
import com.persistit.Management;

/**
 * A JPanel that drops into the UI to allow inspection of Key and Value values.
 * Inspectors offer multiple views, e.g., as a displayable string, a hex dump,
 * the toString() method of a reconstituted object, and via the structure of an
 * object discovered through reflection.
 * 
 * @author Peter Beaman
 * @version 1.0
 */
class InspectorPanel extends JPanel {
    protected AdminUI _adminUI;

    private JTabbedPane _tabbedPane;

    private String _volumeName;
    private String _treeName;
    private Management.LogicalRecord _logicalRecord;

    private boolean _showValue;
    private int _selectedTab = -1;

    Map _menuMap = new HashMap();

    InspectorPanel(AdminUI ui) {
        _adminUI = ui;
        _tabbedPane = new JTabbedPane(JTabbedPane.LEFT);
        setupTabbedPanes();
        _tabbedPane.addChangeListener(new ChangeListener() {
            public void stateChanged(ChangeEvent ce) {
                handleTabChanged();
            }
        });
        setLayout(new BorderLayout());
        add(_tabbedPane, BorderLayout.CENTER);
        _selectedTab = 0;
        handleTabChanged();
    }

    private void setupTabbedPanes() {
        for (int index = 0;; index++) {
            String paneSpecification = _adminUI
                    .getProperty("InspectorTabbedPane." + index);
            if (paneSpecification == null || paneSpecification.startsWith(".")) {
                break;
            }
            StringTokenizer st = new StringTokenizer(paneSpecification, ":");
            String className = st.nextToken();
            String caption = st.nextToken();
            String iconName = null;
            if (st.hasMoreTokens()) {
                iconName = st.nextToken();
            }
            try {
                Class cl = Class.forName(className);
                AbstractInspector panel = (AbstractInspector) cl.newInstance();
                panel.setup(_adminUI, this);
                _tabbedPane.addTab(caption, (JComponent) panel);
            } catch (Exception e) {
                e.printStackTrace(); // TODO
                _adminUI.showMessage(e,
                        _adminUI.getProperty("SetupFailedMessage"),
                        JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    void setLogicalRecord(String volumeName, String treeName,
            Management.LogicalRecord lr) {
        _volumeName = volumeName;
        _treeName = treeName;
        _logicalRecord = lr;
    }

    void setLogicalRecord(Management.LogicalRecord lr) {
        setLogicalRecord(_volumeName, _treeName, lr);
    }

    Management.LogicalRecord getLogicalRecord() {
        return _logicalRecord;
    }

    String getVolumeName() {
        return _volumeName;
    }

    String getTreeName() {
        return _treeName;
    }

    boolean isShowValue() {
        return _showValue;
    }

    void setShowValue(boolean showValue) {
        _showValue = showValue;
    }

    protected synchronized void refresh(boolean reset) {
        // The fetch the updated Value for the current key.
        Management.LogicalRecord lr = getLogicalRecord();
        if (lr == null || lr.getKeyState() == null) {
            nullData();
            return;
        }

        if (_showValue) {
            new Thread(new Fetcher(getLogicalRecord())).start();
        } else {
            refreshed();
        }
    }

    private class Fetcher implements Runnable {
        Management.LogicalRecord _logicalRecord;
        Exception _exception;

        Fetcher(Management.LogicalRecord lr) {
            _logicalRecord = lr;
        }

        public void run() {
            Management management = _adminUI.getManagement();
            if (management == null)
                return;
            try {
                Management.LogicalRecord[] results = management
                        .getLogicalRecordArray(getVolumeName(), getTreeName(),
                                null, _logicalRecord.getKeyState(), Key.EQ, 1,
                                Integer.MAX_VALUE, true

                        );
                if (results == null || results.length == 0) {
                    _logicalRecord = null;
                } else {
                    Management.LogicalRecord lr = results[0];
                    if (_logicalRecord != null
                            && _logicalRecord.getKeyState().equals(
                                    lr.getKeyState())
                            && _logicalRecord.getValueState().equals(
                                    lr.getValueState())) {
                        return; // No need to do anything more.
                    }
                    _logicalRecord = results[0];
                }
                SwingUtilities.invokeLater(new Runnable() {
                    public void run() {
                        if (_exception != null) {
                            _adminUI.postException(_exception);
                        } else {
                            setLogicalRecord(getVolumeName(), getTreeName(),
                                    _logicalRecord);
                        }
                        refreshed();
                    }
                });
            } catch (RemoteException re) {
                _exception = re;
            }
        }
    }

    private void handleTabChanged() {
        int newTab = _tabbedPane.getSelectedIndex();
        if (newTab == _selectedTab)
            return;
        _selectedTab = newTab;
        AbstractInspector inspector = newTab == -1 ? null
                : (AbstractInspector) _tabbedPane.getComponent(newTab);
        if (inspector != null) {
            inspector.refreshed();
        }
    }

    AbstractInspector getCurrentInspector() {
        AbstractInspector inspector = _selectedTab == -1 ? null
                : (AbstractInspector) _tabbedPane.getComponent(_selectedTab);
        return inspector;
    }

    protected void waiting() {
        AbstractInspector inspector = getCurrentInspector();
        if (inspector != null)
            inspector.waiting();
    }

    protected void refreshed() {
        AbstractInspector inspector = getCurrentInspector();
        if (inspector != null)
            inspector.refreshed();
    }

    protected void nullData() {
        AbstractInspector inspector = getCurrentInspector();
        if (inspector != null)
            inspector.nullData();
    }

    protected void setDefaultButton() {
    }
}
