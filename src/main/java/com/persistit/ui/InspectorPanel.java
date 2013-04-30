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

import java.awt.BorderLayout;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import javax.swing.SwingConstants;
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

    private final JTabbedPane _tabbedPane;

    private String _volumeName;
    private String _treeName;
    private Management.LogicalRecord _logicalRecord;

    private boolean _showValue;
    private int _selectedTab = -1;

    Map _menuMap = new HashMap();

    InspectorPanel(final AdminUI ui) {
        _adminUI = ui;
        _tabbedPane = new JTabbedPane(SwingConstants.LEFT);
        setupTabbedPanes();
        _tabbedPane.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent ce) {
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
            final String paneSpecification = _adminUI.getProperty("InspectorTabbedPane." + index);
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
                final AbstractInspector panel = (AbstractInspector) cl.newInstance();
                panel.setup(_adminUI, this);
                _tabbedPane.addTab(caption, panel);
            } catch (final Exception e) {
                e.printStackTrace(); // TODO
                _adminUI.showMessage(e, _adminUI.getProperty("SetupFailedMessage"), JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    void setLogicalRecord(final String volumeName, final String treeName, final Management.LogicalRecord lr) {
        _volumeName = volumeName;
        _treeName = treeName;
        _logicalRecord = lr;
    }

    void setLogicalRecord(final Management.LogicalRecord lr) {
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

    void setShowValue(final boolean showValue) {
        _showValue = showValue;
    }

    protected synchronized void refresh(final boolean reset) {
        // The fetch the updated Value for the current key.
        final Management.LogicalRecord lr = getLogicalRecord();
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

        Fetcher(final Management.LogicalRecord lr) {
            _logicalRecord = lr;
        }

        @Override
        public void run() {
            final Management management = _adminUI.getManagement();
            if (management == null)
                return;
            try {
                final Management.LogicalRecord[] results = management.getLogicalRecordArray(getVolumeName(),
                        getTreeName(), null, _logicalRecord.getKeyState(), Key.EQ, 1, Integer.MAX_VALUE, true

                );
                if (results == null || results.length == 0) {
                    _logicalRecord = null;
                } else {
                    final Management.LogicalRecord lr = results[0];
                    if (_logicalRecord != null && _logicalRecord.getKeyState().equals(lr.getKeyState())
                            && _logicalRecord.getValueState().equals(lr.getValueState())) {
                        return; // No need to do anything more.
                    }
                    _logicalRecord = results[0];
                }
                SwingUtilities.invokeLater(new Runnable() {
                    @Override
                    public void run() {
                        if (_exception != null) {
                            _adminUI.postException(_exception);
                        } else {
                            setLogicalRecord(getVolumeName(), getTreeName(), _logicalRecord);
                        }
                        refreshed();
                    }
                });
            } catch (final RemoteException re) {
                _exception = re;
            }
        }
    }

    private void handleTabChanged() {
        final int newTab = _tabbedPane.getSelectedIndex();
        if (newTab == _selectedTab)
            return;
        _selectedTab = newTab;
        final AbstractInspector inspector = newTab == -1 ? null : (AbstractInspector) _tabbedPane.getComponent(newTab);
        if (inspector != null) {
            inspector.refreshed();
        }
    }

    AbstractInspector getCurrentInspector() {
        final AbstractInspector inspector = _selectedTab == -1 ? null : (AbstractInspector) _tabbedPane
                .getComponent(_selectedTab);
        return inspector;
    }

    protected void waiting() {
        final AbstractInspector inspector = getCurrentInspector();
        if (inspector != null)
            inspector.waiting();
    }

    protected void refreshed() {
        final AbstractInspector inspector = getCurrentInspector();
        if (inspector != null)
            inspector.refreshed();
    }

    protected void nullData() {
        final AbstractInspector inspector = getCurrentInspector();
        if (inspector != null)
            inspector.nullData();
    }

    protected void setDefaultButton() {
    }
}
