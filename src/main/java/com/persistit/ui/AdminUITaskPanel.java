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
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.TreeMap;

import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.persistit.Management;
import com.persistit.Management.TaskStatus;
import com.persistit.Task;
import com.persistit.ui.AdminUI.AdminAction;
import com.persistit.util.Util;

public class AdminUITaskPanel extends AdminPanel implements AdminCommand {

    private static final long serialVersionUID = 1L;

    private final static int FAST_REFRESH_INTERVAL = 1;

    AdminUI _adminUI;

    private static double MAIN_SPLITPANE_RESIZE_WEIGHT = .20;
    private ManagementTableModel _taskStatusArrayModel;

    private final Map _menuMap = new TreeMap();
    private boolean _refreshing;

    private JPanel _taskListPanel;
    private JPanel _detailPanel;

    private JTable _taskTable;

    private JTextField _taskIdField;
    private JTextField _descriptionField;
    private JTextField _ownerField;
    private JTextField _stateField;
    private JTextField _startTimeField;
    private JTextField _endTimeField;
    private JTextField _expirationTimeField;
    private JTextArea _statusDetailArea;
    private JTextField _lastExceptionField;
    private JTextArea _messageLogArea;

    private int _savedRefreshInterval;
    private boolean _fastRefreshIntervalSet;

    private long _selectedTaskId = -1;

    @Override
    protected void setup(final AdminUI ui) throws NoSuchMethodException, RemoteException {
        _adminUI = ui;
        _taskListPanel = new JPanel(new BorderLayout());

        _taskStatusArrayModel = new ManagementTableModel(TaskStatus.class, "TaskStatus", ui);

        _menuMap.put("TASK.1", _adminUI.createMenuArray(this, "TaskPanelMenu", "TASK"));

        _taskTable = new JTable(_taskStatusArrayModel);
        _taskTable.setAutoCreateRowSorter(true);
        _taskTable.setPreferredScrollableViewportSize(new Dimension(800, 100));
        _taskTable.setAutoCreateColumnsFromModel(false);
        _taskTable.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        _taskStatusArrayModel.formatColumns(_taskTable, null);

        _taskTable.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
            @Override
            public void valueChanged(final ListSelectionEvent lse) {
                final int[] selectedRows = _taskTable.getSelectedRows();
                if (!lse.getValueIsAdjusting() && !_refreshing) {
                    if (selectedRows.length == 1) {
                        final int index = selectedRows[0];
                        final TaskStatus[] array = (TaskStatus[]) _taskStatusArrayModel.getInfoArray();
                        if (array != null && index < array.length) {
                            selectTask(array[index]);
                        } else {
                            selectTask(null);
                        }
                    } else {
                        selectTask(null);
                    }
                    setTaskActionEnabledState(selectedRows.length > 0);
                }
            }
        });

        final JScrollPane treeScrollPane = new JScrollPane(_taskTable);
        treeScrollPane.setBorder(null);
        _taskListPanel.setBorder(_adminUI.createTitledBorder("TaskPanel.tasks"));

        final JPanel buttonPanel = new JPanel();
        buttonPanel.add(new JButton(ui.getAction("START_NEW_TASK")));
        buttonPanel.add(new JButton(ui.getAction("SUSPEND_TASKS")));
        buttonPanel.add(new JButton(ui.getAction("RESUME_TASKS")));
        buttonPanel.add(new JButton(ui.getAction("STOP_TASKS")));
        buttonPanel.add(new JButton(ui.getAction("REMOVE_TASKS")));

        _taskListPanel.add(treeScrollPane, BorderLayout.CENTER);
        _taskListPanel.add(buttonPanel, BorderLayout.SOUTH);
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(3, 1, 1, 3);
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 1.0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.BOTH;

        _detailPanel = new JPanel(new GridBagLayout());
        _detailPanel.setBorder(_adminUI.createTitledBorder("TaskPanel.taskDetail"));

        _taskIdField = (JTextField) ui.addLabeledField(_detailPanel, gbc, new JTextField(), "TaskPanel.taskId", false);
        _stateField = (JTextField) ui.addLabeledField(_detailPanel, gbc, new JTextField(), "TaskPanel.state", false);
        _descriptionField = (JTextField) ui.addLabeledField(_detailPanel, gbc, new JTextField(),
                "TaskPanel.description", true);

        _ownerField = (JTextField) ui.addLabeledField(_detailPanel, gbc, new JTextField(), "TaskPanel.owner", true);

        _startTimeField = (JTextField) ui.addLabeledField(_detailPanel, gbc, new JTextField(), "TaskPanel.startTime",
                false);
        _endTimeField = (JTextField) ui
                .addLabeledField(_detailPanel, gbc, new JTextField(), "TaskPanel.endTime", false);
        _expirationTimeField = (JTextField) ui.addLabeledField(_detailPanel, gbc, new JTextField(),
                "TaskPanel.expirationTime", true);

        _lastExceptionField = (JTextField) ui.addLabeledField(_detailPanel, gbc, new JTextField(),
                "TaskPanel.lastException", true);

        _statusDetailArea = (JTextArea) ui.addLabeledField(_detailPanel, gbc, new JTextArea(),
                "TaskPanel.statusDetail", true);
        gbc.gridheight = GridBagConstraints.REMAINDER;
        gbc.weighty = 1.0;
        _messageLogArea = (JTextArea) ui.addLabeledField(_detailPanel, gbc, new JTextArea(), "TaskPanel.messageLog",
                true);

        final JSplitPane mainSplitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        mainSplitPane.setResizeWeight(MAIN_SPLITPANE_RESIZE_WEIGHT);

        mainSplitPane.add(_taskListPanel);
        mainSplitPane.add(_detailPanel);

        setLayout(new BorderLayout());
        add(mainSplitPane, BorderLayout.CENTER);

        setTaskActionEnabledState(false);

        _adminUI.registerTextComponent(_statusDetailArea);
        _adminUI.registerTextComponent(_messageLogArea);
        _adminUI.scheduleRefresh(-1);
    }

    @Override
    public void actionPerformed(final AdminAction action, final ActionEvent ae) {
        final Management management = _adminUI.getManagement();
        if (management == null)
            return;
        try {
            final String name = action.getName();
            final boolean suspend = "SUSPEND_TASKS".equals(name);
            final boolean resume = "RESUME_TASKS".equals(name);
            final boolean stop = "STOP_TASKS".equals(name);
            final boolean remove = "REMOVE_TASKS".equals(name);

            if (suspend || resume || stop || remove) {
                final int[] selectedRows = _taskTable.getSelectedRows();
                final TaskStatus[] tsArray = new TaskStatus[selectedRows.length];
                for (int index = 0; index < tsArray.length; index++) {
                    final int row = selectedRows[index];
                    final TaskStatus ts = (TaskStatus) _taskStatusArrayModel.getValueAt(row, -1);
                    tsArray[index] = ts;
                }
                for (int index = 0; index < tsArray.length; index++) {
                    final TaskStatus ts = tsArray[index];
                    final long taskId = ts.getTaskId();
                    if (suspend || resume) {
                        management.setTaskSuspended(taskId, suspend);
                    } else if (stop || remove) {
                        management.stopTask(taskId, remove);
                    }
                }
            }
            _adminUI.scheduleRefresh(-1);
        } catch (final Exception e) {
            _adminUI.postException(e);
            if (!(e instanceof RemoteException)) {
                e.printStackTrace();
            }
        }
    }

    private boolean selectTask(final TaskStatus taskStatus) {
        _selectedTaskId = -1;
        if (taskStatus != null) {
            _selectedTaskId = taskStatus.getTaskId();
        }
        updateDetailedTaskStatus();
        return false;
    }

    private void updateDetailedTaskStatus() {
        final Management management = _adminUI.getManagement();

        if (_selectedTaskId == -1 || management == null) {
            updateDetailedTaskStatus(null);
        } else {
            try {
                final TaskStatus[] array = management.queryTaskStatus(_selectedTaskId, true, false, false);
                if (array.length == 1) {
                    updateDetailedTaskStatus(array[0]);
                }
            } catch (final RemoteException re) {
                _adminUI.postException(re);
            }
        }
    }

    private void updateDetailedTaskStatus(final TaskStatus taskStatus) {
        if (taskStatus == null) {
            _taskIdField.setText("");
            _stateField.setText("");
            _descriptionField.setText("");
            _ownerField.setText("");
            _startTimeField.setText("");
            _endTimeField.setText("");
            _expirationTimeField.setText("");
            _lastExceptionField.setText("");
            _statusDetailArea.setText("");
            _messageLogArea.setText("");
        } else {
            _taskIdField.setText(Long.toString(taskStatus.getTaskId()));
            _stateField.setText(_adminUI.getTaskStateString(taskStatus.getState()));
            _descriptionField.setText(taskStatus.getDescription());
            _ownerField.setText(taskStatus.getOwner());
            _startTimeField.setText(_adminUI.formatDate(taskStatus.getStartTime()));
            _endTimeField.setText(_adminUI.formatDate(taskStatus.getFinishTime()));
            _expirationTimeField.setText(_adminUI.formatDate(taskStatus.getExpirationTime()));
            _lastExceptionField.setText(taskStatus.getLastException() == null ? "" : taskStatus.getLastException()
                    .toString());
            _statusDetailArea.setText(taskStatus.getStatusDetail());

            final StringBuilder sb = new StringBuilder();
            int size = 0;
            if (taskStatus.getMessages() != null) {
                size = taskStatus.getMessages().length;
            }
            for (int index = 0; index < size; index++) {
                sb.append(taskStatus.getMessages()[index]);
                sb.append(Util.NEW_LINE);
            }
            _messageLogArea.setText(sb.toString());
        }
    }

    private boolean equals(final Object a, final Object b) {
        if (a == null)
            return b == null;
        else
            return a.equals(b);
    }

    private void setTaskActionEnabledState(final boolean enabled) {
        AdminAction action;
        action = _adminUI.getAction("SUSPEND_TASKS");
        if (action != null)
            action.setEnabled(enabled);
        action = _adminUI.getAction("RESUME_TASKS");
        if (action != null)
            action.setEnabled(enabled);
        action = _adminUI.getAction("STOP_TASKS");
        if (action != null)
            action.setEnabled(enabled);
        action = _adminUI.getAction("REMOVE_TASKS");
        if (action != null)
            action.setEnabled(enabled);
    }

    @Override
    protected void refresh(final boolean reset) {
        synchronized (this) {
            if (_refreshing)
                return;
            _refreshing = true;
        }
        try {
            boolean liveTasks = false;
            boolean stillSelected = false;
            final Management management = _adminUI.getManagement();
            if (management != null) {
                final TaskStatus[] taskStatusArray = management.queryTaskStatus(-1, false, false, false);
                _taskStatusArrayModel.setInfoArray(taskStatusArray);
                setTaskActionEnabledState(_taskTable.getSelectedRowCount() > 0);
                for (int index = 0; index < taskStatusArray.length; index++) {
                    final TaskStatus ts = taskStatusArray[index];
                    if (ts.getState() < Task.STATE_DONE)
                        liveTasks = true;
                    if (ts.getTaskId() == _selectedTaskId)
                        stillSelected = true;
                }
            }
            if (!stillSelected)
                selectTask(null);
            updateDetailedTaskStatus();
            scheduleFastRefresh(liveTasks);
        } catch (final RemoteException re) {
            _adminUI.postException(re);
        } finally {
            synchronized (this) {
                _refreshing = false;
            }
        }
    }

    @Override
    public void setIsShowing(final boolean isShowing) {
        if (_fastRefreshIntervalSet) {
            _adminUI.scheduleRefresh(isShowing ? FAST_REFRESH_INTERVAL : _savedRefreshInterval);
        }
    }

    private void scheduleFastRefresh(final boolean liveTasks) {
        if (liveTasks && !_fastRefreshIntervalSet) {
            _savedRefreshInterval = _adminUI.getRefreshInterval();
            _fastRefreshIntervalSet = true;
            _adminUI.scheduleRefresh(FAST_REFRESH_INTERVAL);
        } else if (!liveTasks && _fastRefreshIntervalSet) {
            _fastRefreshIntervalSet = false;
            _adminUI.scheduleRefresh(_savedRefreshInterval);
        }
    }

    @Override
    protected Map getMenuMap() {
        return _menuMap;
    }

    @Override
    protected void setDefaultButton() {
    }
}
