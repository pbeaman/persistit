/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit;

import com.persistit.Management.TaskStatus;

public class TaskCheck extends Task {

    final static String COMMAND_NAME = "task";
    final static String[] ARG_TEMPLATE = {
            "taskId|long:-1:-1|Task ID to to check, or -1 for all",
            "_flag|v|Verbose",
            "_flag|c|Remove completed tasks",
            "_flag|m|Remove delivered messages",
            "_flag|k|Keep task even if completed",
            "_flag|x|Stop the task",
            "_flag|u|Suspend the task",
            "_flag|r|Resume the task",
            };

    private String _status = "not started";
    private long _taskId = -1;
    private boolean _details;
    private boolean _clearMessages;
    private boolean _clearTasks;
    private boolean _stop;
    private boolean _suspend;
    private boolean _resume;
    
    @Override
    public boolean isImmediate() {
        return true;
    }

    @Override
    protected void setupArgs(String[] args) throws Exception {
        if (args.length > 0) {
            _taskId = Long.parseLong(args[0]);
        }
    }

    @Override
    public void setupTaskWithArgParser(final String[] args) throws Exception {
        final ArgParser ap = new ArgParser(this.getClass().getSimpleName(), args, ARG_TEMPLATE);
        _taskId = ap.getLongValue("taskId");
        _details = ap.isFlag('v');
        _clearMessages = ap.isFlag('m');
        _clearTasks = ap.isFlag('c');
        _stop = ap.isFlag('x');
        _suspend = ap.isFlag('u');
        _resume = ap.isFlag('r');
    }

    @Override
    protected void runTask() throws Exception {
        if (_stop) {
            _persistit.getManagement().stopTask(_taskId, false);
        } else if (_suspend) {
            _persistit.getManagement().setTaskSuspended(_taskId, true);
        } else if (_resume) {
            _persistit.getManagement().setTaskSuspended(_taskId, false);
        }
        TaskStatus[] status = _persistit.getManagement().queryTaskStatus(
                _taskId, _details, _clearMessages);
        final StringBuilder sb = new StringBuilder();
        for (final TaskStatus ts : status) {
            if (sb.length() > 0) {
                sb.append(Util.NEW_LINE);
            }
            sb.append(ts.toString(_details));
        }
        if (_clearTasks) {
            _persistit.getManagement().removeFinishedTasks(_taskId);
        }
        _status = sb.toString();
        
    }

    @Override
    public String getStatus() {
        return _status;
    }

}
