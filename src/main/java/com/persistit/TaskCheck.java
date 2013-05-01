/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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

package com.persistit;

import com.persistit.CLI.Arg;
import com.persistit.CLI.Cmd;
import com.persistit.Management.TaskStatus;
import com.persistit.util.Util;

/**
 * A <code>Task</code> which simply checks the status of another
 * <code>Task</code>. The {@link CLI} can invoke this to poll for completion of
 * a long-running task such as <code>IntegrityCheck</code>.
 * 
 * @author peter
 * 
 */
public class TaskCheck extends Task {

    private String _status = "not started";
    private long _taskId = -1;
    private boolean _details;
    private boolean _clearMessages;
    private boolean _clearTasks;
    private boolean _stop;
    private boolean _suspend;
    private boolean _resume;

    @Cmd("task")
    static Task createTaskCheckTask(@Arg("taskId|long:-1:-1|Task ID to to check, or -1 for all") final long taskId,
            @Arg("_flag|v|Verbose") final boolean verbose,
            @Arg("_flag|m|Keep previously delivered messages") final boolean keepMessages,
            @Arg("_flag|k|Keep task even if completed") final boolean keepTasks,
            @Arg("_flag|x|Stop the task") final boolean stop, @Arg("_flag|u|Suspend the task") final boolean suspend,
            @Arg("_flag|r|Resume the task") final boolean resume) throws Exception {

        final TaskCheck task = new TaskCheck();
        task._taskId = taskId;
        task._details = verbose;
        task._clearTasks = !keepTasks;
        task._clearMessages = !keepMessages;
        task._stop = stop;
        task._suspend = suspend;
        task._resume = resume;
        return task;
    }

    @Override
    public boolean isImmediate() {
        return true;
    }

    void setArgs(final long taskId, final boolean verbose, final boolean removeTasks, final boolean removeMessages,
            final boolean stop, final boolean suspend, final boolean resume) {
        _taskId = taskId;
        _details = verbose;
        _clearTasks = removeTasks;
        _clearMessages = removeMessages;
        _stop = stop;
        _suspend = suspend;
        _resume = resume;
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
        final TaskStatus[] status = _persistit.getManagement().queryTaskStatus(_taskId, _details, _clearMessages,
                _clearTasks);
        final StringBuilder sb = new StringBuilder();
        for (final TaskStatus ts : status) {
            final String s = ts.toString(_details);
            if (!s.isEmpty()) {
                if (sb.length() > 0) {
                    sb.append(Util.NEW_LINE);
                }
                sb.append(s);
            }
        }
        _status = sb.toString();

    }

    @Override
    public String getStatus() {
        return _status;
    }

}
