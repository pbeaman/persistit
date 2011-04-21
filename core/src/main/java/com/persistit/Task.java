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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.persistit.exception.PersistitException;
import com.persistit.exception.TaskEndedException;

/**
 * Abstract superclass of classes that perform long-running utility operations,
 * such as export, import and integrity check. Concrete classes should
 * frequently call the poll() method to allow users to stop, suspend or view
 * progress. Concrete classes should post all progress messages to the message
 * log by calling postMessage.
 * 
 * @author Peter Beaman
 * @version 1.0
 */
public abstract class Task implements Runnable {
    /**
     * Status value indicates this <code>Task</code> has been set up but not yet
     * started.
     */
    public final static int STATE_NOT_STARTED = 0;
    /**
     * Status value indicates this <code>Task</code> is running.
     */
    public final static int STATE_RUNNING = 1;
    /**
     * Status value indicates this <code>Task</code> is suspended.
     */
    public final static int STATE_SUSPENDED = 2;
    /**
     * Status value indicates this <code>Task</code> finished normally.
     */
    public final static int STATE_DONE = 3;
    /**
     * Status value indicates this <code>Task</code> failed with an Exception.
     */
    public final static int STATE_FAILED = 4;
    /**
     * Status value indicates this <code>Task</code> was stopped by user
     * intervention.
     */
    public final static int STATE_ENDED = 5;
    /**
     * Status value indicates this <code>Task</code>'s maximum elapsed time
     * expired.
     */
    public final static int STATE_EXPIRED = 6;

    public final static String[] STATE_NAMES = { "notStarted", "running",
            "suspended", "done", "failed", "ended", "expired", };

    /**
     * Indicates normal level of message logging
     */
    public final static int LOG_NORMAL = 0;
    /**
     * Indicates a more verbose level of message logging.
     */
    public final static int LOG_VERBOSE = 1;
    /**
     * Default maximum number of messages held in the message log
     */
    public final static int DEFAULT_MAX_MESSAGE_LOG_SIZE = 500;

    /**
     * The Persistit instance this Task runs against.
     */
    protected Persistit _persistit;
    /**
     * Task ID for this task
     */
    protected long _taskId;
    /**
     * Description of this task
     */
    protected String _description;
    /**
     * Identifies the initiator or owne of this task
     */
    protected String _owner;
    /**
     * System time when started
     */
    protected long _startTime;
    /**
     * System time when finished
     */
    protected long _finishTime;
    /**
     * System time at which Task should terminate if not finished
     */
    protected long _expirationTime;
    /**
     * Total time during which this Task was suspended
     */
    protected long _suspendedDuration;
    /**
     * By default, the {@link #stop} method sets this flag, and the
     * {@link #poll} method throws a {@link TaskEndedException} to stop a
     * running task.
     */
    protected AtomicBoolean _stop = new AtomicBoolean();
    /**
     * When set, the {@link #poll} method waits.
     */
    protected AtomicBoolean _suspend = new AtomicBoolean();
    /**
     * Most recently thrown Exception
     */
    protected Exception _lastException;
    /**
     * State of the task.
     */
    protected int _state = STATE_NOT_STARTED;
    /**
     * Maximum number of messages to hold in the message log before automaticlly
     * culling.
     */
    protected int _maxMessageLogSize = DEFAULT_MAX_MESSAGE_LOG_SIZE;
    /**
     * Number of messages removed from the message log.
     */
    protected int _culledMessageCount;
    /**
     * Degree of verbosity
     */
    protected int _messageLogVerbosity;
    /**
     * Collection of messages posted by the task. Note we are using JDK1.1
     * ArrayList to help port to J2ME.
     */
    protected ArrayList<String> _messageLog = new ArrayList<String>();
    /**
     * An optional PrintStream - if not null, {@link #postMessage(String, int)}
     * writes messages to it.
     */
    protected PrintStream _messageStream;

    /**
     * The Thread created to run this task.
     */
    protected Thread _thread;

    static boolean isFinalStatus(final int statusCode) {
        return statusCode == STATE_DONE || statusCode == STATE_ENDED
                || statusCode == STATE_EXPIRED || statusCode == STATE_FAILED;
    }

    protected Task() {

    }

    protected Task(final Persistit persistit) {
        setPersistit(persistit);
    }

    /**
     * Set the Persistit instance to be accessed by this Task.
     * 
     * @param persistit
     */
    public void setPersistit(final Persistit persistit) {
        _persistit = persistit;
    }

    /**
     * Accept and process an array of input arguments. The number and format of
     * arguments is task-specific.
     * 
     * @param args
     * @throws Exception
     */
    protected abstract void setupArgs(String[] args) throws Exception;

    /**
     * Accept and process of input arguments from an {@link ArgParser} whose
     * argument template is specified by the specific <code>Task</code>.
     * 
     * @param ap
     *            The <code>ArgParser</code> holding parsed argument values.
     * @throws Exception
     */
    protected abstract void setupTaskWithArgParser(String[] args) throws Exception;

    /**
     * Called by a newly created <code>Thread</code> to perform the task.
     * 
     * @throws Exception
     */
    protected abstract void runTask() throws Exception;

    /**
     * Tests whether this <code>Task</code> should stop or suspend. The concrete
     * task implementation should call <code>poll</code> frequently. The default
     * mechanism for stopping a task does not use the deprecated
     * {@link Thread#stop()} method because it is unsafe. Instead, the task
     * should call poll().
     */
    protected void poll() {
        long now = System.currentTimeMillis();
        if (_startTime == 0)
            _startTime = now;
        if (_expirationTime == 0)
            _expirationTime = Long.MAX_VALUE;

        if (now - _suspendedDuration > _expirationTime) {
            _state = STATE_EXPIRED;
            throw new TaskEndedException("Expired");
        }
        if (_stop.get()) {
            _state = STATE_ENDED;
            throw new TaskEndedException("Stopped");
        }
        if (_suspend.get()) {
            while (_suspend.get()) {
                if (_stop.get()) {
                    _state = STATE_ENDED;
                    throw new TaskEndedException("Stopped");
                }
                _state = STATE_SUSPENDED;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    throw new TaskEndedException("Interrupted");
                }
            }
            _suspendedDuration += (System.currentTimeMillis() - now);
            _state = STATE_RUNNING;
        }
    }

    /**
     * Parses a String that represents a list of Volume/Tree pairs. Format:
     * volumeName1,treeName1,treeName2;volumeName2,treeName1,treeName2,..
     * 
     * @param specification
     *            The list of Volume/Tree pairs, specified as a String
     * @return Array of Trees specified by the list.
     */
    protected Tree[] parseTreeList(String specification)
            throws PersistitException {
        List<Tree> list = new ArrayList<Tree>();
        StringBuilder sb = new StringBuilder();
        Volume volume = null;
        int end = specification.length();
        for (int index = 0; index <= end; index++) {
            int c = index < end ? specification.charAt(index) : -1;
            if (c == '\\') {
                if (index++ < specification.length()) {
                    sb.append(specification.charAt(index));
                }
            } else if (c == ';' || c == ',' || c == -1) {
                String name = sb.toString();
                sb.setLength(0);
                if (volume == null) {
                    volume = _persistit.getVolume(name);
                } else {
                    Tree tree = volume.getTree(name, false);
                    if (tree != null) {
                        list.add(tree);
                    }
                }
                if (c != ',')
                    volume = null;
            } else
                sb.append((char) c);
        }

        Tree[] result = (Tree[]) list.toArray(new Tree[list.size()]);

        return result;
    }

    /**
     * Abbreviation for <code>System.currentTimeMillis()</code>.
     * 
     * @return Current system time
     */
    protected long now() {
        return System.currentTimeMillis();
    }

    /**
     * Sets up a <code>Task</code>. This method calls the task-specific
     * {@link #setupTask} method to finish setup.
     * 
     * @param description
     *            Description of this task
     * @param owner
     *            Hostname and/or username
     * @param args
     *            Task-specific arguments
     * @param maxTime
     *            Maximum wall-clock duration, in milliseconds, that Task will
     *            be allowed to run
     * @param verbosity
     *            Level at which messages posted by the running task will be
     *            retained in the message log.
     * @throws Exception
     */
    public void setup(long taskId, String description, String owner,
            long maxTime, int verbosity) throws Exception {
        _taskId = taskId;
        _description = description;
        _owner = owner;
        _messageLogVerbosity = verbosity;
        _expirationTime = maxTime > 0 ? now() + maxTime : Long.MAX_VALUE;
    }

    /**
     * Start this Task
     * 
     * @throws IllegalStateException
     *             If this task has already been started
     */
    public void start() {
        if (_thread != null) {
            throw new IllegalStateException("Already started");
        }
        _thread = new Thread(this);
        _thread.start();
    }

    /**
     * Request this task to stop. Note: a subclass could reimplement this using
     * <code>_thread.stop()</code> if necessary.
     * 
     */
    public void stop() {
        _stop.set(true);
    }

    /**
     * Request this task to suspend. Note: a subclass could reimplement this
     * using <code>_thread.suspend()</code> if necessary.
     * 
     */
    public void suspend() {
        _suspend.set(true);
    }

    /**
     * Request this task to resume. Note: a subclass could reimplement this
     * using <code>_thread.resume()</code> if necessary.
     * 
     */
    public void resume() {
        _suspend.set(false);
    }

    /**
     * Set the maximum amount of wall-clock time this <code>Task</code> will be
     * permitted to run. If the <code>Task</code> is suspended, the amount of
     * time spend in the suspended state is not counted toward this maximum.
     * 
     * @param maxTime
     *            The time, in milliseconds
     */
    public void setMaximumTime(long maxTime) {
        long now = now();
        _expirationTime = now + maxTime;
        if (_expirationTime < now)
            _expirationTime = Long.MAX_VALUE;
    }

    /**
     * Set verbosity level for selecting posted messages. Current available
     * values are {@link #LOG_NORMAL} and {@link #LOG_VERBOSE}.
     * 
     * @param verbosity
     *            Verbosity
     */
    public void setMessageLogVerbosity(int verbosity) {
        _messageLogVerbosity = verbosity;
    }

    /**
     * Returns the current verbosity level
     * 
     * @return Current verbosity
     */
    public int getMessageLogVerbosity() {
        return _messageLogVerbosity;
    }

    /**
     * Sets a <code>PrintStream</code> to receive posted messages.
     * 
     * @param ps
     *            The <code>PrintStream</code>
     */
    public void setMessageStream(PrintStream ps) {
        _messageStream = ps;
    }

    /**
     * Returns the current <code>PrintStream</code>, or <code>null</code> if
     * there is none.
     * 
     * @return Current <code>PrintStream</code>
     */
    public PrintStream getMessageStream() {
        return _messageStream;
    }

    /**
     * Returns a short String message describing the current state of this
     * <code>Task</code>. It should convey a measurement of progress to the
     * end-user.
     * 
     * @return A description of this <code>Task</code>'s current state
     */
    public abstract String getStatus();

    /**
     * Returns a String message describing the current state of this
     * <code>Task<code>, possibly in greater detail than {@link #getStatus}. The
     * default implementation returns the same description as <code>getStatus</code>
     * .
     * 
     * @return A detailed description of this <code>Task</code>'s current state.
     */
    public String getStatusDetail() {
        return getStatus();
    }

    /**
     * Indicates how much time remains (in milliseconds) until this
     * <code>Task</code>'s maximum time has expired.
     */
    public long getRemainingTime() {
        if (_expirationTime == 0 || _expirationTime == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        return _expirationTime - now() + _suspendedDuration;
    }

    /**
     * Posts a message (typically to denote progress, error or other interim
     * information) to the message log.
     * 
     * @param message
     *            The message
     * @param level
     *            Indicates the verbosity level. The message is posted only if
     *            the level of the message is below the current verbosity
     *            threshhold set through {@link #setMessageLogVerbosity(int)}.
     */
    public void postMessage(String message, int level) {
        if (level <= _messageLogVerbosity) {
            synchronized (_messageLog) {
                if (_messageLog.size() >= _maxMessageLogSize) {
                    _messageLog.remove(0);
                }
                _messageLog.add(message);
            }
            if (_messageStream != null) {
                _messageStream.println();
                _messageStream.print(message);
            }
        }
    }

    /**
     * Appends a String to the final message of the log.
     * 
     * @param fragment
     *            The message fragment to append
     * @param level
     *            Indicates the verbosity level. The message is posted only if
     *            the level of the message is below the current verbosity
     *            threshhold set through {@link #setMessageLogVerbosity(int)}.
     */
    public void appendMessage(String fragment, int level) {
        if (level <= _messageLogVerbosity) {
            synchronized (_messageLog) {
                int index = _messageLog.size() - 1;
                if (index >= 0) {
                    String s = (String) _messageLog.get(index) + fragment;
                    _messageLog.set(index, s);
                }
            }
            if (_messageStream != null) {
                _messageStream.print(fragment);
            }
        }
    }

    /**
     * Returns the number of messages available in the message log.
     * 
     * @return Number of messages
     */
    public int getMessageLogSize() {
        synchronized (_messageLog) {
            return _messageLog.size();
        }
    }

    /**
     * Get all the messages, starting from a specified index.
     * 
     * @param from
     *            The index
     * @return The messages
     */
    public String[] getMessages(int from) {
        synchronized (_messageLog) {
            from -= _culledMessageCount;
            if (from < 0)
                from = 0;
            int size = _messageLog.size() - from;
            if (size < 0)
                size = 0;
            String[] results = new String[size];
            for (int index = 0; index < size; index++) {
                results[index] = (String) _messageLog.get(index + from);
            }
            return results;
        }
    }

    /**
     * Remove all the messages up to, but not including the specified index.
     * 
     * @param to
     *            Index of first message not to remove.
     */
    public void cullMessages(int to) {
        synchronized (_messageLog) {
            to -= _culledMessageCount;
            if (to >= _messageLog.size())
                _messageLog.clear();
            else {
                for (int index = to; --index >= 0;) {
                    _messageLog.remove(index);
                    _culledMessageCount++;
                }
            }
        }
    }

    /**
     * Implementation of <code>Runnable</code>.
     */
    public void run() {
        _startTime = now();
        try {
            _state = STATE_RUNNING;
            runTask();
            _state = STATE_DONE;
        } catch (Exception e) {
            _lastException = e;
            if (e instanceof TaskEndedException) {
                _state = STATE_ENDED;
            } else {
                _state = STATE_FAILED;
            }
        }
        _finishTime = now();
    }

    /**
     * Copy status of this <code>Task</code> to a {@link Management.TaskStatus}.
     * 
     * @param ts
     *            The <code>TaskStatus</code>
     * @param details
     *            <code>true</code> to include messages and status detail
     * @param clearMessages
     *            <code>true</code> to cull the messages being returned
     */
    public void populateTaskStatus(Management.TaskStatus ts, boolean details,
            boolean clearMessages) {
        ts.taskId = _taskId;
        ts.state = _state;
        ts.stateName = STATE_NAMES[_state];
        ts.description = _description;
        ts.owner = _owner;
        ts.startTime = _startTime;
        ts.finishTime = _finishTime;
        ts.expirationTime = _expirationTime;
        ts.lastException = _lastException == null ? "none" : _lastException
                .toString();
        ts.statusSummary = getStatus();
        if (details) {
            ts.newMessages = getMessages(0);
            ts.statusDetail = getStatusDetail();
            if (clearMessages)
                cullMessages(Integer.MAX_VALUE);
        }
    }

    public boolean isImmediate() {
        return false;
    }
}
