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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for the background threads that perform various IO tasks.
 * 
 * @author peter
 * 
 */
abstract class IOTaskRunnable implements Runnable {

    protected final Persistit _persistit;

    private volatile Thread _thread;

    private boolean _stopped;

    private boolean _notified;

    private long _pollInterval;

    private Exception _lastException;

    protected AtomicBoolean _urgent = new AtomicBoolean();

    protected IOTaskRunnable(final Persistit persistit) {
        _persistit = persistit;
    }

    protected void start(final String name, final long pollInterval) {
        _pollInterval = pollInterval;
        _thread = new Thread(this, name);
        _thread.start();
    }

    Thread getThread() {
        return _thread;
    }

    void urgent() {
        _urgent.set(true);
        kick();
    }

    synchronized void kick() {
        if (!_notified) {
            _notified = true;
            notify();
        }
    }

    synchronized long getPollInterval() {
        return _pollInterval;
    }

    synchronized void setPollInterval(long pollInterval) {
        _pollInterval = pollInterval;
        kick();
    }

    synchronized Exception getLastException() {
        return _lastException;
    }

    synchronized void setLastException(Exception lastException) {
        _lastException = lastException;
    }

    protected synchronized boolean isStopped() {
        final Thread thread = _thread;
        if (thread == null || !thread.isAlive()) {
            return true;
        }
        return _stopped;
    }

    protected synchronized void setStopped() {
        _stopped = true;
    }

    void join(final long millis) throws InterruptedException {
        if (_thread != null) {
            _thread.join(millis);
        }
    }

    @SuppressWarnings("deprecation")
    // Use only for tests.
    protected void crash() {
        final Thread thread = _thread;
        if (thread != null && thread.isAlive()) {
            thread.stop();
        }
    }

    public void run() {

        while (true) {
            synchronized (this) {
                _notified = false;
            }
            try {
                runTask();
            } catch (Exception e) {
                if (!e.equals(_lastException)) {
                    _lastException = e;
                    _persistit.getLogBase().log(LogBase.LOG_EXCEPTION, e);
                }
            }

            long lastCycleTime = System.currentTimeMillis();
            synchronized (this) {
                if (shouldStop()) {
                    _stopped = true;
                    kick();
                    break;
                }
                long waitTime;
                while (!_notified
                        && (waitTime = lastCycleTime + pollInterval()
                                - System.currentTimeMillis()) > 0) {
                    try {
                        wait(waitTime);
                    } catch (InterruptedException ie) {
                        // do nothing
                    }
                }
            }
        }
    }

    static void crash(final IOTaskRunnable task) {
        if (task != null) {
            task.crash();
        }
    }

    protected long pollInterval() {
        return getPollInterval();
    }

    protected abstract boolean shouldStop();

    protected abstract void runTask() throws Exception;

}
