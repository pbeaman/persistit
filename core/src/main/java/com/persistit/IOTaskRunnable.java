/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit;

import com.persistit.exception.PersistitException;

/**
 * Base class for the background threads that perform various IO tasks.
 * 
 * @author peter
 * 
 */
abstract class IOTaskRunnable implements Runnable {

    private final static long THREAD_DEATH_WAIT_INTERVAL = 5000;

    protected final Persistit _persistit;

    private volatile Thread _thread;

    private boolean _stopped;

    private boolean _notified;

    private long _pollInterval;

    private int _exceptionCount = 0;

    private Exception _lastException;

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

    synchronized void kick() {
        if (!_notified) {
            _notified = true;
            notify();
        }
    }

    public synchronized long getPollInterval() {
        return _pollInterval;
    }

    public synchronized void setPollInterval(long pollInterval) {
        _pollInterval = pollInterval;
        kick();
    }

    public synchronized Exception getLastException() {
        return _lastException;
    }

    public synchronized int getExceptionCount() {
        return _exceptionCount;
    }

    synchronized boolean lastException(Exception exception) {
        _exceptionCount++;
        if (_lastException == null || exception == null || !_lastException.getClass().equals(exception.getClass())) {
            _lastException = exception;
            return true;
        } else {
            return false;
        }
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
        for (int count = 0; (thread != null && thread.isAlive()); count++) {
            if (count > 0) {
                _persistit.getLogBase().crashRetry.log(count, _thread.getName());
            }
            thread.stop();
            try {
                thread.join(THREAD_DEATH_WAIT_INTERVAL);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void run() {

        while (true) {
            synchronized (this) {
                _notified = false;
            }
            try {
                runTask();
            } catch (Exception e) {
                if (lastException(e)) {
                    _persistit.getLogBase().exception.log(e);
                }
            }

            long lastCycleTime = System.currentTimeMillis();
            synchronized (this) {
                if (shouldStop()) {
                    _stopped = true;
                    kick();
                    break;
                }

                while (true) {
                    long pollInterval = pollInterval();
                    if (_notified && pollInterval >= 0) {
                        break;
                    }
                    long waitTime = pollInterval < 0 ? Persistit.SHORT_DELAY : lastCycleTime + pollInterval
                            - System.currentTimeMillis();
                    
                    if (waitTime <= 0) {
                        break;
                    }
                    
                    try {
                        wait(waitTime);
                    } catch (InterruptedException ie) {
                        _persistit.getLogBase().exception.log(ie);
                        break;
                    }
                }
            }
        }
        try {
            _persistit.closeSession();
        } catch (PersistitException e) {
            _persistit.getLogBase().exception.log(e);
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
