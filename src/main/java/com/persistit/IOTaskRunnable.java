package com.persistit;

/**
 * Base class for the background threads that perform various IO tasks.
 * 
 * @author peter
 * 
 */
abstract class IOTaskRunnable implements Runnable {

    private final Persistit _persistit;

    private Thread _thread;

    private boolean _stopped;

    private boolean _notified;

    private long _pollInterval;

    private Exception _lastException;

    protected IOTaskRunnable(final Persistit persistit) {
        _persistit = persistit;
    }

    protected void start(final String name, final long pollInterval) {
        _pollInterval = pollInterval;
        _thread = new Thread(this, name);
        _thread.start();
    }

    public Thread getThread() {
        return _thread;
    }

    public synchronized void kick() {
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

    public synchronized void setLastException(Exception lastException) {
        _lastException = lastException;
    }

    protected synchronized boolean isStopped() {
        return _stopped;
    }

    protected synchronized void setStopped() {
        _stopped = true;
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

    protected long pollInterval() {
        return getPollInterval();
    }

    protected abstract boolean shouldStop();

    protected abstract void runTask() throws Exception;

}
