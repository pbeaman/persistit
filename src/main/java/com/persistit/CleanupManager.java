/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.persistit.AlertMonitor.AlertLevel;
import com.persistit.AlertMonitor.Event;
import com.persistit.exception.PersistitException;
import com.persistit.mxbeans.CleanupManagerMXBean;

class CleanupManager extends IOTaskRunnable implements CleanupManagerMXBean {

    interface CleanupAction extends Comparable<CleanupAction> {

        void performAction(Persistit persistit) throws PersistitException;
    }

    final static long DEFAULT_CLEANUP_INTERVAL = 1000;

    final static int DEFAULT_QUEUE_SIZE = 50000;

    final static int WORKLIST_LENGTH = 500;

    private final static long DEFAULT_MINIMUM_PRUNING_DELAY = 1000;

    final Queue<CleanupAction> _cleanupActionQueue = new ArrayBlockingQueue<CleanupAction>(DEFAULT_QUEUE_SIZE);

    private final AtomicBoolean _closed = new AtomicBoolean();

    private final AtomicLong _accepted = new AtomicLong();

    private final AtomicLong _refused = new AtomicLong();

    private final AtomicLong _performed = new AtomicLong();

    private final AtomicLong _errors = new AtomicLong();

    private final AtomicLong _minimumPruningDelay = new AtomicLong(DEFAULT_MINIMUM_PRUNING_DELAY);

    CleanupManager(final Persistit persistit) {
        super(persistit);
    }

    public void start() {
        _closed.set(false);
        start("CLEANUP_MANAGER", DEFAULT_CLEANUP_INTERVAL);
    }

    public void close(final boolean flush) throws PersistitException {
        _closed.set(true);
    }

    @Override
    protected void runTask() throws Exception {
        poll();
    }

    @Override
    protected boolean shouldStop() {
        return _closed.get();
    }

    synchronized boolean offer(final CleanupAction action) {
        final boolean accepted = _cleanupActionQueue.offer(action);
        if (accepted) {
            _accepted.incrementAndGet();
        } else {
            _refused.incrementAndGet();
            kick();
        }
        return accepted;
    }

    @Override
    public long getAcceptedCount() {
        return _accepted.get();
    }

    @Override
    public long getRefusedCount() {
        return _refused.get();
    }

    @Override
    public long getPerformedCount() {
        return _performed.get();
    }

    @Override
    public long getErrorCount() {
        return _errors.get();
    }

    @Override
    public long getEnqueuedCount() {
        return _cleanupActionQueue.size();
    }

    @Override
    public long getMinimumPruningDelay() {
        return _minimumPruningDelay.get();
    }

    @Override
    public void setMinimumPruningDelay(final long delay) {
        _minimumPruningDelay.set(delay);
    }

    @Override
    public long getPollInterval() {
        if (_cleanupActionQueue.size() < DEFAULT_QUEUE_SIZE / 2) {
            return super.getPollInterval();
        } else {
            return 0;
        }
    }

    @Override
    public void poll() throws Exception {
        _persistit.getIOMeter().poll();
        _persistit.cleanup();
        _persistit.getJournalManager().pruneObsoleteTransactions();
        final List<CleanupAction> workList = new ArrayList<CleanupAction>(WORKLIST_LENGTH);
        synchronized (this) {
            while (workList.size() < WORKLIST_LENGTH) {
                final CleanupAction action;
                action = _cleanupActionQueue.poll();
                if (action == null) {
                    break;
                }
                workList.add(action);
            }
        }
        Collections.sort(workList);

        for (final CleanupAction action : workList) {
            try {
                action.performAction(_persistit);
                _performed.incrementAndGet();
            } catch (final PersistitException e) {
                lastException(e);
                _persistit.getAlertMonitor().post(
                        new Event(AlertLevel.ERROR, _persistit.getLogBase().cleanupException, e, action),
                        AlertMonitor.CLEANUP_CATEGORY);
                _errors.incrementAndGet();
            }
        }
    }

    @Override
    public synchronized void clear() {
        _cleanupActionQueue.clear();
    }

    @Override
    public synchronized String toString() {
        final StringBuilder sb = new StringBuilder("[");
        for (final CleanupAction a : _cleanupActionQueue) {
            if (sb.length() > 1) {
                sb.append(",\n ");
            }
            sb.append(a);
        }
        sb.append("]");
        return sb.toString();
    }

    abstract static class CleanupTreePage implements CleanupAction {

        private final static ThreadLocal<WeakReference<Exchange>> _exchangeThreadLocal = new ThreadLocal<WeakReference<Exchange>>();

        final int _treeHandle;
        final long _page;

        protected CleanupTreePage(final int treeHandle, final long page) {
            _treeHandle = treeHandle;
            _page = page;
        }

        @Override
        public boolean equals(final Object other) {
            if (other instanceof CleanupTreePage) {
                final CleanupTreePage a = (CleanupTreePage) other;
                return a._page == _page && a._treeHandle == _treeHandle && getClass().equals(a.getClass());
            } else {
                return false;
            }

        }

        @Override
        public int compareTo(final CleanupAction other) {
            if (other instanceof CleanupTreePage) {
                final CleanupTreePage a = (CleanupTreePage) other;
                final int d = _treeHandle - a._treeHandle;
                if (d != 0) {
                    return d;
                }
                return _page > a._page ? 1 : _page < a._page ? -1 : 0;
            } else {
                return -1;
            }
        }

        @Override
        public String toString() {
            return String.format("%s on page %,d tree handle [%,d]", getClass().getSimpleName(), _page, _treeHandle);
        }

        protected Exchange getExchange(final Persistit persistit) throws PersistitException {
            final Tree tree = persistit.getJournalManager().treeForHandle(_treeHandle);
            if (tree == null) {
                return null;
            }
            final WeakReference<Exchange> ref = _exchangeThreadLocal.get();
            if (ref != null) {
                final Exchange exchange = ref.get();
                if (exchange != null) {
                    exchange.init(tree);
                    return exchange;
                }
            }
            final Exchange exchange = new Exchange(tree);
            _exchangeThreadLocal.set(new WeakReference<Exchange>(exchange));
            return exchange;
        }
    }

    static class CleanupAntiValue extends CleanupTreePage {

        CleanupAntiValue(final int treeHandle, final long page) {
            super(treeHandle, page);
        }

        @Override
        public void performAction(final Persistit persistit) throws PersistitException {
            final Exchange exchange = getExchange(persistit);
            if (exchange != null) {
                exchange.pruneLeftEdgeValue(_page);
            }
        }
    }

    static class CleanupPruneAction extends CleanupTreePage {

        CleanupPruneAction(final int treeHandle, final long page) {
            super(treeHandle, page);
        }

        @Override
        public void performAction(final Persistit persistit) throws PersistitException {
            final Exchange exchange = getExchange(persistit);
            if (exchange != null) {
                exchange.prune(_page);
            }
        }
    }

    static class CleanupIndexHole extends CleanupTreePage {
        int _level;

        CleanupIndexHole(final int treeHandle, final long page, final int level) {
            super(treeHandle, page);
            _level = level;
        }

        @Override
        public void performAction(final Persistit persistit) throws PersistitException {
            final Exchange exchange = getExchange(persistit);
            if (exchange != null) {
                exchange.fixIndexHole(_page, _level);
            }
        }
    }

}
