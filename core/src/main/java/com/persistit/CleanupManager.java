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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.persistit.exception.PersistitException;

class CleanupManager extends IOTaskRunnable {

    interface CleanupAction extends Comparable<CleanupAction> {
        int getType();
        void performAction(Persistit persistit) throws PersistitException;
    }

    final static long DEFAULT_CLEANUP_INTERVAL = 1000;
    
    final static int DEFAULT_QUEUE_SIZE = 10000;
    
    final static int WORKLIST_LENGTH = 100;

    final static Queue<CleanupAction> _cleanupActionQueue = new ArrayBlockingQueue<CleanupAction>(DEFAULT_QUEUE_SIZE);
    
    final static List<CleanupAction> _workList = new ArrayList<CleanupAction>(WORKLIST_LENGTH);
    
    private AtomicBoolean _closed = new AtomicBoolean();
    
    private AtomicLong _accepted = new AtomicLong();

    private AtomicLong _refused = new AtomicLong();

    private AtomicLong _performed = new AtomicLong();

    private AtomicLong _errors = new AtomicLong();

    CleanupManager(final Persistit persistit) {
        super(persistit);
    }

    public void start() {
        _closed.set(false);
        start("CHECKPOINT_WRITER", DEFAULT_CLEANUP_INTERVAL);
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

    synchronized boolean offer(CleanupAction action) {
        boolean accepted =  _cleanupActionQueue.offer(action);
        if (accepted) {
            _accepted.incrementAndGet();
        } else {
            _refused.incrementAndGet();
        }
        return accepted;
    }
    
    public long getAcceptedCount() {
        return _accepted.get();
    }
    
    public long getRefusedCount() {
        return _refused.get();
    }
    
    public long getPerformedCount() {
        return _performed.get();
    }
    
    public long getErrorCount() {
        return _errors.get();
    }
    
    public long getEnqueuedCount() {
        return _cleanupActionQueue.size();
    }
    
    public synchronized void poll() throws Exception {
        _workList.clear();
        while (_workList.size() < WORKLIST_LENGTH) {
            final CleanupAction action;
                action = _cleanupActionQueue.poll();
            if (action == null) {
                break;
            }
            _workList.add(action);
        }
        if (!_workList.isEmpty()) {
            Collections.sort(_workList);
        }
        
        for (final CleanupAction action : _workList) {
            try {
                action.performAction(_persistit);
                _performed.incrementAndGet();
            } catch (PersistitException e) {
                _persistit.getLogBase().cleanupException.log(e, action.toString());
                _errors.incrementAndGet();
            }
        }

    }
    
    public synchronized void clearQueue() {
        _cleanupActionQueue.clear();
    }
    
    static class CleanupAntiValue implements CleanupAction {
        final int _treeHandle;
        final long _page;
        
        CleanupAntiValue(final int treeHandle, final long page) {
            _treeHandle = treeHandle;
            _page = page;
        }
        
        @Override
        public int getType() {
            return 1;
        }
        
        @Override
        public int compareTo(CleanupAction other) {
            int d = getType() - other.getType();
            if (d != 0) {
                return d;
            }
            CleanupAntiValue a = (CleanupAntiValue)other;
            d = _treeHandle - a._treeHandle;
            if (d != 0) {
                return d;
            }
            return _page  > a._page ? 1 : _page < a._page ? -1 : 0;
        }

        @Override
        public void performAction(final Persistit persistit) throws PersistitException {
            final Tree tree = persistit.getJournalManager().treeForHandle(_treeHandle);
            if (tree != null) {
                final Exchange exchange = persistit.getExchange(tree.getVolume(), tree.getName(), false);
                exchange.pruneAntiValue(_page);
            }
        }
        
        @Override
        public String toString() {
            return String.format("Cleanup Antivalue on page %,d in tree handle [%d]", _page, _treeHandle); 
        }
        
    }
}
