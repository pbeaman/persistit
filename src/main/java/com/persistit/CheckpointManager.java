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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.persistit.Transaction.CommitPolicy;
import com.persistit.exception.MissingThreadException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.mxbeans.CheckpointManagerMXBean;
import com.persistit.util.Util;

class CheckpointManager extends IOTaskRunnable implements CheckpointManagerMXBean {

    /**
     * A structure containing a timestamp and system clock time at which
     * Persistit will attempt to record a valid Checkpoint to disk.
     */
    public static class Checkpoint {

        private final static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        private final long _timestamp;

        private final long _systemTime;

        private volatile boolean _completed = false;

        Checkpoint(final long timestamp, final long systemTime) {
            _timestamp = timestamp;
            _systemTime = systemTime;
        }

        Checkpoint(final long timestamp, final long systemTime, final boolean completed) {
            this(timestamp, systemTime);
            _completed = completed;
        }

        public long getTimestamp() {
            return _timestamp;
        }

        public long getSystemTimeMillis() {
            return _systemTime;
        }

        void completed() {
            _completed = true;
        }

        public boolean isCompleted() {
            return _completed;
        }

        @Override
        public String toString() {
            return String.format("Checkpoint %,d%s @ %s", _timestamp, isCompleted() ? "c" : "u",
                    SDF.format(new Date(_systemTime)));
        }

        @Override
        public boolean equals(final Object object) {
            if (!(object instanceof Checkpoint)) {
                return false;
            }
            final Checkpoint cp = (Checkpoint) object;
            return cp._systemTime == _systemTime && cp._timestamp == _timestamp;
        }
    }

    private final static long NS_PER_S = 1000000000L;

    /**
     * Default interval in nanoseconds between checkpoints - two minutes.
     */
    private final static long DEFAULT_CHECKPOINT_INTERVAL = 120;

    private final static long MINIMUM_CHECKPOINT_INTERVAL = 10;

    private final static long MAXIMUM_CHECKPOINT_INTERVAL = 1800;

    private final static Checkpoint UNAVALABLE_CHECKPOINT = new Checkpoint(0, 0);

    private final SessionId _checkpointTxnSessionId = new SessionId();

    private volatile long _checkpointIntervalNanos = DEFAULT_CHECKPOINT_INTERVAL * NS_PER_S;

    private volatile long _lastCheckpointNanos = Long.MAX_VALUE;

    private final static long SHORT_DELAY = 500;

    private final static long FLUSH_CHECKPOINT_INTERVAL = 5000;

    private volatile Checkpoint _currentCheckpoint = new Checkpoint(0, 0, true);

    private final List<Checkpoint> _outstandingCheckpoints = new ArrayList<Checkpoint>();

    private final AtomicBoolean _closed = new AtomicBoolean();

    private final AtomicBoolean _fastClose = new AtomicBoolean();

    CheckpointManager(final Persistit persistit) {
        super(persistit);
    }

    public void start() {
        _closed.set(false);
        start("CHECKPOINT_WRITER", FLUSH_CHECKPOINT_INTERVAL);
    }

    public void close(final boolean flush) throws PersistitException {
        if (flush) {
            checkpoint();
        } else {
            _fastClose.set(true);
        }
        _closed.set(true);
    }

    Checkpoint getCurrentCheckpoint() {
        return _currentCheckpoint;
    }

    long getCheckpointIntervalNanos() {
        return _checkpointIntervalNanos;
    }

    void setCheckpointIntervalNanos(final long interval) {
        _checkpointIntervalNanos = interval;
    }

    @Override
    public String getProposedCheckpoint() {
        return _currentCheckpoint.toString();
    }

    @Override
    public long getCheckpointInterval() {
        return _checkpointIntervalNanos / NS_PER_S;
    }

    @Override
    public void setCheckpointInterval(final long interval) {
        Util.rangeCheck(interval, MINIMUM_CHECKPOINT_INTERVAL, MAXIMUM_CHECKPOINT_INTERVAL);
        _checkpointIntervalNanos = interval * NS_PER_S;
    }

    @Override
    public synchronized int getOutstandingCheckpointCount() {
        return _outstandingCheckpoints.size();
    }

    @Override
    public synchronized String outstandingCheckpointReport() {
        final StringBuilder sb = new StringBuilder();
        for (final Checkpoint cp : _outstandingCheckpoints) {
            sb.append(cp);
            sb.append(Util.NEW_LINE);
        }
        return sb.toString();
    }

    Checkpoint checkpoint() throws PersistitException {
        final Checkpoint checkpoint = createCheckpoint();
        _persistit.flushBuffers(checkpoint.getTimestamp());

        while (true) {
            kick();
            synchronized (this) {
                if (checkpoint.isCompleted()) {
                    return checkpoint;
                } else if (_currentCheckpoint != checkpoint) {
                    return UNAVALABLE_CHECKPOINT;
                }
            }
            if (!getThread().isAlive()) {
                throw new MissingThreadException(getThread().getName());
            }
            Util.sleep(SHORT_DELAY);
        }
    }

    void pollCreateCheckpoint() throws PersistitException {
        final long now = System.nanoTime();
        if (_lastCheckpointNanos + _checkpointIntervalNanos < now) {
            createCheckpoint();
        }
    }

    /**
     * <p>
     * Allocate a timestamp to serve as the next checkpoint timestamp.
     * Concurrently, compute a snapshot value of each active {@link Accumulator}
     * at that timestamp and store it. This value will serve as the accumulator
     * base value during recovery.
     * </p>
     * <p>
     * Note that invoking this method merely starts the process of creating a
     * checkpoint. Subsequently Persistit writes all dirty pages that were
     * modified before the checkpoint timestamp to the journal. Only when that
     * is finished can a CP (Checkpoint) record be written to the journal to
     * certify that the checkpoint is valid for recovery.
     * </p>
     * <p>
     * This method is synchronized because it computes a checkpoint value for
     * each Accumulator and then serializes that value into the database. That
     * process is not threadsafe, and there is no use case for concurrent
     * checkpoints.
     * 
     * @return The newly created Checkpoint
     * @throws PersistitException
     */
    synchronized Checkpoint createCheckpoint() throws PersistitException {
        /*
         * Run within a transaction to get snapshot accumulator views. The
         * Checkpoint timestamp is the start timestamp of this transaction.
         * Therefore the Accumulator snapshot values represent the aggregation
         * of all transactions that committed before the checkpoint timestamp.
         */
        final SessionId saveSessionId = _persistit.getSessionId();
        try {
            _persistit.setSessionId(_checkpointTxnSessionId);
            final Transaction txn = _persistit.getTransaction();

            _lastCheckpointNanos = System.nanoTime();

            txn.beginCheckpoint();
            try {
                _persistit.flushTransactions(txn.getStartTimestamp());
                final List<Accumulator> accumulators = _persistit.getCheckpointAccumulators();
                _persistit.getTransactionIndex().checkpointAccumulatorSnapshots(txn.getStartTimestamp(), accumulators);
                Accumulator.saveAccumulatorCheckpointValues(accumulators);
                txn.commit(CommitPolicy.HARD);
                _currentCheckpoint = new Checkpoint(txn.getStartTimestamp(), System.currentTimeMillis());
                _outstandingCheckpoints.add(_currentCheckpoint);
                _persistit.getLogBase().checkpointProposed.log(_currentCheckpoint);
                return _currentCheckpoint;
            } catch (final InterruptedException ie) {
                throw new PersistitInterruptedException(ie);
            } finally {
                txn.end();
            }
        } finally {
            _persistit.setSessionId(saveSessionId);
        }
    }

    /**
     * Attempt to find a new checkpoint that can be completed. Look for the
     * earliest live transaction and the earliest dirty page; determine from
     * these whether a currently outstanding checkpoint is ready to complete.
     */
    void pollFlushCheckpoint() {
        final long earliestDirtyTimestamp = _persistit.earliestDirtyTimestamp();
        Checkpoint checkpoint = null;
        synchronized (this) {
            while (!_outstandingCheckpoints.isEmpty()) {
                final Checkpoint cp = _outstandingCheckpoints.get(0);
                if (cp.getTimestamp() <= earliestDirtyTimestamp) {
                    checkpoint = cp;
                    _outstandingCheckpoints.remove(0);
                } else {
                    break;
                }
            }
        }
        if (checkpoint != null) {
            try {
                _persistit.getJournalManager().writeCheckpointToJournal(checkpoint);
            } catch (final PersistitException e) {
                _persistit.getLogBase().exception.log(e);
            }
        }
    }

    @Override
    protected synchronized boolean shouldStop() {
        return _closed.get() && (_currentCheckpoint.isCompleted() || _fastClose.get());
    }

    @Override
    protected void runTask() throws Exception {
        pollCreateCheckpoint();
        pollFlushCheckpoint();
    }
}
