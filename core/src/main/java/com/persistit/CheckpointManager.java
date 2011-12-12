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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.util.Util;

class CheckpointManager extends IOTaskRunnable {

    /**
     * A structure containing a timestamp and system clock time at which
     * Persistit will attempt to record a valid Checkpoint to disk.
     */
    public static class Checkpoint {

        private final static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        private final long _timestamp;

        private final long _systemTime;

        private volatile boolean _completed = false;

        public Checkpoint(final long timestamp, final long systemTime) {
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
            return String.format("Checkpoint %,d%s @ %s", _timestamp, isCompleted() ? "c" : "u", SDF.format(new Date(_systemTime)));
        }

        @Override
        public boolean equals(final Object object) {
            if (!(object instanceof Checkpoint)) {
                return false;
            }
            Checkpoint cp = (Checkpoint) object;
            return cp._systemTime == _systemTime && cp._timestamp == _timestamp;
        }
    }

    /**
     * Default interval in nanoseconds between checkpoints - two minutes.
     */
    private final static long DEFAULT_CHECKPOINT_INTERVAL = 120000000000L;

    private final static Checkpoint UNAVALABLE_CHECKPOINT = new Checkpoint(0, 0);

    private volatile long _checkpointInterval = DEFAULT_CHECKPOINT_INTERVAL;

    private volatile long _lastCheckpointNanos = Long.MAX_VALUE;

    private final static long SHORT_DELAY = 500;

    private final static long FLUSH_CHECKPOINT_INTERVAL = 5000;

    private volatile Checkpoint _currentCheckpoint = new Checkpoint(0, 0, true);

    private AtomicBoolean _closed = new AtomicBoolean();

    private AtomicBoolean _fastClose = new AtomicBoolean();

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

    public Checkpoint getCurrentCheckpoint() {
        return _currentCheckpoint;
    }

    long getCheckpointInterval() {
        return _checkpointInterval;
    }

    Checkpoint checkpoint() throws PersistitException {
        Checkpoint checkpoint = createCheckpoint();
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
            Util.sleep(SHORT_DELAY);
        }
    }

    void pollCreateCheckpoint() throws PersistitException {
        final long now = System.nanoTime();
        if (_lastCheckpointNanos + _checkpointInterval < now) {
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
         * May not be run in a nested Transaction.
         */
        Transaction txn = _persistit.getTransaction();
        if (txn.isActive()) {
            throw new IllegalStateException("Checkpoint may not be created inside a transaction");
        }

        _lastCheckpointNanos = System.nanoTime();
        _currentCheckpoint = UNAVALABLE_CHECKPOINT;

        txn.beginCheckpoint();
        try {
            List<Accumulator> accumulators = _persistit.getCheckpointAccumulators();
            _persistit.getTransactionIndex().checkpointAccumulatorSnapshots(txn.getStartTimestamp(), accumulators);
            Accumulator.saveAccumulatorCheckpointValues(accumulators);
            txn.commit(true);
            _currentCheckpoint = new Checkpoint(txn.getStartTimestamp(), System.currentTimeMillis());
            _persistit.getLogBase().checkpointProposed.log(_currentCheckpoint);
            return _currentCheckpoint;
        } catch (InterruptedException ie) {
            throw new PersistitInterruptedException(ie);
        } finally {
            txn.end();
        }
    }

    /**
     * Attempt to find a new checkpoint that can be completed. Look for the
     * earliest live transaction and the earliest dirty page; determine from
     * these whether a currently outstanding checkpoint is ready to complete.
     */
    void pollFlushCheckpoint() {
        Checkpoint checkpoint = _currentCheckpoint;
        if (!checkpoint.isCompleted()) {
            long earliestDirtyTimestamp = Math.min(_persistit.earliestLiveTransaction(), _persistit
                    .earliestDirtyTimestamp());
            if (checkpoint.getTimestamp() <= earliestDirtyTimestamp) {
                try {
                    _persistit.getJournalManager().writeCheckpointToJournal(checkpoint);
                } catch (PersistitIOException e) {
                    _persistit.getLogBase().exception.log(e);
                }
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
