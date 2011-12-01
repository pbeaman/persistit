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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.PersistitInterruptedException;

class CheckpointManager extends IOTaskRunnable {

    /**
     * A structure containing a timestamp and system clock time at which
     * Persistit will attempt to record a valid Checkpoint to disk.
     */
    public static class Checkpoint {

        private final static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        private final long _timestamp;

        private final long _systemTime;

        public Checkpoint(final long timestamp, final long systemTime) {
            _timestamp = timestamp;
            _systemTime = systemTime;
        }

        public long getTimestamp() {
            return _timestamp;
        }

        public long getSystemTimeMillis() {
            return _systemTime;
        }

        @Override
        public String toString() {
            return String.format("Checkpoint %,d @ %s", _timestamp, SDF.format(new Date(_systemTime)));
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

    private final static int CHECKPOINT_TIMESTAMP_MARKER_INTERVAL = 100;

    private volatile long _checkpointInterval = DEFAULT_CHECKPOINT_INTERVAL;

    private volatile long _lastCheckpointNanos;

    private final static long SHORT_DELAY = 500;

    private final static long FLUSH_CHECKPOINT_INTERVAL = 5000;

    private final List<Checkpoint> _outstandingCheckpoints = new ArrayList<Checkpoint>();

    private volatile Checkpoint _currentCheckpoint = new Checkpoint(0, 0);

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
                if (!_outstandingCheckpoints.contains(checkpoint)) {
                    return checkpoint;
                }
            }
            try {
                Thread.sleep(SHORT_DELAY);
            } catch (InterruptedException ie) {
                throw new PersistitInterruptedException(ie);
            }
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
     * each Accumulator and then serializes that value into the database.
     * That process is not threadsafe, and there is no use case
     * for concurrent checkpoints.
     * 
     * @return The newly created Checkpoint
     * @throws PersistitException
     */
    synchronized Checkpoint createCheckpoint() throws PersistitException {
        _lastCheckpointNanos = System.nanoTime();

        /*
         * Add a gap to the timestamp counter - this is useful only for humans
         * trying to decipher timestamps in the journal - not necessary for
         * correct function.
         */
        _persistit.getTimestampAllocator().bumpTimestamp(CHECKPOINT_TIMESTAMP_MARKER_INTERVAL);
        /*
         * Run within a transaction to get snapshot accumulator views. The
         * Checkpoint timestamp is the start timestamp of this transaction.
         * Therefore the Accumulator snapshot values represent the aggregation
         * of all transactions that committed before the checkpoint timestamp.
         */
        Transaction txn = _persistit.getTransaction();
        txn.begin();
        try {
            List<Accumulator> accumulators = _persistit.getCheckpointAccumulators();
            _persistit.getTransactionIndex().checkpointAccumulatorSnapshots(txn.getStartTimestamp(), accumulators);
            Accumulator.saveAccumulatorCheckpointValues(accumulators);
            txn.commit();
            final Checkpoint checkpoint = new Checkpoint(txn.getStartTimestamp(), System.currentTimeMillis());
            synchronized (this) {
                _outstandingCheckpoints.add(checkpoint);
                _currentCheckpoint = checkpoint;
            }
            _persistit.getLogBase().checkpointProposed.log(checkpoint);
            return checkpoint;
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
        final Checkpoint validCheckpoint;
        synchronized (this) {
            validCheckpoint = findValidCheckpoint(_outstandingCheckpoints);
        }
        if (validCheckpoint != null) {
            try {
                _persistit.getJournalManager().writeCheckpointToJournal(validCheckpoint);
                synchronized (this) {
                    _outstandingCheckpoints.remove(validCheckpoint);
                }
            } catch (PersistitIOException e) {
                _persistit.getLogBase().exception.log(e);
            }
        }
    }

    /**
     * Given a List of outstanding Checkpoints, find the latest one that is safe
     * to write and return it. If there is no safe Checkpoint, return
     * <code>null</code>
     * 
     * @param List
     *            of outstanding Checkpoint instances
     * @return The latest Checkpoint from the list that can be written, or
     *         <code>null</code> if there are none.
     */
    private Checkpoint findValidCheckpoint(final List<Checkpoint> outstandingCheckpoints) {
        if (!outstandingCheckpoints.isEmpty()) {
            long earliestDirtyTimestamp = Math.min(_persistit.earliestLiveTransaction(), _persistit
                    .earliestDirtyTimestamp());
            for (int index = outstandingCheckpoints.size(); --index >= 0;) {
                final Checkpoint checkpoint = outstandingCheckpoints.get(index);
                if (checkpoint.getTimestamp() <= earliestDirtyTimestamp) {
                    for (int k = index - 1; k >= 0; --k) {
                        outstandingCheckpoints.remove(k);
                    }
                    return checkpoint;
                }
            }
        }
        return null;
    }

    @Override
    protected synchronized boolean shouldStop() {
        return _closed.get() && (_outstandingCheckpoints.isEmpty() || _fastClose.get());
    }

    @Override
    protected void runTask() throws Exception {
        pollCreateCheckpoint();
        pollFlushCheckpoint();
    }

}
