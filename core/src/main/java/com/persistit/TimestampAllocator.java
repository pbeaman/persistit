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

import java.util.concurrent.atomic.AtomicLong;

class TimestampAllocator {

    private final static int CHECKPOINT_TIMESTAMP_MARKER_INTERVAL = 100;

    /**
     * Default interval in nanoseconds between checkpoints - two minutes.
     */
    private final AtomicLong _timestamp = new AtomicLong();

    private volatile long _checkpointTimestamp = 0;

    public long updateTimestamp() {
        return _timestamp.incrementAndGet();
    }

    long bumpTimestamp(final long delta) {
        return _timestamp.addAndGet(delta);
    }

    public long updateTimestamp(final long timestamp) {
        _timestamp.incrementAndGet();
        while (true) {
            final long expected = _timestamp.get();
            if (expected < timestamp) {
                if (_timestamp.compareAndSet(expected, timestamp)) {
                    return timestamp;
                }
            } else {
                return expected;
            }
        }
    }

    /**
     * Atomically allocate a new checkpoint timestamp. This method ensures that
     * the proposed checkpoint timestamp contains the largest timestamp ever
     * allocated. In particular, it prevents another thread from allocated a
     * larger timestamp before the checkpointTimestamp field is set.
     * 
     * @return the allocated timestamp
     */
    long allocateCheckpoint() {
        /*
         * Add a gap to the timestamp counter - this is useful only for humans
         * trying to decipher timestamps in the journal - is not necessary for
         * correct function.
         */
        bumpTimestamp(CHECKPOINT_TIMESTAMP_MARKER_INTERVAL);
        while (true) {
            long candidate = _timestamp.get();
            _checkpointTimestamp = candidate;
            if (_timestamp.get() == candidate) {
                return candidate;
            }
        }
    }

    public long getCurrentTimestamp() {
        return _timestamp.get();
    }

    public long getProposedCheckpointTimestamp() {
        return _checkpointTimestamp;
    }

}
