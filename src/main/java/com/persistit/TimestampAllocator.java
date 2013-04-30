/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit;

import java.util.concurrent.atomic.AtomicLong;

import com.persistit.exception.PersistitInterruptedException;
import com.persistit.util.Util;

class TimestampAllocator {

    private final static int CHECKPOINT_TIMESTAMP_MARKER_INTERVAL = 100;

    private final static long UNAVAILABLE_CHECKPOINT_TIMESTAMP = -1;
    /**
     * Default interval in nanoseconds between checkpoints - two minutes.
     */
    private final AtomicLong _timestamp = new AtomicLong();

    private volatile long _checkpointTimestamp;

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
     * <p>
     * Atomically allocate a new checkpoint timestamp. This method temporarily
     * marks the checkpoint timestamp field as "unavailable" until a new value
     * is computed. This prevents another thread executing
     * {@link Buffer#writePageOnCheckpoint(long)} from trusting the old
     * checkpoint timestamp while a new one is being allocated.
     * </p>
     * <p>
     * Here's the execution sequence to avoid:
     * <ol>
     * <li>Let the initial checkpoint timestamp be c0.
     * <li>Thread A (the checkpoint thread invoking this method) gets an updated
     * timestamp c1 which will become the new checkpoint timestamp.</li>
     * <li>Thread B (a thread attempting to modify a page that is already dirty
     * with timestamp t0) gets an updated timestamp t1 greater than c1</li>
     * <li>Thread B in writePageOnCheckpoint determines that t0 is larger than
     * the current checkpoint c0 (because the checkpoint timestamp field has not
     * been updated yet) and therefore continues on to further modify the page
     * without writing its state at t0.</li>
     * <li>Thread A now updates the checkpoint timestamp field to contain c1.
     * </ul> In this scenario the version of the page at t0 was never written to
     * the journal even though t0 < c1 and t1 > c1. This violates the recovery
     * safety requirement.
     * </p>
     * <p>
     * The solution is to stall the {@link #getProposedCheckpointTimestamp()}
     * method called by writePageOnCheckpoint before c1 is allocated until after
     * the checkpointTimestamp field containing c1 is visible.
     * </p>
     * 
     * @return the allocated timestamp
     */
    long allocateCheckpointTimestamp() {
        /*
         * Add a gap to the timestamp counter - this is useful only for humans
         * trying to decipher timestamps in the journal - is not necessary for
         * correct function.
         */
        bumpTimestamp(CHECKPOINT_TIMESTAMP_MARKER_INTERVAL);
        /*
         * Prevents Buffer#writePageOnCheckpoint from seeing the old checkpoint
         * while we are assigning the new one. The
         * getProposedCheckpointTimestamp method below spins until this has been
         * settled.
         */
        _checkpointTimestamp = UNAVAILABLE_CHECKPOINT_TIMESTAMP;
        final long timestamp = updateTimestamp();
        _checkpointTimestamp = timestamp;
        return timestamp;
    }

    public long getCurrentTimestamp() {
        return _timestamp.get();
    }

    long getProposedCheckpointTimestamp() throws PersistitInterruptedException {
        long timestamp;
        /*
         * Spin until stable. This value must not be observed until set.
         */
        for (int iterations = 0; (timestamp = _checkpointTimestamp) == UNAVAILABLE_CHECKPOINT_TIMESTAMP; iterations++) {
            if (iterations > 10) {
                Util.spinSleep();
            }
        }

        return timestamp;
    }

}
