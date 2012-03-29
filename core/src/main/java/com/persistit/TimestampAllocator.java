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
        long timestamp = updateTimestamp();
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
