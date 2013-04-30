/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
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

package com.persistit.stress.unit;

import java.util.Random;

import com.persistit.Accumulator.MaxAccumulator;
import com.persistit.Accumulator.MinAccumulator;
import com.persistit.Accumulator.SeqAccumulator;
import com.persistit.Accumulator.SumAccumulator;
import com.persistit.Configuration;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.persistit.util.ArgParser;
import com.persistit.util.Util;

/**
 * Tests Accumulator values after restart. This test modifies accumulators and
 * then restarts Persistit, both gracefully and after failure, to verify that
 * the recovered Accumulator values are always correct. The test specifically
 * attempts to test the juxtaposition of a Checkpoint and the final Accumulator
 * update to look for bugs similar to 1064565.
 * 
 * @author peter
 * 
 */
public class AccumulatorRestart extends StressBase {
    final static Random RANDOM = new Random(1);

    private final static String[] ARGS_TEMPLATE = { "repeat|int:1:0:1000000000|Repetitions" };

    public final static long CHECKPOINT_INTERVAL = 30;

    long _checkpointTimestamp = 0;
    long _checkpointTime = 0;

    public AccumulatorRestart(final String argsString) {
        super(argsString);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _ap = new ArgParser("com.persistit.Stress1", _args, ARGS_TEMPLATE).strict();
        _repeatTotal = _ap.getIntValue("repeat");
    }

    @Override
    public void executeTest() {
        /*
         * Local copies, managed by this thread for comparison
         */
        long minValue = 0;
        long maxValue = 0;
        long sumValue = 0;
        long seqValue = 0;
        long seqCommit = 0;

        final Configuration config = getPersistit().getConfiguration();
        try {
            for (_count = 1;; _count++) {
                final boolean a = RANDOM.nextInt(10) == 0;
                System.out.println(_threadName + " starting cycle " + _count + " abort=" + a);
                _ex = getPersistit().getExchange("persistit", _rootName + _threadIndex, true);
                final SumAccumulator sum = _ex.getTree().getSumAccumulator(17);
                final MaxAccumulator max = _ex.getTree().getMaxAccumulator(22);
                final MinAccumulator min = _ex.getTree().getMinAccumulator(23);
                final SeqAccumulator seq = _ex.getTree().getSeqAccumulator(47);
                final Transaction txn = _ex.getTransaction();
                if (_count > 1) {
                    txn.begin();
                    try {
                        final long minv = min.getSnapshotValue();
                        final long maxv = max.getSnapshotValue();
                        final long seqv = seq.getLiveValue();
                        final long sumv = sum.getSnapshotValue();
                        if (minv != minValue || maxv != maxValue || seqv != seqValue || sumv != sumValue) {
                            fail(String.format("Values don't match: (min/max/seq/sum) "
                                    + "expected=(%,d/%,d/%,d/%,d) actual=(%,d/%,d/%,d/%,d)", minValue, maxValue,
                                    seqValue, sumValue, minv, maxv, seqv, sumv));
                        }
                        txn.commit();
                    } finally {
                        txn.end();
                    }
                }
                txn.begin();
                try {
                    final long timeOffset = RANDOM.nextInt(1000) - 500;
                    while (!isTriggered(timeOffset)) {
                        final long r = RANDOM.nextInt(1000) - 500;
                        min.minimum(bsum(minValue, r));
                        max.maximum(bsum(maxValue, r));
                        seq.allocate();
                        sum.add(1);
                        seqValue++;
                        final long minWas = getLong(_ex.to("min"), Long.MAX_VALUE);
                        _ex.getValue().put(Math.min(bsum(minValue, r), minWas));
                        _ex.store();
                        final long maxWas = getLong(_ex.to("max"), Long.MIN_VALUE);
                        _ex.getValue().put(Math.max(bsum(maxValue, r), maxWas));
                        _ex.store();
                        final long seqWas = getLong(_ex.to("seq"), 0);
                        _ex.getValue().put(Math.max(seqValue, seqWas));
                        _ex.store();
                        final long sumWas = getLong(_ex.to("sum"), 0);
                        _ex.getValue().put(Math.min(sumValue + r, sumWas));
                        _ex.store();
                        if (!a) {
                            minValue = Math.min(bsum(minValue, r), minValue);
                            maxValue = Math.max(bsum(maxValue, r), maxValue);
                            sumValue = sumValue + r;
                        }
                    }
                    if (a) {
                        txn.rollback();
                    } else {
                        seqCommit = seqValue;
                        txn.commit();
                    }
                } finally {
                    txn.end();
                }

                if (isStopped() || _count >= _repeatTotal) {
                    break;
                }

                if ((_count % 3) == 0) {
                    Persistit db = getPersistit();
                    db.close();
                    db = new Persistit();
                    db.initialize(config);
                    setPersistit(db);
                    seqValue = seqCommit;
                }
            }
        } catch (final Exception ex) {
            handleThrowable(ex);
        } finally {
            final Persistit db = getPersistit();
            try {
                db.close();
            } catch (final PersistitException pe) {
                handleThrowable(pe);
            }
        }
    }

    private boolean isTriggered(final long timeOffset) {
        final long cp = getPersistit().getCurrentCheckpoint().getTimestamp();
        final long now = System.nanoTime();

        if (_checkpointTime == 0) {
            if (cp > _checkpointTimestamp) {
                _checkpointTime = now;
                _checkpointTimestamp = cp;
            }
        }
        if (_checkpointTime != 0) {
            if (timeOffset >= 0 && now > _checkpointTime + timeOffset) {
                _checkpointTime = 0;
                return true;
            } else if (timeOffset < 0 && now > _checkpointTime + timeOffset + CHECKPOINT_INTERVAL * Util.MS_PER_S) {
                _checkpointTime = 0;
                return true;
            }
        }
        return false;
    }

    private long bsum(final long a, final long b) {
        if (a < 0) {
            return a + b > 0 ? a : a + b;
        } else {
            return a + b < 0 ? a : a + b;
        }
    }

    private long getLong(final Exchange ex, final long dflt) throws PersistitException {
        if (ex.getValue().isDefined()) {
            return ex.getValue().getLong();
        } else {
            return dflt;
        }
    }
}
