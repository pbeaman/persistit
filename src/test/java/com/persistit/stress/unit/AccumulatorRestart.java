/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit.stress.unit;

import java.rmi.RemoteException;
import java.util.Random;

import javax.management.ObjectName;

import com.persistit.Accumulator;
import com.persistit.Configuration;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.persistit.mxbeans.CheckpointManagerMXBean;
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
    final static Random RANDOM = new Random();

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

        for (_count = 1;; _count++) {
            try {
                System.out.println(_threadName + " starting cycle " + _count);
                _ex = getPersistit().getExchange("persistit", _rootName + _threadIndex, true);
                final Accumulator sum = _ex.getTree().getAccumulator(Accumulator.Type.SUM, 17);
                final Accumulator max = _ex.getTree().getAccumulator(Accumulator.Type.MAX, 22);
                final Accumulator min = _ex.getTree().getAccumulator(Accumulator.Type.MIN, 23);
                final Accumulator seq = _ex.getTree().getAccumulator(Accumulator.Type.SEQ, 47);
                final Transaction txn = _ex.getTransaction();

                if (_count > 1) {
                    txn.begin();
                    try {
                        final long minv = min.getSnapshotValue(txn);
                        final long maxv = max.getSnapshotValue(txn);
                        final long seqv = seq.getSnapshotValue(txn);
                        final long sumv = sum.getSnapshotValue(txn);
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
                int seqValueHold = 0;
                final boolean a = RANDOM.nextInt(50) == 0;
                txn.begin();
                try {
                    final long timeOffset = RANDOM.nextInt(1000) - 500;
                    while (!isTriggered(timeOffset)) {
                        final long r = RANDOM.nextInt(1000) - 500;
                        min.update(bsum(minValue, r), txn);
                        max.update(bsum(maxValue, r), txn);
                        seq.update(1, txn);
                        sum.update(r, txn);
                        long minWas = getLong(_ex.to("min"), Long.MAX_VALUE);
                        _ex.getValue().put(Math.min(bsum(minValue, r), minWas));
                        _ex.store();
                        long maxWas = getLong(_ex.to("max"), Long.MIN_VALUE);
                        _ex.getValue().put(Math.max(bsum(maxValue, r), maxWas));
                        _ex.store();
                        long seqWas = getLong(_ex.to("seq"), 0);
                        _ex.getValue().put(Math.max(seqValue + 1, seqWas));
                        _ex.store();
                        long sumWas = getLong(_ex.to("sum"), 0);
                        _ex.getValue().put(Math.min(sumValue + r, sumWas));
                        _ex.store();
                        seqValueHold++;
                        if (!a) {
                            minValue = Math.min(bsum(minValue, r), minValue);
                            maxValue = Math.max(bsum(maxValue, r), maxValue);
                            seqValue = seqValue + seqValueHold;
                            sumValue = sumValue + r;
                            seqValueHold = 0;
                        }
                    }
                    if (a) {
                        txn.rollback();
                    } else {
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

                    final Configuration config = db.getConfiguration();
                    db = new Persistit();
                    db.initialize(config);
                    setPersistit(db);
                }
            } catch (final Exception ex) {
                handleThrowable(ex);
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
            if (timeOffset > 0 && now > _checkpointTime + timeOffset) {
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
