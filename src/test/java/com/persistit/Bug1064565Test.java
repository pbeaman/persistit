/**
 * Copyright 2012 Akiban Technologies, Inc.
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

import static com.persistit.unit.UnitTestProperties.VOLUME_NAME;
import static com.persistit.util.SequencerConstants.ACCUMULATOR_CHECKPOINT_A;
import static com.persistit.util.SequencerConstants.ACCUMULATOR_CHECKPOINT_B;
import static com.persistit.util.SequencerConstants.ACCUMULATOR_CHECKPOINT_C;
import static com.persistit.util.SequencerConstants.ACCUMULATOR_CHECKPOINT_SCHEDULED;
import static com.persistit.util.ThreadSequencer.addSchedules;
import static com.persistit.util.ThreadSequencer.enableSequencer;
import static com.persistit.util.ThreadSequencer.sequence;
import static com.persistit.util.ThreadSequencer.setCondition;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.persistit.Accumulator.SumAccumulator;
import com.persistit.exception.PersistitException;
import com.persistit.util.ThreadSequencer.Condition;

/**
 * https://bugs.launchpad.net/akiban-persistit/+bug/1064565
 * 
 * The state of an Accumulator is sometimes incorrect after shutting down and
 * restarting Persistit and as a result an application can read a count or value
 * that is inconsistent with the history of committed transactions.
 * 
 * The bug mechanism is a race between the CheckpointManager#createCheckpoint
 * method and the Accumulator#update method in which an update which occurs in a
 * transaction that starts immediately after the checkpoint begins its
 * transaction can be lost. The probability of failure is low but may be
 * increased by intense I/O activity.
 * 
 * This is a data loss error and is therefore critical.
 */

public class Bug1064565Test extends PersistitUnitTestCase {

    private final static String TREE_NAME = "Bug1064565Test";

    private Exchange getExchange() throws PersistitException {
        return _persistit.getExchange(VOLUME_NAME, TREE_NAME, true);
    }

    @Test
    public void accumulatorRace() throws Exception {
        enableSequencer(false);
        addSchedules(ACCUMULATOR_CHECKPOINT_SCHEDULED);
        final AtomicBoolean once = new AtomicBoolean(true);
        setCondition(ACCUMULATOR_CHECKPOINT_A, new Condition() {
            @Override
            public boolean enabled() {
                return once.getAndSet(false);
            }
        });

        Exchange exchange = getExchange();
        Transaction txn = exchange.getTransaction();
        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    _persistit.checkpoint();
                } catch (final PersistitException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        t.start();

        txn.begin();
        SumAccumulator acc = exchange.getTree().getSumAccumulator(0);
        acc.add(42);
        sequence(ACCUMULATOR_CHECKPOINT_B);
        txn.commit();
        txn.end();
        sequence(ACCUMULATOR_CHECKPOINT_C);

        final Configuration config = _persistit.getConfiguration();
        _persistit.close();
        _persistit = new Persistit(config);

        exchange = getExchange();
        txn = exchange.getTransaction();
        txn.begin();
        acc = exchange.getTree().getSumAccumulator(0);
        assertEquals("Accumulator state should have been checkpointed", 42, acc.getSnapshotValue());
        txn.commit();
        txn.end();

        _persistit.checkpoint();
        _persistit.checkpoint();
        _persistit.checkpoint();
    }

    /**
     * ThreadSequencer is not even needed: this sequence shows how setting
     * checkpointNeeded inside of the main transaction is not correctly
     * sequenced against the checkpoint.
     */
    @Test
    public void nathansVersion() throws Exception {
        Exchange exchange = getExchange();
        Transaction txn = exchange.getTransaction();
        txn.begin();
        SumAccumulator acc = exchange.getTree().getSumAccumulator(0);
        acc.add(42);
        _persistit.checkpoint();
        txn.commit();
        txn.end();
        _persistit.copyBackPages();
        final Configuration config = _persistit.getConfiguration();
        _persistit.close();
        _persistit = new Persistit(config);

        exchange = getExchange();
        txn = exchange.getTransaction();
        txn.begin();
        acc = exchange.getTree().getSumAccumulator(0);
        assertEquals("Accumulator state should have been checkpointed", 42, acc.getSnapshotValue());
        txn.commit();
        txn.end();

    }
}
