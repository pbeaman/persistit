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

import junit.framework.TestCase;

public class AccumulatorTest extends TestCase {

    private final TimestampAllocator _tsa = new TimestampAllocator();

    public void testBasicMethodsOneBucket() throws Exception {
        TransactionIndex ti = new TransactionIndex(_tsa, 1);
        Accumulator acc = Accumulator.accumulator(Accumulator.Type.SUM, null, 0, 0, ti);
        TransactionStatus status = ti.registerTransaction();
        acc.update(1, status, 0);
        assertEquals(1, acc.getLiveValue());
        assertEquals(0, acc.getSnapshotValue(1, 0));
        status.commit(_tsa.updateTimestamp());
        assertEquals(0, acc.getSnapshotValue(status.getTc() + 1, 0));
        ti.notifyCompleted(status);
        assertEquals(0, acc.getSnapshotValue(status.getTs(), 0));
        assertEquals(1, acc.getSnapshotValue(status.getTc() + 1, 0));
    }

    public void testBasicMethodsMultipleBuckets() throws Exception {
        TransactionIndex ti = new TransactionIndex(_tsa, 1000);
        Accumulator countAcc = Accumulator.accumulator(Accumulator.Type.SUM, null, 0, 0, ti);
        Accumulator sumAcc = Accumulator.accumulator(Accumulator.Type.SUM, null, 0, 0, ti);
        Accumulator minAcc = Accumulator.accumulator(Accumulator.Type.MIN, null, 0, 0, ti);
        Accumulator maxAcc = Accumulator.accumulator(Accumulator.Type.MAX, null, 0, 0, ti);
        for (int count = 0; count < 100000; count++) {
            TransactionStatus status = ti.registerTransaction();
            assertEquals(count, countAcc.getLiveValue());
            countAcc.update(1, status, 0);
            sumAcc.update(count, status, 0);
            minAcc.update(-1000 - (count % 17), status, 0);
            maxAcc.update(1000 + (count % 17), status, 0);
            status.commit(_tsa.updateTimestamp());
            ti.notifyCompleted(status);
            if ((count % 1000) == 0) {
                ti.updateActiveTransactionCache();
            }
        }
        long after = _tsa.updateTimestamp();
        assertEquals(100000, countAcc.getSnapshotValue(after, 0));
        assertEquals(-1016, minAcc.getSnapshotValue(after, 0));
        assertEquals(1016, maxAcc.getSnapshotValue(after, 0));
        assertEquals(sumAcc.getLiveValue(), sumAcc.getSnapshotValue(after, 0));
    }
}
