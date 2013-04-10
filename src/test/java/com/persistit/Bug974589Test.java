/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.persistit.Accumulator.SumAccumulator;
import com.persistit.exception.PersistitException;
import com.persistit.unit.UnitTestProperties;

public class Bug974589Test extends PersistitUnitTestCase {
    /*
     * https://bugs.launchpad.net/akiban-persistit/+bug/974589
     * IllegalStateException Thrown from BucketSampler.add During analyze table
     * 
     * Distilled: database is left with row count Accumulator holding a value of
     * 0 when there is actually a row.
     */

    private static final String TREE_NAME = "Bug974589Test";

    private static Exchange getExchange(final Persistit persistit) throws PersistitException {
        return persistit.getExchange(UnitTestProperties.VOLUME_NAME, TREE_NAME, true);
    }

    @Test
    public void testBug980292() throws Exception {
        for (int loop = 1; loop < 5; loop++) {
            _persistit.checkpoint();
            Exchange ex1 = getExchange(_persistit);
            Transaction txn1 = ex1.getTransaction();
            /*
             * Touch an Accumulator, then immediately checkpoint.
             */
            txn1.begin();
            SumAccumulator acc1 = ex1.getTree().getSumAccumulator(1);
            acc1.increment();
            txn1.commit();
            txn1.end();
            ex1 = null;
            txn1 = null;
            acc1 = null;
            _persistit.checkpoint();

            final Configuration config = _persistit.getConfiguration();
            _persistit.close();
            _persistit = new Persistit(config);

            Exchange ex2 = getExchange(_persistit);
            Transaction txn2 = ex2.getTransaction();
            txn2.begin();
            SumAccumulator acc2 = ex2.getTree().getSumAccumulator(1);
            assertEquals(loop, acc2.getSnapshotValue());
            txn2.commit();
            txn2.end();
            System.out.printf("Iteration %,d completed\n", loop);
            ex2 = null;
            txn2 = null;
            acc2 = null;
        }
    }
}
