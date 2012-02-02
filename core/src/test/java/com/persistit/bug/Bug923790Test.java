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

package com.persistit.bug;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.TestShim;
import com.persistit.Transaction;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.util.Util;

/**
 * This is a subtle but serious database corruption issue that could lead to
 * data loss.
 * 
 * The Buffer#pruneMvvValues method in some cases must write the page being
 * pruned before modifying it. This happens when the page is already dirty, a
 * checkpoint was allocated, and the pruning operation is taking place after the
 * checkpoint. The bug occurred occasionally in AccumulatorRecoveryTest after
 * background pruning was added, but the mechanism could cause this failure in
 * other circumstances.
 * 
 * The issue is that before writing the page Persistit reorganizes it to squeeze
 * out unused space using the Buffer#clearSlack() method. The problem is that
 * pruneMvvValues calls this method while traversing keys and does not reset its
 * internal state after the buffer has been reorganized. The result is that
 * internal pointers subsequently point to invalid data and can cause serious
 * corruption.
 * 
 * Plan for a unit test:
 * 
 * 1. Create a tree with MVV values using a transaction that aborts after
 * creating them.
 * 
 * 2. Allocate a new checkpoint.
 * 
 * 3. Prune pages of that tree.
 * 
 * Pruning should result in removing the aborted MVVs which will create gaps in
 * the allocated space within the page. The bug mechanism should then corrupt
 * the page and lead to one of several different kinds of exceptions.
 */

public class Bug923790Test extends PersistitUnitTestCase {

    public void testInduceBug923790() throws Exception {
        Exchange ex = _persistit.getExchange("persistit", "Bug923790Test", true);
        
        ex.getValue().put("abcdef");
        ex.to(-1).store();
        for (int i = 200; --i >= 100;) {
            ex.getValue().put(RED_FOX.substring(0, i % RED_FOX.length()));
            ex.to(i).store();
        }
        
        Transaction txn = _persistit.getTransaction();
        txn.begin();
        for (int i = 100; --i >= 0;) {
            ex.getValue().put(RED_FOX.substring(0, i % RED_FOX.length()));
            ex.to(i).store();
        }
        new Thread(new Runnable() {
            public void run() {
                Transaction txn = _persistit.getTransaction();
                try {
                    txn.begin();
                    Util.sleep(1000);
                    txn.commit();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    txn.end();
                }
            }
        });
        txn.commit();
        txn.end();
        ex.to(-1).remove();

        Util.sleep(2000);
        TestShim.allocateCheckpointTimestamp(_persistit);
        ex.to(1);
        TestShim.prune(ex);
        txn.begin();
        for (int i = 0; i < 200; i++) {
            ex.to(i).fetch();
            assertEquals(RED_FOX.substring(0, i % RED_FOX.length()), ex.getValue().get());
        }
        txn.commit();
        txn.end();
    }
}
