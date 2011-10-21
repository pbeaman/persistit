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

public class TransactionIndexTest extends TestCase {

    private final TimestampAllocator _tsa = new TimestampAllocator();

    public void testBasicMethods() throws Exception {
        final TransactionIndex ti = new TransactionIndex(_tsa, 1);
        TransactionStatus ts1 = ti.registerTransaction();
        ti.updateActiveTransactionCache();
        assertTrue(ti.hasConcurrentTransaction(0, ts1.getTs() + 1));
        ts1.commit(_tsa.updateTimestamp());
        TransactionStatus ts2 = ti.registerTransaction();
        assertTrue(ti.hasConcurrentTransaction(0, ts1.getTs() + 1));
        assertTrue(ti.hasConcurrentTransaction(0, ts2.getTs() + 1));
        ti.updateActiveTransactionCache();
        assertFalse(ti.hasConcurrentTransaction(0, ts1.getTs() + 1));
        assertTrue(ti.hasConcurrentTransaction(0, ts2.getTs() + 1));
        ts2.commit(_tsa.updateTimestamp());
        ti.updateActiveTransactionCache();
        assertFalse(ti.hasConcurrentTransaction(0, ts2.getTs() + 1));
        TransactionStatus ts3 = ti.registerTransaction();
        _tsa.updateTimestamp();
        assertEquals(TransactionStatus.UNCOMMITTED, ti.commitStatus(TransactionIndex.ts2vh(ts3.getTs()), _tsa.getCurrentTimestamp(), 0));
        assertEquals(ts3.getTs(), ti.commitStatus(TransactionIndex.ts2vh(ts3.getTs()), ts3.getTs(), 0));
        ts3.incrementMvvCount();
        ts3.abort();
        assertEquals(TransactionStatus.ABORTED, ti.commitStatus(TransactionIndex.ts2vh(ts3.getTs()), _tsa.getCurrentTimestamp(), 0));
        assertEquals(3, ti.getCurrentCount());
        ti.notifyCompleted(ts1.getTs());
        assertEquals(ts1.getTs(), ti.commitStatus(TransactionIndex.ts2vh(ts1.getTs()), _tsa.getCurrentTimestamp(), 0));
        ti.notifyCompleted(ts2.getTs());
        ti.notifyCompleted(ts3.getTs());
        assertEquals(0, ti.getCurrentCount());
        assertEquals(2, ti.getFreeCount());
        assertEquals(1, ti.getAbortedCount());
        ts3.decrementMvvCount();
        
    }
    
    public void testWwDependency() throws Exception {
        final TransactionIndex ti = new TransactionIndex(_tsa, 1);
        final TransactionStatus ts1 = ti.registerTransaction();

    }
}
