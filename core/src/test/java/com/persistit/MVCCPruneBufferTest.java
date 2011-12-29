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

import com.persistit.exception.PersistitException;

public class MVCCPruneBufferTest extends MVCCTestBase {

    public void testPrunePrimordialAntiValues() throws PersistitException {
        trx1.begin();
        
        ex1.getValue().put(RED_FOX);
        for (int i = 0; i < 5000; i++) {
            ex1.to(i).store();
        }
        for (int i = 2; i < 5000; i+= 2) {
            ex1.to(i).remove();
        }
        ex1.to(2500);
        Buffer buffer1 = ex1.fetchBufferCopy(0);
        int mvvCount = buffer1.getMvvCount();
        assertTrue(mvvCount > 0);
        int available = buffer1.getAvailableSize();
        int keys = buffer1.getKeyCount();
        buffer1.claim(true);
        buffer1.pruneMvvValues(null, ex1.getAuxiliaryKey1());
        assertEquals(keys, buffer1.getKeyCount());
        assertTrue(buffer1.getMvvCount() > 0);
        assertEquals(available, buffer1.getAvailableSize());
        
        trx1.commit();
        trx1.end();
        
        _persistit.getTransactionIndex().updateActiveTransactionCache();
        
        buffer1.pruneMvvValues(null, ex1.getAuxiliaryKey1());
        assertEquals(0, _persistit.getCleanupManager().getAcceptedCount());
        assertTrue("Pruning should have removed primordial Anti-values", keys > buffer1.getKeyCount());
        assertEquals("Pruning should leave mvvCount==0", 0, buffer1.getMvvCount());
        assertTrue("Pruning should liberate space", buffer1.getAvailableSize() > available);
    }

    public void testPruneCleanup() throws Exception {
        trx1.begin();
        
        ex1.getValue().put(RED_FOX);
        for (int i = 0; i < 5000; i++) {
            ex1.to(i).store();
        }
        ex1.removeAll();
        ex1.to(2500);
        Buffer buffer1 = ex1.fetchBufferCopy(0);
        int mvvCount = buffer1.getMvvCount();
        assertTrue(mvvCount > 0);
        int available = buffer1.getAvailableSize();
        int keys = buffer1.getKeyCount();
        buffer1.claim(true);
        buffer1.pruneMvvValues(null, ex1.getAuxiliaryKey1());
        assertEquals(keys, buffer1.getKeyCount());
        assertTrue(buffer1.getMvvCount() > 0);
        assertEquals(available, buffer1.getAvailableSize());
        
        trx1.commit();
        trx1.end();
        
        _persistit.getTransactionIndex().updateActiveTransactionCache();
        ex1.ignoreMVCCFetch(true);
        int antiValueCount1 = 0;
        ex1.to(Key.BEFORE);
        while (ex1.next()) {
            antiValueCount1++;
        }
        assertTrue(antiValueCount1 > 0);
        
        /*
         * Prune without enqueuing edge key AntiValues
         */
        final long pageCount = ex1.getVolume().getStorage().getNextAvailablePage();
        for (long page = 1; page < pageCount; page++) {
            final Buffer buffer = ex1.getBufferPool().get(ex1.getVolume(), page, true, true);
            try {
                buffer.pruneMvvValues(null, ex1.getAuxiliaryKey1());
            } finally {
                buffer.release();
            }
        }
        
        int antiValueCount2 = 0;
        ex1.to(Key.BEFORE);
        while (ex1.next()) {
            antiValueCount2++;
        }
        
        assertTrue(antiValueCount2 > 0);
        assertTrue(antiValueCount2 < antiValueCount1);
        
        /*
         * Prune with enqueuing edge key AntiValues
         */
        for (long page = 1; page < pageCount; page++) {
            final Buffer buffer = ex1.getBufferPool().get(ex1.getVolume(), page, true, true);
            try {
                buffer.pruneMvvValues(ex1.getTree(), ex1.getAuxiliaryKey1());
            } finally {
                buffer.release();
            }
        }
        
        assertTrue(_persistit.getCleanupManager().getAcceptedCount() > 0);
        _persistit.getCleanupManager().poll();
        
        assertEquals(_persistit.getCleanupManager().getAcceptedCount(), _persistit.getCleanupManager().getPerformedCount());

        int antiValueCount3 = 0;
        ex1.to(Key.BEFORE);
        while (ex1.next()) {
            antiValueCount3++;
        }
        
        assertEquals(0, antiValueCount3);
    }
}
