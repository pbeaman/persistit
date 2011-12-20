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

import java.io.PrintWriter;

import org.junit.Test;

import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;

public class IntegrityCheckTest extends PersistitUnitTestCase {

    private String _volumeName = "persistit";

    @Test
    public void testSimplePrimordialTree() throws Exception {
        final Exchange ex = _persistit.getExchange(_volumeName, "primordial", true);
        nonTransactionalStore(ex);
        
        IntegrityCheck icheck = icheck();
        icheck.checkTree(ex.getTree());
        assertTrue(icheck.getDataByteCount() >= RED_FOX.length() * 1000);
        assertTrue(icheck.getDataPageCount() > 0);
        assertEquals(0, icheck.getFaults().length);
        assertTrue(icheck.getIndexByteCount() > 0);
        assertTrue(icheck.getIndexPageCount() > 0);
        assertEquals(0, icheck.getLongRecordByteCount());
        assertEquals(0, icheck.getLongRecordPageCount());
        assertEquals(0, icheck.getMvvCount());
        assertEquals(0, icheck.getMvvOverhead());
        assertEquals(0, icheck.getMvvAntiValues());
    }
    
    @Test
    public void testSimpleMvvTree() throws Exception {
        final Exchange ex = _persistit.getExchange(_volumeName, "mvv", true);
        _persistit.getCleanupManager().setPollInterval(Integer.MAX_VALUE);
        
        transactionalStore(ex);
        
        IntegrityCheck icheck = icheck();
        icheck.checkTree(ex.getTree());
        assertTrue(icheck.getDataByteCount() >= RED_FOX.length() * 1000);
        assertTrue(icheck.getDataPageCount() > 0);
        assertEquals(0, icheck.getFaults().length);
        assertTrue(icheck.getIndexByteCount() > 0);
        assertTrue(icheck.getIndexPageCount() > 0);
        assertEquals(0, icheck.getLongRecordByteCount());
        assertEquals(0, icheck.getLongRecordPageCount());
        assertEquals(1000, icheck.getMvvCount());
        assertTrue(icheck.getMvvOverhead() > 0);
        assertEquals(500, icheck.getMvvAntiValues());
    }

    @Test
    public void testBrokenKeySequence() throws Exception {
        final Exchange ex = _persistit.getExchange(_volumeName, "mvv", true);
        _persistit.getCleanupManager().setPollInterval(Integer.MAX_VALUE);
        
        transactionalStore(ex);
        
        corrupt1(ex);
        IntegrityCheck icheck = icheck();
        icheck.checkTree(ex.getTree());
        assertTrue(icheck.getFaults().length > 0);

    }
    
    @Test
    public void testBrokenMVVs() throws Exception {
        final Exchange ex = _persistit.getExchange(_volumeName, "mvv", true);
        _persistit.getCleanupManager().setPollInterval(Integer.MAX_VALUE);
        transactionalStore(ex);
        corrupt2(ex);
        IntegrityCheck icheck = icheck();
        icheck.checkTree(ex.getTree());
        assertTrue(icheck.getFaults().length > 0);
    }
    
    private String key(final int i) {
        return String.format("%05d%s", i, RED_FOX);
    }
    
    private void nonTransactionalStore(final Exchange ex) throws PersistitException {
        ex.getValue().put(RED_FOX);
        for (int i = 0; i < 1000; i++) {
            ex.to(key(i));
            ex.store();
        }
        for (int i = 1; i < 1000; i += 2) {
            ex.to(key(i));
            ex.remove();
        }
        
    }
    
    
    private void transactionalStore(final Exchange ex) throws PersistitException {
        final Transaction txn = ex.getTransaction();
        txn.begin();
        ex.getValue().put(RED_FOX);
        for (int i = 0; i < 1000; i++) {
            ex.to(key(i));
            ex.store();
        }
        txn.commit();
        txn.end();
        txn.begin();
        for (int i = 1; i < 1000; i += 2) {
            ex.to(key(i));
            ex.remove();
        }
        txn.commit();
        txn.end();

    }
    private void corrupt1(final Exchange ex) throws PersistitException {
        Key key = ex.getKey();
        ex.clear().to(key(500));
        Buffer copy = ex.fetchBufferCopy(0);
        Buffer buffer = ex.getBufferPool().get(ex.getVolume(), copy.getPageAddress(), true, true);
        buffer.nextKey(key, buffer.toKeyBlock(0));
        assertTrue(key.getEncodedSize() > 1);
        int t = (int)(buffer.at(buffer.toKeyBlock(0)) >>> 32);
        buffer.getBytes()[t - key.getEncodedSize() + 1]++;
        buffer.setDirtyAtTimestamp(_persistit.getTimestampAllocator().updateTimestamp());
        buffer.release();
    }
    
    private void corrupt2(final Exchange ex) throws PersistitException {
        ex.ignoreMVCCFetch(true);
        ex.clear().to(key(500));
        int corrupted = 0;
        while (corrupted < 10 && ex.next()) {
            byte[] bytes = ex.getValue().getEncodedBytes();
            int length = ex.getValue().getEncodedSize();
            if (MVV.isArrayMVV(bytes, 0, length)) {
                bytes[9]++;
                ex.store();
                corrupted++;
            }
        }
        ex.ignoreMVCCFetch(false);
    }
    
    private IntegrityCheck icheck() {
        IntegrityCheck icheck = new IntegrityCheck(_persistit);
        icheck.setMessageLogVerbosity(Task.LOG_VERBOSE);
        icheck.setMessageWriter(new PrintWriter(System.out));
        return icheck;
    }
}
