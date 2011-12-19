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

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

public class IntegrityCheckTest extends PersistitUnitTestCase {

    private String _volumeName = "persistit";

    @Test
    public void testCheckIntegritySimplePrimordialTree() throws Exception {
        final Exchange ex = _persistit.getExchange(_volumeName, "primordial", true);
        ex.getValue().put(RED_FOX);
        for (int i = 0; i < 1000; i++) {
            ex.to(String.format("%05d%s", i, RED_FOX));
            ex.store();
        }
        
        IntegrityCheck icheck = new IntegrityCheck(_persistit);
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
        System.out.println(icheck.toString(true));
    }
    
    @Test
    public void testCheckIntegritySimpleMvvTree() throws Exception {
        final Exchange ex = _persistit.getExchange(_volumeName, "primordial", true);
        _persistit.getCleanupManager().setPollInterval(Integer.MAX_VALUE);
        final Transaction txn = ex.getTransaction();
        txn.begin();
        ex.getValue().put(RED_FOX);
        for (int i = 0; i < 1000; i++) {
            ex.to(String.format("%05d%s", i, RED_FOX));
            ex.store();
        }
        txn.commit();
        txn.end();
        txn.begin();
        for (int i = 1; i < 1000; i += 2) {
            ex.to(String.format("%05d%s", i, RED_FOX));
            ex.remove();
        }
        txn.commit();
        txn.end();
        IntegrityCheck icheck = new IntegrityCheck(_persistit);
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

        System.out.println(icheck.toString(true));
    }

}
