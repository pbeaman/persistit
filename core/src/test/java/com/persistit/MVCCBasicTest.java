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
import com.persistit.unit.PersistitUnitTestCase;

public class MVCCBasicTest extends PersistitUnitTestCase {
    private static final String VOL_NAME = "persistit";
    private static final String TREE_NAME = "mvccbasic";

    private static final String KEY1 = "k1";
    private static final String KEY2 = "k2";
    private static final long VALUE1 = 12345L;
    private static final long VALUE2 = 67890L;
    
    public void testSingleTrxWriteAndRead() throws PersistitException {
        Exchange ex = _persistit.getExchange(VOL_NAME, TREE_NAME, true);
        Transaction trx = ex.getTransaction();

        trx.begin();
        try {
            ex.append(KEY1).getValue().put(VALUE1);
            ex.store();
            ex.clear().append(KEY1).fetch();
            assertEquals("fetch before commit", VALUE1, ex.getValue().getLong());
            trx.commit();
        }
        finally {
            trx.end();
        }

        trx.begin();
        try {
            ex.clear().append(KEY1).fetch();
            assertEquals("fetch after commit", VALUE1, ex.getValue().getLong());
            trx.commit();
        }
        finally {
            trx.end();
        }
    }

    public void testTwoTrxDistinctWritesOverlappedReads() throws PersistitException {
        final SessionId session1 = new SessionId();
        final SessionId session2 = new SessionId();

        _persistit.setSessionId(session1);
        Exchange ex1 = _persistit.getExchange(VOL_NAME, TREE_NAME, true);
        Transaction trx1 = ex1.getTransaction();

        _persistit.setSessionId(session2);
        Exchange ex2 = _persistit.getExchange(VOL_NAME, TREE_NAME, true);
        Transaction trx2 = ex2.getTransaction();

        trx1.begin();
        trx2.begin();
        try {
            assertFalse("differing start timestamps", trx1.getStartTimestamp() == trx2.getStartTimestamp());

            ex1.clear().append(KEY1).getValue().put(VALUE1);
            ex1.store();

            ex2.clear().append(KEY1).fetch();
            assertFalse("uncommitted trx1 value", ex2.getValue().isDefined());

            ex2.clear().append(KEY2).getValue().put(VALUE2);
            ex2.store();

            ex1.clear().append(KEY2).fetch();
            assertFalse("uncommitted trx2 value", ex1.getValue().isDefined());

            trx1.commit();

            ex2.clear().append(KEY1).fetch();
            assertFalse("committed trx1 value post trx2 start", ex2.getValue().isDefined());

            trx2.commit();
        }
        finally {
            trx1.end();
            trx2.end();
        }

        // Both should see both now
        trx1.begin();
        trx2.begin();
        try {
            ex1.clear().append(KEY1).fetch();
            assertEquals("original trx1 value from new trx1", VALUE1, ex1.getValue().getLong());
            ex1.clear().append(KEY2).fetch();
            assertEquals("original trx2 value from new trx1", VALUE2, ex1.getValue().getLong());
            trx1.commit();

            ex2.clear().append(KEY1).fetch();
            assertEquals("original trx1 value from new trx2", VALUE1, ex2.getValue().getLong());
            ex2.clear().append(KEY2).fetch();
            assertEquals("original trx2 value from new trx2", VALUE2, ex2.getValue().getLong());
            trx2.commit();
        }
        finally {
            trx1.end();
            trx2.end();
        }
    }
}
