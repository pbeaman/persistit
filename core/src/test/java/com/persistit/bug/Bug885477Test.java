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
import com.persistit.Key;
import com.persistit.Transaction;
import com.persistit.unit.PersistitUnitTestCase;

/**
 * Suppose a persistit tree has keys (1, 10), (1, 20), (2, 30). If an exchange's
 * key is (1), then traverse(GTEQ, false) should traverse to the first record,
 * resulting in the key still containing (1). Instead, traverse results in an
 * exchange with key (2), as if the EQ part of the direction was ignored.
 */

public class Bug885477Test extends PersistitUnitTestCase {

    @Test
    public void testNotInTransaction() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "bug885477", true);
        ex.clear().append(1).append(10).store();
        ex.clear().append(1).append(20).store();
        ex.clear().append(2).append(30).store();
        
        ex.clear().append(1);
        assertTrue(ex.traverse(Key.GTEQ, false));
        assertEquals(1, ex.getKey().decodeInt());

        ex.clear().append(2);
        assertTrue(ex.traverse(Key.LTEQ, false));
        assertEquals(2, ex.getKey().decodeInt());
    }

    @Test
    public void testInTransaction() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "bug885477", true);
        final Transaction txn = _persistit.getTransaction();
        txn.begin();
        ex.clear().append(1).append(10).store();
        ex.clear().append(1).append(20).store();
        ex.clear().append(2).append(30).store();
        
        ex.clear().append(1);
        assertTrue(ex.traverse(Key.GTEQ, false));
        assertEquals(1, ex.getKey().decodeInt());

        ex.clear().append(2);
        assertTrue(ex.traverse(Key.LTEQ, false));
        assertEquals(2, ex.getKey().decodeInt());
        txn.commit();
        txn.end();
    }

}
