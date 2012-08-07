/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

        assertTrue(ex.hasChildren());
        assertFalse(ex.isValueDefined());
        ex.append(30);
        assertFalse(ex.hasChildren());
        assertTrue(ex.isValueDefined());

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

        assertTrue(ex.hasChildren());
        assertFalse(ex.isValueDefined());

        txn.commit();
        txn.end();
    }

}
