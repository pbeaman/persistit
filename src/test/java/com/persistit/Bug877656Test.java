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

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

public class Bug877656Test extends PersistitUnitTestCase {

    @Test
    public void testForward() throws Exception {
        final Transaction txn = _persistit.getTransaction();
        txn.begin();
        final Exchange ex = _persistit.getExchange("persistit", "Bug877656Test", true);
        for (int i = 1; i < 10; i++) {
            ex.to(i).store();
        }
        txn.commit();
        txn.end();
        // ----------
        txn.begin();
        for (int i = 1; i < 10; i++) {
            ex.to(i).remove();
        }
        ex.to(0);
        int count = 0;
        for (; count < 10; count++) {
            if (!ex.traverse(Key.GTEQ, true)) {
                break;
            }
        }
        assertEquals("{{before}}", ex.getKey().toString());
        txn.commit();
        txn.end();
        assertEquals(0, count);
    }

    @Test
    public void testReverse() throws Exception {
        final Transaction txn = _persistit.getTransaction();
        txn.begin();
        final Exchange ex = _persistit.getExchange("persistit", "Bug877656Test", true);
        for (int i = 1; i < 10; i++) {
            ex.to(i).store();
        }
        txn.commit();
        txn.end();
        // ----------
        txn.begin();
        for (int i = 1; i < 10; i++) {
            ex.to(i).remove();
        }
        ex.to(11);
        int count = 0;
        for (; count < 10; count++) {
            if (!ex.traverse(Key.LTEQ, true)) {
                break;
            }
        }
        assertEquals("{{after}}", ex.getKey().toString());
        txn.commit();
        txn.end();
        assertEquals(0, count);
    }

}
