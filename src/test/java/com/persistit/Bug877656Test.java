/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Transaction;

public class Bug877656Test extends PersistitUnitTestCase {

    @Test
    public void testForward() throws Exception {
        Transaction txn = _persistit.getTransaction();
        txn.begin();
        Exchange ex = _persistit.getExchange("persistit", "Bug877656Test", true);
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
        Transaction txn = _persistit.getTransaction();
        txn.begin();
        Exchange ex = _persistit.getExchange("persistit", "Bug877656Test", true);
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
