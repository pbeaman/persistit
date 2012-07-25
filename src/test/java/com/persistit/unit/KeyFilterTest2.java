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

package com.persistit.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.PersistitUnitTestCase;
import com.persistit.exception.PersistitException;

/**
 * @version 1.0
 */
public class KeyFilterTest2 extends PersistitUnitTestCase {

    private String ks(final int i) {
        return "abcdefghij".substring(i, i + 1);
    }

    @Test
    public void test1() throws PersistitException {
        System.out.print("test1 ");
        final Exchange ex = _persistit.getExchange("persistit", "KeyFilter2", true);
        final Key key = ex.getKey();
        ex.removeAll();
        for (int i = 0; i < 1000; i++) {
            ex.getValue().put("Value " + i);
            // 10 unique keys
            ex.clear().append(2).append(ks(i / 100));
            ex.store();
            // 100 unique keys
            ex.clear().append(2).append(ks(i / 100)).append(3).append(ks((i / 10) % 10));
            ex.store();
            if ((i % 2) == 0) {
                // 500 unique keys
                ex.clear().append(2).append(ks(i / 100)).append(3).append(ks((i / 10) % 10)).append(4).append(
                        ks(i % 10));
                ex.store();
                // 500 unique keys
                ex.clear().append(2).append(ks(i / 100)).append(3).append(ks((i / 10) % 10)).append(5).append(
                        ks(i % 10));
                ex.store();
                // 500 unique keys
                ex.clear().append(2).append(ks(i / 100)).append(3).append(ks((i / 10) % 10)).append(4).append(
                        ks(i % 10)).append(5).append("x");
                ex.store();
            }
        }

        ex.clear();
        assertTrue(ex.traverse(Key.GT, new KeyFilter("{2,*,3,*,4,>[\"a\":\"e\"]<}"), 0));
        assertEquals("{2,\"a\",3,\"a\",4,\"a\"}", ex.getKey().toString());

        assertEquals(600, countKeys(ex, "{2,*,>3,*,4,*<}"));
        assertEquals(500, countKeys(ex, "{2,*,3,*,4,>*<}"));
        assertEquals(610, countKeys(ex, "{2,>*,3,*,4,*<}"));
        assertEquals(300, countKeys(ex, "{2,*,3,*,4,>[\"a\":\"e\"]<}"));
        assertEquals(10, countKeys(ex, "{2,*<}"));
        assertEquals(610, countKeys(ex, "{2,>*,3,*,5,*<}"));
        assertEquals(0, countKeys(ex, "{3,*,>3,*,4,*<}"));
        assertEquals(0, countKeys(ex, "{2,*,3,*,>6,*<}"));
        assertEquals(500, countKeys(ex, "{2,\"a\":\"z\",3,*,4,*,5,>\"x\"}"));
        assertEquals(90, countKeys(ex, "{2,{\"a\",\"b\",\"c\"},3,*,4,>[\"a\":\"e\"]<}"));
        assertEquals(300, countKeys(ex, "{2,*,3,*,4,>[\"a\":\"e\"]<}"));

        System.out.println("- done");
    }

    private int countKeys(final Exchange ex, String kfString) throws PersistitException {
        ex.clear();
        int count = 0;
        final KeyFilter kf = new KeyFilter(kfString);
        while (ex.traverse(Key.GT, kf, Integer.MAX_VALUE)) {
            count++;
        }
        return count;

    }

    public static void main(final String[] args) throws Exception {
        new KeyFilterTest2().initAndRunTest();
    }

    @Override
    public void runAllTests() throws Exception {
        test1();

    }

}
