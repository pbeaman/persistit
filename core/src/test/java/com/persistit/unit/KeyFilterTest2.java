/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit.unit;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
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
