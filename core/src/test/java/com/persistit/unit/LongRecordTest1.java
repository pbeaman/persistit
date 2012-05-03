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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;
import com.persistit.util.Debug;
import com.persistit.util.Util;

/**
 * @author Peter Beaman
 * @version 1.0
 */
public class LongRecordTest1 extends PersistitUnitTestCase {

    public static void main(final String[] args) throws Exception {
        new LongRecordTest1().initAndRunTest();
    }

    @Override
    public void runAllTests() throws Exception {
        test1();
        test2();
        test3();

    }

    @Test
    public void test1() throws PersistitException {
        System.out.print("test1");
        final Exchange ex = _persistit.getExchange("persistit", "LongRecordTest1", true);
        ex.removeAll();
        ex.getValue().setMaximumSize(8 * 1024 * 1024);

        final StringBuilder sb = new StringBuilder(8000000);
        for (int counter = 0; counter < 8000000; counter += 10) {
            Util.fill(sb, counter, 10);
        }

        final Key key = ex.getKey();
        key.clear().append(8000000);
        ex.getValue().putString(sb);
        final int size1 = ex.getValue().getEncodedSize();
        final String bigString1 = ex.getValue().getString();

        long time = System.nanoTime();
        ex.store();
        time = System.nanoTime() - time;
        System.out.println(String.format("ex.store() took %,dns", time));
        final int size2 = ex.getValue().getEncodedSize();
        ex.getValue().clear();

        ex.fetch();
        final int size3 = ex.getValue().getEncodedSize();
        assertEquals(size2, size1);
        assertEquals(size3, size2);

        final String bigString2 = ex.getValue().getString();
        assertEquals(bigString1, bigString2);
        System.out.println(" - done");
    }

    @Test
    public void test2() throws PersistitException {
        System.out.print("test2");
        final Exchange ex = _persistit.getExchange("persistit", "LongRecordTest1", true);
        ex.removeAll();
        final StringBuilder sb1 = new StringBuilder(1000000);
        final StringBuilder sb2 = new StringBuilder(1000000);
        ex.getKey().append("foo");
        final int size = 100000;
        for (int count = 0; count < 10; count++) {
            store(ex, size, sb1);
            check(ex, size, sb1, sb2);
        }
        for (int count = 0; count < 10; count++) {
            store(ex, size, sb1);
            check(ex, size, sb1, sb2);
        }
        System.out.println(" - done");
    }

    @Test
    public void test3() throws PersistitException {
        System.out.print("test3");
        final Exchange ex = _persistit.getExchange("persistit", "LongRecordTest1", true);
        for (int cycle = 0; cycle < 5; cycle++) {
            ex.removeAll();
            final StringBuilder sb1 = new StringBuilder(1000000);
            final StringBuilder sb2 = new StringBuilder(1000000);
            ex.getKey().append("foo");
            for (int size = 0; size < 500000; size += 10000) {
                store(ex, size, sb1);
                check(ex, size, sb1, sb2);
            }
            for (int size = 500000; (size -= 10000) >= 0;) {
                store(ex, size, sb1);
                check(ex, size, sb1, sb2);
            }
        }
        System.out.println(" - done");
    }

    void store(final Exchange ex, final int size, final StringBuilder sb1) throws PersistitException {
        sb1.setLength(0);
        for (int counter = 0; counter < size; counter += 10) {
            Util.fill(sb1, counter, 10);
        }
        ex.getValue().putString(sb1);
        final int size2 = ex.getValue().getEncodedSize() - 1;
        final int size1 = sb1.length();
        assertEquals(size1, size2);
        ex.store();
    }

    void check(final Exchange ex, final int size, final StringBuilder sb1, final StringBuilder sb2)
            throws PersistitException {
        sb1.setLength(0);
        sb2.setLength(0);
        for (int counter = 0; counter < size; counter += 10) {
            Util.fill(sb1, counter, 10);
        }
        ex.fetch().getValue().getString(sb2);
        boolean ok = equals(sb1, sb2);
        Debug.$assert1.t(ok);
        assertTrue(equals(sb1, sb2));
    }

    boolean equals(final StringBuilder sb1, final StringBuilder sb2) {
        final int size = sb1.length();
        if (size != sb2.length()) {
            return false;
        }
        for (int i = 0; i < size; i++) {
            if (sb1.charAt(i) != sb2.charAt(i)) {
                return false;
            }
        }
        return true;
    }
}
