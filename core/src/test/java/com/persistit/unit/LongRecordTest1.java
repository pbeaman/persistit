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

package com.persistit.unit;

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

    public void test2() throws PersistitException {
        System.out.print("test2");
        final Exchange ex = _persistit.getExchange("persistit", "LongRecordTest1", true);
        ex.removeAll();
        final StringBuilder sb1 = new StringBuilder(1000000);
        final StringBuilder sb2 = new StringBuilder(1000000);
        ex.getKey().append("foo");
        System.out.println();
        final int size = 100000;
        for (int count = 0; count < 10; count++) {
            store(ex, size, sb1);
            check(ex, size, sb1, sb2);
        }
        System.out.println();
        for (int count = 0; count < 10; count++) {
            store(ex, size, sb1);
            check(ex, size, sb1, sb2);
        }
        System.out.println(" - done");
    }

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
            System.out.println();
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
