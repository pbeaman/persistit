/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Mar 11, 2005
 */
package com.persistit.unit;

import com.persistit.Debug;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Util;
import com.persistit.exception.PersistitException;

/**
 * @author Peter Beaman
 * @version 1.0
 */
public class LongRecordTest1 extends PersistitUnitTestCase {

    public static void main(final String[] args) throws Exception {
        new LongRecordTest1().initAndRunTest();
    }

    public void runAllTests() throws Exception {
        test1();
        test2();
        test3();

    }

    public void test1() throws PersistitException {
        System.out.print("test1");
        final Exchange ex = _persistit.getExchange("persistit",
                "LongRecordTest1", true);
        ex.removeAll();
        ex.getValue().setMaximumSize(8 * 1024 * 1024);

        final StringBuffer sb = new StringBuffer(8000000);
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
        final Exchange ex = _persistit.getExchange("persistit",
                "LongRecordTest1", true);
        ex.removeAll();
        final StringBuffer sb1 = new StringBuffer(1000000);
        final StringBuffer sb2 = new StringBuffer(1000000);
        ex.getKey().append("foo");
        System.out.println();
        final int size = 100000;
        for (int count = 0; count < 10; count++) {
            store(ex, size, sb1);
            check(ex, size, sb1, sb2);
            System.out.print(".");
        }
        System.out.println();
        for (int count = 0; count < 10; count++) {
            store(ex, size, sb1);
            check(ex, size, sb1, sb2);
            System.out.print("-");
        }
    }

    public void test3() throws PersistitException {
        System.out.print("test3");
        final Exchange ex = _persistit.getExchange("persistit",
                "LongRecordTest1", true);
        ex.removeAll();
        final StringBuffer sb1 = new StringBuffer(1000000);
        final StringBuffer sb2 = new StringBuffer(1000000);
        ex.getKey().append("foo");
        System.out.println();
        for (int size = 0; size < 500000; size += 10000) {
            store(ex, size, sb1);
            check(ex, size, sb1, sb2);
            System.out.print(".");
        }
        System.out.println();
        for (int size = 500000; (size -= 10000) >= 0;) {
            store(ex, size, sb1);
            check(ex, size, sb1, sb2);
            System.out.print("-");
        }
    }

    void store(final Exchange ex, final int size, final StringBuffer sb1)
            throws PersistitException {
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

    void check(final Exchange ex, final int size, final StringBuffer sb1,
            final StringBuffer sb2) throws PersistitException {
        sb1.setLength(0);
        for (int counter = 0; counter < size; counter += 10) {
            Util.fill(sb1, counter, 10);
        }
        ex.fetch().getValue().getString(sb2);
        boolean ok = equals(sb1, sb2);
        Debug.debug0(!ok);
        assertTrue(equals(sb1, sb2));
    }

    boolean equals(final StringBuffer sb1, final StringBuffer sb2) {
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
