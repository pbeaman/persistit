/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.Transaction;
import com.persistit.Value;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

public class TransactionTest1 extends PersistitUnitTestCase {

    @Test
    public void test1() throws PersistitException {
        System.out.print("test1 ");

        final Exchange ex = _persistit.getExchange("persistit", "TransactionTest1", true);
        ex.removeAll();

        final Transaction txn = ex.getTransaction();

        txn.begin();
        try {
            for (int i = 0; i < 10; i++) {
                ex.getValue().put("String value #" + i + " for test1");
                ex.clear().append("test1").append(i).store();
            }
            for (int i = 3; i < 10; i += 3) {
                ex.clear().append("test1").append(i).remove(Key.GTEQ);
            }
            txn.commit();
        } finally {
            txn.end();
        }

        for (int i = -1; i < 12; i++) {
            ex.clear().append("test1").append(i).fetch();
            if ((i < 0) || (i >= 10) || ((i != 0) && ((i % 3) == 0))) {
                assertTrue(!ex.getValue().isDefined());
            } else {
                assertTrue(ex.getValue().isDefined());
                assertEquals(ex.getValue().get(), "String value #" + i + " for test1");
            }
        }

        txn.begin();
        try {
            ex.removeAll();
            txn.commit();
        } finally {
            txn.end();
        }
        for (int i = -1; i < 12; i++) {
            ex.clear().append("test1").append(i).fetch();
            assertTrue(!ex.getValue().isDefined());
        }

        txn.begin();
        try {
            ex.removeTree();
            txn.commit();
        } finally {
            txn.end();
        }
        assertTrue(!ex.getTree().isValid());
        try {
            ex.clear().append("test1").hasChildren();
            fail("Should have thrown an exception");
        } catch (final Exception e) {
            // ok
        }

        System.out.println("- done");
    }

    @Test
    public void test2() throws PersistitException {
        System.out.print("test2 ");
        final Exchange ex = _persistit.getExchange("persistit", "TransactionTest1", true);
        ex.removeAll();

        for (int i = 0; i < 10; i++) {
            ex.getValue().put("String value #" + i + " for test1");
            ex.clear().append("test1").append(i).store();
        }

        final Transaction txn = ex.getTransaction();
        txn.begin();
        try {
            for (int i = 3; i < 10; i += 3) {
                ex.clear().append("test1").append(i).remove(Key.GTEQ);
            }
            txn.commit();
        } finally {
            txn.end();
        }

        for (int i = -1; i < 12; i++) {
            ex.clear().append("test1").append(i).fetch();
            if ((i < 0) || (i >= 10) || ((i != 0) && ((i % 3) == 0))) {
                assertTrue(!ex.getValue().isDefined());
            } else {
                assertEquals(ex.getValue().get(), "String value #" + i + " for test1");
            }
        }
        System.out.println("- done");
    }

    @Test
    public void test3() throws PersistitException {
        System.out.print("test3 ");
        final Exchange ex = _persistit.getExchange("persistit", "TransactionTest1", true);
        ex.removeAll();

        for (int i = 0; i < 10; i++) {
            ex.getValue().put("String value #" + i + " for test1");
            ex.clear().append("test1").append(i).store();
        }

        final Transaction txn = ex.getTransaction();
        txn.begin();
        try {
            for (int i = 3; i < 10; i += 3) {
                ex.clear().append("test1").append(i).remove(Key.GTEQ);
            }
            txn.rollback();
        } finally {
            txn.end();
        }

        for (int i = -1; i < 12; i++) {
            ex.clear().append("test1").append(i).fetch();
            if ((i < 0) || (i >= 10)) {
                assertTrue(!ex.getValue().isDefined());
            } else {
                assertEquals(ex.getValue().get(), "String value #" + i + " for test1");
            }
        }
        System.out.println("- done");
    }

    @Test
    public void test4() throws PersistitException {
        System.out.print("test4 ");
        final Exchange ex = _persistit.getExchange("persistit", "TransactionTest1", true);
        ex.removeAll();

        final StringBuilder sb = new StringBuilder();
        for (int i = 10000; i < 15000; i++) {
            sb.append(" ");
            sb.append(Integer.toString(i).substring(1));
        }
        ex.getValue().putString(sb);
        ex.clear().append("without").store();

        final Transaction txn = ex.getTransaction();

        txn.begin();
        try {
            ex.clear().append("with").store();
            txn.commit();
        } finally {
            txn.end();
        }

        ex.clear().append("with").fetch();
        final String with = ex.getValue().getString();

        ex.clear().append("without").fetch();
        final String without = ex.getValue().getString();

        assertEquals(with, without);

        ex.clear().append(Key.BEFORE);
        while (ex.next()) {
            final String keyValue = ex.getKey().reset().decodeString();
            final String valValue = ex.getValue().getString();
        }
        System.out.println("- done");
    }

    @Test
    public void test5() throws PersistitException {
        System.out.print("test5 ");
        final Exchange ex = _persistit.getExchange("persistit", "TransactionTest1", true);
        ex.removeAll();
        final StringBuilder sb = new StringBuilder();
        for (int i = 10000; i < 15000; i++) {
            sb.append(" ");
            sb.append(Integer.toString(i).substring(1));
        }
        final String strValue = sb.toString();

        ex.getValue().putString(strValue);

        for (int i = 0; i < 100; i++) {
            final Transaction txn = ex.getTransaction();
            txn.begin();
            try {
                ex.clear().append(i).store();
                if ((i % 2) == 0) {
                    txn.rollback();
                }
                txn.commit();
            } catch (final RollbackException re) {
                assertEquals(0, i % 2);
            } finally {
                txn.end();
            }
        }

        for (int i = -1; i < 110; i++) {
            final Transaction txn = ex.getTransaction();
            txn.begin();
            try {
                ex.clear().append(i).fetch();
                final Value value = ex.getValue();
                if ((i < 0) || (i >= 100) || ((i % 2) == 0)) {
                    assertTrue(!value.isDefined());
                } else {
                    assertTrue(value.isDefined());
                    assertEquals(value.get(), strValue);
                }
                txn.commit();
            } finally {
                txn.end();
            }
        }
        System.out.println("- done");
    }

    @Test
    public void test7() throws PersistitException {
        final Exchange ex = _persistit.getExchange("persistit", "TransactionTest1", true);
        ex.removeAll();
        ex.clear();

        for (int i = 10; i < 20; i += 3) {
            ex.getValue().put("Record #" + i + "'");
            ex.to(i).store();
        }

        final Set<Integer> remainingKeys = new HashSet<Integer>();
        final KeyFilter kf = new KeyFilter("{[12:17]}");

        final Transaction txn = ex.getTransaction();
        txn.begin();
        try {
            for (int i = 10; i < 20; i++) {
                if ((i - 10) % 3 != 0) {
                    ex.getValue().put("Record #" + i + "'");
                    ex.to(i).store();
                }
            }
            for (int i = 5; i < 25; i += 2) {
                ex.to(i).remove();
            }
            // Make sure everything is correct before the commit
            traverseTest7(remainingKeys, ex, kf);

            txn.commit();
        } finally {
            txn.end();
        }
        // Make sure everything is correct after the commit
        traverseTest7(remainingKeys, ex, kf);
    }

    private void traverseTest7(final Set<Integer> remainingKeys, final Exchange ex, final KeyFilter kf)
            throws PersistitException {
        remainingKeys.clear();
        ex.clear().append(Key.BEFORE);
        while (ex.traverse(Key.GT, true, Integer.MAX_VALUE)) {
            checkTest7(remainingKeys, ex);
        }
        assertEquals(5, remainingKeys.size()); // 10, 12, 14, 16, 18

        remainingKeys.clear();
        ex.clear().append(Key.AFTER);
        while (ex.traverse(Key.LT, true, Integer.MAX_VALUE)) {
            checkTest7(remainingKeys, ex);
        }
        assertEquals(5, remainingKeys.size()); // 10, 12, 14, 16, 18

        remainingKeys.clear();
        ex.clear().append(Key.BEFORE);
        while (ex.traverse(Key.GT, kf, Integer.MAX_VALUE)) {
            checkTest7(remainingKeys, ex);
        }
        assertEquals(3, remainingKeys.size()); // 12, 14, 16

        remainingKeys.clear();
        ex.clear().append(Key.AFTER);
        while (ex.traverse(Key.LT, kf, Integer.MAX_VALUE)) {
            checkTest7(remainingKeys, ex);
        }
        assertEquals(3, remainingKeys.size()); // 12, 14, 16

        remainingKeys.clear();
        for (int i = 5; i < 25; i++) {
            ex.to(i);
            if (ex.traverse(Key.GTEQ, true, Integer.MAX_VALUE)) {
                checkTest7(remainingKeys, ex);
            }
        }
        assertEquals(5, remainingKeys.size()); // 10, 12, 14, 16, 18

        remainingKeys.clear();
        for (int i = 5; i < 25; i++) {
            ex.to(i);
            if (ex.traverse(Key.LTEQ, true, Integer.MAX_VALUE)) {
                checkTest7(remainingKeys, ex);
            }
        }
        assertEquals(5, remainingKeys.size()); // 10, 12, 14, 16, 18

    }

    private void checkTest7(final Set<Integer> remainingKeys, final Exchange ex) throws PersistitException {
        final int k = ex.getKey().reset().decodeInt();
        remainingKeys.add(k);
        assertTrue(ex.traverse(Key.GTEQ, true, Integer.MAX_VALUE));
        final int m = ex.getKey().reset().decodeInt();
        assertTrue(ex.traverse(Key.LTEQ, true, Integer.MAX_VALUE));
        final int n = ex.getKey().reset().decodeInt();
        assertEquals(m, k);
        assertEquals(n, k);
    }

    @Override
    public Properties getProperties(final boolean cleanup) {
        return UnitTestProperties.getBiggerProperties(cleanup);
    }

    public static void main(final String[] args) throws Exception {
        new TransactionTest1().initAndRunTest();
    }

    @Override
    public void runAllTests() throws Exception {
        test1();
        test2();
        test3();
        test4();
        test5();
    }
}
