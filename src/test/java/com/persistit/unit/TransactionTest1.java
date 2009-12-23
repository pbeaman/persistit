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
 * Created on Jul 25, 2004
 */
package com.persistit.unit;

import java.util.Properties;

import com.persistit.Debug;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Transaction;
import com.persistit.Value;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

public class TransactionTest1 extends PersistitUnitTestCase {
    
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
                assertEquals(ex.getValue().get(), "String value #" + i
                    + " for test1");
            }
        }
        System.out.println("- done");
    }

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
                assertEquals(ex.getValue().get(), "String value #" + i
                    + " for test1");
            }
        }
        System.out.println("- done");
    }

    public void test3() throws PersistitException {
        System.out.print("test3 ");
        final Exchange ex = _persistit.getExchange("persistit", "TransactionTest1", true);
        ex.removeAll();

        for (int i = 0; i < 10; i++) {
            ex.getValue().put("String value #" + i + " for test1");
            ex.clear().append("test1").append(i).store();
        }
        boolean rollbackThrown = false;
        final Transaction txn = ex.getTransaction();
        txn.begin();
        try {
            for (int i = 3; i < 10; i += 3) {
                ex.clear().append("test1").append(i).remove(Key.GTEQ);
            }
            txn.rollback();
        } catch (final RollbackException rbe) {
            rollbackThrown = true;
        } finally {
            txn.end();
        }

        assertTrue(rollbackThrown);

        for (int i = -1; i < 12; i++) {
            ex.clear().append("test1").append(i).fetch();
            if ((i < 0) || (i >= 10)) {
                assertTrue(!ex.getValue().isDefined());
            } else {
                assertEquals(ex.getValue().get(), "String value #" + i
                    + " for test1");
            }
        }
        System.out.println("- done");
    }

    public void test4() throws PersistitException {
        System.out.print("test4 ");
        final Exchange ex = _persistit.getExchange("persistit", "TransactionTest1", true);
        ex.removeAll();

        final StringBuffer sb = new StringBuffer();
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

    public void test5() throws PersistitException {
        System.out.print("test5 ");
        final Exchange ex = _persistit.getExchange("persistit", "TransactionTest1", true);
        ex.removeAll();
        final StringBuffer sb = new StringBuffer();
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
            } finally {
                txn.end();
            }
        }

        for (int i = -1; i < 110; i++) {
            ex.clear().append(i).fetch();
            final Value value = ex.getValue();
            if ((i < 0) || (i >= 100) || ((i % 2) == 0)) {
                assertTrue(!value.isDefined());
            } else {
                assertTrue(value.isDefined());
                assertEquals(value.get(), strValue);
            }
        }
        System.out.println("- done");
    }

    public void test6() throws PersistitException {
        System.out.print("test6 ");
        final Exchange ex = _persistit.getExchange("persistit", "TransactionTest1", true);
        ex.removeAll();
        ex.getValue().put("record b");
        ex.clear().append("b").store();
        for (int i = 0; i < 100; i++) {
            ex.getValue().put("Record #" + i);
            ex.clear().append("a").append(i).store();
            ex.clear().append("b").append(i).store();
            ex.clear().append("c").append(i).store();
        }
        final Transaction txn = ex.getTransaction();
        txn.begin();
        try {
            for (int i = 10; i < 20; i++) {
                ex.getValue().put("Record #" + i + "'");
                ex.clear().append("a").append(i).store();
                ex.clear().append("b").append(i).store();
                ex.clear().append("c").append(i).store();
            }
            for (int i = 50; --i >= 0;) {
                ex.clear().append("a").append(i);
                ex.fetch();
                final String s1 = ex.getValue().getString();
                String s2 = "Record #" + i;
                if ((i >= 10) && (i < 20)) {
                    s2 += "'";
                }
                Debug.$assert(s1.equals(s2));
                assertEquals(s1, s2);
            }
            for (int i = 0; i < 10; i++) {
                ex.clear().append("c").incrementValue();
            }
            long c1;
            c1 = ex.getValue().getLong();
            assertEquals(c1, 9);
            ex.getValue().put((long) 20);
            ex.fetchAndStore();
            c1 = ex.getValue().getLong();
            assertEquals(c1, 9);
            ex.incrementValue();
            c1 = ex.getValue().getLong();
            assertEquals(c1, 21);

            ex.clear().append("b").remove(Key.GT);

            assertTrue(ex.fetch().getValue().isDefined());
            final String s1 = ex.getValue().getString();
            assertEquals(s1, "record b");
            for (int i = 0; i < 50; i++) {
                ex.clear().append("b").append(i).fetch();
                assertTrue(!ex.getValue().isDefined());
            }
            ex.clear().append("c").remove(Key.GTEQ);
            ex.incrementValue();
            c1 = ex.getValue().getLong();
            assertEquals(c1, 0);

            txn.commit();
        } finally {
            txn.end();
        }

        for (int i = 50; --i >= 0;) {
            ex.clear().append("a").append(i);
            ex.fetch();
            final String s1 = ex.getValue().getString();
            String s2 = "Record #" + i;
            if ((i >= 10) && (i < 20)) {
                s2 += "'";
            }
            Debug.$assert(s1.equals(s2));
            assertEquals(s1, s2);
        }

        ex.clear().append("b");
        assertTrue(ex.fetch().getValue().isDefined());
        final String s1 = ex.getValue().getString();
        assertEquals(s1, "record b");
        for (int i = 0; i < 50; i++) {
            ex.clear().append("b").append(i).fetch();
            assertTrue(!ex.getValue().isDefined());
        }

        ex.clear().append("c").fetch();
        final long c1 = ex.getValue().getLong();
        assertEquals(c1, 0);

        System.out.println("- done");
    }

    public Properties getProperties() {
        return UnitTestProperties.getBiggerProperties();
    }
    
  
    public static void main(final String[] args) throws Exception {
        new TransactionTest1().initAndRunTest();
    }

    public void runAllTests() throws Exception {
        test1();
        test2();
        test3();
        test4();
        test5();
        test6();
    }
}
