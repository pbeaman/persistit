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

package com.persistit.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Exchange.TraverseVisitor;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.PersistitUnitTestCase;
import com.persistit.Transaction;
import com.persistit.Volume;
import com.persistit.exception.ConversionException;
import com.persistit.exception.PersistitException;

public class ExchangeTest extends PersistitUnitTestCase {

    @Test
    public void testAppend() throws PersistitException {
        final Exchange ex = _persistit.getExchange("persistit", "tree", true);
        final String mockValue = "PADRAIG";

        /* test boolean append */
        ex.clear().append(true);
        ex.getValue().put(mockValue);
        ex.store();
        ex.clear().append(true);
        ex.fetch();
        assertEquals(mockValue, ex.getValue().getString());

        /* test float append */
        final float floatKey = 5.545454f;
        ex.clear().append(floatKey);
        ex.getValue().put(mockValue);
        ex.store();
        ex.clear().append(floatKey);
        ex.fetch();
        assertEquals(mockValue, ex.getValue().getString());

        /* test double append */
        final double doubleKey = 6.66;
        ex.clear().append(doubleKey);
        ex.getValue().put(mockValue);
        ex.store();
        ex.clear().append(doubleKey);
        ex.fetch();
        assertEquals(mockValue, ex.getValue().getString());

        /* test int append */
        final int intKey = 6;
        ex.clear().append(intKey);
        ex.getValue().put(mockValue);
        ex.store();
        ex.clear().append(intKey);
        ex.fetch();
        assertEquals(mockValue, ex.getValue().getString());

        /* test byte append */
        final byte oneByte = 1;
        ex.clear().append(oneByte);
        ex.getValue().put(mockValue);
        ex.store();
        ex.clear().append(oneByte);
        ex.fetch();
        assertEquals(mockValue, ex.getValue().getString());

        /* test short append */
        final short smallShort = 1234;
        ex.clear().append(smallShort);
        ex.getValue().put(mockValue);
        ex.store();
        ex.clear().append(smallShort);
        ex.fetch();
        assertEquals(mockValue, ex.getValue().getString());
    }

    @Test
    public void testStringAppend() throws PersistitException {
        int initialLength = 256;
        final String randomString = createString(initialLength);
        final Exchange ex = _persistit.getExchange("persistit", randomString, true);

        ex.clear().append(randomString);
        ex.getValue().put(randomString);
        ex.store();
        ex.clear().append(randomString);
        ex.fetch();
        assertEquals(randomString, ex.getValue().getString());

        /* lets double key length but keep value the same */
        initialLength *= 2;
        String randomKey = createString(initialLength);
        ex.clear().append(randomKey);
        ex.getValue().put(randomString);
        ex.store();
        ex.clear().append(randomKey);
        ex.fetch();
        assertEquals(randomString, ex.getValue().getString());

        /* now lets keep doubling value length for kicks */
        for (int i = 0; i < 12; i++) {
            initialLength *= 2;
            final String randomValue = createString(initialLength);
            ex.clear().append(randomKey);
            ex.getValue().put(randomValue);
            ex.store();
            ex.clear().append(randomKey);
            ex.fetch();
            assertEquals(randomValue, ex.getValue().getString());
        }

        /* now double the key length */
        initialLength = 256;
        for (int i = 0; i < 2; i++) {
            initialLength *= 2;
            randomKey = createString(initialLength);
            ex.clear().append(randomKey);
            ex.getValue().put(randomString);
            ex.store();
            ex.clear().append(randomKey);
            ex.fetch();
            assertEquals(randomString, ex.getValue().getString());
        }

        /*
         * set key length to value larger than max and make sure exception is
         * thrown
         */
        initialLength = 2048; // 2047 is max key length
        randomKey = createString(initialLength);
        try {
            ex.clear().append(randomKey);
            fail("ConversionException should have been thrown");
        } catch (final ConversionException expected) {
        }
    }

    @Test
    public void testConstructors() throws PersistitException {
        try {
            final Exchange exchange = new Exchange(_persistit, "volume", "tree", true);
            fail("NullPointerException should have been thrown for unknown Volume");
        } catch (final NullPointerException expected) {
        }
        try {
            final Volume nullVol = null;
            final Exchange ex = new Exchange(_persistit, nullVol, "whatever", true);
            fail("NullPointerException should have been thrown for null Volume");
        } catch (final NullPointerException expected) {
        }
    }

    @Test
    public void testTraversal() throws PersistitException {
        final Exchange ex = _persistit.getExchange("persistit", "gogo", true);
        final String mockValue = createString(256);
        /* insert 1000 records */
        for (int i = 0; i < 1000; i++) {
            final String key = createString(32);
            ex.clear().append(key);
            ex.getValue().put(mockValue);
            ex.store();
        }
        /* traverse forwards through those values */
        ex.clear().to(Key.BEFORE);
        while (ex.next()) {
            ex.fetch();
            assertEquals(mockValue, ex.getValue().getString());
        }
        ex.clear().to(Key.BEFORE);
        while (ex.hasNext()) {
            ex.next();
            ex.fetch();
            assertEquals(mockValue, ex.getValue().getString());
        }
        /* now traverse backwards through those values */
        ex.clear().to(Key.AFTER);
        while (ex.previous()) {
            ex.fetch();
            assertEquals(mockValue, ex.getValue().getString());
        }
        ex.clear().to(Key.AFTER);
        while (ex.hasPrevious()) {
            ex.previous();
            ex.fetch();
            assertEquals(mockValue, ex.getValue().getString());
        }
        /* now use the traverse method with various directions */
        ex.clear().to(Key.BEFORE);
        final Key key = ex.getKey();
        final KeyFilter kf = new KeyFilter(key);
        /* this is mostly to test if we can trigger bad things */
        assertEquals(false, ex.traverse(Key.EQ, kf, 4));
        assertEquals(false, ex.traverse(Key.GT, kf, 4));
        assertEquals(false, ex.traverse(Key.GTEQ, kf, 4));
        assertEquals(false, ex.traverse(Key.LT, kf, 4));
        assertEquals(false, ex.traverse(Key.LTEQ, kf, 4));

    }

    @Test
    public void testTraverseVisitor() throws PersistitException {
        final Exchange ex = _persistit.getExchange("persistit", "gogo", true);
        final String mockValue = createString(64);
        /* insert 1000 records */
        for (int i = 0; i < 1000; i++) {
            ex.clear().append(i);
            ex.getValue().put(mockValue);
            ex.store();
        }
        final AtomicInteger visited = new AtomicInteger();
        final AtomicInteger limit = new AtomicInteger(Integer.MAX_VALUE);
        final TraverseVisitor tv = new Exchange.TraverseVisitor() {

            @Override
            public boolean visit(final Exchange ex) {
                visited.incrementAndGet();
                return visited.get() < limit.get();
            }
        };

        visited.set(0);
        ex.clear().to(Key.BEFORE);
        assertFalse(ex.traverse(Key.GT, false, Integer.MAX_VALUE, tv));
        assertEquals(1000, visited.get());

        visited.set(0);
        ex.clear().to(Key.BEFORE);
        assertFalse(ex.traverse(Key.GT, false, Integer.MAX_VALUE, tv));
        assertEquals(1000, visited.get());

        visited.set(0);
        limit.set(1);
        ex.clear().to(Key.BEFORE);
        assertTrue(ex.traverse(Key.GT, false, Integer.MAX_VALUE, tv));
        assertEquals(1, visited.get());

        visited.set(0);
        limit.set(Integer.MAX_VALUE);
        ex.clear().to(Key.AFTER);
        assertFalse(ex.traverse(Key.LT, false, Integer.MAX_VALUE, tv));
        assertEquals(1000, visited.get());

        visited.set(0);
        ex.clear().to(Key.AFTER);
        assertFalse(ex.traverse(Key.LT, false, Integer.MAX_VALUE, tv));
        assertEquals(1000, visited.get());

        visited.set(0);
        limit.set(1);
        ex.clear().to(Key.AFTER);
        assertTrue(ex.traverse(Key.LT, false, Integer.MAX_VALUE, tv));
        assertEquals(1, visited.get());

    }

    @Test
    public void testKeyValues() throws PersistitException {
        final Exchange ex = _persistit.getExchange("persistit", "gogo", true);
        final String firstValue = new String("PADRAIG");
        final String secondValue = new String("SARAH");
        final String thirdValue = new String("TEENY");
        final String fourthValue = new String("NIMBUS");
        ex.clear().append(-2);
        ex.getValue().put(firstValue);
        ex.store();
        ex.clear().append(-1);
        ex.getValue().put(secondValue);
        ex.store();
        ex.clear().append(0);
        ex.getValue().put(thirdValue);
        ex.store();
        ex.clear().append(1);
        ex.getValue().put(fourthValue);
        ex.store();
        ex.clear().to(-2);
        ex.fetch();
        assertEquals(firstValue, ex.getValue().getString());
        ex.clear().to(-1);
        ex.fetch();
        assertEquals(secondValue, ex.getValue().getString());
        ex.clear().to(0);
        ex.fetch();
        assertEquals(thirdValue, ex.getValue().getString());
        ex.clear().to(1);
        ex.fetch();
        assertEquals(fourthValue, ex.getValue().getString());

        ex.clear().append(-2);
        ex.remove();
        ex.getValue().put(-2);
        ex.store();
        ex.clear().append(-1);
        ex.remove();
        ex.getValue().put(-1);
        ex.store();
        ex.clear().append(0);
        ex.remove();
        ex.getValue().put(0);
        ex.store();
        ex.clear().append(1);
        ex.remove();
        ex.getValue().put(1);
        ex.store();

        ex.clear().to(-2);
        ex.fetch();
        assertEquals(-2, ex.getValue().getInt());
        ex.clear().to(-1);
        ex.fetch();
        assertEquals(-1, ex.getValue().getInt());
        ex.clear().to(0);
        ex.fetch();
        assertEquals(0, ex.getValue().getInt());
        ex.clear().to(1);
        ex.fetch();
        assertEquals(1, ex.getValue().getInt());
    }

    @Test
    public void testRemoveAndFetch() throws Exception {
        testRemoveAndFetch(false);
        testRemoveAndFetch(true);
    }

    private void testRemoveAndFetch(final boolean inTransaction) throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "gogo", true);
        final Transaction txn = ex.getTransaction();
        ex.getValue().put(RED_FOX);
        for (int i = 1; i < 10000; i++) {
            if (inTransaction) {
                txn.begin();
            }
            ex.to(i).store();
            if (inTransaction) {
                txn.commit();
                txn.end();
            }
        }
        for (int i = 1; i < 10000; i++) {
            if (inTransaction) {
                txn.begin();
            }
            ex.getValue().clear();
            ex.to(i).fetchAndRemove();
            assertTrue("A value was fetched", ex.getValue().isDefined());
            if (inTransaction) {
                txn.commit();
                txn.end();
            }
        }
    }

    @Test
    public void testWrongThreadAssertion() throws Exception {
        boolean assertsEnabled = false;
        try {
            assert false;
        } catch (final AssertionError e) {
            assertsEnabled = true;
        }
        if (!assertsEnabled) {
            // Does not count as a failure
            System.out.println("Test requires asserts enabled");
            return;
        }
        final AtomicReference<Exchange> ref = new AtomicReference<Exchange>();
        final Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    final Exchange exchange = _persistit.getExchange("persistit", "gogo", true);
                    ref.set(exchange);
                } catch (final Exception e) {
                    // leave null ref
                }
            }
        });
        thread.start();
        thread.join();
        final Exchange exchange = ref.get();
        assertTrue("Failed to get an Exchange in background thread", exchange != null);
        // Verifies that all the parameter-less methods assert.
        int tested = 0;
        for (final Method m : Exchange.class.getMethods()) {
            if (m.getParameterTypes().length == 0 && m.getDeclaringClass().equals(Exchange.class)) {
                if ("toString".equals(m.getName())) {
                    continue;
                }
                try {
                    System.out.println("Testing " + m.getName());
                    m.invoke(exchange);
                    fail("Method " + m + " failed to throw an AssertionError");
                } catch (final InvocationTargetException e) {
                    if (e.getCause() instanceof AssertionError) {
                        tested++;
                        // expected
                    } else {
                        throw e;
                    }
                }
            }
        }
        assertTrue("Not enough methods were tested: " + tested, tested > 10);
    }

    /**
     * Test for https://bugs.launchpad.net/akiban-persistit/+bug/1023549:
     * 
     * traverse(EQ, false, 0) returns incorrect result
     * 
     * This method returns true even when the tree is empty. traverse(EQ, true,
     * 0) returns the correct value.
     * 
     * @throws Exception
     */
    @Test
    public void traverse_EQ_false_0__IsCorrect() throws Exception {
        traverseCases(false);
        traverseCases(true);
    }

    /**
     * Test for https://bugs.launchpad.net/akiban-persistit/+bug/1023549:
     * 
     * traverse(EQ, false, 0) returns incorrect result
     * 
     * This method returns true even when the tree is empty. traverse(EQ, true,
     * 0) returns the correct value.
     * 
     * @throws Exception
     */
    @Test
    public void traverse_EQ_false_0__IsCorrect_Txn() throws Exception {
        final Transaction txn = _persistit.getTransaction();
        txn.begin();
        traverseCases(false);
        txn.commit();
        txn.end();

        txn.begin();
        traverseCases(true);
        txn.commit();
        txn.end();
    }

    private void traverseCases(final boolean deep) throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "gogo", true);

        ex.removeAll();
        ex.clear();
        assertEquals("Should be false", false, ex.traverse(Key.EQ, deep, -1));
        ex.clear();
        assertEquals("Should be false", false, ex.traverse(Key.GTEQ, deep, -1));
        ex.clear();
        assertEquals("Should be false", false, ex.traverse(Key.GT, deep, -1));
        ex.clear();
        assertEquals("Should be false", false, ex.traverse(Key.LTEQ, deep, -1));
        ex.clear();
        assertEquals("Should be false", false, ex.traverse(Key.LT, deep, -1));
        ex.clear();

        ex.append(1);
        assertEquals("Should be false", false, ex.traverse(Key.EQ, deep, -1));
        ex.clear().append(1);
        assertEquals("Should be false", false, ex.traverse(Key.GTEQ, deep, -1));
        ex.clear().append(1);
        assertEquals("Should be false", false, ex.traverse(Key.GT, deep, -1));
        ex.clear().append(1);
        assertEquals("Should be false", false, ex.traverse(Key.LTEQ, deep, -1));
        ex.clear().append(1);
        assertEquals("Should be false", false, ex.traverse(Key.LT, deep, -1));
        ex.clear().append(1);

        ex.append(1).append(2).store();
        ex.clear().append(Key.BEFORE);
        assertEquals("Should be false", false, ex.traverse(Key.EQ, deep, -1));
        assertEquals("Should be true", true, ex.traverse(Key.GTEQ, deep, -1));
        assertEquals("Should be true", true, ex.traverse(Key.GTEQ, deep, -1));
        ex.clear().append(1);
        assertEquals("Should be " + !deep, !deep, ex.traverse(Key.EQ, deep, -1));

        ex.clear().append(Key.AFTER);
        assertEquals("Should be false", false, ex.traverse(Key.EQ, deep, -1));
        ex.clear().append(Key.AFTER);
        assertEquals("Should be true", true, ex.traverse(Key.LTEQ, deep, -1));
        assertEquals("Should be true", true, ex.traverse(Key.LTEQ, deep, -1));

        ex.removeAll();
        ex.clear();
        assertEquals("Should be false", false, ex.traverse(Key.EQ, deep, 0));
        keyCheck(ex, "{{before}}");
        ex.clear();
        assertEquals("Should be false", false, ex.traverse(Key.GTEQ, deep, 0));
        keyCheck(ex, "{{before}}");
        ex.clear();
        assertEquals("Should be false", false, ex.traverse(Key.GT, deep, 0));
        keyCheck(ex, "{{before}}");
        ex.clear();
        assertEquals("Should be false", false, ex.traverse(Key.LTEQ, deep, 0));
        keyCheck(ex, "{{after}}");
        ex.clear();
        assertEquals("Should be false", false, ex.traverse(Key.LT, deep, 0));
        keyCheck(ex, "{{after}}");
        ex.clear();

        ex.append(1).append(2).store();
        ex.clear().append(Key.BEFORE);
        assertEquals("Should be false", false, ex.traverse(Key.EQ, deep, 0));
        keyCheck(ex, "{{before}}");
        assertEquals("Should be true", true, ex.traverse(Key.GTEQ, deep, 0));
        keyCheck(ex, deep ? "{1,2}" : "{1}");
        assertEquals("Should be true", true, ex.traverse(Key.GTEQ, deep, 0));
        keyCheck(ex, deep ? "{1,2}" : "{1}");
        assertEquals("Should be true", true, ex.traverse(Key.EQ, deep, 0));
        keyCheck(ex, deep ? "{1,2}" : "{1}");

        ex.clear().append(Key.AFTER);
        assertEquals("Should be false", false, ex.traverse(Key.EQ, deep, 0));
        keyCheck(ex, "{{before}}");
        ex.clear().append(Key.AFTER);
        assertEquals("Should be true", true, ex.traverse(Key.LTEQ, deep, 0));
        keyCheck(ex, deep ? "{1,2}" : "{1}");
        assertEquals("Should be true", true, ex.traverse(Key.LTEQ, deep, 0));
        keyCheck(ex, deep ? "{1,2}" : "{1}");
        assertEquals("Should be true", true, ex.traverse(Key.EQ, deep, 0));
        keyCheck(ex, deep ? "{1,2}" : "{1}");
    }

    private void keyCheck(final Exchange ex, final String expected) {
        assertEquals("Key should be " + expected, expected, ex.getKey().toString());
    }

}
