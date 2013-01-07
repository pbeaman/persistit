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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Exchange.TraverseVisitor;
import com.persistit.Key;
import com.persistit.PersistitUnitTestCase;
import com.persistit.ReadOnlyExchange;
import com.persistit.exception.PersistitException;

public class TraverseVisitorTest extends PersistitUnitTestCase {

    private final AtomicInteger visited = new AtomicInteger();
    private final AtomicInteger limit = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicBoolean reverse = new AtomicBoolean();

    @Test
    public void simpleTraverseVisitor() throws PersistitException {
        final TraverseVisitor tv = new Exchange.TraverseVisitor() {

            @Override
            public boolean visit(final ReadOnlyExchange ex) {
                visited.incrementAndGet();
                return visited.get() < limit.get();
            }
        };
        doCombo(tv);

    }

    @Test
    public void traverseVisitorNonMutatingTraverseMethods() throws Exception {

        final TraverseVisitor tv = new Exchange.TraverseVisitor() {

            @Override
            public boolean visit(final ReadOnlyExchange ex) throws PersistitException {
                visited.incrementAndGet();
                final int v = visited.get();
                final boolean leftEdge = reverse.get() ? v == 1000 : v == 1;
                final boolean rightEdge = reverse.get() ? v == 1 : v == 1000;
                final Exchange mutableExchange = new Exchange((Exchange) ex);
                assertEquals(!rightEdge, mutableExchange.hasNext());
                assertEquals(!leftEdge, mutableExchange.hasPrevious());
                assertFalse(mutableExchange.hasChildren());
                return v < limit.get();
            }
        };

        doCombo(tv);
    }

    /**
     * Demonstrates that a TraverseVisitor can (carefully) modify the Key of a
     * supplied ReadOnlyExchange.
     * 
     * @throws Exception
     */
    @Test
    public void testMutateKey() throws Exception {
        final TraverseVisitor tv = new Exchange.TraverseVisitor() {

            @Override
            public boolean visit(final ReadOnlyExchange ex) throws PersistitException {
                visited.incrementAndGet();
                final int v = visited.get();
                if (reverse.get()) {
                    final int k = ex.getKey().reset().decodeInt();
                    ex.getKey().clear().append(k - 1).append(0);
                } else {
                    ex.getKey().append("a");
                }
                return v < limit.get();
            }
        };

        doCombo(tv);

    }

    private void doCombo(final TraverseVisitor tv) throws PersistitException {
        final Exchange ex = _persistit.getExchange("persistit", "gogo", true);
        final String mockValue = createString(64);
        for (int i = 0; i < 1000; i++) {
            ex.clear().append(i);
            ex.getValue().put(mockValue);
            ex.store();
        }

        doTraverse(ex, tv, false, Key.GT, Integer.MAX_VALUE);
        doTraverse(ex, tv, false, Key.GT, Integer.MAX_VALUE);
        doTraverse(ex, tv, false, Key.LT, Integer.MAX_VALUE);
        doTraverse(ex, tv, false, Key.LT, Integer.MAX_VALUE);
        doTraverse(ex, tv, false, Key.GT, 1);
        doTraverse(ex, tv, false, Key.GT, 1);
        doTraverse(ex, tv, false, Key.GT, 10);
        doTraverse(ex, tv, false, Key.GT, 10);
        doTraverse(ex, tv, false, Key.LT, 1);
        doTraverse(ex, tv, false, Key.LT, 1);
        doTraverse(ex, tv, false, Key.LT, 10);
        doTraverse(ex, tv, false, Key.LT, 10);

        doTraverse(ex, tv, true, Key.GT, Integer.MAX_VALUE);
        doTraverse(ex, tv, true, Key.GT, Integer.MAX_VALUE);
        doTraverse(ex, tv, true, Key.LT, Integer.MAX_VALUE);
        doTraverse(ex, tv, true, Key.LT, Integer.MAX_VALUE);
        doTraverse(ex, tv, true, Key.GT, 1);
        doTraverse(ex, tv, true, Key.GT, 1);
        doTraverse(ex, tv, true, Key.GT, 10);
        doTraverse(ex, tv, true, Key.GT, 10);
        doTraverse(ex, tv, true, Key.LT, 1);
        doTraverse(ex, tv, true, Key.LT, 1);
        doTraverse(ex, tv, true, Key.LT, 10);
        doTraverse(ex, tv, true, Key.LT, 10);

    }

    private void doTraverse(final Exchange ex, final TraverseVisitor tv, final boolean deep,
            final Key.Direction direction, final int max) throws PersistitException {
        reverse.set(direction == Key.LT || direction == Key.LTEQ);
        visited.set(0);
        limit.set(max);
        ex.clear().to(reverse.get() ? Key.AFTER : Key.BEFORE);
        assertEquals(max < 1000, ex.traverse(direction, deep, Integer.MAX_VALUE, tv));
        assertEquals(Math.min(max, 1000), visited.get());
    }

}
