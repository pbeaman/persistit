/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit.stress.unit;

import java.util.Random;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;

public class TestSamePageOptimization extends PersistitUnitTestCase {
    final int KEYS = 1000000;

    public void runTest() throws Exception {
        final Exchange ex = store();
        final int[] jumps = { 1, 2, 5, 10, 100, 1000, 10000, 50000, 1, 2, 5, 10, 100, 1000, 10000, 50000 };

        System.out.printf("\nFetch time per jump count, with LevelCache enabled\n");
        for (int i = 0; i < jumps.length; i++) {
            System.out.printf("jump=%,8d", jumps[i]);
            System.out.printf("  %,15dns\n", 10000000000L / fetchesPerUnitTime(ex, jumps[i], 10000, true));
        }
        System.out.printf("\nFetch time per jump count, with LevelCache disabled\n");
        for (int i = 0; i < jumps.length; i++) {
            System.out.printf("jump=%,8d", jumps[i]);
            System.out.printf("  %,15dns\n", 10000000000L / fetchesPerUnitTime(ex, jumps[i], 10000, false));
        }

    }

    public long fetchesPerUnitTime(final Exchange ex, final int jump, final long duration, final boolean enabled)
            throws Exception {
        final Random random = new Random(1);
        int count = 0;
        int key = 0;
        final long expires = System.currentTimeMillis() + duration;
        while (System.currentTimeMillis() < expires) {
            for (int j = 0; j < 100; j++) {
                key = (key + random.nextInt(jump) - jump / 2 + KEYS) % KEYS;
                if (!enabled) {
                    ex.initCache();
                }
                ex.to(key).fetch();
                count++;
            }
        }
        return count;
    }

    private Exchange store() throws PersistitException {
        final Exchange ex = _persistit.getExchange("persistit", "TestSamePageOptimization", true);
        ex.append(1).append("Pretty long key prefix").append(2).append("Another pretty long prefix").append(3)
                .append(Key.BEFORE);
        ex.getValue().put(RED_FOX);
        for (int i = 0; i < KEYS; i++) {
            ex.to(i).store();
        }
        _persistit.flush();
        return ex;
    }

    public static void main(final String[] args) throws Exception {
        final TestSamePageOptimization tspo = new TestSamePageOptimization();
        tspo.setUp();
        try {
            tspo.runTest();
        } finally {
            tspo.tearDown();
        }
    }
}
