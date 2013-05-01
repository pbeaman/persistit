/**
 * Copyright 2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit.stress.unit;

import java.util.Random;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.PersistitUnitTestCase;
import com.persistit.exception.PersistitException;

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
