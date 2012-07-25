/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
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
        int[] jumps = { 1, 2, 5, 10, 100, 1000, 10000, 50000, 1, 2, 5, 10, 100, 1000, 10000, 50000 };

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

    public long fetchesPerUnitTime(final Exchange ex, final int jump, final long duration, boolean enabled)
            throws Exception {
        Random random = new Random(1);
        int count = 0;
        int key = 0;
        long expires = System.currentTimeMillis() + duration;
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
        ex.append(1).append("Pretty long key prefix").append(2).append("Another pretty long prefix").append(3).append(
                Key.BEFORE);
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
