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

package com.persistit.stress;

import java.util.Random;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;

public class TestSamePageOptimization extends PersistitUnitTestCase {
    final int KEYS = 1000000;

    public void runTest() throws Exception {
        final Exchange ex = store();
        int[] jumps = {1, 2, 5, 10, 100, 1000, 10000, 50000, 1, 2, 5, 10, 100, 1000, 10000, 50000};
        
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
    
    public long fetchesPerUnitTime(final Exchange ex, final int jump, final long duration, boolean enabled) throws Exception {
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
