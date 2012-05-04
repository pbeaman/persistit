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

package com.persistit;

import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.junit.Test;

import com.persistit.util.InternalHashSet;

/**
 * 
 * @version 1.0
 */
public class InternalHashSetUnitTest {
    private static class TestEntry extends InternalHashSet.Entry {
        int _integer;

        @Override
        public int hashCode() {
            return _integer;
        }
    }

    @Test
    public void test1() {
        final InternalHashSet iset = new InternalHashSet();
        final HashSet hset = new HashSet();

        // fixed seed so that results are repeatable.
        final Random random = new Random(2);

        for (int j = 0; j < 50; j++) {
            final int count = random.nextInt(100000);
            for (int i = 1; i < count; i++) {
                final int r = random.nextInt(10000);
                final Integer o = new Integer(r);
                TestEntry entry = (TestEntry) iset.lookup(r);
                while (entry != null) {
                    if (entry._integer == r) {
                        break;
                    }
                    entry = (TestEntry) entry.getNext();
                }
                if (entry != null) {
                    assertTrue(hset.contains(entry));
                }

                if (entry == null) {
                    entry = new TestEntry();
                    entry._integer = r;
                    iset.put(entry);
                    assertTrue(!hset.contains(entry));
                    hset.add(entry);
                }
            }

            TestEntry entry = null;
            while ((entry = (TestEntry) iset.next(entry)) != null) {
                assertTrue(hset.contains(entry));
            }

            for (final Iterator iter = hset.iterator(); iter.hasNext();) {
                final TestEntry entry1 = (TestEntry) iter.next();
                TestEntry entry2 = (TestEntry) iset.lookup(entry1._integer);
                while (entry2 != null) {
                    if (entry2 == entry1) {
                        break;
                    }
                    entry2 = (TestEntry) entry2.getNext();
                }
                assertTrue(entry2 != null);
            }

            assertTrue(iset.size() == hset.size());
            iset.clear();
            hset.clear();
        }

    }

    public static void main(final String[] args) throws Exception {
        runTest(args);
    }

    public static void runTest(final String[] args) throws Exception {
        final InternalHashSetUnitTest ihst = new InternalHashSetUnitTest();
        ihst.test1();
    }

}
