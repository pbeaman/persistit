/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
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
