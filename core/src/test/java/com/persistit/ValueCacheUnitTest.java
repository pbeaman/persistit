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

import junit.framework.Assert;
import junit.framework.TestCase;

import com.persistit.Value.ValueCache;

/**
 * 
 * @version 1.0
 */
public class ValueCacheUnitTest extends TestCase {
    public void test1() {
        System.out.print("test1 ");
        final ValueCache vc = new ValueCache();
        for (int handle = 0; handle < 100; handle++) {
            final Object o = new Integer(handle);
            assertEquals(-1, vc.lookup(o));
            assertEquals(-1, vc.put(handle, o));
            assertEquals(handle, vc.put(handle, o));
        }
        for (int handle = 0; handle < 100; handle++) {
            final Object o = new Integer(handle);
            assertEquals(-1, vc.lookup(o));
        }
        for (int handle = 0; handle < 100; handle++) {
            final Object o = vc.get(handle);
            assertEquals(handle, vc.lookup(o));
        }

        System.out.println("- done");
    }

    public void test2() {
        System.out.print("test2 ");
        final ValueCache vc = new ValueCache();
        for (int handle = 0; handle < 5000; handle++) {
            final Object o = new Integer(handle);
            assertEquals(-1, vc.lookup(o));
            assertEquals(-1, vc.put(handle, o));
            assertEquals(handle, vc.put(handle, o));
        }
        for (int handle = 0; handle < 5000; handle++) {
            final Object o = new Integer(handle);
            assertEquals(-1, vc.lookup(o));
        }
        for (int handle = 0; handle < 5000; handle++) {
            final Object o = vc.get(handle);
            assertEquals(handle, vc.lookup(o));
        }
        System.out.println("- done");
    }

    public void test3() {
        System.out.print("test3 ");
        final long ts = System.currentTimeMillis();
        final ValueCache vc = new ValueCache();

        for (int cycle = 0; cycle < 10; cycle++) {
            vc.clear();
            for (int handle = 0; handle < 1000; handle++) {
                final Object o = new Integer(handle);
                assertEquals(-1, vc.lookup(o));
                assertEquals(-1, vc.put(handle, o));
                assertEquals(handle, vc.put(handle, o));
            }
            for (int handle = 0; handle < 1000; handle++) {
                final Object o = new Integer(handle);
                assertEquals(-1, vc.lookup(o));
            }
            for (int handle = 0; handle < 1000; handle++) {
                final Object o = vc.get(handle);
                assertEquals(handle, vc.lookup(o));
            }
        }
        System.out.print(" - " + (System.currentTimeMillis() - ts) + " ");
        System.out.println("- done");
    }

    public static void assertEquals(final int a, final int b) {
        if (a != b) {
            Assert.assertEquals(a, b);
        }
    }

    public static void main(final String[] args) throws Exception {

        runTest(args);
    }

    public static void runTest(final String[] args) throws Exception {
        final ValueCacheUnitTest vcst = new ValueCacheUnitTest();
        vcst.test1();
        vcst.test2();
        vcst.test3();
    }

}
