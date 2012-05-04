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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

public class ClassIndexTest extends PersistitUnitTestCase {

    private int _maxHandle = 0;

    final Map<Integer, ClassInfo> map = new HashMap<Integer, ClassInfo>();

    @Override
    public void tearDown() throws Exception {
        map.clear();
        super.tearDown();
    }

    @Test
    public void testOneClassInfo() throws Exception {
        final ClassIndex cx = _persistit.getClassIndex();
        cx.registerClass(this.getClass());
        ClassInfo ci = cx.lookupByClass(this.getClass());
        assertEquals(this.getClass().getName(), ci.getName());
        assertTrue(ci.getHandle() > 0);
    }

    @Test
    public void test2() throws Exception {
        _maxHandle = 0;
        final ClassIndex cx = _persistit.getClassIndex();
        Class<?> clazz = Persistit.class;
        test2a(cx, clazz);
        for (int handle = 0; handle < _maxHandle + 10; handle++) {
            assertEquals(map.get(handle), cx.lookupByHandle(handle));
        }

        final ClassIndex cy = new ClassIndex(_persistit);
        for (int handle = 0; handle < _maxHandle + 10; handle++) {
            assertEquals(map.get(handle), cy.lookupByHandle(handle));
        }
        for (int handle = 0; handle < _maxHandle + 10; handle++) {
            ClassInfo ci = map.get(handle);
            if (ci != null) {
                assertEquals(ci, cy.lookupByClass(ci.getDescribedClass()));
            }
        }

        final ClassIndex cz = new ClassIndex(_persistit);
        for (int handle = 0; handle < _maxHandle + 10; handle++) {
            ClassInfo ci = map.get(handle);
            if (ci != null) {
                assertEquals(ci, cz.lookupByClass(ci.getDescribedClass()));
            }
        }
        System.out.println(cx.size() + " classes");
    }

    private void test2a(final ClassIndex cx, final Class<?> clazz) throws Exception {
        if (clazz.isPrimitive()) {
            return;
        }
        ClassInfo ci = cx.lookupByClass(clazz);
        ClassInfo copy = map.get(ci.getHandle());
        if (ci.getHandle() > _maxHandle) {
            assertNull(copy);
            map.put(ci.getHandle(), ci);
            _maxHandle = ci.getHandle();
            Field[] fields = clazz.getDeclaredFields();
            for (final Field field : fields) {
                if (cx.size() < 1000) {
                    test2a(cx, field.getType());
                }
            }
        } else {
            assert (copy.equals(ci));
        }

    }

    @Override
    public void runAllTests() throws Exception {
        // TODO Auto-generated method stub

    }

}
