/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */
package com.persistit;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

public class ClassIndexTest extends PersistitUnitTestCase {

    private int _maxHandle = 0;
    
    final Map<Integer, ClassInfo> map = new HashMap<Integer, ClassInfo>();
    
    @Override
    public void tearDown() {
        map.clear();
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
