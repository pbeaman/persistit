/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import org.junit.Test;

import com.persistit.Transaction.CommitPolicy;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.unit.UnitTestProperties;

public class ClassIndexTest extends PersistitUnitTestCase {

    private int _maxHandle = 0;

    final Map<Integer, ClassInfo> map = new HashMap<Integer, ClassInfo>();

    @SuppressWarnings("serial")
    public static class A implements Serializable {

    }

    @SuppressWarnings("serial")
    public static class B implements Serializable {

    }
    
    public static class C {
        
    }

    @Override
    public void tearDown() throws Exception {
        map.clear();
        super.tearDown();
    }

    @Test
    public void oneClassInfo() throws Exception {
        final ClassIndex cx = _persistit.getClassIndex();
        cx.registerClass(this.getClass());
        ClassInfo ci = cx.lookupByClass(this.getClass());
        assertEquals(this.getClass().getName(), ci.getName());
        assertTrue(ci.getHandle() > 0);
    }

    @Test
    public void manyClassInfo() throws Exception {
        _maxHandle = 0;
        final ClassIndex cx = _persistit.getClassIndex();
        Class<?> clazz = Persistit.class;
        test2a(cx, clazz);

        assertEquals("Cache misses should match map size", map.size(), cx.getCacheMisses()
                - cx.getDiscardedDuplicates());

        for (int handle = 0; handle < _maxHandle + 10; handle++) {
            assertTrue(equals(map.get(handle), cx.lookupByHandle(handle)));
        }

        final ClassIndex cy = new ClassIndex(_persistit);
        for (int handle = 0; handle < _maxHandle + 10; handle++) {
            assertTrue(equals(map.get(handle), cy.lookupByHandle(handle)));
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

    @Test
    public void multiThreaded() throws Exception {
        final int threadCount = 50;

        final Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    final Transaction transaction = _persistit.getTransaction();
                    for (int k = 0; k < 10; k++) {
                        try {
                            transaction.begin();
                            try {
                                test2a(_persistit.getClassIndex(), Persistit.class);
                                if ((index % 3) == 0) {
                                    transaction.rollback();
                                } else {
                                    transaction.commit();
                                }
                            } finally {
                                transaction.end();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }

        final ClassIndex cx = _persistit.getClassIndex();

        /*
         * Verify that almost all lookups were satisfied from cache. There
         * should be one "cache miss" for every class in the map. There may be
         * more: these are caused by concurrent execution and are explicitly
         * permitted - they will happen only rarely and only when multiple
         * threads are contending to register a new class. Such instances are
         * safely discarded and the following adjusts the cache miss count by
         * the number so discarded.
         */
        assertEquals("Cache misses should match map size", map.size(), cx.getCacheMisses()
                - cx.getDiscardedDuplicates());

        for (int handle = 0; handle < _maxHandle + 10; handle++) {
            assertTrue(equals(map.get(handle), cx.lookupByHandle(handle)));
        }

    }

    @Test
    public void rollback() throws Exception {
        Exchange ex = _persistit.getExchange("persistit", "ClassIndexTest", true);
        Transaction txn = ex.getTransaction();
        txn.begin();
        try {

            ex.getValue().put(new A());
            ex.to("A").store();
            ex.getValue().put(new B());
            ex.to("B").store();
            txn.rollback();
        } finally {
            txn.end();
        }

        txn.begin();
        try {
            ex.getValue().put(new B());
            ex.to("B").store();
            ex.getValue().put(new A());
            ex.to("A").store();
            txn.commit(CommitPolicy.HARD);
        } finally {
            txn.end();
        }

        final Configuration config = _persistit.getConfiguration();
        _persistit.crash();
        _persistit = new Persistit();
        _persistit.initialize(config);
        ex = _persistit.getExchange("persistit", "ClassIndexTest", false);
        Object b = ex.to("B").fetch().getValue().get();
        Object a = ex.to("A").fetch().getValue().get();
        assertTrue("Incorrect class", a instanceof A);
        assertTrue("Incorrect class", b instanceof B);
    }

    @Test
    public void knownNull() throws Exception {
        ClassIndex cx = _persistit.getClassIndex();
        final ClassInfo ci = cx.lookupByHandle(12345);
        assertEquals("Should return cached known null", ci, cx.lookupByHandle(12345));
    }

    @Test
    public void multipleClassVersions() throws Exception {
        ClassIndex cx = _persistit.getClassIndex();
        cx.clearAllEntries();
        int count = 10;
        final Class<?>[] classes = new Class<?>[count];
        for (int i = 0; i < count; i++) {
            classes[i] = makeAClass("SomeClass", i);
            assertNotNull("this test requires tools.jar on the classpath", classes[i]);
            cx.lookupByClass(classes[i]);
        }
        assertEquals("Each version should be unique", count, cx.getCacheMisses());
        Set<Integer> handles = new HashSet<Integer>();
        for (int i = 0; i < count; i++) {
            ClassInfo ci = cx.lookupByClass(classes[i]);
            int handle = ci.getHandle();
            assertTrue("Handles must be unique", handles.add(handle));
        }
        assertEquals("Each version should be in cache", count, cx.getCacheMisses());
        for (final Integer handle : handles) {
            ClassInfo ci = cx.lookupByHandle(handle);
            assertEquals("Lookup by handle and class must match", ci, cx.lookupByClass(ci.getDescribedClass()));
        }
    }

    private boolean equals(final ClassInfo a, final ClassInfo b) {
        if (a == b) {
            return true;
        }
        if (a == null) {
            return b.getDescribedClass() == null;
        }
        return a.equals(b);
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

    private static Class<?> makeAClass(final String name, final int index) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("public class " + name + " implements java.io.Serializable {\n");
        for (int i = 0; i < index; i++) {
            sb.append("    int v" + i + ";\n");
        }
        sb.append("}\n");
        if (!compile(name + ".java", sb)) {
            return null;
        }
        ClassLoader loader = new URLClassLoader(new URL[] { new File(UnitTestProperties.DATA_PATH).toURI().toURL() },
                ClassIndexTest.class.getClassLoader());
        Class<?> clazz = loader.loadClass(name);
        return clazz;
    }

    private static boolean compile(final String name, final CharSequence source) throws Exception {
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            return false;
        }
        final SourceString sourceObject = new SourceString(name, source);
        CompilationTask task = compiler.getTask(null, null, null, Arrays.asList("-d", UnitTestProperties.DATA_PATH),
                null, Arrays.asList(sourceObject));
        return task.call().booleanValue();
    }

    static class SourceString extends SimpleJavaFileObject {
        final CharSequence _source;

        SourceString(final String name, final CharSequence source) throws Exception {
            super(new URI(name), Kind.SOURCE);
            _source = source;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreCodingErrors) {
            return _source;
        }
    }

}
