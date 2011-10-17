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

package com.persistit.unit;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.ObjectStreamField;
import java.io.Serializable;
import java.util.ArrayList;

import com.persistit.DefaultValueCoder;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Value;
import com.persistit.encoding.CoderContext;
import com.persistit.encoding.CoderManager;
import com.persistit.encoding.SerialValueCoder;
import com.persistit.encoding.ValueCoder;
import com.persistit.exception.PersistitException;

public class ValueTest3 extends PersistitUnitTestCase {

    /**
     * Tests JSA 1.1 default serialization. Requires the
     * enableCompatibleConstructors to be true.
     */
    Exchange _exchange;

    public static class CustomSet extends ArrayList {
        private final static long serialVersionUID = 1L;
    }

    private static class T implements Serializable {
        private final static long serialVersionUID = 1L;

        private String _a;
        private String _b;

        private T(final String a, final String b) {
            _a = a;
            _b = b;
        }

        private T(final String a, final String b, final boolean f) {
            _a = a;
            _b = b;
            assertTrue("T 3-arg constructor should not be called", false);
        }

        private T(final int x, final boolean y, final String z) {
            assertTrue("T 3-arg constructor should not be called", false);
        }

        public String toString() {
            return "T:" + _a + _b;
        }
    }

    private static class TT extends T {
        private final static long serialVersionUID = 1L;

        private TT(final String a, final String b) {
            super(a, b);
        }

        private TT(final String a, final String b, final boolean f) {
            super(a, b, f);
        }

        private TT(final int x, final boolean y, final String z) {
            super(x, y, z);
        }

        public String toString() {
            return "T" + super.toString();
        }
    }

    private static class TTT extends TT {
        private final static long serialVersionUID = 1L;

        private TTT(final String a, final String b) {
            super(a, b);
            assertTrue(a != null);
        }

        private TTT(final String a, final String b, final boolean f) {
            super(a, b, f);
            assertTrue(a != null);
        }

        private TTT(final int x, final boolean y, final String z) {
            super(x, y, z);
        }

        public String toString() {
            return "T" + super.toString();
        }

    }

    private static class TTTValueCoder extends DefaultValueCoder {
        static int _getCounter;

        TTTValueCoder(final Persistit persistit) {
            super(persistit, TTT.class);
        }

        public Object get(final Value value, final Class clazz, final CoderContext context) {
            _getCounter++;
            final TTT ttt = new TTT("x", "y");
            value.registerEncodedObject(ttt);
            render(value, ttt, clazz, context);
            return ttt;
        }
    }

    private static class S implements Serializable {
        String _a;
        String _b;

        private final static long serialVersionUID = 1L;

        public void readObject(final ObjectInputStream ois) throws IOException, ClassNotFoundException {
            ois.defaultReadObject();
        }

        public void writeObject(final ObjectOutputStream oos) throws IOException {
            oos.defaultWriteObject();
        }

        public String toString() {
            return "S:" + _a + _b;
        }
    }

    private static class SS extends S {
        private final static long serialVersionUID = 1L;

        private final String _c;
        private final String _d;
        protected String _ff; // Can only be marked final on JDK 1.5 and above

        private SS(final String c, final String d) {
            _c = c;
            _d = d;
            _ff = "Final field";
        }

        public String toString() {
            return "S" + super.toString() + _c + _d;
        }
    }

    private static class SSS extends SS {
        private final static long serialVersionUID = 1L;

        R _e;
        private final W _f;

        private SSS(final String c, final String d, final boolean e, final int f) {
            super(c, d);
            _e = new R(e);
            _f = new W(f);
        }

        public String toString() {
            return "S" + super.toString() + _e + _f;

        }
    }

    private static class SSSS extends SSS {
        private final static long serialVersionUID = 1L;

        private String _g;
        private String _h;

        private SSSS() {
            super("SSSS-c", "SSSS-d", true, 42);
            _g = "Field g";
            _h = "Field h";
        }

        private static final ObjectStreamField[] serialPersistentFields = { new ObjectStreamField("_g", String.class), };

        public String toString() {
            return "S" + super.toString() + _g + _h + _ff;
        }
    }

    private static class R implements Serializable {
        private final static long serialVersionUID = 1L;

        final static R R_TRUE = new R(true);
        final static R R_FALSE = new R(false);

        boolean _value;

        R(final boolean b) {
            _value = b;
        }

        public Object readResolve() throws ObjectStreamException {
            return _value ? R_TRUE : R_FALSE;
        }

        public String toString() {
            if (this == R_TRUE) {
                return "true";
            } else if (this == R_FALSE) {
                return "false";
            } else {
                return "notReplaced";
            }
        }
    }

    private static class W implements Serializable {
        private final static long serialVersionUID = 1L;

        int _value;

        W(final int v) {
            _value = v;
        }

        private Object writeReplace() throws ObjectStreamException {
            return new WReplacement(_value);
        }

        public String toString() {
            return "W:" + _value;
        }
    }

    private static class WReplacement implements Serializable {
        private final static long serialVersionUID = 1L;

        int _value;

        WReplacement(final int v) {
            _value = v;
        }

        private Object readResolve() throws ObjectStreamException {
            return new W(_value);
        }

        public String toString() {
            return "WReplacement:" + _value;
        }

    }

    private static class E implements Externalizable {
        private final static long serialVersionUID = 1L;
        String _a;
        String _b;

        public E() {
        }

        public void readExternal(final ObjectInput oi) throws IOException {
            oi.readUTF(); // "foo"
            _a = oi.readUTF();
            _b = oi.readUTF();
        }

        public void writeExternal(final ObjectOutput oo) throws IOException {
            oo.writeUTF("hello");
            oo.writeUTF(_a);
            oo.writeUTF(_b);
        }

        public String toString() {
            return "E:" + _a + _b;
        }
    }

    private static class EE extends E {
        private final static long serialVersionUID = 1L;
        private final Thread _thread = Thread.currentThread(); // intentionally

        // not
        // Serializable

        public EE() {
            super();
        }

        EE(final String a, final String b) {
            _a = a;
            _b = b;
        }

        public String toString() {
            return "E" + super.toString() + (_thread != null ? "" : "");
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _exchange = _persistit.getExchange("persistit", getClass().getSimpleName(), true);
    }

    @Override
    public void tearDown() throws Exception {
        _persistit.releaseExchange(_exchange);
        super.tearDown();
    }

    public void test1() throws PersistitException {
        System.out.print("test1 ");
        final S s = new S();
        s._a = "1";
        s._b = "2";
        _exchange.getValue().put(s);
        _exchange.clear().append("test1").store();
        final Object x = _exchange.getValue().get();
        assertEquals("S:12", x.toString());
        System.out.println("- done");
    }

    public void test2() throws PersistitException {
        System.out.print("test2 ");
        final SS ss = new SS("3", "4");
        ss._a = "1";
        ss._b = "2";
        _exchange.getValue().put(ss);
        _exchange.clear().append("test2").store();
        final Object x = _exchange.getValue().get();
        assertEquals("SS:1234", x.toString());
        System.out.println("- done");
    }

    public void test3() throws PersistitException {
        System.out.print("test3 ");
        final E e = new E();
        e._a = "1";
        e._b = "2";
        _exchange.getValue().put(e);
        _exchange.clear().append("test3").store();
        final Object x = _exchange.getValue().get();
        assertEquals("E:12", x.toString());
        System.out.println("- done");
    }

    public void test4() throws PersistitException {
        System.out.print("test4 ");
        final EE ee = new EE("6", "7");
        ee._a = "1";
        ee._b = "2";
        _exchange.getValue().put(ee);
        _exchange.clear().append("test4").store();
        final Object x = _exchange.getValue().get();
        assertEquals("EE:12", x.toString());
        System.out.println("- done");
    }

    public void test5() throws PersistitException {
        System.out.print("test5 ");
        final T t = new T("1", "2");
        _exchange.getValue().put(t);
        _exchange.clear().append("test5").store();
        final Object x = _exchange.getValue().get();
        assertEquals("T:12", x.toString());
        System.out.println("- done");
    }

    public void test6() throws PersistitException {
        System.out.print("test6 ");
        final TT tt = new TT("1", "2");
        _exchange.getValue().put(tt);
        _exchange.clear().append("test6").store();
        final Object x = _exchange.getValue().get();
        assertEquals("TT:12", x.toString());
        System.out.println("- done");
    }

    public void test7() throws PersistitException {
        System.out.print("test7 ");
        final CoderManager cm = _persistit.getCoderManager();
        cm.registerValueCoder(TTT.class, new TTTValueCoder(_persistit));
        TTTValueCoder._getCounter = 0;
        final TTT ttt = new TTT("1", "2");
        _exchange.getValue().put(ttt);
        _exchange.clear().append("test7").store();
        final Object x = _exchange.getValue().get();
        assertEquals("TTT:12", x.toString());
        assertEquals(1, TTTValueCoder._getCounter);
        cm.unregisterValueCoder(TTT.class);
        System.out.println("- done");
    }

    public void test8() throws PersistitException {
        System.out.print("test8 ");
        final CoderManager cm = _persistit.getCoderManager();
        cm.registerValueCoder(TTT.class, new TTTValueCoder(_persistit));
        TTTValueCoder._getCounter = 0;
        final TTT ttt = new TTT("1", "2");
        _exchange.getValue().put(ttt);
        _exchange.clear().append("test8").store();
        final Object x = _exchange.getValue().get();
        assertEquals("TTT:12", x.toString());
        assertEquals(1, TTTValueCoder._getCounter);
        cm.unregisterValueCoder(TTT.class);
        System.out.println("- done");
    }

    public void test9() throws PersistitException {
        System.out.print("test9 ");
        final SSS sss = new SSS("3", "4", true, 5);
        sss._a = "1";
        sss._b = "2";
        _exchange.getValue().put(sss);
        _exchange.clear().append("test9").store();
        final Object x = _exchange.getValue().get();
        assertEquals("SSS:1234trueW:5", x.toString());
        System.out.println("- done");
    }

    public void test10() throws PersistitException {
        System.out.print("test10 ");
        final SSS sss = new SSS("3", "4", true, 5);
        sss._a = "1";
        sss._b = "2";
        _exchange.getValue().put(sss);
        _exchange.clear().append("test10").store();
        final Object x = _exchange.getValue().get();
        assertEquals("SSS:1234trueW:5", x.toString());
        System.out.println("- done");
    }

    public void test11() throws PersistitException {
        System.out.print("test11 ");
        final SSSS ssss = new SSSS();
        ssss._a = "Field a";
        ssss._b = "Field b";
        ssss._g = "Field g";
        ssss._h = "Field h";
        _exchange.getValue().put(ssss);
        _exchange.clear().append("test11").store();
        final Object x = _exchange.getValue().get();
        // assertEquals("SSSS:0000falsenull", x.toString());
        System.out.print("  x=" + x + " ");
        System.out.println();
        System.out.println("Value: " + _exchange.getValue());
        System.out.println("- done");
    }

    public void test12() throws PersistitException {
        System.out.print("test12 ");
        final CoderManager cm = _persistit.getCoderManager();
        final ValueCoder defaultCoder = cm.getValueCoder(SSSS.class);
        _persistit.getCoderManager().registerValueCoder(SSSS.class, new SerialValueCoder(SSSS.class));
        final SSSS ssss = new SSSS();
        ssss._a = "Field a";
        ssss._b = "Field b";
        ssss._g = "Field g";
        ssss._h = "Field h";
        _exchange.getValue().put(ssss);
        _exchange.clear().append("test12").store();
        final Object x = _exchange.getValue().get();
        // assertEquals("SSSS:0000falsenull", x.toString());
        System.out.print("  x=" + x + " ");
        System.out.println();
        System.out.println("Value: " + _exchange.getValue());
        cm.registerValueCoder(SSSS.class, defaultCoder);
        System.out.println("- done");
    }

    public static void main(final String[] args) throws Exception {
        new ValueTest3().initAndRunTest();
    }

    public void runAllTests() throws Exception {
        _exchange = _persistit.getExchange("persistit", "ValueTest3", true);

        test1();
        test2();
        test3();
        test4();
        test5();
        test6();
        test7();
        test8();
        test9();
        test10();
        test11();
        test12();
    }

}
