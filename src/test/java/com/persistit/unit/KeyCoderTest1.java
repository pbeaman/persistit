/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;

import junit.framework.Assert;

import org.junit.Test;

import com.persistit.Key;
import com.persistit.TestShim;
import com.persistit.encoding.CoderContext;
import com.persistit.encoding.KeyCoder;
import com.persistit.encoding.KeyDisplayer;
import com.persistit.encoding.KeyRenderer;
import com.persistit.exception.ConversionException;
import com.persistit.exception.PersistitException;
import com.persistit.util.Util;

public class KeyCoderTest1 extends PersistitUnitTestCase {

    @Test
    public void normalKeyCoder() throws MalformedURLException {
        final KeyCoder coder = new TestKeyRenderer1();
        _persistit.getCoderManager().registerKeyCoder(URL.class, coder);
        Key key = new Key(_persistit);
        final URL url1 = new URL("http://w/z");
        final URL url2 = new URL("http://w:8080/z?userid=pb");
        key.clear();
        key.append("a");
        key.append(url1);
        key.append("b");
        key.append(url2);
        key.append("c");

        key.reset();
        final String a = key.decodeString();
        final Object obj1 = key.decode();
        final String b = key.decodeString();
        final Object obj2 = key.decode();
        final String c = key.decodeString();

        assertEquals("a", a);
        assertEquals("b", b);
        assertEquals("c", c);
        assertEquals(url1.toString(), obj1.toString());
        assertEquals(url2.toString(), obj2.toString());

        final StringBuilder sb = new StringBuilder();
        key.indexTo(1);
        key.decode(sb);
        assertEquals(url1.toString(), sb.toString());

        sb.setLength(0);
        key.indexTo(3);
        key.decode(sb);
        assertEquals(url2.toString(), sb.toString());

        key.reset();
        final String toString = key.toString();
        assertEquals(toString, "{\"a\",(java.net.URL){\"http\",\"w\",-1,\"/z\"},\"b\","
                + "(java.net.URL){\"http\",\"w\",8080,\"/z?userid=pb\"},\"c\"}");
    }
    
    @Test
    public void zeroByteFreeKeyCoder() throws Exception {
        final KeyCoder coder = new TestKeyRenderer2();
        _persistit.getCoderManager().registerKeyCoder(WrappedString.class, coder);
        Key key = new Key(_persistit);
        key.append(1.2f);
        key.append(new WrappedString(RED_FOX));
        key.append("another key");
        key.append(new WrappedString("abcde"));
        key.reset();
        assertEquals(Float.valueOf(1.2f),key.decode());
        Object ws = key.decode();
        assertTrue(ws instanceof WrappedString);
        assertEquals(RED_FOX, ((WrappedString)ws)._value);
        assertEquals("another key", key.decodeString());
        key.decode(ws);
        assertEquals("abcde", ((WrappedString)ws)._value);
    }
    
    @Test
    public void normalKeyCoderQuotesNulls() throws Exception {
        final KeyCoder coder = new TestKeyRenderer3();
        _persistit.getCoderManager().registerKeyCoder(WrappedLong.class, coder);
        Key key = new Key(_persistit);
        for (long value = 0; value < 32; value++) {
            key.append(new WrappedLong(value));
        }
        key.reset();
        for (int depth = 0; depth < 50; depth++) {
            key.indexTo(depth);
            if (key.getIndex() == key.getEncodedSize()) {
                assertEquals(32, depth);
                break;
            }
            long value = ((WrappedLong)key.decode())._value;
            assertEquals(depth, value);
        }
    }
    
    @Test(expected = ConversionException.class)
    public void zeroByteFreeKeyCoderThrows() throws Exception {
        final KeyCoder coder = new TestKeyRenderer2();
        _persistit.getCoderManager().registerKeyCoder(WrappedString.class, coder);
        Key key = new Key(_persistit);
        WrappedString ws = new WrappedString(new String(new byte[]{'a', 'b', 0, 'c'}));
        key.append(ws);
    }

    @Test
    public void keyHandleEncodeDecode() throws Exception {
        final int[] handles = { 64, 0x1FE, 0x1FF, 0x200, 0x201, 0x7FFE, 0x7FFF, 0x8000, 0x8001, Integer.MAX_VALUE };
        final KeyCoder coder = new TestKeyRenderer1();
        final Key key1 = new Key(_persistit);
        final URL url1 = new URL("http://w/z");
        final URL url2 = new URL("http://w:8080/z?userid=pb");

        for (int handle : handles) {
            registerCoderDefinedKeyHandle(coder, handle);

            key1.clear();
            key1.append(url1);
            key1.append(url2);

            key1.reset();
            final Object obj1 = key1.decode();
            final Object obj2 = key1.decode();

            assertEquals(url1.toString(), obj1.toString());
            assertEquals(url2.toString(), obj2.toString());

            final StringBuilder sb = new StringBuilder();
            key1.indexTo(1);
            key1.decode(sb);
            assertEquals(url2.toString(), sb.toString());

        }
    }
    
    private void registerCoderDefinedKeyHandle(final KeyCoder coder, final int handle) throws PersistitException {
        TestShim.clearAllClassIndexEntries(_persistit);
        TestShim.setClassIndexTestIdFloor(_persistit, handle);
        _persistit.getCoderManager().registerKeyCoder(URL.class, coder);
    }

    public static void main(final String[] args) throws Exception {
        new KeyCoderTest1().initAndRunTest();
    }

    public void debugAssert(boolean condition) {
        Assert.assertTrue(condition);
    }

    public static class TestKeyRenderer1 implements KeyRenderer {

        @Override
        public void appendKeySegment(final Key key, final Object object, final CoderContext context) {
            final URL url = (URL) object;
            key.append(url.getProtocol());
            key.append(url.getHost());
            key.append(url.getPort());
            key.append(url.getFile());
        }

        @Override
        public Object decodeKeySegment(final Key key, final Class<?> cl, final CoderContext context) {
            final String protocol = key.decodeString();
            final String host = key.decodeString();
            final int port = key.decodeInt();
            final String file = key.decodeString();
            try {
                return new URL(protocol, host, port, file);
            } catch (final MalformedURLException mue) {
                throw new ConversionException(mue);
            }
        }

        @Override
        public void renderKeySegment(final Key key, final Object target, final Class<?> cl, final CoderContext context) {
            final StringBuilder sb = (StringBuilder) target;
            key.decodeString(sb);
            sb.append("://");
            key.decodeString(sb);
            final int port = key.decodeInt();
            if (port != -1) {
                sb.append(':');
                sb.append(port);
            }
            key.decodeString(sb);
        }
        
        @Override
        public boolean isZeroByteFree() {
            return false;
        }
    }
    
    @SuppressWarnings("serial")
    public static class WrappedString implements Serializable {
        String _value;
        WrappedString(final String value) {
            _value = value;
        }
    }
    
    @SuppressWarnings("serial")
    public static class WrappedLong implements Serializable {
        long _value;
        WrappedLong(final long value) {
            _value = value;
        }
    }
    
    
    public static class TestKeyRenderer2 implements KeyRenderer {

        @Override
        public void appendKeySegment(final Key key, final Object object, final CoderContext context) {
            final WrappedString cstring = (WrappedString) object;
            byte[] bytes = cstring._value.getBytes();
            int size = key.getEncodedSize();
            System.arraycopy(bytes, 0, key.getEncodedBytes(), size, bytes.length);
            key.setEncodedSize(size + bytes.length);
        }

        @Override
        public Object decodeKeySegment(final Key key, final Class<?> cl, final CoderContext context) {
            WrappedString target = new WrappedString(null);
            renderKeySegment(key, target, cl, context);
            return target;
        }

        @Override
        public void renderKeySegment(final Key key, final Object target, final Class<?> cl, final CoderContext context) {
            WrappedString cstring = (WrappedString)target;
            int size = key.getEncodedSize();
            int index = key.getIndex();
            byte[] b = new byte[size - index];
            System.arraycopy(key.getEncodedBytes(), index, b, 0, b.length);
            cstring._value = new String(b);
        }
        
        @Override
        public boolean isZeroByteFree() {
            return true;
        }
    }

    public static class TestKeyRenderer3 implements KeyRenderer, KeyDisplayer {

        @Override
        public void appendKeySegment(final Key key, final Object object, final CoderContext context) {
            final WrappedLong wlong = (WrappedLong)object;
           int size = key.getEncodedSize();
           Util.putLong(key.getEncodedBytes(), size, wlong._value);
            key.setEncodedSize(size + 8);
        }

        @Override
        public Object decodeKeySegment(final Key key, final Class<?> cl, final CoderContext context) {
            WrappedLong target = new WrappedLong(Long.MIN_VALUE);
            renderKeySegment(key, target, cl, context);
            return target;
        }

        @Override
        public void renderKeySegment(final Key key, final Object target, final Class<?> cl, final CoderContext context) {
            WrappedLong wl = (WrappedLong)target;
            int size = key.getEncodedSize();
            int index = key.getIndex();
            assertEquals(8, size - index);
            wl._value = Util.getLong(key.getEncodedBytes(), index);
            key.setIndex(index + 8);
        }
        
        @Override
        public boolean isZeroByteFree() {
            return false;
        }

        @Override
        public void displayKeySegment(Key key, Appendable target, Class<?> clazz, CoderContext context)
                throws ConversionException {
            WrappedLong wl = new WrappedLong(Long.MIN_VALUE);
            renderKeySegment(key, wl, clazz, context);
            Util.append(target, Long.toString(wl._value));
        }
    }

}
