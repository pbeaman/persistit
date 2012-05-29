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

import java.net.MalformedURLException;
import java.net.URL;

import junit.framework.Assert;

import org.junit.Test;

import com.persistit.Key;
import com.persistit.TestShim;
import com.persistit.encoding.CoderContext;
import com.persistit.encoding.KeyCoder;
import com.persistit.encoding.KeyRenderer;
import com.persistit.exception.ConversionException;
import com.persistit.exception.PersistitException;

public class KeyCoderTest1 extends PersistitUnitTestCase {

    @Test
    public void test2() throws MalformedURLException {
        System.out.print("test2 ");
        final KeyCoder coder = new TestKeyRenderer();
        _persistit.getCoderManager().registerKeyCoder(URL.class, coder);
        Key key1;

        key1 = new Key(_persistit);
        final URL url1 = new URL("http://w/z");
        final URL url2 = new URL("http://w:8080/z?userid=pb");
        key1.clear();
        key1.append("a");
        key1.append(url1);
        key1.append("b");
        key1.append(url2);
        key1.append("c");

        key1.reset();
        final String a = key1.decodeString();
        final Object obj1 = key1.decode();
        final String b = key1.decodeString();
        final Object obj2 = key1.decode();
        final String c = key1.decodeString();

        assertEquals("a", a);
        assertEquals("b", b);
        assertEquals("c", c);
        assertEquals(url1.toString(), obj1.toString());
        assertEquals(url2.toString(), obj2.toString());

        final StringBuilder sb = new StringBuilder();
        key1.indexTo(1);
        key1.decode(sb);
        assertEquals(url1.toString(), sb.toString());

        sb.setLength(0);
        key1.indexTo(3);
        key1.decode(sb);
        assertEquals(url2.toString(), sb.toString());

        key1.reset();
        final String toString = key1.toString();
        assertEquals(toString, "{\"a\",(java.net.URL){\"http\",\"w\",-1,\"/z\"},\"b\","
                + "(java.net.URL){\"http\",\"w\",8080,\"/z?userid=pb\"},\"c\"}");
        System.out.println("- done");
    }

    @Test
    public void testKeyHandleEncodeDecode() throws Exception {
        final int[] handles = { 0, 0x1FE, 0x1FF, 0x200, 0x201, 0x7FFE, 0x7FFF, 0x8000, 0x8001, Integer.MAX_VALUE };
        final KeyCoder coder = new TestKeyRenderer();
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

    public static class TestKeyRenderer implements KeyRenderer {

        @Override
        public void appendKeySegment(final Key key, final Object object, final CoderContext context) {
            final URL url = (URL) object;
            key.append(url.getProtocol());
            key.append(url.getHost());
            key.append(url.getPort());
            key.append(url.getFile());
        }

        @Override
        public Object decodeKeySegment(final Key key, final Class cl, final CoderContext context) {
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
        public void renderKeySegment(final Key key, final Object target, final Class cl, final CoderContext context) {
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
    }
}
