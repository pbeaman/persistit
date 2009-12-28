/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Apr 6, 2004
 */
package com.persistit.unit;

import java.net.MalformedURLException;
import java.net.URL;

import org.junit.Test;

import junit.framework.Assert;

import com.persistit.Key;
import com.persistit.Util;
import com.persistit.encoding.CoderContext;
import com.persistit.encoding.KeyCoder;
import com.persistit.encoding.KeyRenderer;
import com.persistit.encoding.KeyStringCoder;
import com.persistit.exception.ConversionException;

public class KeyCoderTest1 extends PersistitUnitTestCase {
    
    Key _key1;
    Key _key2;

    @Test
    public void test1() {
        System.out.print("test1 ");
        final KeyStringCoder coder = new TestStringCoder();
        _key1 = new Key(_persistit);
        _key2 = new Key(_persistit);
        _key1.setKeyStringCoder(coder);
        _key2.setKeyStringCoder(coder);

        final String a1 = "Abcde";
        final String b1 = "abCDE";
        final String c1 = "Bcde";

        _key1.clear().append(a1);
        _key2.clear().append(b1);

        final String a2 = _key1.indexTo(0).decodeString();
        final String b2 = _key2.indexTo(0).decodeString();

        assertEquals(a1, a2);
        assertEquals(b1, b2);
        assertTrue(_key1.compareTo(_key2) < 0);

        _key1.clear().append(c1);
        final String c2 = (String) _key1.indexTo(0).decode();
        assertTrue(_key1.compareTo(_key2) > 0);
        assertEquals(c1, c2);

        System.out.println("- done");
    }

    @Test
    public void test2() throws MalformedURLException {
        System.out.print("test2 ");
        final KeyCoder coder = new TestKeyRenderer();
        _persistit.getCoderManager().registerKeyCoder(URL.class,
            coder);
        _key1 = new Key(_persistit);
        final URL url1 = new URL("http://w/z");
        final URL url2 = new URL("http://w:8080/z?userid=pb");
        _key1.clear();
        _key1.append("a");
        _key1.append(url1);
        _key1.append("b");
        _key1.append(url2);
        _key1.append("c");

        _key1.reset();
        final String a = _key1.decodeString();
        final Object obj1 = _key1.decode();
        final String b = _key1.decodeString();
        final Object obj2 = _key1.decode();
        final String c = _key1.decodeString();

        assertEquals("a", a);
        assertEquals("b", b);
        assertEquals("c", c);
        assertEquals(url1.toString(), obj1.toString());
        assertEquals(url2.toString(), obj2.toString());

        final StringBuffer sb = new StringBuffer();
        _key1.indexTo(1);
        _key1.decode(sb);
        assertEquals(url1.toString(), sb.toString());

        sb.setLength(0);
        _key1.indexTo(3);
        _key1.decode(sb);
        assertEquals(url2.toString(), sb.toString());

        _key1.reset();
        final String toString = _key1.toString();
        assertEquals(toString,
            "{\"a\",(java.net.URL){\"http\",\"w\",-1,\"/z\"},\"b\","
                + "(java.net.URL){\"http\",\"w\",8080,\"/z?userid=pb\"},\"c\"}");
        System.out.println("- done");
    }

    public static void main(final String[] args) throws Exception {
        new KeyCoderTest1().initAndRunTest();
    }

    public void runAllTests() throws Exception {
        test1();
        test2();
    }

    public void debugAssert(boolean condition) {
        if (!condition) {
            System.out.println();
            System.out.println(" key1="
                + Util.hexDump(_key1.getEncodedBytes(), 0, _key1
                    .getEncodedSize()));
            System.out.println(" key2="
                + Util.hexDump(_key2.getEncodedBytes(), 0, _key2
                    .getEncodedSize()));
            System.out.println("Assertion failure breakpoint");
        }
        Assert.assertTrue(condition);
    }

    private void debug(boolean condition) {
        if (!condition) {
            return;
        }
        return; // <-- breakpoint here
    }

    public static class TestKeyRenderer implements KeyRenderer {

        public void appendKeySegment(final Key key, final Object object,
            final CoderContext context) {
            final URL url = (URL) object;
            key.append(url.getProtocol());
            key.append(url.getHost());
            key.append(url.getPort());
            key.append(url.getFile());
        }

        public Object decodeKeySegment(final Key key, final Class cl,
            final CoderContext context) {
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

        public void renderKeySegment(final Key key, final Object target,
            final Class cl, final CoderContext context) {
            final StringBuffer sb = (StringBuffer) target;
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

    public static class TestStringCoder implements KeyStringCoder {
        public void appendKeySegment(final Key key, final Object object,
            final CoderContext context) {
            if (object instanceof CharSequence) {
                final CharSequence s = (CharSequence) object;
                final byte[] bytes = key.getEncodedBytes();
                final int start = key.getEncodedSize();
                int end = start;
                for (int i = 0; i < s.length(); i++) {
                    int ch = s.charAt(i);
                    if ((ch >= 'A') && (ch < 'M')) {
                        ch = 'A' + 2 * (ch - 'A');
                    } else if ((ch >= 'M') && (ch <= 'Z')) {
                        ch = 'a' + 2 * (ch - 'M');
                    } else if ((ch >= 'a') && (ch < 'm')) {
                        ch = 'B' + 2 * (ch - 'a');
                    } else if ((ch >= 'm') && (ch <= 'z')) {
                        ch = 'b' + 2 * (ch - 'm');
                    }
                    bytes[end++] = (byte) ch;
                }
                key.setEncodedSize(end);
            }
        }

        public Object decodeKeySegment(final Key key, final Class cl,
            final CoderContext context) {
            final StringBuffer sb = new StringBuffer();
            renderKeySegment(key, sb, cl, context);
            if (cl == StringBuffer.class) {
                return sb;
            }
            if (cl == String.class) {
                return sb.toString();
            }
            throw new ConversionException("String conversion to class "
                + cl.getName() + " is not supported");
        }

        public void renderKeySegment(final Key key, final Object target,
            final Class cl, final CoderContext context) {
            final StringBuffer sb = (StringBuffer) target;
            final byte[] bytes = key.getEncodedBytes();
            final int start = key.getIndex();
            for (int end = start;; end++) {
                int ch = bytes[end];
                if (ch == 0) {
                    key.setIndex(end);
                    break;
                }
                final boolean lower = (ch & 1) == 0;
                if ((ch >= 'A') && (ch <= 'Z')) {
                    ch = 'A' + (ch - 'A') / 2;
                    if (lower) {
                        ch += ('a' - 'A');
                    }
                } else if ((ch >= 'a') && (ch <= 'z')) {
                    ch = 'M' + (ch - 'a') / 2;
                    if (lower) {
                        ch += ('a' - 'A');
                    }
                }
                sb.append((char) ch);
            }
        }
    }
}
