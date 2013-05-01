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

package com.persistit.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

import com.persistit.KeyState;
import com.persistit.Persistit;
import com.persistit.ValueState;
import com.persistit.exception.AppendableIOException;
import com.persistit.exception.PersistitInterruptedException;

/**
 * @author pbeaman
 */
public class Util {
    final static byte[] NULLS = new byte[1024];

    final static String SPACES = "                                                                    ";
    private final static String UTF8 = "UTF-8";
    public final static String NEW_LINE = System.getProperty("line.separator");
    private final static String REGEX_QUOTE = "^$*+?()[].";

    public final static long NS_PER_S = 1000000000L;
    public final static long MS_PER_S = 1000L;
    public final static long NS_PER_MS = 1000000L;

    public final static char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D',
            'E', 'F' };

    public static int getByte(final byte[] bytes, final int index) {
        return (bytes[index + 0] & 0xFF);
    }

    public static int getShort(final byte[] bytes, final int index) {
        if (Persistit.BIG_ENDIAN) {
            return (bytes[index + 1] & 0xFF) | (bytes[index + 0]) << 8;
        } else {
            return (bytes[index + 0] & 0xFF) | (bytes[index + 1]) << 8;
        }
    }

    public static int getChar(final byte[] bytes, final int index) {
        if (Persistit.BIG_ENDIAN) {
            return (bytes[index + 1] & 0xFF) | (bytes[index + 0] & 0xFF) << 8;
        } else {
            return (bytes[index + 0] & 0xFF) | (bytes[index + 1] & 0xFF) << 8;
        }
    }

    public static int getInt(final byte[] bytes, final int index) {
        if (Persistit.BIG_ENDIAN) {
            return (bytes[index + 3] & 0xFF) | (bytes[index + 2] & 0xFF) << 8 | (bytes[index + 1] & 0xFF) << 16
                    | (bytes[index + 0] & 0xFF) << 24;
        } else {
            return (bytes[index + 0] & 0xFF) | (bytes[index + 1] & 0xFF) << 8 | (bytes[index + 2] & 0xFF) << 16
                    | (bytes[index + 3] & 0xFF) << 24;
        }
    }

    public static long getLong(final byte[] bytes, final int index) {
        if (Persistit.BIG_ENDIAN) {
            return (bytes[index + 7] & 0xFF) | (long) (bytes[index + 6] & 0xFF) << 8
                    | (long) (bytes[index + 5] & 0xFF) << 16 | (long) (bytes[index + 4] & 0xFF) << 24
                    | (long) (bytes[index + 3] & 0xFF) << 32 | (long) (bytes[index + 2] & 0xFF) << 40
                    | (long) (bytes[index + 1] & 0xFF) << 48 | (long) (bytes[index + 0] & 0xFF) << 56;
        } else {
            return (bytes[index + 0] & 0xFF) | (long) (bytes[index + 1] & 0xFF) << 8
                    | (long) (bytes[index + 2] & 0xFF) << 16 | (long) (bytes[index + 3] & 0xFF) << 24
                    | (long) (bytes[index + 4] & 0xFF) << 32 | (long) (bytes[index + 5] & 0xFF) << 40
                    | (long) (bytes[index + 6] & 0xFF) << 48 | (long) (bytes[index + 7] & 0xFF) << 56;
        }
    }

    public static void putByte(final byte[] bytes, final int index, final int value) {
        bytes[index + 0] = (byte) (value);
    }

    public static void putShort(final byte[] bytes, final int index, final int value) {
        if (Persistit.BIG_ENDIAN) {
            bytes[index + 1] = (byte) (value);
            bytes[index + 0] = (byte) (value >>> 8);
        } else {
            bytes[index + 0] = (byte) (value);
            bytes[index + 1] = (byte) (value >>> 8);
        }
    }

    public static void putChar(final byte[] bytes, final int index, final int value) {
        if (Persistit.BIG_ENDIAN) {
            bytes[index + 1] = (byte) (value);
            bytes[index + 0] = (byte) (value >>> 8);
        } else {
            bytes[index + 0] = (byte) (value);
            bytes[index + 1] = (byte) (value >>> 8);
        }
    }

    public static void putInt(final byte[] bytes, final int index, final int value) {
        if (Persistit.BIG_ENDIAN) {
            bytes[index + 3] = (byte) (value);
            bytes[index + 2] = (byte) (value >>> 8);
            bytes[index + 1] = (byte) (value >>> 16);
            bytes[index + 0] = (byte) (value >>> 24);
        } else {
            bytes[index + 0] = (byte) (value);
            bytes[index + 1] = (byte) (value >>> 8);
            bytes[index + 2] = (byte) (value >>> 16);
            bytes[index + 3] = (byte) (value >>> 24);
        }
    }

    public static void putLong(final byte[] bytes, final int index, final long value) {
        if (Persistit.BIG_ENDIAN) {
            bytes[index + 7] = (byte) (value);
            bytes[index + 6] = (byte) (value >>> 8);
            bytes[index + 5] = (byte) (value >>> 16);
            bytes[index + 4] = (byte) (value >>> 24);
            bytes[index + 3] = (byte) (value >>> 32);
            bytes[index + 2] = (byte) (value >>> 40);
            bytes[index + 1] = (byte) (value >>> 48);
            bytes[index + 0] = (byte) (value >>> 56);
        } else {
            bytes[index + 0] = (byte) (value);
            bytes[index + 1] = (byte) (value >>> 8);
            bytes[index + 2] = (byte) (value >>> 16);
            bytes[index + 3] = (byte) (value >>> 24);
            bytes[index + 4] = (byte) (value >>> 32);
            bytes[index + 5] = (byte) (value >>> 40);
            bytes[index + 6] = (byte) (value >>> 48);
            bytes[index + 7] = (byte) (value >>> 56);
        }
    }

    public static int putBytes(final byte[] bytes, final int index, final byte[] value) {
        System.arraycopy(value, 0, bytes, index, value.length);
        return value.length;
    }

    public static boolean changeBytes(final byte[] bytes, final int index, final byte[] value) {
        final boolean same = equalsByteSubarray(bytes, index, value);
        if (same) {
            return false;
        } else {
            putBytes(bytes, index, value);
            return true;
        }
    }

    public static boolean changeByte(final byte[] bytes, final int index, final byte value) {
        if (getByte(bytes, index) == value) {
            return false;
        } else {
            putByte(bytes, index, value);
            return true;
        }
    }

    public static boolean changeChar(final byte[] bytes, final int index, final char value) {
        if (getChar(bytes, index) == value) {
            return false;
        } else {
            putChar(bytes, index, value);
            return true;
        }
    }

    public static boolean changeShort(final byte[] bytes, final int index, final short value) {
        if (getShort(bytes, index) == value) {
            return false;
        } else {
            putShort(bytes, index, value);
            return true;
        }
    }

    public static boolean changeInt(final byte[] bytes, final int index, final int value) {
        if (getInt(bytes, index) == value) {
            return false;
        } else {
            putInt(bytes, index, value);
            return true;
        }
    }

    public static boolean changeLong(final byte[] bytes, final int index, final long value) {
        if (getLong(bytes, index) == value) {
            return false;
        } else {
            putLong(bytes, index, value);
            return true;
        }
    }

    public static String format(final String s, final int width, final boolean right) {
        final int pad = width - s.length();
        if (pad < 0)
            return s.substring(0, width - 1) + "&";
        if (pad == 0)
            return s;
        if (right)
            return SPACES.substring(0, pad) + s;
        else
            return s + SPACES.substring(0, pad);
    }

    public static String format(final int i) {
        return format(i, 10);
    }

    public static String format(final long i) {
        return format(i, 22);
    }

    public static String format(final long i, final int width) {
        return format(Long.toString(i), width, true);
    }

    public static String abridge(final String s, final int maxLength) {
        if (s.length() > maxLength) {
            return s.substring(0, maxLength - 3) + "...";
        } else {
            return s;
        }
    }

    public static boolean equalsByteSubarray(final byte[] source, final int next, final byte[] target) {
        return equalsByteSubarray(source, next, target, 0, target.length);
    }

    public static boolean equalsByteSubarray(final byte[] source, final int soffset, final byte[] target,
            final int toffset, final int length) {
        for (int index = 0; index < length; index++) {
            if (source[soffset + index] != target[toffset + index])
                return false;
        }
        return true;
    }

    public static void fill(final StringBuilder sb, final long value, final int width) {
        fill(sb, Long.toString(value), width);
    }

    public static void fill(final StringBuilder sb, final String s, final int width) {
        for (int i = s.length(); i < width; i++)
            sb.append(' ');
        sb.append(s);
    }

    public static String dump(final KeyState ks) {
        final byte[] bytes = ks.getBytes();
        return dump(bytes, 0, bytes.length);
    }

    public static String dump(final ValueState vs) {
        final byte[] bytes = vs.getEncodedBytes();
        return dump(bytes, 0, bytes.length);
    }

    public static String dump(final byte[] b, final int offset, final int size) {
        final StringBuilder sb = new StringBuilder();
        final StringBuilder sb2 = new StringBuilder();
        for (int m = 0; m < size - offset; m += 16) {
            sb2.setLength(0);
            hex(sb, m, 4);
            sb.append(": ");
            for (int i = 0; i < 16; i++) {
                sb.append(" ");
                if (i % 8 == 0)
                    sb.append(" ");
                final int j = m + i;
                if (j < size - offset) {
                    hex(sb, b[j + offset], 2);
                    final char c = (char) (b[j + offset] & 0xFF);
                    if (c >= 32 && c < 127)
                        sb2.append(c);
                    else
                        sb2.append(".");
                } else
                    sb.append("  ");
            }
            sb.append("   ");
            sb.append(sb2.toString());
            sb.append(NEW_LINE);
        }
        return sb.toString();
    }

    public static String dump(final char[] c, final int offset, final int size) {
        final StringBuilder sb = new StringBuilder();
        final StringBuilder sb2 = new StringBuilder();
        for (int m = 0; m < size - offset; m += 8) {
            sb2.setLength(0);
            hex(sb, m, 4);
            sb.append(":");
            for (int i = 0; i < 8; i++) {
                sb.append("  ");
                if (i % 4 == 0)
                    sb.append(" ");
                final int j = m + i;
                if (j < size - offset) {
                    hex(sb, c[j + offset], 4);
                    if (c[j + offset] >= 32 && c[j] < 127)
                        sb2.append(c[j]);
                    else
                        sb2.append(".");
                } else
                    sb.append("    ");
            }
            sb.append("    ");
            sb.append(sb2.toString());
            sb.append(NEW_LINE);
        }
        return sb.toString();
    }

    public static String hexDump(final byte[] b) {
        return hexDump(b, 0, b.length);
    }

    public static String hexDump(final byte[] b, final int offset, final int length) {
        final StringBuilder sb = new StringBuilder();
        final StringBuilder sb2 = new StringBuilder();
        sb2.setLength(0);
        for (int i = offset; i < offset + length; i++) {
            sb.append(" ");
            hex(sb, b[i], 2);
            final char c = (char) (b[i] & 0xFF);
            if (c >= 32 && c < 127)
                sb2.append(c);
            else
                sb2.append(".");
        }
        sb.append(" / ");
        sb.append(sb2.toString());
        return sb.toString();
    }

    public static Appendable hex(final Appendable sb, final long value, final int length) {
        for (int i = length - 1; i >= 0; i--) {
            append(sb, HEX_DIGITS[(int) (value >> (i * 4)) & 0xF]);
        }
        return sb;
    }

    public static byte[] stringToBytes(final String s) {
        if (s == null)
            return new byte[0];
        try {
            return s.getBytes(UTF8);
        } catch (final UnsupportedEncodingException e) {
            return s.getBytes();
        }
    }

    public static boolean isValidName(final String s) {
        if (s == null || s.length() == 0)
            return false;
        if (!Character.isJavaIdentifierStart(s.charAt(0)))
            return false;
        for (int i = 1; i < s.length(); i++) {
            if (!Character.isJavaIdentifierPart(s.charAt(i)))
                return false;
        }
        return true;
    }

    public static String bytesToHex(final byte[] bytes) {
        final StringBuilder sb = new StringBuilder();
        bytesToHex(sb, bytes, 0, bytes.length);
        return sb.toString();
    }

    public static void bytesToHex(final Appendable sb, final byte[] bytes, final int offset, int length) {
        length += offset;
        for (int i = offset; i < length; i++) {
            append(sb, HEX_DIGITS[(bytes[i] >>> 4) & 0x0F]);
            append(sb, HEX_DIGITS[(bytes[i] >>> 0) & 0x0F]);
        }
    }

    public static byte[] hexToBytes(final String hex) {
        int count = 0;
        for (int i = 0; i < hex.length(); i++) {
            final int c = hex.charAt(i);
            if (c == '/' || c == '}')
                break;
            if (c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
                count++;
            } else if (c != ' ' && c != '\t' && c != '\r' && c != '\n') {
                throw new IllegalArgumentException();
            }
        }
        if ((count % 2) == 1)
            throw new IllegalArgumentException();
        final byte[] result = new byte[count / 2];
        int t = 0;
        count = 0;

        for (int i = 0; i < hex.length(); i++) {
            final int c = hex.charAt(i);

            if (c == '/' || c == '}')
                break;

            if (c >= '0' && c <= '9') {
                t = t * 16 + c - '0';
            } else if (c >= 'a' && c <= 'f') {
                t = t * 16 + c - 'a' + 10;
            } else if (c >= 'A' && c <= 'F') {
                t = t * 16 + c - 'A' + 10;
            } else
                continue;

            count++;
            if ((count % 2) == 0) {
                result[(count / 2) - 1] = (byte) t;
                t = 0;
            }
        }
        return result;
    }

    public static String replaceFileSuffix(final String name, final String suffix) {
        //
        // If suffix is a full path, return it.
        //
        if (suffix.indexOf('/') >= 0 || suffix.indexOf('\\') > 0)
            return suffix;
        //
        // Find final file separator
        //
        int p = name.lastIndexOf('/');
        p = Math.max(p, name.lastIndexOf('\\'));
        //
        // Just replace the extension - i.e., the part of the path name to the
        // right of the period.
        //
        if (suffix.startsWith(".")) {
            final int q = name.lastIndexOf('.');
            if (q > p)
                p = q - 1;
        }
        if (p > 0)
            return name.substring(0, p + 1) + suffix;
        else
            return name + suffix;
    }

    public static void clearBytes(final byte[] bytes, final int from, final int to) {
        for (int offset = from; offset < to; offset += NULLS.length) {
            int count = to - offset;
            if (count > NULLS.length)
                count = NULLS.length;
            System.arraycopy(NULLS, 0, bytes, offset, count);
        }
    }

    public static void appendQuotedString(final Appendable sb, final String s, final int start, final int length) {
        final int end = Math.min(start + length, s.length());
        for (int index = start; index < end; index++) {
            appendQuotedChar(sb, s.charAt(index));
        }
    }

    public static void append(final Appendable sb, final char c) {
        try {
            sb.append(c);
        } catch (final IOException e) {
            throw new AppendableIOException(e);
        }
    }

    public static void append(final Appendable sb, final CharSequence s) {
        try {
            sb.append(s);
        } catch (final IOException e) {
            throw new AppendableIOException(e);
        }
    }

    public static void appendQuotedChar(final Appendable sb, final int c) {
        int q = 0;
        if (c == '\b')
            q = 'b';
        else if (c == '\n')
            q = 'n';
        else if (c == '\r')
            q = 'r';
        else if (c == '\t')
            q = 't';
        else if (c == '\"' || c == '\\')
            q = c;
        if (q != 0) {
            append(sb, '\\');
            append(sb, (char) q);
        } else if (c >= 127 || c < 20) {
            append(sb, '\\');
            append(sb, 'u');
            Util.hex(sb, c, 4);
        } else
            append(sb, (char) c);
    }

    public static String date(final long t) {
        if (t == 0) {
            return "none";
        } else {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(t));
        }
    }

    public static void println(final String template, final Object... args) {
        System.out.println(String.format(template, args));
    }

    /**
     * Utility method to determine whether a subarray of bytes in a byte-array
     * <code>source</code> matches the byte-array in <code>target</code>.
     * 
     * @param source
     *            The source byte array
     * @param offset
     *            The offset of the sub-array within the source.
     * @param target
     *            The target byte array
     * @return <code>true</code> if the byte subarrays are equal
     */
    public static boolean bytesEqual(final byte[] source, final int offset, final byte[] target) {
        for (int index = 0; index < target.length; index++) {
            if (source[index + offset] != target[index]) {
                return false;
            }
        }
        return true;
    }

    public static void sleep(final long millis) throws PersistitInterruptedException {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException ie) {
            throw new PersistitInterruptedException(ie);
        }
    }

    public static void spinSleep() throws PersistitInterruptedException {
        sleep(1);
    }

    public static String toString(final Object object) {
        return object == null ? null : object.toString();
    }

    public static int rangeCheck(final int value, final int min, final int max) {
        if (value >= min && value <= max) {
            return value;
        }
        if (min == Integer.MIN_VALUE) {
            throw new IllegalArgumentException(String.format("Value must be less than or equals to %,d: %,d", max,
                    value));
        }
        if (max == Integer.MAX_VALUE) {
            throw new IllegalArgumentException(String.format("Value must be greater than or equal to %,d: %,d", min,
                    value));
        } else {
            throw new IllegalArgumentException(String.format("Value must be between %d and %d, inclusive: ", min, max,
                    value));
        }
    }

    public static long rangeCheck(final long value, final long min, final long max) {
        if (value >= min && value <= max) {
            return value;
        }
        if (min == Long.MIN_VALUE) {
            throw new IllegalArgumentException(String.format("Value must be less than or equals to %,d: %,d", max,
                    value));
        }
        if (max == Long.MAX_VALUE) {
            throw new IllegalArgumentException(String.format("Value must be greater than or equal to %,d: %,d", min,
                    value));
        } else {
            throw new IllegalArgumentException(String.format("Value must be between %d and %d, inclusive: ", min, max,
                    value));
        }
    }

    public static float rangeCheck(final float value, final float min, final float max) {
        if (value >= min && value <= max) {
            return value;
        }
        if (min == Float.MIN_VALUE) {
            throw new IllegalArgumentException(String.format("Value must be less than or equals to %,f: %,f", max,
                    value));
        }
        if (max == Float.MAX_VALUE) {
            throw new IllegalArgumentException(String.format("Value must be greater than or equal to %,f: %,f", min,
                    value));
        } else {
            throw new IllegalArgumentException(String.format("Value must be between %f and %f, inclusive: ", min, max,
                    value));
        }
    }

    public static Pattern pattern(final String s, final boolean caseInsensitive) {
        final StringBuilder sb = new StringBuilder();
        if (s == null) {
            sb.append(".*");
        } else {
            for (int index = 0; index < s.length(); index++) {
                final char c = s.charAt(index);
                if (c == '*') {
                    sb.append(".*");
                } else if (c == '?') {
                    sb.append(".");
                } else if (REGEX_QUOTE.indexOf(c) != -1) {
                    sb.append('\\');
                    sb.append(c);
                } else {
                    sb.append(c);
                }
            }
        }
        return Pattern.compile(sb.toString(), caseInsensitive ? Pattern.CASE_INSENSITIVE : 0);
    }

}
