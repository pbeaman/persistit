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

import com.persistit.Management.RecordInfo;
import com.persistit.unit.PersistitUnitTestCase;

public class BufferTest extends PersistitUnitTestCase {

    public void testJjoinBuffer() throws Exception {
        if (false) {
        _persistit = new Persistit();
        final StringBuilder sb = new StringBuilder();
        final Buffer b1 = new Buffer(1024, 0, null, _persistit);
        b1.init(Buffer.PAGE_TYPE_DATA);
        final Buffer b2 = new Buffer(1024, 0, null, _persistit);
        b2.init(Buffer.PAGE_TYPE_DATA);
        b1.claim(true);
        b2.claim(true);
        final Key key = new Key((Persistit) null);
        final Key indexKey = new Key((Persistit) null);
        final Value value = new Value((Persistit) null);

        for (int i = 'a'; i < 'z'; i++) {
            sb.append((char) i).append((char) i);
            key.to(sb);
            value.putString(sb);
            b1.putValue(key, value);
        }

        sb.setLength(20);
        key.to(sb);
        final int foundAt = b1.findKey(key);
        b1.split(b2, key, value, foundAt, indexKey, SplitPolicy.NICE_BIAS);

        // final String s1 = bufferDump(b1);
        // final String s2= bufferDump(b2);
        // System.out.println("b1:");
        // System.out.println(s1);
        // System.out.println("b2:");
        // System.out.println(s2);

        final RecordInfo info = b2.getRecords()[0];
        info.getKeyState().copyTo(key);
        final Key key1 = new Key(key);
        final Key key2 = new Key(key);
        key1.nudgeLeft();
        key2.nudgeRight();
        final int foundAt1 = b1.findKey(key1);
        final int foundAt2 = b2.findKey(key2);
        b1.join(b2, foundAt1, foundAt2, key1, key2, JoinPolicy.EVEN_BIAS);

        final String s3 = bufferDump(b1);
        // System.out.println("b1':");
        // System.out.println(s3);

        final RecordInfo r = b1.getRecords()[b1.getRecords().length - 1];
        r.getKeyState().copyTo(key);
        assertTrue(key.toString().contains("ttuuvv"));
        }
    }

    @Override
    public void runAllTests() throws Exception {

    }

    public String bufferDump(final Buffer buffer) {
        final StringBuilder sb = new StringBuilder();
        final Key key = new Key((Persistit) null);
        final Value value = new Value((Persistit) null);

        for (RecordInfo r : buffer.getRecords()) {
            r.getKeyState().copyTo(key);
            r.getValueState().copyTo(value);
            sb.append(String.format("%5d: db=%3d ebc=%3d  %s=%s\n",
                    r.getKbOffset(), r.getDb(), r.getEbc(), key, value));
        }
        return sb.toString();
    }

}
