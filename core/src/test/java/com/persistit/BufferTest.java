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

import com.persistit.Exchange.Sequence;
import com.persistit.Management.RecordInfo;
import com.persistit.unit.PersistitUnitTestCase;

public class BufferTest extends PersistitUnitTestCase {

    public void testJoinBuffer() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "BufferTest", true);
        final StringBuilder sb = new StringBuilder();
        final Buffer b1 = ex.getBufferPool().get(ex.getVolume(), 1, true, false);
        final Buffer b2 = ex.getBufferPool().get(ex.getVolume(), 2, true, false);
        b1.init(Buffer.PAGE_TYPE_DATA);
        b2.init(Buffer.PAGE_TYPE_DATA);
        b1.getFastIndex();
        b2.getFastIndex();
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
        b1.split(b2, key, value, foundAt, indexKey, Sequence.NONE, SplitPolicy.NICE_BIAS);

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
        b1.release();
        b2.release();
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
            sb.append(String.format("%5d: db=%3d ebc=%3d  %s=%s\n", r.getKbOffset(), r.getDb(), r.getEbc(), key, value));
        }
        return sb.toString();
    }

}
