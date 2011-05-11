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

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.KeyParser;
import com.persistit.SplitPolicy;

public class BufferBug708592 extends PersistitUnitTestCase {

    @Test
    public void test1() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "bug708592",
                true);
        ex.clear();
        ex.setSplitPolicy(SplitPolicy.LEFT_BIAS);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(
                getClass().getClassLoader().getResourceAsStream(
                        "Bug780592_data.txt")));
        String line = null;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("{")) {
                ex.getKey().clear();
                final KeyParser parser = new KeyParser(line);
                parser.parseKey(ex.getKey());
                ex.store();
            } else if (line.startsWith("*")) {
                System.out.println(line);
                ex.setSplitPolicy(SplitPolicy.PACK_BIAS);
            }
        }
    }
    @Override
    public void runAllTests() throws Exception {
        test1();
    }

}
