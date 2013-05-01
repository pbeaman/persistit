/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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

package com.persistit;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class Bug870352Test extends PersistitUnitTestCase {

    @Test
    public void testBug() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "Bug870352Test", true);
        ex.clear().append(null).append(null).append(null).append((long) 2007).store();
        ex.clear().append(null).append(null).append((long) 5).append((long) 2006).store();
        ex.clear().append(null).append((long) 4).append(null).append((long) 2005).store();
        ex.clear().append(null).append((long) 4).append((long) 5).append((long) 2004).store();

        ex.clear().append(Key.BEFORE).next();
        ex.append(Key.AFTER).previous();
        ex.traverse(Key.GT, true);

        assertEquals("{null,(long)4,null,(long)2005}", ex.getKey().toString());
    }
}
