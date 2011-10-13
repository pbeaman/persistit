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

package com.persistit.bug;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.unit.PersistitUnitTestCase;

public class Bug870352Test extends PersistitUnitTestCase{

    public void testBug() throws Exception {
        Exchange ex = _persistit.getExchange("persistit", "Bug870352Test", true);
        ex.clear().append(null).append(null).append(null).append((long)2007).store();
        ex.clear().append(null).append(null).append((long)5).append((long)2006).store();
        ex.clear().append(null).append((long)4).append(null).append((long)2005).store();
        ex.clear().append(null).append((long)4).append((long)5).append((long)2004).store();
        
        ex.clear().append(Key.BEFORE).next();
        ex.append(Key.AFTER).previous();
        ex.traverse(Key.GT,true);
        
        assertEquals("{null,(long)4,null,(long)2005}", ex.getKey().toString());
    }
}
