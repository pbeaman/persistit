/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
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
