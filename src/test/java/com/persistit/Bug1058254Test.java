/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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

import static com.persistit.unit.UnitTestProperties.VOLUME_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.persistit.exception.PersistitException;

/**
 * A range delete that crosses at least two page boundaries can write an
 * incorrect key when merging records; this causes it and subsequent keys within
 * the page to be wrong and also leads to a discontinuity between the max key in
 * the page and the first key of its right sibling.
 */

public class Bug1058254Test extends PersistitUnitTestCase {

    private final static String TREE_NAME = "Bug1022567";

    private Exchange getExchange() throws PersistitException {
        return _persistit.getExchange(VOLUME_NAME, TREE_NAME, true);
    }

    @Test
    public void joinDiscontinuity() throws Exception {
        final Exchange ex = getExchange();
        ex.getValue().put(createString(2000));
        for (int i = 3444; i < 3600; i++) {
            ex.to(i).store();
        }
        final Key key1 = new Key(_persistit);
        final Key key2 = new Key(_persistit);
        key1.to(3445);
        key2.to(3557);
        ex.removeKeyRange(key1, key2);
        ex.to(Key.BEFORE);
        assertTrue(ex.next());
        assertEquals(3444, ex.getKey().decodeInt());
        assertTrue(ex.next());
        assertEquals(3557, ex.getKey().decodeInt());
    }
}
