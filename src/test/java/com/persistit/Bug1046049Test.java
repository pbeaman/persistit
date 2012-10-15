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
 * https://bugs.launchpad.net/akiban-persistit/+bug/1046049
 * 
 * If a key filter is set up to specify a key in the range of BEFORE ->
 * searchValue, and a LTEQ traverse from AFTER is performed, the traverse fails
 * to find any key.
 * 
 * The complementary traverse in the forward direction, i.e., filter searchValue
 * -> AFTER and a GTEQ traverse from BEFORE, seems to work, hence this report.
 * 
 * Attached is a program to exhibit the observed behavior. If I'm
 * misunderstanding something here, I apologize in advance.
 * 
 * With thanks to j5b
 * 
 */

public class Bug1046049Test extends PersistitUnitTestCase {

    @Test
    public void testLTEQfiltered() throws Exception {
        // insert two keys into tree
        Exchange dbex = _persistit.getExchange("persistit", "こんにちは", true);
        dbex.getKey().clear().append("arigatou");
        dbex.getValue().put("世界");
        dbex.store();
        // this one collates last
        dbex.getKey().clear().append("konnichiha");
        dbex.getValue().put("世界");
        dbex.store();
        dbex.getKey().clear().to(Key.BEFORE);
        while (dbex.next()) {
            System.out.println(dbex.getKey().reset().decode() 
                    + " " + dbex.getValue().get());
        }
        System.out.println("----");
        // now attempt to position to the right edge and find a key <= arigatou 
        dbex.getKey().clear().to(Key.AFTER);
        KeyFilter kf = new KeyFilter(new KeyFilter.Term[]{KeyFilter.rangeTerm(Key.BEFORE, "arigatou", false, true)});
        if (dbex.traverse(Key.LTEQ, kf, Value.DEFAULT_MAXIMUM_SIZE)) {
            // traverse worked as expected
            System.out.println(dbex.getKey().reset().decode() 
                    + " " + dbex.getValue().get() + " expected technique succeeded in reverse direction");
        } else {
            System.out.println("traverse from AFTER to arigatou failed");
        }
        // now attempt to position to the left edge and find a key >= konnichiha
        dbex.getKey().clear().to(Key.BEFORE);
        kf = new KeyFilter(new KeyFilter.Term[]{KeyFilter.rangeTerm("konnichiha", Key.AFTER, true, false)});
        if (dbex.traverse(Key.GTEQ, kf, Value.DEFAULT_MAXIMUM_SIZE)) {
            // traverse worked as expected
            System.out.println(dbex.getKey().reset().decode() 
                    + " " + dbex.getValue().get() + " expected technique succeeded in forward direction");
        }
    }
}
