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
 * Created on Aug 24, 2004
 */
package com.persistit.unit;

import java.io.IOException;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;

public class JoinTest1 extends PersistitUnitTestCase {

    String _volumeName = "persistit";

    @Test
    public void test1() throws PersistitException {
        // Tests join calculation.
        //
        System.out.print("test1 ");

        final StringBuffer sb = new StringBuffer(4000);
        final Exchange exchange =
            _persistit.getExchange(_volumeName, "SimpleTest1BadJoin", true);
        exchange.removeAll();
        final Key key = exchange.getKey();
        final Value value = exchange.getValue();

        key.clear().append("A").append(0);
        setupString(sb, 1675);
        value.putString(sb);
        exchange.store();

        key.clear().append("A").append(1);
        setupString(sb, 1000);
        value.putString(sb);
        exchange.store();

        key.clear().append("A").append(2);
        setupString(sb, 1000);
        value.putString(sb);
        exchange.store();

        key.clear().append("A").append(3);
        setupString(sb, 1000);
        value.putString(sb);
        exchange.store();

        key.clear().append("A").append(4);
        setupString(sb, 1000);
        value.putString(sb);
        exchange.store();

        key
            .clear()
            .append("B")
            .append(
                "... a pretty long key value. The goal is to get the the record "
                    + "for this key into the penultimate slot of the left page, followed "
                    + "by a short key on the edge.  Then delete that short key, so that"
                    + "this becomes the edge key.");
        setupString(sb, 1);
        value.putString(sb);
        exchange.store();

        // Here's where we want the page to split...
        key.clear().append("B").append("z");
        setupString(sb, 102);
        value.putString(sb);
        exchange.store();

        key.clear().append("C").append(0);
        setupString(sb, 720);
        value.putString(sb);
        exchange.store();

        key.clear().append("C").append(1);
        setupString(sb, 1000);
        value.putString(sb);
        exchange.store();

        key.clear().append("C").append(2);
        setupString(sb, 1000);
        value.putString(sb);
        exchange.store();

        key.clear().append("A").append(3);
        setupString(sb, 1000 + 2160);
        value.putString(sb);
        exchange.store();

        key.clear().append("C").append(1);
        setupString(sb, 1000 + 2640);
        value.putString(sb);
        exchange.store();

        key.clear().append("C").append(2);
        setupString(sb, 1000 + 2640);
        value.putString(sb);
        exchange.store();

         key.clear().append("C").append(3);
         setupString(sb, 1000);
         value.putString(sb);
         exchange.store();
                
         for (int len = 1000; len < 1600; len += 1)
         {
         key.clear().append("A").append(1);
         setupString(sb, len);
         value.putString(sb);
         exchange.store();
        
         key.clear().append("A").append(2);
         setupString(sb, len);
         value.putString(sb);
         exchange.store();
        
         key.clear().append("A").append(3);
         setupString(sb, len);
         value.putString(sb);
         exchange.store();
        
         key.clear().append("A").append(4);
         setupString(sb, len);
         value.putString(sb);
         exchange.store();
        
         key.clear().append("C").append(1);
         setupString(sb, len);
         value.putString(sb);
         exchange.store();
        
         key.clear().append("C").append(2);
         setupString(sb, len);
         value.putString(sb);
         exchange.store();
        
         }
                
         // Now the page should be split with the {"B", "z"} on the edge.
        
        key.clear().append("A").append(1);
        exchange.fetch();

        key.clear().append("B").append("z");
        exchange.fetch();

        key.clear().append("C").append(3);
        exchange.fetch();

        key.clear().append("B").append("z");
        exchange.remove(); // may cause wedge failure.

        System.out.println("- done");
    }

    void setupString(final StringBuffer sb, final int length) {
        sb.setLength(length);
        final String s = "length=" + length;
        sb.replace(0, s.length(), s);
        for (int i = s.length(); i < length; i++) {
            sb.setCharAt(i, ' ');
        }
    }

    public static void pause(final String prompt) {
        System.out.print(prompt + "  Press ENTER to continue");
        System.out.flush();
        try {
            while (System.in.read() != '\r') {
            }
        } catch (final IOException ioe) {
        }
        System.out.println();
    }

    public static void main(final String[] args) throws Exception {
        new JoinTest1().initAndRunTest();
    }

    public void runAllTests() throws Exception {
        test1();
    }
}
