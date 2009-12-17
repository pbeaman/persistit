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
 * Created on Aug 17, 2004
 */
package com.persistit.tools;
import java.util.*;
import java.text.*;

import com.persistit.Util;

public class MakeKeyStringCoder
{

    public static void main(String[] args)
    {
        RuleBasedCollator rbc = (RuleBasedCollator)
            Collator.getInstance(
                Locale.US);
        String s = args[0];
        CollationKey ck = rbc.getCollationKey(s);
        byte[] bits = ck.toByteArray();
        System.out.println(Util.dump(bits, 0, bits.length));
    }
}
