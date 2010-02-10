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
 * Created on Aug 15, 2004
 */
package com.persistit.unit;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.exception.PersistitException;

/**
 * @version 1.0
 */
public class KeyFilterTest2 extends PersistitUnitTestCase {

	private String ks(final int i) {
		return "abcdefghij".substring(i, i + 1);
	}

	@Test
	public void test1() throws PersistitException {
		System.out.print("test1 ");
		final Exchange ex = _persistit.getExchange("persistit", "KeyFilter2",
				true);
		final Key key = ex.getKey();
		ex.removeAll();
		for (int i = 0; i < 1000; i++) {
			ex.getValue().put("Value " + i);
			// 10 unique keys
			ex.clear().append(2).append(ks(i / 100));
			ex.store();
			// 100 unique keys
			ex.clear().append(2).append(ks(i / 100)).append(3).append(
					ks((i / 10) % 10));
			ex.store();
			if ((i % 2) == 0) {
				// 500 unique keys
				ex.clear().append(2).append(ks(i / 100)).append(3).append(
						ks((i / 10) % 10)).append(4).append(ks(i % 10));
				ex.store();
				// 500 unique keys
				ex.clear().append(2).append(ks(i / 100)).append(3).append(
						ks((i / 10) % 10)).append(5).append(ks(i % 10));
				ex.store();
				// 500 unique keys
				ex.clear().append(2).append(ks(i / 100)).append(3).append(
						ks((i / 10) % 10)).append(4).append(ks(i % 10)).append(
						5).append("x");
				ex.store();
			}
		}

		assertEquals(600, countKeys(ex, "{2,*,>3,*,4,*<}"));
		assertEquals(500, countKeys(ex, "{2,*,3,*,4,>*<}"));
		assertEquals(610, countKeys(ex, "{2,>*,3,*,4,*<}"));
		assertEquals(300, countKeys(ex, "{2,*,3,*,4,>[\"a\":\"e\"]<}"));
		assertEquals(10, countKeys(ex, "{2,*<}"));
		assertEquals(610, countKeys(ex, "{2,>*,3,*,5,*<}"));
		assertEquals(0, countKeys(ex, "{3,*,>3,*,4,*<}"));
		assertEquals(0, countKeys(ex, "{2,*,3,*,>6,*<}"));
		assertEquals(500, countKeys(ex, "{2,\"a\":\"z\",3,*,4,*,5,>\"x\"}"));
		assertEquals(90, countKeys(ex, "{2,{\"a\",\"b\",\"c\"},3,*,4,>[\"a\":\"e\"]<}"));
		assertEquals(300, countKeys(ex, "{2,*,3,*,4,>[\"a\":\"e\"]<}"));

		System.out.println("- done");
	}
	
	private int countKeys(final Exchange ex, String kfString) throws PersistitException {
		ex.clear();
		int count = 0;
		final KeyFilter kf = new KeyFilter(kfString);
		while (ex.traverse(Key.GT, kf, Integer.MAX_VALUE)) {
			count++;
		}
		return count;
		
	}

	public static void main(final String[] args) throws Exception {
		new KeyFilterTest2().initAndRunTest();
	}

	public void runAllTests() throws Exception {
		test1();

	}

}
