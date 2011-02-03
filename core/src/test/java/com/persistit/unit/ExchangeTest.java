package com.persistit.unit;

import java.util.Random;

import com.persistit.Exchange;
import com.persistit.Volume;
import com.persistit.exception.PersistitException;
import com.persistit.exception.ConversionException;

public class ExchangeTest extends PersistitUnitTestCase {
	
	public void testAppend() throws PersistitException {
		Exchange ex = _persistit.getExchange("persistit", "tree", true);
		String mockValue = "PADRAIG";
		
		/* test boolean append */
		ex.clear().append(true);
		ex.getValue().put(mockValue);
		ex.store();
		ex.clear().append(true);
		ex.fetch();
		assertEquals(mockValue, ex.getValue().getString());
		
		/* test float append */
		float floatKey = 5.545454f;
		ex.clear().append(floatKey);
		ex.getValue().put(mockValue);
		ex.store();
		ex.clear().append(floatKey);
		ex.fetch();
		assertEquals(mockValue, ex.getValue().getString());
		
		/* test double append */
		double doubleKey = 6.66;
		ex.clear().append(doubleKey);
		ex.getValue().put(mockValue);
		ex.store();
		ex.clear().append(doubleKey);
		ex.fetch();
		assertEquals(mockValue, ex.getValue().getString());
		
		/* test int append */
		int intKey = 6;
		ex.clear().append(intKey);
		ex.getValue().put(mockValue);
		ex.store();
		ex.clear().append(intKey);
		ex.fetch();
		assertEquals(mockValue, ex.getValue().getString());
		
		/* test byte append */
		byte oneByte = 1;
		ex.clear().append(oneByte);
		ex.getValue().put(mockValue);
		ex.store();
		ex.clear().append(oneByte);
		ex.fetch();
		assertEquals(mockValue, ex.getValue().getString());
		
		/* test short append */
		short smallShort = 1234;
		ex.clear().append(smallShort);
		ex.getValue().put(mockValue);
		ex.store();
		ex.clear().append(smallShort);
		ex.fetch();
		assertEquals(mockValue, ex.getValue().getString());
	}
	
	public void testStringAppend() throws PersistitException {
		int initialLength = 256;
		String randomString = generateASCIIString(initialLength);
		Exchange ex = _persistit.getExchange("persistit", randomString, true);
		
		ex.clear().append(randomString);
		ex.getValue().put(randomString);
		ex.store();
		ex.clear().append(randomString);
		ex.fetch();
		assertEquals(randomString, ex.getValue().getString());
		
		/* lets double key length but keep value the same */
		initialLength *= 2;
		String randomKey = generateASCIIString(initialLength);
		ex.clear().append(randomKey);
		ex.getValue().put(randomString);
		ex.store();
		ex.clear().append(randomKey);
		ex.fetch();
		assertEquals(randomString, ex.getValue().getString());
		
		/* now lets keep doubling value length for kicks */
		for (int i = 0; i < 12; i++) {
		    initialLength *= 2;
		    String randomValue = generateASCIIString(initialLength);
		    ex.clear().append(randomKey);
		    ex.getValue().put(randomValue);
		    ex.store();
		    ex.clear().append(randomKey);
		    ex.fetch();
		    assertEquals(randomValue, ex.getValue().getString());
		}
		
		/* now double the key length */
		initialLength = 256;
		for (int i = 0; i < 2; i++) {
			initialLength *= 2;
			randomKey = generateASCIIString(initialLength);
			ex.clear().append(randomKey);
			ex.getValue().put(randomString);
			ex.store();
			ex.clear().append(randomKey);
			ex.fetch();
			assertEquals(randomString, ex.getValue().getString());
		}
		
		/* set key length to value larger than max and make sure exception is thrown */
		initialLength = 2048; // 2047 is max key length
		randomKey = generateASCIIString(initialLength);
		try {
		    ex.clear().append(randomKey);
		    fail("ConversionException should have been thrown");
		} catch (ConversionException expected) {}
	}
	
	public void testConstructors() throws PersistitException {
		try {
		    Exchange exchange = new Exchange(_persistit, "volume", "tree", true);
		    fail("NullPointerException should have been thrown for unknown Volume");
		} catch (NullPointerException expected) {}
		try {
			Volume nullVol = null;
			Exchange ex = new Exchange(_persistit, nullVol, "whatever", true);
			fail("NullPointerException should have been thrown for null Volume");
		} catch (NullPointerException expected) {}
	}
	
	/*
	 * This function is "borrowed" from the YCSB benchmark framework
	 * developed by Yahoo Research.
	 */
	private String generateASCIIString(int length) {
		int interval = '~'-' '+1;
		byte[] buf = new byte[length];
		Random random = new Random();
		random.nextBytes(buf);
		for (int i = 0; i < length; i++) {
			if (buf[i] < 0) {
				buf[i] = (byte) ((-buf[i] % interval) + ' ');
			} else {
				buf[i] = (byte) ((buf[i] % interval) + ' ');
			}
		}
		return new String(buf);
	}
	
	public void runAllTests() {
	}

}
