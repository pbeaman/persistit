package com.persistit;

import junit.framework.TestCase;

public class SplitPolicyTest extends TestCase {
	
	public void testLeftBias() {
		Buffer nullBuffer = null;
		int mockLeftSize = 20;
		int capacity = 0;
		SplitPolicy leftBias = SplitPolicy.LEFT_BIAS;
		assertEquals("LEFT_BIAS", leftBias.toString());
		int measure = leftBias.splitFit(nullBuffer, 
				                        0, 
				                        0, 
				                        false, 
				                        mockLeftSize,
				                        0,
				                        0,
				                        0,
				                        capacity,
				                        0);
		/* splitFit should return 0 since leftSize is larger than capcity */
		assertEquals(0, measure);
		
		capacity = 21;
		measure = leftBias.splitFit(nullBuffer, 
                                    0, 
                                    0, 
                                    false, 
                                    mockLeftSize,
                                    0,
                                    0,
                                    0,
                                    capacity,
                                    0);
		/* splitFit just returns the given leftSize for LEFT_BIAS policy */
		assertEquals(mockLeftSize, measure); 
	}
	
	public void testRightBias() {
		Buffer nullBuffer = null;
		int mockRightSize = 20;
		int capacity = 0;
		SplitPolicy rightBias = SplitPolicy.RIGHT_BIAS;
		assertEquals("RIGHT_BIAS", rightBias.toString());
		int measure = rightBias.splitFit(nullBuffer, 
				                         0, 
				                         0, 
				                         false, 
				                         0,
				                         mockRightSize,
				                         0,
				                         0,
				                         capacity,
				                         0);
		/* splitFit should return 0 since rightSize is larger than capacity */
		assertEquals(0, measure);
		
		capacity = 21;
		measure = rightBias.splitFit(nullBuffer, 
                                     0, 
                                     0, 
                                     false, 
                                     0,
                                     mockRightSize,
                                     0,
                                     0,
                                     capacity,
                                     0);
		/* splitFit just returns the given rightSize for RIGHT_BIAS policy */
		assertEquals(mockRightSize, measure); 
	}
	
	public void testEvenBias() {
		Buffer nullBuffer = null;
		int mockRightSize = 20;
		int mockLeftSize = 20;
		int capacity = 0;
		SplitPolicy evenBias = SplitPolicy.EVEN_BIAS;
		assertEquals("EVEN_BIAS", evenBias.toString());
		int measure = evenBias.splitFit(nullBuffer, 
				                        0, 
				                        0, 
				                        false, 
				                        mockLeftSize,
				                        mockRightSize,
				                        0,
				                        0,
				                        capacity,
				                        0);
		/* splitFit should return 0 since rightSize & leftSize are larger than capacity */
		assertEquals(0, measure);
		
		capacity = 21;
		measure = evenBias.splitFit(nullBuffer, 
                                    0, 
                                    0, 
                                    false, 
                                    mockLeftSize,
                                    mockRightSize,
                                    0,
                                    0,
                                    capacity,
                                    0);
		/* splitFit returns (capacity - abs(rightSize - leftSize)) for EVEN_BIAS policy */
		assertEquals(capacity, measure);
		
		capacity = 21;
		mockLeftSize = 5;
		mockRightSize = 15;
		measure = evenBias.splitFit(nullBuffer, 
                                    0, 
                                    0, 
                                    false, 
                                    mockLeftSize,
                                    mockRightSize,
                                    0,
                                    0,
                                    capacity,
                                    0);
		/* splitFit returns (capacity - abs(rightSize - leftSize)) for EVEN_BIAS policy */
		assertEquals(11, measure); 
	}
	
	public void testNiceBias() {
		Buffer nullBuffer = null;
		int mockRightSize = 20;
		int mockLeftSize = 20;
		int capacity = 0;
		SplitPolicy niceBias = SplitPolicy.NICE_BIAS;
		assertEquals("NICE_BIAS", niceBias.toString());
		int measure = niceBias.splitFit(nullBuffer, 
				                        0, 
				                        0, 
				                        false, 
				                        mockLeftSize,
				                        mockRightSize,
				                        0,
				                        0,
				                        capacity,
				                        0);
		/* splitFit should return 0 since rightSize & leftSize are larger than capacity */
		assertEquals(0, measure);
		
		capacity = 21;
		measure = niceBias.splitFit(nullBuffer, 
                                    0, 
                                    0, 
                                    false, 
                                    mockLeftSize,
                                    mockRightSize,
                                    0,
                                    0,
                                    capacity,
                                    0);
		/* 
		 * splitFit returns ((capacity * 2) - abs((2 * rightSize) - leftSize))
		 * for EVEN_BIAS policy 
		 */
		assertEquals(22, measure);
		
		capacity = 21;
		mockLeftSize = 5;
		mockRightSize = 15;
		measure = niceBias.splitFit(nullBuffer, 
                                    0, 
                                    0, 
                                    false, 
                                    mockLeftSize,
                                    mockRightSize,
                                    0,
                                    0,
                                    capacity,
                                    0);
		/* 
		 * splitFit returns ((capacity * 2) - abs((2 * rightSize) - leftSize))
		 * for EVEN_BIAS policy 
		 */
		assertEquals(17, measure);
	}

}
