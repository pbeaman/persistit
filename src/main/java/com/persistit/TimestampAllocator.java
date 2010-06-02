package com.persistit;

import java.util.concurrent.atomic.AtomicLong;

public class TimestampAllocator {

	private AtomicLong timestamp = new AtomicLong();
	
	public long timestamp() {
		return timestamp.incrementAndGet();
	}
}
