package com.persistit;

import java.util.concurrent.atomic.AtomicInteger;

public class SessionId {

    final static AtomicInteger counter = new AtomicInteger(1);
    
    private final int _id = counter.getAndIncrement();
    
    @Override
    public boolean equals(final Object id) {
        return this._id == ((SessionId)id)._id;
    }
    
    @Override
    public int hashCode() {
        return _id;
    }
    
    @Override
    public String toString() {
        return "[" + _id + "]";
    }
}
