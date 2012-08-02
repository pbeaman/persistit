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


/**
 * State of an accumulator recovered from a checkpoint.
 * 
 * @author peter
 */
final class AccumulatorState {

    String _treeName;
    int _index;
    Accumulator.Type _type;
    long _value;

    /**
     * Deserialize stored statistics values from the supplied byte array
     * starting at <code>index</code>.
     * 
     * @param bytes
     *            serialized statistics
     * @param index
     *            at which serialized statistics start in the byte array
     */
    void load(final Value value) {
        _treeName = value.getString();
        _index = value.getInt();
        _type = Accumulator.Type.valueOf(value.getString());
        _value = value.getLong();
    }

    @Override
    public String toString() {
        return String.format("Accumulator(tree=%s index=%d type=%s value=%,d)", _treeName, _index, _type, _value);
    }
    
    public String getTreeName() {
        return _treeName;
    }
    
    public int getIndex() {
        return _index;
    }
    
    public Accumulator.Type getType() {
        return _type;
    }
    
    public long getValue() {
        return _value;
    }
}
