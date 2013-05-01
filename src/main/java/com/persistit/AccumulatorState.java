/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
