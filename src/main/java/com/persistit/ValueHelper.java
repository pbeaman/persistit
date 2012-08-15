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

interface ValueHelper {

    int requiredLength(final byte[] target, int targetOffset, int targetLength);

    int storeVersion(byte[] target, int targetOffset, int targetLength, int targetLimit);

    void saveValue(byte[] target, int targetOffset, int targetLength);

    long getPointerValue();

    void setPointerValue(long poiner);

    boolean isMVV();

    final static ValueHelper EMPTY_VALUE_WRITER = new ValueHelper() {

        @Override
        public int requiredLength(final byte[] target, final int targetOffset, final int targetLength) {
            return 0;
        }

        @Override
        public int storeVersion(final byte[] target, final int targetOffset, final int targetLength,
                final int targetLimit) {
            return 0;
        }

        @Override
        public void saveValue(final byte[] target, final int targetOffset, final int targetLength) {
        }

        @Override
        public long getPointerValue() {
            return 0;
        }

        @Override
        public void setPointerValue(final long poiner) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isMVV() {
            return false;
        }

    };

    static class RawValueWriter implements ValueHelper {
        Value _value;

        void init(final Value value) {
            _value = value;
        }

        @Override
        public int storeVersion(final byte[] target, final int targetOffset, final int targetLength,
                final int targetLimit) {
            System.arraycopy(_value.getEncodedBytes(), 0, target, targetOffset, _value.getEncodedSize()); // TODO
                                                                                                          // -
                                                                                                          // limit
            return _value.getEncodedSize();
        }

        @Override
        public void saveValue(final byte[] target, final int targetOffset, final int targetLength) {
        }

        @Override
        public int requiredLength(final byte[] target, final int targetOffset, final int targetLength) {
            return _value.getEncodedSize();
        }

        @Override
        public long getPointerValue() {
            return _value.getPointerValue();
        }

        @Override
        public void setPointerValue(final long pointer) {
            _value.setPointerValue(pointer);
        }

        @Override
        public boolean isMVV() {
            return false;
        }
    };

    static class MVVValueWriter implements ValueHelper {
        Value _value;
        Value _spareValue;
        long _version;

        void init(final Value value, final Value spareValue, final long version) {
            _value = value;
            _spareValue = spareValue;
            _version = version;
        }

        @Override
        public int storeVersion(final byte[] target, final int targetOffset, final int targetLength,
                final int targetLimit) {
            return MVV.storeVersion(target, targetOffset, targetLength, target.length, _version,
                    _value.getEncodedBytes(), 0, _value.getEncodedSize());
        }

        @Override
        public void saveValue(final byte[] target, final int targetOffset, final int targetLength) {
        }

        @Override
        public int requiredLength(final byte[] target, final int targetOffset, final int targetLength) {
            return MVV.exactRequiredLength(target, targetOffset, targetLength, _version, _value.getEncodedSize());
        }

        @Override
        public long getPointerValue() {
            return _value.getPointerValue();
        }

        @Override
        public void setPointerValue(final long pointer) {
            _value.setPointerValue(pointer);
        }

        @Override
        public boolean isMVV() {
            return true;
        }
    };
}
