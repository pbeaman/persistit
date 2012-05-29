/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
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
        public int requiredLength(byte[] target, int targetOffset, int targetLength) {
            return 0;
        }

        @Override
        public int storeVersion(byte[] target, int targetOffset, int targetLength, int targetLimit) {
            return 0;
        }

        @Override
        public void saveValue(byte[] target, int targetOffset, int targetLength) {
        }

        @Override
        public long getPointerValue() {
            return 0;
        }

        @Override
        public void setPointerValue(long poiner) {
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
        public int storeVersion(byte[] target, int targetOffset, int targetLength, int targetLimit) {
            System.arraycopy(_value.getEncodedBytes(), 0, target, targetOffset, _value.getEncodedSize()); // TODO
                                                                                                          // -
                                                                                                          // limit
            return _value.getEncodedSize();
        }

        @Override
        public void saveValue(byte[] target, int targetOffset, int targetLength) {
        }

        @Override
        public int requiredLength(byte[] target, int targetOffset, int targetLength) {
            return _value.getEncodedSize();
        }

        @Override
        public long getPointerValue() {
            return _value.getPointerValue();
        }

        @Override
        public void setPointerValue(long pointer) {
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
        public int storeVersion(byte[] target, int targetOffset, int targetLength, int targetLimit) {
            return MVV.storeVersion(target, targetOffset, targetLength, target.length, _version, _value
                    .getEncodedBytes(), 0, _value.getEncodedSize());
        }

        @Override
        public void saveValue(byte[] target, int targetOffset, int targetLength) {
        }

        @Override
        public int requiredLength(byte[] target, int targetOffset, int targetLength) {
            return MVV.exactRequiredLength(target, targetOffset, targetLength, _version, _value.getEncodedSize());
        }

        @Override
        public long getPointerValue() {
            return _value.getPointerValue();
        }

        @Override
        public void setPointerValue(long pointer) {
            _value.setPointerValue(pointer);
        }

        @Override
        public boolean isMVV() {
            return true;
        }
    };
}
