/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit;

public interface ValueHelper {

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
