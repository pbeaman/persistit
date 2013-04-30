/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
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

package com.persistit.ui;

import java.rmi.RemoteException;

import javax.swing.SwingUtilities;

import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.Management;
import com.persistit.Management.LogicalRecordCount;

/**
 * @version 1.0
 */
class ManagementSlidingTableModel extends ManagementTableModel {
    public final static int DEFAULT_ROW_CACHE_SIZE = 2048;
    public final static int DEFAULT_INITIAL_SIZE_ESTIMATE = 10000;
    public final static int SCROLL_BAR_ESTIMATE_MULTIPLIER = 3;
    public final static int MAXIMUM_GROWTH_ESTIMATE = 100000;
    public final static int DEFAULT_MAXIMUM_VALUE_SIZE = 400;
    public final static int MINIMUM_REQUESTED_ROW_COUNT = 256;

    /**
     * Offset from beginning of logical keyspace to the first row in the row
     * cache.
     */
    private int _offset;
    private boolean _deletingRows;

    /**
     * Size of _infoArray row cache
     */
    private final int _rowCacheSize = DEFAULT_ROW_CACHE_SIZE;

    private final int _maxValueSize = DEFAULT_MAXIMUM_VALUE_SIZE;
    /**
     * Current estimate of total row count
     */
    private int _currentRowCount;
    /**
     * Indicates whether the total size is definite, meaning that we have
     * traversed to the end of the logical key space and discovered the actual
     * count.
     */
    private boolean _definite;
    /**
     * Count of valid rows in the row cache.
     */
    private int _valid = 0;

    /**
     * The volume name
     */
    private String _volumeName;
    /**
     * The tree name
     */
    private String _treeName;
    /**
     * The KeyFilter, represented as a string. If null, then all keys in the
     * tree are selected.
     */
    private String _keyFilterString;

    /**
     * Indicates that the row cache is unstable because a fetch operation is
     * currently pending.
     */
    private boolean _waiting;

    /**
     * Construct a TableModel to display one of the management info array
     * structures such as BufferInfo[]. Each column is described by a property
     * defined in the <code>ResourceBundle</code> associated with this class.
     * The property name is constructed from the last "." piece of the name of
     * the supplied class and the column index as follows: <code><pre>
     * property Name ::= 
     *      <i>classNameTail</i>.column.<i>columnIndex</i>
     * </pre></code> For example, the specification for the first column of the
     * {@link com.persistit.Management.BufferInfo} class is <code><pre>
     *      BufferInfo.column.0
     * </pre></code> The value associated with this property has the following
     * structure: <code><pre>
     * property Value ::=
     *      <i>accessorMethodName</i>:<i>width</i>:<i>flags</i>:<i>header</i>
     * </code></pre> For example <code><pre>
     *      getRightSiblingAddress:10:A:Right Pointer
     * </code></pre> where the <i>accessorMethodName</code> is simply a method
     * name in the supplied class (the method must take no arguments), the
     * <i>width</i> is a percentage of the total table width, and
     * <i>justification</i> is L, C or R.
     * 
     * @param clazz
     */

    public ManagementSlidingTableModel(final Class clazz, final String className, final AdminUI ui)
            throws NoSuchMethodException {
        super(clazz, className, ui);
    }

    void reset() {
        _volumeName = null;
        _treeName = null;
        _keyFilterString = null;
        _valid = 0;
        _offset = 0;
        _currentRowCount = DEFAULT_INITIAL_SIZE_ESTIMATE;
        _definite = false;
    }

    void set(final String volumeName, final String treeName, final String keyFilterString) {
        reset();
        _volumeName = volumeName;
        _treeName = treeName;
        _keyFilterString = keyFilterString;
        fireTableDataChanged();
    }

    boolean isDeletingRows() {
        return _deletingRows;
    }

    @Override
    public int getRowCount() {
        return _currentRowCount;
    }

    @Override
    public Object getValueAt(final int row, final int col) {
        // if ((row % 1000) == 0 && col == 0) System.out.println("Getting row "
        // + row);
        if (row < 0 || (_definite && row >= _currentRowCount)) {
            return null;
        } else if (col >= 0 && (row < _offset || row >= _offset + _valid)) {
            return fetch(row);
        } else {
            return super.getValueAt(row - _offset, col);
        }
    }

    private synchronized Object fetch(final int row) {
        if (_waiting) {
            return _adminUI.getWaitingMessage();
        }
        _waiting = true;

        int fromOffset;
        int requestedCount;
        int skipCount = 0;
        boolean forward;

        if (_valid == 0) {
            forward = true;
            fromOffset = -1;
            requestedCount = row + 1;
        } else if (row >= _offset + _valid) {
            forward = true;
            fromOffset = _offset + _valid - 1;
            requestedCount = row - (_offset + _valid) + 1;
        } else if (row < _offset && row > _offset - row) {
            forward = false;
            requestedCount = _offset - row;
            fromOffset = _offset;
        } else {
            forward = true;
            fromOffset = -1;
            requestedCount = row + 1;
        }

        if (requestedCount < MINIMUM_REQUESTED_ROW_COUNT) {
            requestedCount = MINIMUM_REQUESTED_ROW_COUNT;
        } else {
            if (requestedCount > _rowCacheSize) {
                skipCount = requestedCount - (_rowCacheSize / 2);
                requestedCount = _rowCacheSize;
            }
        }

        // System.out.println(
        // "new Fetcher(forward=" + forward +
        // ",fromOffset=" + fromOffset + ",requestedCount=" + requestedCount +
        // ",skipCount=" + skipCount + "): _offset=" + _offset +
        // " and _valid=" + _valid);
        final Fetcher fetcher = new Fetcher(forward, fromOffset, requestedCount, skipCount);
        new Thread(fetcher).start();
        return _adminUI.getWaitingMessage();
    }

    private synchronized void receiveData(final Fetcher fetcher) {
        _waiting = false;
        if (fetcher._exception != null) {
            _adminUI.postException(fetcher._exception);
        }
        if (fetcher._resultRows == null)
            return;

        if (_infoArray == null || _infoArray.length != _rowCacheSize) {
            _infoArray = new Object[_rowCacheSize];
            _offset = 0;
            _valid = 0;
        }

        final int count = fetcher._resultRows.length;
        int newValid = _valid + count;

        // System.out.println(
        // "receivedData(_offset=" + _offset +
        // ",_valid=" + _valid +
        // ",count=" + count +
        // ",fetcher._from=" + fetcher._from +
        // ",fetcher._forward=" + fetcher._forward +
        // ",fetcher._skipCount=" + fetcher._skipCount +
        // ",fetcher._requestedCount=" + fetcher._requestedCount
        // );

        final int oldOffset = _offset;
        int newOffset = oldOffset;
        final int oldRowCount = _currentRowCount;
        int firstUpdatedRow;
        int lastUpdatedRow;

        int lost = 0; // rows lost from row cache
        int kept = _valid; // rows kept from row cache
        int cut = 0; // rows cut from fetcher._resultRows

        if (fetcher._forward) {
            if (fetcher._from != _offset + _valid - 1) {
                _valid = 0;
                kept = 0;
                newValid = count;
                newOffset = fetcher._from + 1;
            }
            if (newValid > _rowCacheSize) {
                lost = newValid - _rowCacheSize;
                newOffset += lost;

                if (lost < _valid) {
                    kept = _valid - lost;
                    System.arraycopy(_infoArray, lost, _infoArray, 0, kept);
                } else {
                    cut = lost - _valid;
                    kept = 0;
                }
                newValid = _rowCacheSize;
            }
            System.arraycopy(fetcher._resultRows, cut, _infoArray, kept, count - cut);

            firstUpdatedRow = newOffset + kept;
            lastUpdatedRow = firstUpdatedRow + count - cut - 1;

            if (count < fetcher._requestedCount) {
                changeRowCount(newOffset + newValid, true);
            }
        } else {
            if (fetcher._from != _offset) {
                _valid = 0;
                kept = 0;
                newValid = count;
                newOffset = fetcher._from - count;
            } else {
                newOffset = oldOffset - count;
            }
            if (newValid > _rowCacheSize) {
                lost = newValid - _rowCacheSize;
                if (lost < _valid) {
                    kept = _valid - lost;
                    System.arraycopy(_infoArray, 0, _infoArray, _valid - kept, kept);
                } else {
                    cut = lost - _valid;
                    kept = 0;
                }
                newValid = _rowCacheSize;
            }
            System.arraycopy(fetcher._resultRows, 0, _infoArray, 0, count - cut);

            firstUpdatedRow = newOffset;
            lastUpdatedRow = firstUpdatedRow + count - cut - 1;

            if (count < fetcher._requestedCount) {
                _definite = false;
                _currentRowCount = DEFAULT_INITIAL_SIZE_ESTIMATE;
                _offset = 0;
            }
        }
        _offset = newOffset;
        _valid = newValid;

        if (!_definite) {
            final int receivedRowCount = newOffset + newValid;
            int estimatedRowCount = (newOffset + _valid) * SCROLL_BAR_ESTIMATE_MULTIPLIER;
            if (estimatedRowCount - receivedRowCount > MAXIMUM_GROWTH_ESTIMATE) {
                estimatedRowCount = receivedRowCount + MAXIMUM_GROWTH_ESTIMATE;
            }

            if (_currentRowCount < estimatedRowCount && _offset + _valid > _currentRowCount) {
                changeRowCount(estimatedRowCount, false);
            }
        }
        fireTableRowsUpdated(0, _currentRowCount - 1);
    }

    private void changeRowCount(final int newRowCount, final boolean definite) {
        // System.out.println(
        // "Changing row count from " + _currentRowCount +
        // " to " + newRowCount +
        // " definite=" + definite);

        _deletingRows = true;

        final int oldRowCount = _currentRowCount;
        _definite = definite;
        _currentRowCount = newRowCount;

        if (oldRowCount < newRowCount) {
            // System.out.println("fireTableRowsInserted(" + oldRowCount + "," +
            // (newRowCount - 1) + ")");
            fireTableRowsInserted(oldRowCount, newRowCount - 1);
        } else if (oldRowCount > newRowCount) {
            // System.out.println("fireTableRowsDeleted(" + newRowCount + "," +
            // (oldRowCount - 1) + ")");
            fireTableRowsDeleted(newRowCount, oldRowCount - 1);
        }

        _deletingRows = false;
    }

    private class Fetcher implements Runnable {
        boolean _forward;
        int _from;
        int _requestedCount;
        int _skipCount;

        Object[] _resultRows;
        Exception _exception;

        Fetcher(final boolean forward, final int from, final int requestedCount, final int skipCount) {
            _forward = forward;
            _from = from;
            _requestedCount = requestedCount;
            _skipCount = skipCount;
        }

        @Override
        public void run() {
            Management.LogicalRecord rec = null;
            if (_from != -1) {
                rec = (Management.LogicalRecord) _infoArray[_from - _offset];
            }
            KeyState ks = rec == null ? KeyState.LEFT_GUARD_KEYSTATE : rec.getKeyState();
            final Management management = _adminUI.getManagement();
            _resultRows = new Management.LogicalRecord[0];
            if (management != null) {
                try {
                    if (_skipCount > 0) {
                        final LogicalRecordCount lrc = management.getLogicalRecordCount(_volumeName, _treeName,
                                _keyFilterString, ks, _forward ? Key.GT : Key.LT, _skipCount);
                        final int skipped = lrc.getCount();
                        if (skipped > 0) {
                            ks = lrc.getKeyState();
                            _from = _forward ? _from + skipped : _from - skipped;
                        }
                    }

                    _resultRows = management.getLogicalRecordArray(_volumeName, _treeName, _keyFilterString, ks,
                            _forward ? Key.GT : Key.LT, _requestedCount, _maxValueSize, true);
                } catch (final RemoteException remoteException) {
                    // TODO
                    _exception = remoteException;
                }
            }
            SwingUtilities.invokeLater(new Runnable() {
                @Override
                public void run() {
                    receiveData(Fetcher.this);
                }
            });
        }
    }

}
