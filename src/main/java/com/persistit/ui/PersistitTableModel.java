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

import java.awt.Component;
import java.awt.Font;
import java.awt.event.MouseEvent;
import java.io.IOException;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JTable;
import javax.swing.SwingConstants;
import javax.swing.event.TableModelEvent;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;

public class PersistitTableModel extends AbstractTableModel {
    private final static int SCROLL_EXTRA = 100;
    private final static float SCROLL_FACTOR = 1.3f;
    private final static int ROW_CACHE_SIZE = 100;
    private final static int INITIAL_ROW_COUNT_ESTIMATE = 10000;

    private final static String[] COLUMN_HEADER_NAMES = { "Index", "Key", "Type", "Value", };

    private final static int[] COLUMN_HEADER_WIDTHS = { 50, 450, 50, 450, };

    private final static int COLUMN_COUNT = COLUMN_HEADER_NAMES.length;
    /**
     * The Exchange on which the display will be based.
     */
    private Exchange _exchange;
    /**
     * The root key for this display.
     */
    private Key _rootKey;
    /**
     * The Row that corresponds to the root Key.
     */
    private final Row _rootRow = new Row(new byte[0]);
    /**
     * A list of contiguous rows. The first element in the list corresponds with
     * the row index in _rowCacheOffset.
     */
    private final RowCache _rowCache = new RowCache();
    /**
     * There are at least this many rows.
     */
    private int _rowCountActual;
    /**
     * The last row count estimate that was returned by getRowCount().
     */
    private int _rowCountEstimate;
    /**
     * Count of getValue operations satisfied from cache
     */
    private int _rowGetValueHits;
    /**
     * Count of getValue operations
     */
    private int _rowGetValueOperations;

    /**
     * Establishes the supplied Exchange as the root of a display tree.
     * 
     * @param exchange
     */
    public void setExchange(final Exchange exchange) {
        _exchange = exchange;
        _rootKey = new Key(_exchange.getKey());
        _rowCache.clear();
        _rowCountActual = -1;
        _rowCountEstimate = INITIAL_ROW_COUNT_ESTIMATE;
        fireTableChanged(new TableModelEvent(this));
    }

    public Exchange getExchange() {
        return _exchange;
    }

    @Override
    public Object getValueAt(final int rowIndex, final int columnIndex) {
        if (rowIndex < 0) {
            throw new IllegalArgumentException("rowIndex=" + rowIndex);
        }

        if (_rowCountActual >= 0 && rowIndex > _rowCountActual) {
            return _rootRow;
        }

        _rowGetValueOperations++;

        try {
            Row row = _rowCache.get(rowIndex);
            if (row != null) {
                _rowGetValueHits++;
                return row;
            }

            int minCachedIndex = _rowCache.getFirstIndex();
            int maxCachedIndex = _rowCache.getLastIndex();

            if (rowIndex < minCachedIndex) {
                // (1) are we going to get the row by backing up
                // or by starting at the beginning?
                //
                if (rowIndex > (minCachedIndex / 2)) {
                    row = _rowCache.get(minCachedIndex);
                    for (int index = minCachedIndex; index > rowIndex;) {
                        if (row == null)
                            return null;
                        row = row.getNextRow(false);
                        index--;
                        if (row == null) {
                            row = _rootRow;
                        }
                        _rowCache.put(index, row);
                    }
                    return row;
                } else {
                    maxCachedIndex = 0;
                    minCachedIndex = 0;
                }
            }
            int index;
            if (maxCachedIndex == 0) {
                index = 0;
                row = _rootRow;
                if (rowIndex == 0)
                    _rowCache.put(index, row);
            } else {
                index = maxCachedIndex - 1;
                row = _rowCache.get(index);
            }
            for (; index < rowIndex;) {
                if (row == null)
                    return null;
                row = row.getNextRow(true);
                index++;

                if (row == null) {
                    final boolean changed = index != _rowCountEstimate;
                    // we are at the end!
                    final int previousEstimate = _rowCountEstimate;
                    _rowCountEstimate = index;
                    _rowCountActual = index;
                    if (changed)
                        fireSizeChanged(previousEstimate);
                    return null;
                } else
                    _rowCache.put(index, row);
            }
            if (_rowCountActual < 0 && _rowCountEstimate - rowIndex < SCROLL_EXTRA) {
                final int previousEstimate = _rowCountEstimate;
                _rowCountEstimate = (int) ((_rowCountEstimate + SCROLL_EXTRA) * SCROLL_FACTOR);
                fireSizeChanged(previousEstimate);
            }
            return row;
        } catch (final PersistitException de) {
            return de;
        }
    }

    @Override
    public int getRowCount() {
        return _rowCountEstimate;
    }

    @Override
    public int getColumnCount() {
        return COLUMN_COUNT;
    }

    /**
     * Sets up the TableColumnModel for a supplied JTable
     * 
     * @param table
     */
    public void setupColumns(final JTable table) {
        final TableColumnModel tcm = table.getColumnModel();
        final int count = tcm.getColumnCount();
        final String[] headers = COLUMN_HEADER_NAMES;
        final int[] widths = COLUMN_HEADER_WIDTHS;
        final TableCellRenderer renderer = new ExchangeTableCellRenderer();

        for (int i = 0; i < headers.length; i++) {
            TableColumn tc;
            if (i >= count) {
                tc = new TableColumn();
                tcm.addColumn(tc);
            } else {
                tc = tcm.getColumn(i);
            }
            tc.setHeaderValue(headers[i]);
            tc.setPreferredWidth(widths[i]);
            tc.setCellRenderer(renderer);
        }
    }

    private void fireSizeChanged(final int previousEstimate) {
        if (_rowCountEstimate > previousEstimate) {
            fireTableRowsInserted(previousEstimate, _rowCountEstimate - 1);
        } else if (_rowCountEstimate < previousEstimate) {
            fireTableRowsDeleted(_rowCountEstimate, previousEstimate - 1);
        }
    }

    //
    // Helper classes
    //

    private class Row {
        private final byte[] _keyBytes;
        private int _index = -1; // TODO - debugging

        private Row(final byte[] keyBytes) {
            _keyBytes = keyBytes;
        }

        /**
         * Determines whether children of the Key described by this Row should
         * be displayed.
         * 
         * @return <i>true</i> if the children of this Row should be displayed.
         */
        private boolean isExpanded() {
            return true;
        }

        private Row getNextRow(final boolean forward) throws PersistitException {
            final Exchange ex = setupExchange();
            final boolean expanded = (isExpanded());
            if (!ex.traverse(forward ? Key.GT : Key.LT, expanded)) {
                return null;
            }
            final Key key = ex.getKey();
            if (key.compareKeyFragment(_rootKey, 0, _rootKey.getEncodedSize()) != 0) {
                return null;
            }
            final int keyBytesSize = key.getEncodedSize() - _rootKey.getEncodedSize();
            final byte[] keyBytes = new byte[keyBytesSize];
            System.arraycopy(key.getEncodedBytes(), _rootKey.getEncodedSize(), keyBytes, 0, keyBytesSize);
            return new Row(keyBytes);
        }

        private String getValueTypeString() {
            try {
                final Exchange ex = setupExchange();
                if (ex.getKey().getEncodedSize() == 0)
                    return "";
                ex.fetch();
                final Value value = ex.getValue();
                if (!value.isDefined())
                    return "undefined";
                return value.getType().getName();
            } catch (final PersistitException de) {
                return de.toString();
            }
        }

        private String getValueString() {
            try {
                final Exchange ex = setupExchange();
                if (ex.getKey().getEncodedSize() == 0)
                    return "";
                ex.fetch();
                final Value value = ex.getValue();
                if (value.getEncodedSize() > 50000) {
                    return "Too long to display: " + value.getEncodedSize();
                } else {
                    return value.toString();
                }
            } catch (final PersistitException de) {
                return de.toString();
            }
        }

        private Key getKey() {
            final Exchange ex = setupExchange();
            return ex.getKey();
        }

        private Value getValue() throws PersistitException, IOException {
            final Exchange ex = setupExchange();
            ex.fetch();
            return ex.getValue();
        }

        private String getKeyString() {
            final Exchange ex = setupExchange();
            ex.getKey().setIndex(0);
            return ex.getKey().toString();
        }

        private Exchange setupExchange() {
            final Exchange ex = PersistitTableModel.this.getExchange();
            final Key key = ex.getKey();
            System.arraycopy(_rootKey.getEncodedBytes(), 0, key.getEncodedBytes(), 0, _rootKey.getEncodedSize());
            System.arraycopy(_keyBytes, 0, key.getEncodedBytes(), _rootKey.getEncodedSize(), _keyBytes.length);
            key.setEncodedSize(_rootKey.getEncodedSize() + _keyBytes.length);
            return ex;
        }

        @Override
        public String toString() {
            return _index + ": " + setupExchange().getKey().toString() + "-->" + getValueString();
        }
    }

    private static class RowCache {
        /**
         * Array of cached rows, organized as a ring buffer.
         */
        private final Row[] _cache = new Row[ROW_CACHE_SIZE];
        /**
         * Offset of lowest indexed Row in the cache.
         */
        private int _tail = 0;
        /**
         * Offset one past highest indexed Row in the cache.
         */
        private int _head = 0;
        /**
         * The index within the table of the lowest indexed Row in the cache.
         */
        private int _firstCachedIndex = 0;

        private void clear() {
            _tail = 0;
            _head = 0;
            for (int index = 0; index < ROW_CACHE_SIZE; index++) {
                _cache[index] = null;
            }
        }

        private int getFirstIndex() {
            return _firstCachedIndex;
        }

        private int getLastIndex() {
            return _firstCachedIndex + size();
        }

        private int size() {
            int size = _head - _tail;
            if (size < 0)
                size += ROW_CACHE_SIZE;
            return size;
        }

        private Row get(int index) {
            index -= _firstCachedIndex;
            if (index < 0)
                return null;
            if (index >= size())
                return null;
            index = (index + _tail) % ROW_CACHE_SIZE;
            return _cache[index];
        }

        private void put(int index, final Row row) {
            row._index = index;
            index -= _firstCachedIndex;
            final int size = size();
            if (index == -1) {
                _tail--;
                if (_tail < 0)
                    _tail += ROW_CACHE_SIZE;
                if (_head == _tail) {
                    _head--;
                    if (_head < 0)
                        _head += ROW_CACHE_SIZE;
                }
                _firstCachedIndex--;
                _cache[_tail] = row;
            } else if (index == size) {
                _cache[_head] = row;
                _head++;
                if (_head == ROW_CACHE_SIZE)
                    _head = 0;
                if (_head == _tail) {
                    _cache[_tail] = null;
                    _tail++;
                    if (_tail == ROW_CACHE_SIZE)
                        _tail = 0;
                    _firstCachedIndex++;
                }
            } else if (index >= 0 && index < size) {
                index -= _tail;
                if (index < 0)
                    index += ROW_CACHE_SIZE;
                _cache[index] = row;
            } else {
                clear();
                _cache[0] = row;
                _head++;
                _firstCachedIndex += index;
            }
        }
    }

    private static class ExchangeTableCellRenderer extends DefaultTableCellRenderer {
        private ExchangeTableCellRenderer() {
            setFont(new Font("Lucida", Font.PLAIN, 13));
            setBorder(BorderFactory.createEmptyBorder(1, 4, 1, 4));
        }

        @Override
        public Component getTableCellRendererComponent(final JTable table, final Object value,
                final boolean isSelected, final boolean hasFocus, final int rowIndex, final int columnIndex) {

            final JLabel label = (JLabel) super.getTableCellRendererComponent(table, value, isSelected, hasFocus,
                    rowIndex, columnIndex);

            final Row row = (Row) value;
            if (row == null) {
                label.setText("");
            } else
                switch (columnIndex) {
                case 0: {
                    label.setText(Integer.toString(rowIndex));
                    label.setHorizontalAlignment(SwingConstants.RIGHT);
                    break;
                }
                case 1: {
                    label.setText(row.getKeyString());
                    label.setHorizontalAlignment(SwingConstants.LEFT);
                    break;
                }

                case 2: {
                    label.setText(row.getValueTypeString());
                    label.setHorizontalAlignment(SwingConstants.CENTER);
                    break;
                }

                case 3: {
                    label.setText(row.getValueString());
                    label.setHorizontalAlignment(SwingConstants.LEFT);
                    break;
                }
                default: {
                    label.setText("Unknown column " + columnIndex);
                }
                }
            return label;
        }

        @Override
        protected void setValue(final Object value) {
        }

    }

    public JTable createTreeDisplayTable() {
        final TreeDisplayTable tdt = new TreeDisplayTable(this);
        setupColumns(tdt);
        return tdt;
    }

    private static class TreeDisplayTable extends JTable {
        TreeDisplayTable(final PersistitTableModel dtm) {
            super(dtm);
        }

        @Override
        public String getToolTipText(final MouseEvent me) {
            final int row = rowAtPoint(me.getPoint());
            final int col = columnAtPoint(me.getPoint());
            if (row >= 0 && col >= 0) {
                final Object value = getModel().getValueAt(row, col);
                if (value != null) {
                    final TableCellRenderer tcr = getCellRenderer(row, col);
                    final Component rc = tcr.getTableCellRendererComponent(this, value, false, false, row, col);
                    if (rc instanceof JLabel) {
                        final String s = ((JLabel) rc).getText();
                        if (s.length() > 20)
                            return s;
                    }
                }
            }
            return null;
        }
    }

}
