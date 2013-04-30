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

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.text.DecimalFormat;

import javax.swing.AbstractAction;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JToggleButton;
import javax.swing.table.AbstractTableModel;

import com.persistit.Management;
import com.persistit.Management.BufferInfo;
import com.persistit.Management.BufferPoolInfo;

public class PoolDisplayPanel extends JPanel {
    private final static String[] HEADER_NAMES = { "Index", "Page", "Type", "Status", "Writer Thread",
            "Available Space", "Right Sibling", };

    private final Management _management;
    private final BufferTableModel _btm = new BufferTableModel();

    public PoolDisplayPanel(final Management management) {
        _management = management;

        setLayout(new BorderLayout());
        setMinimumSize(new Dimension(800, 600));

        add(new JScrollPane(new JTable(_btm)), BorderLayout.CENTER);
        final JPanel selector = new JPanel(new FlowLayout());
        add(selector, BorderLayout.SOUTH);
        final ButtonGroup bg = new ButtonGroup();

        JRadioButton rb;
        final JLabel stats = new JLabel();
        stats.setMinimumSize(new Dimension(150, 15));

        rb = new JRadioButton(new AbstractAction("All") {
            @Override
            public void actionPerformed(final ActionEvent ae) {
                _btm.setSource(0);
                stats.setText(_btm.getBufferStats());
            }
        });
        bg.add(rb);
        selector.add(rb);
        rb.setSelected(true);

        rb = new JRadioButton(new AbstractAction("LRU Queue") {
            @Override
            public void actionPerformed(final ActionEvent ae) {
                _btm.setSource(1);
            }
        });
        bg.add(rb);
        selector.add(rb);

        rb = new JRadioButton(new AbstractAction("Invalid Queue") {
            @Override
            public void actionPerformed(final ActionEvent ae) {
                _btm.setSource(2);
                stats.setText(_btm.getBufferStats());
            }
        });

        bg.add(rb);
        selector.add(rb);

        JToggleButton tb;
        tb = new JToggleButton(new AbstractAction("Valid") {
            @Override
            public void actionPerformed(final ActionEvent ae) {
                _btm.setIncludeMask("v", ((JToggleButton) (ae.getSource())).isSelected());
                stats.setText(_btm.getBufferStats());
            }
        });
        selector.add(tb);

        tb = new JToggleButton(new AbstractAction("Dirty") {
            @Override
            public void actionPerformed(final ActionEvent ae) {
                _btm.setIncludeMask("d", ((JToggleButton) (ae.getSource())).isSelected());
                stats.setText(_btm.getBufferStats());
            }
        });
        selector.add(tb);

        tb = new JToggleButton(new AbstractAction("Reader") {
            @Override
            public void actionPerformed(final ActionEvent ae) {
                _btm.setIncludeMask("r", ((JToggleButton) (ae.getSource())).isSelected());
                stats.setText(_btm.getBufferStats());
            }
        });
        selector.add(tb);

        tb = new JToggleButton(new AbstractAction("Writer") {
            @Override
            public void actionPerformed(final ActionEvent ae) {
                _btm.setIncludeMask("w", ((JToggleButton) (ae.getSource())).isSelected());
                stats.setText(_btm.getBufferStats());
            }
        });
        selector.add(tb);

        final JButton b = new JButton(new AbstractAction("Refresh") {
            @Override
            public void actionPerformed(final ActionEvent ae) {
                _btm._selectedBufferCount = -1;
                _btm.fireTableDataChanged();
                stats.setText(_btm.getBufferStats());
            }
        });
        selector.add(b);
        selector.add(stats);
    }

    private class BufferTableModel extends AbstractTableModel {
        private int _traversalType = 0;
        private String _includeMask = null;
        private final String _excludeMask = null;

        private long _missCount;
        private long _newCount;
        private long _hitCount;
        private double _hitRatio;

        private final DecimalFormat _df = new DecimalFormat("#####.#####");

        private BufferInfo[] _buffers;
        private int _selectedBufferCount = -1;

        void setIncludeMask(final String mask, final boolean selected) {
            if (selected && _includeMask == null)
                _includeMask = "";
            if (selected && (_includeMask.indexOf(mask) == -1)) {
                _includeMask += mask;
                _selectedBufferCount = -1;
                fireTableDataChanged();
            } else if (!selected && (_includeMask != null) && (_includeMask.indexOf(mask) != -1)) {
                final int p = _includeMask.indexOf(mask);
                _includeMask = _includeMask.substring(0, p) + _includeMask.substring(p + 1);
                _selectedBufferCount = -1;
                if (_includeMask.length() == 0)
                    _includeMask = null;
                fireTableDataChanged();
            }
        }

        void setSource(final int code) {
            if (_traversalType != code) {
                _traversalType = code;
                _selectedBufferCount = -1;
                fireTableDataChanged();
            }
        }

        @Override
        public int getRowCount() {
            if (_selectedBufferCount < 0)
                loadBuffers();
            return _selectedBufferCount;
        }

        @Override
        public int getColumnCount() {
            return HEADER_NAMES.length;
        }

        @Override
        public String getColumnName(final int column) {
            return HEADER_NAMES[column];
        }

        @Override
        public Object getValueAt(final int row, final int column) {
            if (_selectedBufferCount < 0)
                loadBuffers();
            if (row >= _selectedBufferCount)
                return "";

            final BufferInfo bufferInfo = _buffers[row];
            switch (column) {
            case 0:
                return Integer.toString(bufferInfo.getPoolIndex());

            case 1:
                return Long.toString(bufferInfo.getPageAddress());

            case 2:
                if (bufferInfo.getStatusName().indexOf('v') != -1 && bufferInfo.getPageAddress() == 0)
                    return "Base";
                else
                    return bufferInfo.getTypeName();

            case 3:
                return bufferInfo.getStatusName();

            case 4:
                final String threadName = bufferInfo.getWriterThreadName();
                return threadName == null ? "" : threadName;

            case 5:
                return Integer.toString(bufferInfo.getAvailableBytes());

            case 6:
                return Long.toString(bufferInfo.getRightSiblingAddress());

            default:
                return "?";
            }
        }

        private void loadBuffers() {
            try {
                final BufferPoolInfo[] pools = _management.getBufferPoolInfoArray();

                if (pools.length < 1) {
                    _selectedBufferCount = 0;

                    _hitCount = 0;
                    _missCount = 0;
                    _newCount = 0;
                    _hitRatio = 0;
                } else {
                    final String managementClassName = _management.getClass().getName();
                    final boolean isRemote = managementClassName.indexOf("Stub") > 0;

                    final BufferPoolInfo info = pools[0];

                    if (isRemote) {
                        _buffers = _management.getBufferInfoArray(info.getBufferSize(), _traversalType, _includeMask,
                                _excludeMask);
                        _selectedBufferCount = _buffers == null ? -1 : _buffers.length;
                    } else {
                        if (_buffers == null || _buffers.length < pools[0].getBufferCount()) {
                            _buffers = new BufferInfo[info.getBufferCount()];
                        }

                        _selectedBufferCount = _management.populateBufferInfoArray(_buffers, info.getBufferSize(),
                                _traversalType, _includeMask, _excludeMask);
                    }

                    _hitCount = info.getHitCount();
                    _missCount = info.getMissCount();
                    _newCount = info.getNewCount();
                    _hitRatio = info.getHitRatio();
                }
            } catch (final Exception ex) {
                ex.printStackTrace();
                _selectedBufferCount = 0;

                _hitCount = 0;
                _missCount = 0;
                _hitRatio = 0;
            }
        }

        private String getBufferStats() {
            return Long.toString(_hitCount) + " / (" + Long.toString(_missCount) + "+" + Long.toString(_newCount)
                    + ") = " + _df.format(_hitRatio);
        }
    }
}
