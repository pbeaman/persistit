/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Feb 10, 2004
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
    private final static String[] HEADER_NAMES = { "Index", "Page", "Type",
            "Status", "Writer Thread", "Available Space", "Right Sibling", };

    private Management _management;
    private BufferTableModel _btm = new BufferTableModel();

    public PoolDisplayPanel(Management management) {
        _management = management;

        setLayout(new BorderLayout());
        setMinimumSize(new Dimension(800, 600));

        add(new JScrollPane(new JTable(_btm)), BorderLayout.CENTER);
        JPanel selector = new JPanel(new FlowLayout());
        add(selector, BorderLayout.SOUTH);
        ButtonGroup bg = new ButtonGroup();

        JRadioButton rb;
        final JLabel stats = new JLabel();
        stats.setMinimumSize(new Dimension(150, 15));

        rb = new JRadioButton(new AbstractAction("All") {
            public void actionPerformed(ActionEvent ae) {
                _btm.setSource(0);
                stats.setText(_btm.getBufferStats());
            }
        });
        bg.add(rb);
        selector.add(rb);
        rb.setSelected(true);

        rb = new JRadioButton(new AbstractAction("LRU Queue") {
            public void actionPerformed(ActionEvent ae) {
                _btm.setSource(1);
            }
        });
        bg.add(rb);
        selector.add(rb);

        rb = new JRadioButton(new AbstractAction("Invalid Queue") {
            public void actionPerformed(ActionEvent ae) {
                _btm.setSource(2);
                stats.setText(_btm.getBufferStats());
            }
        });

        bg.add(rb);
        selector.add(rb);

        JToggleButton tb;
        tb = new JToggleButton(new AbstractAction("Valid") {
            public void actionPerformed(ActionEvent ae) {
                _btm.setIncludeMask("v",
                        ((JToggleButton) (ae.getSource())).isSelected());
                stats.setText(_btm.getBufferStats());
            }
        });
        selector.add(tb);

        tb = new JToggleButton(new AbstractAction("Dirty") {
            public void actionPerformed(ActionEvent ae) {
                _btm.setIncludeMask("d",
                        ((JToggleButton) (ae.getSource())).isSelected());
                stats.setText(_btm.getBufferStats());
            }
        });
        selector.add(tb);

        tb = new JToggleButton(new AbstractAction("Reader") {
            public void actionPerformed(ActionEvent ae) {
                _btm.setIncludeMask("r",
                        ((JToggleButton) (ae.getSource())).isSelected());
                stats.setText(_btm.getBufferStats());
            }
        });
        selector.add(tb);

        tb = new JToggleButton(new AbstractAction("Writer") {
            public void actionPerformed(ActionEvent ae) {
                _btm.setIncludeMask("w",
                        ((JToggleButton) (ae.getSource())).isSelected());
                stats.setText(_btm.getBufferStats());
            }
        });
        selector.add(tb);

        JButton b = new JButton(new AbstractAction("Refresh") {
            public void actionPerformed(ActionEvent ae) {
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
        private String _excludeMask = null;

        private long _getCounter;
        private long _hitCounter;
        private double _hitRatio;

        private DecimalFormat _df = new DecimalFormat("#####.#####");

        private BufferInfo[] _buffers;
        private int _selectedBufferCount = -1;

        void setIncludeMask(String mask, boolean selected) {
            if (selected && _includeMask == null)
                _includeMask = "";
            if (selected && (_includeMask.indexOf(mask) == -1)) {
                _includeMask += mask;
                _selectedBufferCount = -1;
                fireTableDataChanged();
            } else if (!selected && (_includeMask != null)
                    && (_includeMask.indexOf(mask) != -1)) {
                int p = _includeMask.indexOf(mask);
                _includeMask = _includeMask.substring(0, p)
                        + _includeMask.substring(p + 1);
                _selectedBufferCount = -1;
                if (_includeMask.length() == 0)
                    _includeMask = null;
                fireTableDataChanged();
            }
        }

        void setSource(int code) {
            if (_traversalType != code) {
                _traversalType = code;
                _selectedBufferCount = -1;
                fireTableDataChanged();
            }
        }

        public int getRowCount() {
            if (_selectedBufferCount < 0)
                loadBuffers();
            return _selectedBufferCount;
        }

        public int getColumnCount() {
            return HEADER_NAMES.length;
        }

        public String getColumnName(int column) {
            return HEADER_NAMES[column];
        }

        public Object getValueAt(int row, int column) {
            if (_selectedBufferCount < 0)
                loadBuffers();
            if (row >= _selectedBufferCount)
                return "";

            BufferInfo bufferInfo = _buffers[row];
            switch (column) {
            case 0:
                return Integer.toString(bufferInfo.getPoolIndex());

            case 1:
                return Long.toString(bufferInfo.getPageAddress());

            case 2:
                if (bufferInfo.getStatusName().indexOf('v') != -1
                        && bufferInfo.getPageAddress() == 0)
                    return "Base";
                else
                    return bufferInfo.getTypeName();

            case 3:
                return bufferInfo.getStatusName();

            case 4:
                String threadName = bufferInfo.getWriterThreadName();
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
                BufferPoolInfo[] pools = _management.getBufferPoolInfoArray();

                if (pools.length < 1) {
                    _selectedBufferCount = 0;

                    _hitCounter = 0;
                    _getCounter = 0;
                    _hitRatio = 0;
                } else {
                    String managementClassName = _management.getClass()
                            .getName();
                    boolean isRemote = managementClassName.indexOf("Stub") > 0;

                    BufferPoolInfo info = pools[0];

                    if (isRemote) {
                        _buffers = _management.getBufferInfoArray(
                                info.getBufferSize(), _traversalType,
                                _includeMask, _excludeMask);
                        _selectedBufferCount = _buffers == null ? -1
                                : _buffers.length;
                    } else {
                        if (_buffers == null
                                || _buffers.length < pools[0].getBufferCount()) {
                            _buffers = new BufferInfo[info.getBufferCount()];
                        }

                        _selectedBufferCount = _management
                                .populateBufferInfoArray(_buffers,
                                        info.getBufferSize(), _traversalType,
                                        _includeMask, _excludeMask);
                    }

                    _hitCounter = info.getHitCounter();
                    _getCounter = info.getGetCounter();
                    _hitRatio = info.getHitRatio();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                _selectedBufferCount = 0;

                _hitCounter = 0;
                _getCounter = 0;
                _hitRatio = 0;
            }
        }

        private String getBufferStats() {
            return Long.toString(_hitCounter) + " / "
                    + Long.toString(_getCounter) + " = "
                    + _df.format(_hitRatio);
        }
    }
}
