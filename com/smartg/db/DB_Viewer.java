/*
 * Copyright (c) Andrey Kuznetsov. All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  o Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  o Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 *  o Neither the name of imagero Andrey Kuznetsov nor the names of
 *    its contributors may be used to endorse or promote products derived
 *    from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.smartg.db;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Vector;
import java.util.logging.Logger;

import javax.swing.Box;
import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.DefaultTableModel;

public class DB_Viewer extends JPanel {

    private static final long serialVersionUID = 6644818061049030786L;

    JavaDB_Connection connection;

    JList<String> tableList;
    DefaultListModel<String> tableListModel = new DefaultListModel<String>();

    DefaultTableModel dataTableModel = new DefaultTableModel();
    JTable dataTable = new JTable(dataTableModel);

    Box box = Box.createHorizontalBox();
    JComboBox<String> jComboBox = new JComboBox<String>(new String[] { "Select..." });
    JTextField filterText = new JTextField(10);
    JButton refresh = new JButton("Refresh");
    JButton reset = new JButton("Reset");

    public DB_Viewer(JavaDB_Connection conn) {
	this.connection = conn;
	tableList = new JList<String>(tableListModel);

	setLayout(new BorderLayout());
	add(new JScrollPane(tableList), BorderLayout.WEST);
	add(new JScrollPane(dataTable), BorderLayout.CENTER);

	box.add(jComboBox);
	box.add(filterText);
	// box.add(Box.createHorizontalGlue());
	box.add(refresh);
	box.add(reset);

	refresh.addActionListener(new ActionListener() {
	    public void actionPerformed(ActionEvent e) {
		updateValues();
	    }
	});

	reset.addActionListener(new ActionListener() {
	    public void actionPerformed(ActionEvent e) {
		jComboBox.setSelectedIndex(0);
		filterText.setText("");
		updateValues();
	    }
	});

	// box.add(Box.createHorizontalGlue());

	add(box, BorderLayout.SOUTH);

	Result tables = connection.getTables();
	if (tables != null) {
	    while (tables.hasMoreElements()) {
		ArrayList<Object> next = tables.nextElement();
		Logger.getLogger("com.imagero.db").info(String.valueOf(next));
//		System.out.println(next);
		if (!"SYSTEM TABLE".equals(next.get(3))) {
		    String tableName = (String) next.get(2);
		    if (tableName != null) {
			TableMetadata tableMetadata;
			if (connection.isExcelConnection()) {
			    tableName = tableName.substring(0, tableName.indexOf('$') + 1);
			    tableName = "[" + tableName + "]";
			    String sql = "select * from " + tableName;
			    Result result = connection.executeAndGetResult(sql);
			    tableMetadata = result.getResultMetadata();

			} else {
			    tableMetadata = connection.getTableMetadata(tableName);
			}
			StringBuffer sb = new StringBuffer(tableName);
			sb.append(" [ ");
			if (tableMetadata != null) {
			    String[] columnNames = tableMetadata.getColumnNames();
			    for (int i = 0; i < columnNames.length; i++) {
				sb.append(columnNames[i]);
				if (i < columnNames.length - 1) {
				    sb.append(", ");
				}
				if (sb.length() > 50) {
				    sb.setLength(50);
				    sb.append("...");
				    break;
				}
			    }
			}
			sb.append(" ]");
			tableListModel.addElement(sb.toString());
		    }
		}
	    }
	}

	tableList.addListSelectionListener(new ListSelectionListener() {
	    public void valueChanged(ListSelectionEvent e) {
		if (!e.getValueIsAdjusting()) {
		    updateValues();
		}
	    }
	});
    }

    private String getFilter() {
	String text = filterText.getText();
	if (text == null || text.trim().isEmpty()) {
	    return "";
	}
	if (jComboBox.getSelectedIndex() == 0) {
	    return "";
	}
	return "WHERE " + jComboBox.getSelectedItem() + " = " + text + " ";
    }

    private void updateValues() {
	String tableName = tableList.getSelectedValue();
	tableName = tableName.substring(0, tableName.indexOf(" ["));
	String columnName = "";

	String key = tableName;

	int selectedIndex = jComboBox.getSelectedIndex();
	if (selectedIndex > 0) {
	    columnName = jComboBox.getSelectedItem().toString();
	    key += "." + columnName;
	    if (!connection.hasStatement(key)) {
		String sql = "select * from " + tableName + " where " + columnName + " = ? ";
		connection.prepareStatement(key, sql);
	    }
	}

	String filter = getFilter();
	Result result = null;
	String sql = "select * from " + tableName + " " + filter;
	if (!connection.isExcelConnection()) {
	    if (filter.isEmpty()) {
		result = connection.executeAndGetResult(sql + " FETCH FIRST 1000 ROWs ONLY ");
	    } else {
		try {
		    connection.execute(key, new Object[] { filterText.getText() });
		    result = connection.getResult(key);

		} catch (SQLException ex) {
		    ex.printStackTrace();
		}
		// result = connection.executeAndGetResult(sql);
	    }
	} else {
	    result = connection.executeAndGetResult(sql);
	}

	applyResult(result);
    }

    void applyResult(Result result) {
	TableMetadata resultMetadata = result.getResultMetadata();
	String[] columnNames = resultMetadata.getColumnNames();

	Vector<String> v = new Vector<String>();
	v.add("Select...");
	v.addAll(Arrays.asList(columnNames));

	jComboBox.setModel(new DefaultComboBoxModel<String>(v));

	int columnCount = resultMetadata.getColumnCount();
	dataTableModel.setColumnCount(columnCount);
	dataTableModel.setColumnIdentifiers(columnNames);
	int rowCount = 1000;
	dataTableModel.setRowCount(rowCount);

	String[] columnTypes = resultMetadata.getColumnTypes();
	for (int i = 0; i < columnCount; i++) {
	    dataTableModel.setValueAt(columnTypes[i], 0, i);
	}

	for (int row = 1; row < rowCount; row++) {
	    if (result.hasMoreElements()) {
		ArrayList<Object> next = result.nextElement();
		for (int i = 0; i < columnCount; i++) {
		    dataTableModel.setValueAt(next.get(i), row, i);
		}
	    } else {
		for (int i = 0; i < columnCount; i++) {
		    dataTableModel.setValueAt("", row, i);
		}
	    }
	}
    }
}
