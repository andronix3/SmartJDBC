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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.logging.Level;

import com.smartg.java.util.SafeIterator;
import com.sun.istack.internal.logging.Logger;

public class TableDef {
    public final String tableName;
    private Columns columns;
    private final int columnsLengthForInsert;

    private ArrayList<int[]> conditionColumns = new ArrayList<int[]>();

    static class Columns {

	private HashMap<String, Column> byName = new HashMap<String, Column>();
	private HashMap<String, Integer> name2index = new HashMap<String, Integer>();

	private Column[] columns;

	Columns(Column[] columns) {
	    this.columns = columns;
	    for (Column c : columns) {
		byName.put(c.getName().toUpperCase(), c);
	    }
	    for (int i = 0; i < columns.length; i++) {
		Column c = columns[i];
		name2index.put(c.getName().toUpperCase(), i);
	    }
	}

	Column getColumn(String name) {
	    return byName.get(name.toUpperCase());
	}

	Column getColumn(int column) {
	    return columns[column];
	}

	int getColumnIndex(String name) {
	    Integer k = name2index.get(name.toUpperCase());
	    if (k == null) {
		Logger.getLogger(getClass()).log(Level.SEVERE, "Column not found: " + name);
		return -1;
	    }
	    return k.intValue();
	}

	public Enumeration<Column> columns() {
	    return new SafeIterator<Column>(Arrays.asList(columns).iterator());
	}

	int getColumnsLengthForInsert() {
	    int count = columns.length;
	    for (Column c : columns) {
		if (c.isAutoIncrement) {
		    count--;
		}
	    }
	    return count;
	}
    }

    public TableDef(String tableName, Column[] columns) {
	this.tableName = tableName;
	this.columns = new Columns(columns);
	columnsLengthForInsert = this.columns.getColumnsLengthForInsert();
    }

    public void addConditionColumns(int... cc) {
	conditionColumns.add(cc);
    }

    public void addConditionColumns(String... cc) {
	int[] nn = new int[cc.length];
	for (int i = 0; i < cc.length; i++) {
	    nn[i] = columns.name2index.get(cc[i].toUpperCase());
	}
	Arrays.sort(nn);
	addConditionColumns(nn);
    }

    public Enumeration<int[]> getConditionColumns() {
	return new SafeIterator<int[]>(conditionColumns.iterator());
    }

    public Enumeration<Column> columns() {
	return columns.columns();
    }

    public String getInsertStatementName() {
	return "insertStat_" + tableName;
    }

    public String getInsertStatementName(int... columns) {
	String s = "";
	for (int c : columns) {
	    s += c;
	    s += "_";
	}
	return "insertStat_" + tableName + s;
    }

    public String getInsertStatementName(String... names) {
	String s = "";
	for (String col : names) {
	    int c = columns.getColumnIndex(col);
	    s += c;
	    s += "_";
	}
	return "insertStat_" + tableName + s;
    }

    public String getSelectStatementName() {
	return "selectStat_" + tableName;
    }
    
    public String getCountStatementName() {
	return "countStat_" + tableName;
    }

    public String getSelectStatementName(int... columns) {
	String s = "";
	for (int c : columns) {
	    s += c;
	    s += "_";
	}
	return "selectStat_" + tableName + s;
    }
    
    public String getCountStatementName(int... columns) {
	String s = "";
	for (int c : columns) {
	    s += c;
	    s += "_";
	}
	return "countStat_" + tableName + s;
    }


    public String getSelectStatementName(String... names) {
	String s = "";
	for (String col : names) {
	    int c = columns.getColumnIndex(col);
	    s += c;
	    s += "_";
	}
	return "selectStat_" + tableName + s;
    }
    
    public String getCountStatementName(String... names) {
	String s = "";
	for (String col : names) {
	    int c = columns.getColumnIndex(col);
	    s += c;
	    s += "_";
	}
	return "countStat_" + tableName + s;
    }


    public String getPreparedInsertStatement() {
	return "insert into " + tableName + " values ( " + createString('?', columnsLengthForInsert) + ")";
    }

    public String getPreparedInsertStatement(int... columns) {
	String names = getColumns(columns);
	return "insert into " + tableName + names + " values ( " + createString('?', columnsLengthForInsert) + ")";
    }

    public String getPreparedInsertStatement(String... columns) {
	String names = getColumns(columns);
	return "insert into " + tableName + names + " values ( " + createString('?', columnsLengthForInsert) + ")";
    }

    public String getPreparedSelectStatement(int column) {
	Column column2 = columns.getColumn(column);
	return "select * from " + tableName + " where " + column2.getName() + " = ? ";
    }

    public String getPreparedCountStatement(int column) {
	Column column2 = columns.getColumn(column);
	return "select COUNT(*) from " + tableName + " where " + column2.getName() + " = ? ";
    }

    public String getPreparedSelectStatement(String column) {
	Column column2 = columns.getColumn(column);
	return "select * from " + tableName + " where " + column2.getName() + " = ? ";
    }

    public String getPreparedCountStatement(String column) {
	Column column2 = columns.getColumn(column);
	return "select COUNT(*) from " + tableName + " where " + column2.getName() + " = ? ";
    }

    public String getPreparedSelectStatement(int... column) {
	String res = "select * from " + tableName + " where ";
	String ws = fillCoulumns(column);
	return res + ws;
    }

    public String getPreparedCountStatement(int... column) {
	String res = "select COUNT(*) from " + tableName + " where ";
	String ws = fillCoulumns(column);
	return res + ws;
    }

    public String getPreparedSelectStatement(String... column) {
	String res = "select * from " + tableName + " where ";
	String ws = fillColumns(column);
	return res + ws;
    }

    public String getPreparedCountStatement(String... column) {
	String res = "select COUNT(*) from " + tableName + " where ";
	String ws = fillColumns(column);
	return res + ws;
    }

    private String fillCoulumns(int... column) {
	String ws = "";
	for (int c : column) {
	    ws += columns.getColumn(c).getName() + " = ? AND ";
	}
	if (column.length > 0) {
	    int index = ws.lastIndexOf(" AND ");
	    ws = ws.substring(0, index);
	}
	return ws;
    }

    private String fillColumns(String... column) {
	String ws = "";
	for (String c : column) {
	    ws += columns.getColumn(c).getName() + " = ? AND ";
	}
	if (column.length > 0) {
	    int index = ws.lastIndexOf(" AND ");
	    ws = ws.substring(0, index);
	}
	return ws;
    }

    public String getPreparedSelectStatement() {
	return "select * from " + tableName;
    }

    public String getPreparedCountStatement() {
	return "select COUNT(*) from " + tableName;
    }

    public String getUpdateStatementName() {
	return "updateStat_" + tableName;
    }

    public String getPreparedUpdateStatement(int conditionColumn, int updateColumn) {
	return "UPDATE " + tableName + " SET " + columns.getColumn(updateColumn).getName() + " = ? WHERE " + columns.getColumn(conditionColumn).getName()
		+ " = ? ";
    }

    public String getPreparedUpdateStatement(String conditionColumn, String updateColumn) {
	return "UPDATE " + tableName + " SET " + columns.getColumn(updateColumn).getName() + " = ? WHERE " + columns.getColumn(conditionColumn).getName()
		+ " = ? ";
    }

    private String getColumns(int... columns) {
	String res = "(";
	for (int c : columns) {
	    res += this.columns.getColumn(c).getName();
	    res += ",";
	}
	res = res.substring(0, res.length() - 1);
	res = res + ")";

	return res;
    }

    private String getColumns(String... columns) {
	String res = "(";
	for (String c : columns) {
	    res += this.columns.getColumn(c).getName();
	    res += ",";
	}
	res = res.substring(0, res.length() - 1);
	res = res + ")";

	return res;
    }

    private static String createString(char c, int count) {
	String s = " ";
	int cnt = count - 1;
	for (int i = 0; i < cnt; i++) {
	    s += c + ", ";
	}
	s += c;
	return s;
    }
}