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

import com.smartg.java.util.SafeIterator;

public class TableDef {
    public final String tableName;
    private Columns columns;
    private final int columnsLengthForInsert;

    private ArrayList<int[]> conditionColumns = new ArrayList<int[]>();

    static class Columns {
	private Column[] columns;

	Columns(Column[] columns) {
	    this.columns = columns;
	}

	Column getColumn(int column) {
	    return columns[column];
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
	}
	return "insertStat_" + tableName + s;
    }

    public String getSelectStatementName() {
	return "selectStat_" + tableName;
    }

    public String getSelectStatementName(int... columns) {
	String s = "";
	for (int c : columns) {
	    s += c;
	}
	return "selectStat_" + tableName + s;
    }

    public String getPreparedInsertStatement() {
	return "insert into " + tableName + " values ( " + createString('?', columnsLengthForInsert) + ")";
    }

    public String getPreparedInsertStatement(int... columns) {
	String names = getColumns(columns);
	return "insert into " + tableName + names + " values ( " + createString('?', columnsLengthForInsert) + ")";
    }

    public String getPreparedSelectStatement(int column) {
	Column column2 = columns.getColumn(column);
	return "select * from " + tableName + " where " + column2.getName() + " = ? ";
    }

    public String getPreparedSelectStatement(int... column) {
	String res = "select * from " + tableName + " where ";
	String ws = "";
	for (int c : column) {
	    ws += columns.getColumn(c).getName() + " = ? AND ";
	}
	if (column.length > 0) {
	    int index = ws.lastIndexOf(" AND ");
	    ws = ws.substring(0, index);
	}
	return res + ws;

    }

    public String getPreparedSelectStatement() {
	return "select * from " + tableName;
    }

    public String getUpdateStatementName() {
	return "updateStat_" + tableName;
    }

    public String getPreparedUpdateStatement(int conditionColumn, int updateColumn) {
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