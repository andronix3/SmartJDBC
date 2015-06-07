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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;

public class TableMetadata {

    private String[] columnNames;
    private String[] columnTypes;

    private int columnCount;

    private ResultSetMetaData rs_metadata;

    public TableMetadata(ResultSetMetaData rsMetadata) throws SQLException {
	this.rs_metadata = rsMetadata;
	columnCount = rs_metadata.getColumnCount();
    }
    
    public int getColumnCount() {
	return columnCount;
    }

    public String[] getColumnNames() {
	if (columnNames == null) {
	    columnNames = new String[columnCount];
	    for (int i = 0; i < columnCount; i++) {
		try {
		    columnNames[i] = rs_metadata.getColumnName(i + 1);
		} catch (SQLException ex) {
		    // ignore
		}
	    }
	}
	return Arrays.copyOf(columnNames, columnNames.length);
    }

    public String[] getColumnTypes() {
	if (columnTypes == null) {
	    columnTypes = new String[columnCount];
	    for (int i = 0; i < columnCount; i++) {
		try {
		    columnTypes[i] = rs_metadata.getColumnTypeName(i + 1);
		} catch (SQLException ex) {
		    // ignore
		}
	    }
	}
	return Arrays.copyOf(columnTypes, columnTypes.length);
    }
}
