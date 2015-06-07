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

public class Column {

    public static enum ColumnType {
	BIGINT, BIGINT_ID, BLOB, BOOLEAN, CHAR, CHAR_FBD, CLOB, DATE, DECIMAL, DOUBLE, FLOAT, INTEGER, INTEGER_ID, LONG_VARCHAR, LONG_VARCHAR_FBD, NUMERIC, REAL, SMALLINT, SMALLINT_ID, TIME, TIMESTAMP, USED_DEFINED, VARCHAR, VARCHAR_FBD, XML
    }

    private String name;
    private ColumnType dataType;

    private int size;

    private String toString;
    private String userDefinedType;
    private boolean index;
    public final boolean isAutoIncrement;

    public Column(String name, ColumnType dataType) {
	this(name, dataType, 0);
    }

    public Column(String name, ColumnType dataType, boolean indexed) {
	this(name, dataType, 0, indexed);
    }

    public Column(String name, ColumnType dataType, int size) {
	this(name, dataType, size, false);
    }

    public Column(String name, ColumnType dataType, int size, boolean indexed) {
	this.name = name;
	this.dataType = dataType;
	this.index = indexed;
	if (size > 0) {
	    this.size = size;
	}
	isAutoIncrement = isAutoIncrement();
    }

    public Column(String name, String userDefinedType) {
	this.name = name;
	this.dataType = ColumnType.USED_DEFINED;
	this.userDefinedType = userDefinedType;
	isAutoIncrement = isAutoIncrement();
    }

    private boolean isAutoIncrement() {
	switch (dataType) {
	case BIGINT_ID:
	case INTEGER_ID:
	case SMALLINT_ID:
	    return true;
	default:
	    return false;
	}
    }

    public boolean needIndex() {
	return index;
    }

    @Override
    public String toString() {
	if (toString == null) {
	    toString = create();
	}
	return toString;
    }

    public String getName() {
	return name;
    }

    private String create() {
	StringBuffer sb = new StringBuffer();
	sb.append(name);
	sb.append(" ");

	if (dataType != ColumnType.USED_DEFINED) {
	    String s = dataType.toString();

	    boolean forBitData = false;
	    if (s.endsWith("_FBD")) {
		forBitData = true;
		s = s.replaceAll("_FBD", "");
	    }

	    boolean identity = false;
	    if (s.endsWith("_ID")) {
		identity = true;
		s = s.replaceAll("_ID", "");
	    }

	    sb.append(s);

	    if (size > 0) {
		sb.append("(");
		sb.append(size);
		sb.append(")");
	    }

	    if (forBitData) {
		sb.append(" FOR BIT DATA ");
	    }

	    if (identity) {
		sb.append(" GENERATED ALWAYS AS IDENTITY ");
	    }
	} else {
	    sb.append(userDefinedType);
	}

	return sb.toString();
    }
}
