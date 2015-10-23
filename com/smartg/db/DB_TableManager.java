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

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;

public abstract class DB_TableManager {

    protected JavaDB_Connection jdbc;
    private ArrayList<TableDef> tdList;
    private HashMap<String, TableDef> map = new HashMap<String, TableDef>();

    public void dbInit() {
	createTypes();
	createTables();
	prepareStatements();
    }

    protected ArrayList<TableDef> getTableList() {
	if (tdList == null) {
	    tdList = new ArrayList<TableDef>();
	    Field[] declaredFields = getClass().getDeclaredFields();
	    for (Field f : declaredFields) {
		if (f.getType().equals(TableDef.class)) {
		    try {
			TableDef td = getTD(f);
			tdList.add(td);
			map.put(td.tableName, td);
		    } catch (Throwable ex) {
			ex.printStackTrace();
		    }
		}
	    }
	}
	return tdList;
    }

    protected ArrayList<UserDefinedType> getUserDefinedTypes() {
	ArrayList<UserDefinedType> tdList = new ArrayList<UserDefinedType>();
	Field[] declaredFields = getClass().getDeclaredFields();
	for (Field f : declaredFields) {
	    if (f.getType().equals(UserDefinedType.class)) {
		try {
		    UserDefinedType td = getUDT(f);
		    System.out.println(td.getUserType());
		    tdList.add(td);
		} catch (Throwable ex) {
		    ex.printStackTrace();
		}
	    }
	}
	return tdList;
    }

    /**
     * This method should be implemented by extending classes as follow:
     * 
     * <code>
     * protected TableDef getTD(Field f) throws IllegalAccessException {
     * return (TableDef) f.get(this);
     * }
     * </code>
     * 
     * @param f
     * @return
     * @throws IllegalAccessException
     */
    protected abstract TableDef getTD(Field f) throws IllegalAccessException;

    /**
     * This method should be implemented by extending classes as follow:
     * 
     * <code>
     * protected UserDefinedType getTD(Field f) throws IllegalAccessException {
     * return (UserDefinedType) f.get(this);
     * }
     * </code>
     * 
     * @param f
     * @return
     * @throws IllegalAccessException
     */
    protected abstract UserDefinedType getUDT(Field f) throws IllegalAccessException;

    public void createTypes() {
	ArrayList<UserDefinedType> udts = getUserDefinedTypes();
	for (UserDefinedType udt : udts) {
	    jdbc.createType(udt.getUserType(), udt.getUserClass());
	}
    }

    public void createTables() {
	ArrayList<TableDef> tables = getTableList();
	for (TableDef td : tables) {
	    if (!jdbc.hasTable(td.tableName)) {
		createTable(td, jdbc);
	    }
	}

	jdbc.printTables();
    }

    private void createTable(TableDef td, JavaDB_Connection jdbc) {
	if (!jdbc.createTable(td)) {
	    SQLException lastError = jdbc.getLastError();
	    System.out.println(lastError);
	}
    }

    private void prepareInsertStatement(TableDef tableDef) {
	jdbc.prepareStatement(tableDef.getInsertStatementName(), tableDef.getPreparedInsertStatement());
    }

    private void prepareSelectStatement(TableDef tableDef, int... conditionColumn) {
	jdbc.prepareStatement(tableDef.getSelectStatementName(conditionColumn), tableDef.getPreparedSelectStatement(conditionColumn));
    }

    private void prepareCountStatement(TableDef tableDef, int... conditionColumn) {
	jdbc.prepareStatement(tableDef.getCountStatementName(conditionColumn), tableDef.getPreparedCountStatement(conditionColumn));
    }

    private void prepareSelectStatement(TableDef tableDef) {
	jdbc.prepareStatement(tableDef.getSelectStatementName(), tableDef.getPreparedSelectStatement());
    }

    private void prepareCountStatement(TableDef tableDef) {
	jdbc.prepareStatement(tableDef.getCountStatementName(), tableDef.getPreparedCountStatement());
    }

    // TODO add support for updates
    @SuppressWarnings("unused")
    private void prepareUpdateStatement(TableDef tableDef, JavaDB_Connection jdbc, int conditionColumn, int updateColumn) {
	jdbc.prepareStatement(tableDef.getUpdateStatementName() + conditionColumn + " " + updateColumn,
		tableDef.getPreparedUpdateStatement(conditionColumn, updateColumn));
    }

    public void prepareStatements() {
	ArrayList<TableDef> tables = getTableList();

	for (TableDef td : tables) {
	    prepareInsertStatement(td);
	    prepareSelectStatement(td);
	    prepareCountStatement(td);
	}

	for (TableDef td : tables) {
	    Enumeration<int[]> list = td.getConditionColumns();
	    while (list.hasMoreElements()) {
		int[] next = list.nextElement();
		prepareSelectStatement(td, next);
		prepareCountStatement(td, next);
	    }
	}
    }

    public void insert(String tableName, ArrayList<Object> objects) throws SQLException, NullPointerException {
	TableDef td = map.get(tableName);
	if (td == null) {
	    throw new NullPointerException("Table not found: " + tableName);
	}
	insert(td, objects);
    }

    public void insert(TableDef tableDef, ArrayList<Object> objects) throws SQLException {
	jdbc.execute(tableDef.getInsertStatementName(), objects);
    }

    public void insert(String tableName, Object... objects) throws SQLException, NullPointerException {
	TableDef td = map.get(tableName);
	if (td == null) {
	    throw new NullPointerException("Table not found: " + tableName);
	}
	insert(td, objects);
    }

    public void insert(TableDef tableDef, Object... objects) throws SQLException {
	jdbc.execute(tableDef.getInsertStatementName(), objects);
    }

    public void select(String tableName, int conditionColumn, Object value) throws SQLException, NullPointerException {
	TableDef td = map.get(tableName);
	if (td == null) {
	    throw new NullPointerException("Table not found: " + tableName);
	}
	select(td, conditionColumn, value);
    }
    
    public void count(String tableName, int conditionColumn, Object value) throws SQLException, NullPointerException {
	TableDef td = map.get(tableName);
	if (td == null) {
	    throw new NullPointerException("Table not found: " + tableName);
	}
	count(td, conditionColumn, value);
    }


    public void select(TableDef tableDef, int conditionColumn, Object value) throws SQLException {
	String name = tableDef.getSelectStatementName(conditionColumn);
	jdbc.execute(name, new Object[] { value });
    }

    public void count(TableDef tableDef, int conditionColumn, Object value) throws SQLException {
	String name = tableDef.getCountStatementName(conditionColumn);
	jdbc.execute(name, new Object[] { value });
    }

    public void select(String tableName) throws SQLException, NullPointerException {
	TableDef td = map.get(tableName);
	if (td == null) {
	    throw new NullPointerException("Table not found: " + tableName);
	}
	select(td);
    }

    public void count(String tableName) throws SQLException, NullPointerException {
	TableDef td = map.get(tableName);
	if (td == null) {
	    throw new NullPointerException("Table not found: " + tableName);
	}
	count(td);
    }

    public void select(String tableName, String[] columns, Object[] values) throws SQLException {
	TableDef td = map.get(tableName);
	if (td == null) {
	    throw new NullPointerException("Table not found: " + tableName);
	}
	select(td, columns, values);
    }

    public void count(String tableName, String[] columns, Object[] values) throws SQLException {
	TableDef td = map.get(tableName);
	if (td == null) {
	    throw new NullPointerException("Table not found: " + tableName);
	}
	count(td, columns, values);
    }

    public void select(TableDef tableDef, String[] columns, Object... values) throws SQLException {
	String name = tableDef.getSelectStatementName(columns);
	jdbc.execute(name, values);
    }

    public void count(TableDef tableDef, String[] columns, Object... values) throws SQLException {
	String name = tableDef.getCountStatementName(columns);
	jdbc.execute(name, values);
    }

    public void select(TableDef tableDef) throws SQLException {
	jdbc.execute2(tableDef.getSelectStatementName());
    }

    public void count(TableDef tableDef) throws SQLException {
	jdbc.execute2(tableDef.getCountStatementName());
    }

    public void update(String tableName, int conditionColumn, int updateColumn, Object value) throws SQLException, NullPointerException {
	TableDef td = map.get(tableName);
	if (td == null) {
	    throw new NullPointerException("Table not found: " + tableName);
	}
	update(td, conditionColumn, updateColumn, value);
    }

    public void update(TableDef tableDef, int conditionColumn, int updateColumn, Object value) throws SQLException {
	jdbc.execute(tableDef.getUpdateStatementName() + conditionColumn + " " + updateColumn, new Object[] { value });
    }
}
