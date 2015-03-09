package com.db.configuration;

import java.util.ArrayList;
import java.util.HashMap;

public class Schema {
	private HashMap<String,Configuration.DATA_TYPES> columns = null;
	private ArrayList<Integer> sizeOfEachColumns = null;
	private String primaryKey = null;
	private Integer primaryKeyPosition = null;
	public Schema(){
		columns = new HashMap<String, Configuration.DATA_TYPES>();
		sizeOfEachColumns = new ArrayList<Integer>();
	}
	public void addColumns(String column, Integer sizeOfTheCol, Configuration.DATA_TYPES dataType, boolean isPrimaryKey){
		columns.put(column,dataType);
		sizeOfEachColumns.add(sizeOfTheCol);
		if(isPrimaryKey){
			primaryKey = column;
			primaryKeyPosition = sizeOfEachColumns.size() - 1;
		}
	}
	public HashMap<String, Configuration.DATA_TYPES> getColumns() {
		return columns;
	}
	public void setColumns(HashMap<String, Configuration.DATA_TYPES> columns) {
		this.columns = columns;
	}
	public ArrayList<Integer> getSizeOfEachColumns() {
		return sizeOfEachColumns;
	}
	public void setSizeOfEachColumns(ArrayList<Integer> sizeOfEachColumns) {
		this.sizeOfEachColumns = sizeOfEachColumns;
	}
	public String getPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}
	public Integer getPrimaryKeyPosition() {
		return primaryKeyPosition;
	}
	public void setPrimaryKeyPosition(Integer primaryKeyPosition) {
		this.primaryKeyPosition = primaryKeyPosition;
	}
	
}
