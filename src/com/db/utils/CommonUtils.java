package com.db.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;

import com.db.configuration.Configuration;
import com.db.configuration.Schema;

public class CommonUtils {
	public static Map<Object, String> parseBlock(String blockName, Schema schema)
			throws IOException {
		Map<Object, String> contentMap = null;
		try {
			String fileContent = FileUtils
					.readFileToString(new File(blockName));
			String content[] = fileContent.split(System
					.getProperty("line.separator"));
			ArrayList<Integer> sizeOfCols = schema.getSizeOfEachColumns();
			Integer primaryKeyPosition = schema.getPrimaryKeyPosition();
			Configuration.DATA_TYPES primaryKeyDataType = schema.getColumns()
					.get(schema.getPrimaryKey());
			Integer primaryKeySize = null;
			int offsetBytes = 0;
			int i = 0;
			for (i = 0; i < sizeOfCols.size() && i < primaryKeyPosition; i++) {
				offsetBytes += (sizeOfCols.get(i));
			}
			primaryKeySize = sizeOfCols.get(i);
			contentMap = new TreeMap<Object, String>();
			for (String val : content) {
				String primarKey = val.substring(offsetBytes, primaryKeySize
						+ offsetBytes);
				if (primaryKeyDataType == Configuration.DATA_TYPES.INT) {
					contentMap.put(new Integer(primarKey), val);
				} else {
					contentMap.put(new String(primarKey), val);
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
		return contentMap;
	}

	public static void validateSchema(Schema schema) throws Exception {
		boolean throwException = true;
		if (schema != null) {
			String primaryKey = schema.getPrimaryKey();
			HashMap<String, Configuration.DATA_TYPES> columnInfo = schema
					.getColumns();
			ArrayList<Integer> sizeOfColumns = schema.getSizeOfEachColumns();
			if (primaryKey != null && primaryKey.trim().length() == 0) {
				if (columnInfo != null && sizeOfColumns != null
						&& columnInfo.size() == sizeOfColumns.size()) {
					throwException = false;
				}
			}
		}
		if (throwException)
			throw new Exception("The schema is not valid..");
	}

	public static int calculateNumberOfBlocksToFillTheMem(
			Configuration.MEM_SIZE memSize) {
		int result = 0;
		if (memSize == Configuration.MEM_SIZE.FIVE_MB) {
			result = Math.round(5 * Configuration.MB_TO_KB_VAL
					/ (float) Configuration.BLOCKSIZE);
		} else if (memSize == Configuration.MEM_SIZE.TEN_MB) {
			result = Math.round(10 * Configuration.MB_TO_KB_VAL
					/ (float) Configuration.BLOCKSIZE);
		}
		return result;

	}
	public static int calTheNumberOfBlocksRequiredForTheRelation(){
		return Math.round(Configuration.TOT_NUMBER_OF_TUPLES
				/ (float)Configuration.NUMBER_OF_RECORDS_PER_BLOCK);
	}
	public static int calculateTheNumberOfSublistsRequired(
			int totNoOfBlocksToFillInMemory) {
		int totNoOfBlocks = calTheNumberOfBlocksRequiredForTheRelation();
		return Math.round(totNoOfBlocks
				/ (float) totNoOfBlocksToFillInMemory);
	}
}
