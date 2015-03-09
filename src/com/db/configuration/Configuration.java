package com.db.configuration;

public class Configuration {
	public static enum MEM_SIZE {
		FIVE_MB, TEN_MB
	};

	public static enum DATA_TYPES {
		INT, CHAR
	};

	// in KB
	public static int BLOCKSIZE = 4;
	public static int NUMBER_OF_RECORDS_PER_BLOCK = 10;
	public static int MB_TO_KB_VAL = 1024;
	public static String PATH_DELIMITER = "/";
	public static String OUTPUT_PATH_DIR = "";
	// SUBLIST naming scheme should be SUBLIST_<SUBLIST_NO>_BLOCKNO. And
	// sublistNo should start from 0. And blockNo should start from 0.
	public static String SUBLIST_NAMING_SCHEME = "SUBLIST";
	// OUTPUT naming scheme should be MERGE_<SUBLIST_NO>_BLOCKNO
	public static String NAMING_SCHEME_FOR_OUTPUT_AFTER_MERGE = "MERGE";
	public static String INPUT_PATH_FOR_RECORD_MERGER = "";
	public static int TOT_NUMBER_OF_TUPLES = 1000000;

}
