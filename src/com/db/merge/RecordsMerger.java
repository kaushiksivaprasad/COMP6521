package com.db.merge;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;

import com.db.configuration.Configuration;
import com.db.configuration.Schema;
import com.db.utils.CommonUtils;

public class RecordsMerger {
	private Schema schema;
	private int noOfBlocksInSublist = 0;
	private Configuration.MEM_SIZE memSize = null;
	private TreeMap<Object, String> sublistContents1 = null;
	private TreeMap<Object, String> sublistContents2 = null;
	private ArrayList<String> outputBlock = null;
	private int noOfBlocksToBeReadForEachSubList = 0;
	private int noOfSublistPerMerge = 2;
	private int noOfOutputBlock = 1;
	private int totSublistAfterSorting = 0;
	private int currentMergedSublistNumber = 0;
	private int totMergedBlocksForTwoSublists = 0;
	private int outputBlockNoWritten = 0;

	public RecordsMerger(Schema schema, Configuration.MEM_SIZE memSize)
			throws Exception {
		CommonUtils.validateSchema(schema);
		this.schema = schema;
		this.memSize = memSize;
		this.noOfBlocksInSublist = CommonUtils
				.calculateNumberOfBlocksToFillTheMem(memSize);
		this.totSublistAfterSorting = CommonUtils
				.calculateTheNumberOfSublistsRequired(noOfBlocksInSublist);
		int temp = noOfBlocksInSublist - noOfOutputBlock;
		noOfBlocksToBeReadForEachSubList = temp / noOfSublistPerMerge;
		noOfOutputBlock = noOfOutputBlock
				+ (noOfBlocksInSublist % noOfSublistPerMerge);
	}

	public void mergeWrapper() throws IOException {
		int noOfOutputRecords = noOfOutputBlock
				* Configuration.NUMBER_OF_RECORDS_PER_BLOCK;
		while (totSublistAfterSorting != 0) {
			totMergedBlocksForTwoSublists = noOfBlocksInSublist * 2;
			currentMergedSublistNumber = 0;
			for (int i = 0; i < totSublistAfterSorting - 1; i++) {
				doMergeSublists(noOfOutputRecords, i, i + 1);
			}
			totSublistAfterSorting = Math.round(totSublistAfterSorting / 2.0f);
			noOfBlocksInSublist = noOfBlocksInSublist * 2;
		}
	}

	public int loadBlocksInMemory(int blockNumber, int sublistNumber,
			TreeMap<Object, String> sublistObj) throws IOException {
		String concatenatedPath = Configuration.INPUT_PATH_FOR_RECORD_MERGER
				+ Configuration.PATH_DELIMITER
				+ Configuration.SUBLIST_NAMING_SCHEME + "_" + sublistNumber
				+ "_" + blockNumber;
		int i = blockNumber;
		for (int k = 0; i < noOfBlocksInSublist
				&& k < noOfBlocksToBeReadForEachSubList; i++, k++) {
			try {
				Map<Object, String> tempMap = CommonUtils.parseBlock(
						concatenatedPath, schema);
				sublistObj.putAll(tempMap);
			} catch (IOException e) {
				return i;
			}
		}
		return i;
	}

	private void doMergeSublists(int noOfOutputRecords, int sublistNumber1,
			int sublistNumber2) throws IOException {
		outputBlock = new ArrayList<String>();
		sublistContents1 = new TreeMap<Object, String>();
		sublistContents2 = new TreeMap<Object, String>();
		int blockNumber1 = 0;
		int blockNumber2 = 0;
		ArrayList<String> cacheList = null;
		boolean reachedEndOfTheSublist1 = false;
		boolean reachedEndOfTheSublist2 = false;
		while (true) {
			if (sublistContents1.isEmpty() && !reachedEndOfTheSublist1) {
				int temp = blockNumber1;
				blockNumber1 = loadBlocksInMemory(blockNumber1, sublistNumber1,
						sublistContents1);
				int diff = blockNumber1 - temp;
				if (diff == 0)
					reachedEndOfTheSublist1 = true;
			}
			if (sublistContents2.isEmpty() && !reachedEndOfTheSublist2) {
				int temp = blockNumber2;
				blockNumber2 = loadBlocksInMemory(blockNumber2, sublistNumber2,
						sublistContents2);
				int diff = blockNumber2 - temp;
				if (diff == 0)
					reachedEndOfTheSublist2 = true;
			}
			if (reachedEndOfTheSublist1 && reachedEndOfTheSublist2) {
				if (!cacheList.isEmpty() || !outputBlock.isEmpty()) {
					while (!cacheList.isEmpty()) {
						splitCacheListToSmallerChunksThatCanFitInOutputBlock(
								cacheList, noOfOutputRecords);
						writeOutputBlocks();
					}
					if (!outputBlock.isEmpty())
						writeOutputBlocks();
				}
				return;
			} else {
				ArrayList<String> values = null;
				String lastVal = null;
				Collection<String> tempVal = null;
				if (reachedEndOfTheSublist1) {
					tempVal = sublistContents2.values();
				} else if (reachedEndOfTheSublist2) {
					tempVal = sublistContents1.values();
				} else {
					Map.Entry<Object, String> entry = sublistContents1
							.pollFirstEntry();
					SortedMap<Object, String> sortedMap = sublistContents2
							.headMap(entry.getKey());
					tempVal = sortedMap.values();
					lastVal = entry.getValue();
				}
				values = new ArrayList<String>(tempVal);
				tempVal.clear();
				if (cacheList == null)
					cacheList = new ArrayList(values);
				else
					cacheList.addAll(values);
				if (lastVal != null) {
					cacheList.add(lastVal);
				}
				if ((cacheList.size() + outputBlock.size()) >= noOfOutputRecords) {
					splitCacheListToSmallerChunksThatCanFitInOutputBlock(
							cacheList, noOfOutputRecords);
					writeOutputBlocks();
				}
			}
		}
	}

	private void writeOutputBlocks() throws IOException {
		int initIndex = 0;
		for (int i = 0; i < noOfOutputBlock; i++) {
			int bound = Configuration.NUMBER_OF_RECORDS_PER_BLOCK > outputBlock
					.size() ? outputBlock.size()
					: Configuration.NUMBER_OF_RECORDS_PER_BLOCK;
			List<String> subList = outputBlock.subList(initIndex, bound);
			initIndex += Configuration.NUMBER_OF_RECORDS_PER_BLOCK;
			StringBuilder builder = new StringBuilder();
			for (String s : subList) {
				builder.append(s);
				builder.append(System.getProperty("line.separator"));
			}
			String fileName = Configuration.INPUT_PATH_FOR_RECORD_MERGER
					+ Configuration.PATH_DELIMITER
					+ Configuration.SUBLIST_NAMING_SCHEME + "_"
					+ currentMergedSublistNumber + "_"
					+ (outputBlockNoWritten++);
			FileUtils.writeStringToFile(new File(fileName), builder.toString());
			if (outputBlockNoWritten == totMergedBlocksForTwoSublists) {
				outputBlockNoWritten = 0;
				currentMergedSublistNumber++;
			}
		}
		outputBlock = new ArrayList<String>();
	}

	private void splitCacheListToSmallerChunksThatCanFitInOutputBlock(
			ArrayList<String> cacheList, int noOfOutputRecords) {
		int extraRecordIndex = (outputBlock.size() + cacheList.size())
				- noOfOutputRecords;
		if(extraRecordIndex < 0)
			extraRecordIndex = 0;
		extraRecordIndex = cacheList.size() - extraRecordIndex;
		List<String> subList = cacheList.subList(0, extraRecordIndex);
		outputBlock.addAll(new ArrayList<String>(subList));
		subList.clear();
	}
}
