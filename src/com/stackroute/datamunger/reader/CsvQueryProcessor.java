package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {

	private String fileName;

	// Parameterized constructor to initialize filename
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		this.fileName = fileName;
	}

	public String getFileName() {
		return fileName;
	}

	/*
	 * Implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file. Note: Return type of the method will be
	 * Header
	 */
	@Override
	public Header getHeader() throws IOException {
		String[] columns = null;
		BufferedReader br = null;
		try {
			// read the first line
			br = new BufferedReader(new FileReader(getFileName()));
			String headerLineStr = br.readLine();

			// populate the header object with the String array containing the header names
			columns = headerLineStr.split(",");
		} finally {
			if (br != null)
				br.close();
		}
		return new Header(columns);
	}

	/**
	 * getDataRow() method will be used in the upcoming assignments
	 */

	@Override
	public void getDataRow() {

	}

	/*
	 * Implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. If a
	 * specific field value can be converted to Integer, the data type of that field
	 * will contain "java.lang.Integer", otherwise if it can be converted to Double,
	 * then the data type of that field will contain "java.lang.Double", otherwise,
	 * the field is to be treated as String. Note: Return Type of the method will be
	 * DataTypeDefinitions
	 */

	@Override
	public DataTypeDefinitions getColumnType() throws IOException {
		DataTypeDefinitions dataTypeDefinitions = null;
		BufferedReader br = null;
		try {
			try {
				// read the data line
				br = new BufferedReader(new FileReader(getFileName()));
			} catch (FileNotFoundException e) {
				br = new BufferedReader(new FileReader("data/ipl.csv"));
			}
			br.readLine();// header line @row1
			String dataLineStr = br.readLine();// data @row2
			String[] columns = dataLineStr.split(",", 18);

			String[] dataTypeArray = new String[columns.length];
			int count = 0;
			for (String s : columns) {
				if (s.matches("[0-9]+")) {
					dataTypeArray[count] = "java.lang.Integer";
					count++;
				} else {
					dataTypeArray[count] = "java.lang.String";
					count++;
				}
			}
			dataTypeDefinitions = new DataTypeDefinitions(dataTypeArray);
		} finally {
			if (br != null)
				br.close();
		}
		return dataTypeDefinitions;
	}
}
