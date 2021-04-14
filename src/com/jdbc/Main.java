package com.jdbc;

import java.io.BufferedReader;      //This class is used to read the text from a character-based input stream
import java.io.FileReader;          //This class is used to read data from the file & returns data in byte format like FileInputStream class.
import java.io.IOException;         //This is related to Input and Output operations.Happens when there is a failure during reading, writing and searching file .
import java.sql.Connection;         // to Connect Java Application with mysql database
import java.sql.DriverManager;
import java.sql.PreparedStatement;  //Sub-interface of Statement-> used to execute parameterized query.
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;  //This class provides methods to format and parse date and time in java. 
import java.util.ArrayList;       
import java.sql.Date;               //This class represents only date in java. Used because it represents the date that can be stored in database

public class Main {
	
  //JDBC driver name and database URL
  static final String JDBC_DRIVER = "Your_JDBC_DRIVER";
  static final String DB_URL = "Your_DB_URL"; 
  
  //Database credentials
  static final String USER = "Your_USER_Name"; 
  static final String PASS = "Your_Password"; 

  public static Date parseDate(String date, String dateFormat) {
    if (date.isEmpty())              // Checking for empty records in date column, if any, it will return NULL.
    {
      return null;
    } 
    else 
    {
      SimpleDateFormat format = new SimpleDateFormat(dateFormat);  //Creating instance of this concrete class for formatting and parsing date(string->date type)
      java.util.Date ret;            // ret is a variable in which parsed date is stored.
      try
      {
        ret = format.parse(date);    // now date is being parsed(string to date type) and stored in ret.
        Date clearDate = new Date(ret.getTime()); //returns the number of milliseconds, helps to assign a date and time to another Date object
        return clearDate;   
      } 
      catch (ParseException e)       // catch if any parse exception is found.
      {
        e.printStackTrace();         // prints that exception.
        return null;
      }

    }
  }

  public static ArrayList<Invoice> readinvoices() throws Exception {
	  
    String line = "";
    String splitBy = ",";
    ArrayList<Invoice> invoices = new ArrayList<Invoice>();        //Creating arraylist of Invoice class(POJO class)
    
    // parsing a CSV file into BufferedReader class constructor
    // BufferedReader br = new BufferedReader(Reader in)
    // FileReader is the child class of Reader class.
    BufferedReader br = new BufferedReader(new FileReader("dataset.csv"));
    br.readLine();                                                 // method of BufferReader class, which is used to read data line by line.

    try {   
    	while ((line = br.readLine()) != null)                     // returns a Boolean value
        {
        String[] row = line.split(splitBy);                        // use comma as separator
        String businessCode = row[0];
        String custNumber = row[1];
        String nameCustomer = row[2];
        Date clearDate = parseDate(row[3], "yyyy-MM-dd hh:mm:ss"); //parseDate: parse a string to date value, and return standard date in specified format.
        int businessYear = (int) Float.parseFloat(row[4]);         // converting float data type of businessYear to int(type conversion).
        long docId = (long) Double.parseDouble(row[5]);
        Date postingDate = parseDate(row[6], "yyyy-MM-dd");
        Date documentCreateDate = parseDate(row[8], "yyyyMMdd");
        Date dueInDate = parseDate(row[9], "yyyyMMdd");
        String invoiceCurrency = row[10];
        String documentType = row[11];
        boolean postingId = Boolean.parseBoolean(row[12]);         // postingID returns 0 or 1.
        String areaBusiness = row[13];
        Double totalOpenAmount = Double.parseDouble(row[14]);
        Date baselineCreateDate = parseDate(row[15], "yyyyMMdd");
        String custPaymentTerms = row[16];
        long invoiceId = row[17] != null && !row[17].isEmpty() ? (long) Double.parseDouble(row[17]) : 0;  // checks whether invoice id is not null and not empty, if so invoice id has long datatype or else it returns 0.
        boolean isOpen = Boolean.parseBoolean(row[18]);
        //object
        Invoice invoice = new Invoice(businessCode, custNumber, nameCustomer, clearDate, businessYear, docId,
            postingDate, documentCreateDate, dueInDate, invoiceCurrency, documentType, postingId,
            areaBusiness, totalOpenAmount, baselineCreateDate, custPaymentTerms, invoiceId, isOpen);      //All the columns are stored in invoice instance of Invoice class.
        invoices.add(invoice);                                     // Adding the columns in invoices variable.
      }
    } 
    catch (IOException e) 
    {
      e.printStackTrace();
    }
    System.out.println("First Row: \n");
    System.out.println(invoices.get(0));  //prints the first row of the database.
    return invoices;
  }
  public static void insert(ArrayList<Invoice> invoices) {
	    String query = "INSERT into invoice_details (business_code,cust_number,name_customer,clear_date,business_year,doc_id,posting_date,document_create_date,due_in_date,sales_order_currency,document_type,posting_id,area_business,total_open_amount,baseline_create_date,cust_payment_terms,sales_order_id,isOpen) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	    //Reference variables : refers to the object of class which implements these interfaces.
	    Connection conn = null;
	    PreparedStatement stmt = null;   
	    
	    try {
	      //Class : predefined class in java
	      //forName : static method as we are calling it directly with the help of Class.	
	      Class.forName("com.mysql.jdbc.Driver");                   // Loads the driver
	      conn = DriverManager.getConnection(DB_URL, USER, PASS);   // Establishes the connection with the database.
	      stmt = conn.prepareStatement(query);                      // prepareStatement: properly prepare and compile the sql query and store it in an object
	      for (int i = 0; i < invoices.size(); ++i) {               // Inserting the data into its specific columns.
	        Invoice invoice = invoices.get(i);
	        stmt.setString(1, invoice.getBusinessCode());
	        stmt.setString(2, invoice.getCustNumber());
	        stmt.setString(3, invoice.getNameCustomer());
	        stmt.setDate(4, invoice.getClearDate());
	        stmt.setInt(5, invoice.getBusinessYear());
	        stmt.setLong(6, invoice.getDocId());
	        stmt.setDate(7, invoice.getPostingDate());
	        stmt.setDate(8, invoice.getDocumentCreateDate());
	        stmt.setDate(9, invoice.getDueInDate());
	        stmt.setString(10, invoice.getInvoiceCurrency());
	        stmt.setString(11, invoice.getDocumentType());
	        stmt.setBoolean(12, invoice.isPostingId());
	        stmt.setString(13, invoice.getAreaBusiness());
	        stmt.setDouble(14, invoice.getTotalOpenAmount());
	        stmt.setDate(15, invoice.getBaselineCreateDate());
	        stmt.setString(16, invoice.getCustPaymentTerms());
	        stmt.setLong(17, invoice.getInvoiceId());
	        stmt.setBoolean(18, invoice.isOpen());
	        stmt.addBatch();         //
	        if (i % 5000 == 0) {     // batch-size = 5000
	          stmt.executeBatch();
	        }
	      }
	      stmt.executeBatch();       // If any number of statement will be left for execution, it will be executed from here.
	    } 
	    catch (SQLException se)      // Handle errors for JDBC
	    {
	      se.printStackTrace();
	    } 
	    catch (Exception e)          // Handle errors for Class.forName
	    {
	      e.printStackTrace();
	    } 
	    finally                      // finally block is used to close resources
	    {
	      try 
	      {
	        if (stmt != null)
	          stmt.close();           
	      } 
	      catch (SQLException se2) {  // Handles the sql exceptions if any.
	      }
	      try 
	      {
	        if (conn != null)
	          conn.close();           // To close the established connection.
	      } 
	      catch (SQLException se)      
	      {
	        se.printStackTrace();
	      }
	    }
	  }

	  public static void main(String args[]) throws Exception {
	    ArrayList<Invoice> invoices = readinvoices();
	    System.out.println("\nThe size of the databse is: "+invoices.size());       // Print the total number of rows of the dataset.
	    insert(invoices);                                                           // Calls the insert function inserting invoices in the database.
	    System.out.println("\nFinally all the data have been successfully entered into the database");
	  }
	}