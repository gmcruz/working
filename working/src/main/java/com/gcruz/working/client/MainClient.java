/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.gcruz.working.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author cruz
 */
public class MainClient {
     
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";
    
    
    static String dsname_OLD = "jdbc:h2:C:\\Users\\Cruz\\mpi_arv_24062015"; //Our last release db
    static String dsname_NEW = "jdbc:h2:D:\\Projekte\\Software\\MMI-Service-Platform-v2\\trunk\\Java\\MMIServicePlatformV2\\db\\mpi_arv"; //NEW database with changes 
/*
    static String dsname_OLD = "jdbc:h2:C:\\Training\\MMI\\mpi_main_24062015"; //Our last release db
    static String dsname_NEW = "jdbc:h2:C:\\Training\\MMI\\db\\mpi_main"; //NEW database with changes 
 */   
    
    static String username = "sa";
    static String password = "b1l8u1b9b6e5r";
    static Connection conn_NEW = null;
    static Connection conn_OLD = null;
    static Map tablesAndCounts = new HashMap<Integer, String>();
    
    
    
    public static void main(String[] args) throws Exception {
        
        try{            
/*
           dsname_OLD = dsname_OLD + args[1];
           dsname_OLD = dsname_NEW + args[2];
*/
           conn_NEW = DriverManager.getConnection(dsname_NEW, username, password);
           conn_OLD = DriverManager.getConnection(dsname_OLD, username, password);
           startThisRun(); 
        }catch (Exception e ) {
            System.out.println(e.getMessage());
        }finally{
             if(conn_NEW != null && !conn_NEW.isClosed()){
                conn_NEW.close();
             } 
             if(conn_OLD != null && !conn_OLD.isClosed()){
                conn_OLD.close();
             }    
        }
    }
    
    public static void startThisRun() throws Exception {

        try {

            getThisNewDBsTables();

            Iterator it = tablesAndCounts.entrySet().iterator();
            
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                System.out.println("tablesAndCounts: says " + pair.getValue() + " has " + pair.getKey() + " row(s)");    
                loopTableAndCompare(pair.getValue().toString(), getThisTablesColumns(pair.getValue().toString()));
            }
                        
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {

        }

    }

    
    public static void getThisNewDBsTables() throws Exception {        
        
        Statement stmt = null;
        String queryGetTables = "SELECT TABLE_NAME, ROW_COUNT_ESTIMATE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_CLASS = 'org.h2.table.RegularTable' ORDER BY ROW_COUNT_ESTIMATE DESC";

        try {
            
            stmt = conn_NEW.createStatement();
            ResultSet rs = stmt.executeQuery(queryGetTables);
            while (rs.next()) {                
                String tablename = rs.getString("TABLE_NAME");
                int rowcount = rs.getInt("ROW_COUNT_ESTIMATE");
                tablesAndCounts.put(rowcount, tablename);                             
            }
                        
        } catch (SQLException e ) {
            System.out.println(e.getMessage());
        } finally {
            if (stmt != null) { stmt.close(); }            
        }
        
       System.out.println("**getThisNewDBsTables DONE. NUM Records: " + tablesAndCounts.size());
       
    }
    
    
    private static List<String> getThisTablesColumns(String tablename) throws Exception {     
                
        List<String> columns = new ArrayList<String>();
        Statement stmt = null;
        String queryGetColumns = "SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, TYPE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tablename + "' ORDER BY ORDINAL_POSITION ASC";

        try {
            
            stmt = conn_NEW.createStatement();
            ResultSet rs = stmt.executeQuery(queryGetColumns);
            while(rs.next()){                
                String COLUMN_METADATA = rs.getString("COLUMN_NAME") + "|" + rs.getString("ORDINAL_POSITION") + "|" + rs.getString("COLUMN_DEFAULT") + "|" + rs.getString("IS_NULLABLE") + "|" + rs.getString("DATA_TYPE") + "|" + rs.getString("TYPE_NAME");                
                columns.add(COLUMN_METADATA);                
            }
            
        } catch (SQLException e ) {
            System.out.println(e.getMessage());
        } finally {
            if (stmt != null) { stmt.close(); }
        }
        
        return columns;     
    }

    private static void loopTableAndCompare(String tableName, List<String> thisTablesColumns) throws SQLException {
        
        Statement stmt = null;
        String columnNames = "";
        int countColumnMetadata = 0; 
        String primarykeys = "";
        String csvColumns = "";
        String queryGetRows = "";
        int count = 0;
        
        for(String thisCol : thisTablesColumns){
            String[] colNames = thisCol.split("\\|");
            columnNames = columnNames + colNames[0];
            countColumnMetadata++;
            if(countColumnMetadata != thisTablesColumns.size()){
                columnNames = columnNames + ",";
            }            
        }

        csvColumns = columnNames.toString().replace("[", "").replace("]", "").replace(", ", ",");
        queryGetRows = "SELECT " + csvColumns + " FROM " + tableName;
                
        System.out.println("queryGetRows: "+ queryGetRows);
       
        
        try {            
            primarykeys = getThisTablesPrimaryKey(tableName);
            if(primarykeys.length() > 0){
                System.out.println("PRIMARY KEY: "+ primarykeys);
            }else{             
                System.out.println(ANSI_RED + "PRIMARY KEY: " + primarykeys + ANSI_RESET);
            }
            
        } catch (Exception ex) {
            Logger.getLogger(MainClient.class.getName()).log(Level.SEVERE, null, ex);
        }

        
        try {
            stmt = conn_NEW.createStatement();
            ResultSet rs = stmt.executeQuery(queryGetRows);
            while (rs.next()) {
                String[] colNames2 = columnNames.split("\\,");
                String colVals = "";
                int colCount = 0;
                Map<String,String> thisRowsValsMap = new HashMap();
                for (String col : colNames2){ 
                    thisRowsValsMap.put(col, rs.getString(colNames2[colCount]));
                    colCount++;
                }   
                count++;                
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
        System.out.println("csvColumns:" + csvColumns);
        System.out.println(tableName + " looped over " + count + " times.");

    }
    
    //Get a tables priary keys (it only comes back as a comma delimeted list)
    private static String getThisTablesPrimaryKey(String tablename) throws Exception {     
                
        String key = "";
        Statement stmt = null;
        String queryGetColumns = "SELECT COLUMN_LIST FROM INFORMATION_SCHEMA.CONSTRAINTS WHERE TABLE_NAME = '" + tablename + "' AND CONSTRAINT_TYPE='PRIMARY KEY'";

        try {
            
            stmt = conn_NEW.createStatement();
            ResultSet rs = stmt.executeQuery(queryGetColumns);
            while (rs.next()) {                
                key = rs.getString("COLUMN_LIST");  
            }
            
        } catch (SQLException e ) {
            System.out.println(e.getMessage());
        } finally {
            if (stmt != null) { stmt.close(); }
        }
        
        return key;     
    }
    
}

