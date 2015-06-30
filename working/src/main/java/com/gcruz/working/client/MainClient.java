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
    static String dsname_OLD = "jdbc:h2:C:\\Training\\MMI\\mpi_arv_24062015"; //Our last release db
    static String dsname_NEW = "jdbc:h2:C:\\Training\\MMI\\db\\mpi_arv"; //NEW database with changes 
 */   
    
    static String username = "sa";
    static String password = "b1l8u1b9b6e5r";
    static Connection conn_NEW = null;
    static Connection conn_OLD = null;
    static Map tablesAndCounts = new HashMap<Integer, String>();
    
    
    
    public static void main(String[] args) throws Exception {
        
        try{
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
                System.out.println("tablesAndCounts: says " + pair.getValue() + " has " + ANSI_GREEN + pair.getKey() + ANSI_RESET + " row(s)");    
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
        
        Statement stmt_NEW = null;       
        String columnNames = "";
        int countColumnMetadata = 0; 
        List<String> primarykeys = null;
        String csvColumns = "";
        String queryGetRows = "";
        int count_RSNEW = 0;
                
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
        } catch (Exception ex) {
            Logger.getLogger(MainClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        try {            
            stmt_NEW = conn_NEW.createStatement();
            ResultSet rs_NEW = stmt_NEW.executeQuery(queryGetRows);
           
            while (rs_NEW.next()) {            
                process_NEWRS(rs_NEW, csvColumns, queryGetRows, primarykeys);                    
                count_RSNEW++;                
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            if (stmt_NEW != null) {
                stmt_NEW.close();
            }
        }
        
        System.out.println("csvColumns:" + csvColumns);
        System.out.println(ANSI_GREEN + tableName + " looped over " + count_RSNEW + " times." + ANSI_RESET);

    }


    private static void process_NEWRS(ResultSet rs_NEW, String csvColumns, String queryGetRows, List<String> primaryKeys) {
        try {
            Statement stmt_OLD = null;
            
            String[] colNames = csvColumns.split("\\,");
            String colVals = "";
            int colCount = 0;
            Map<String,String> thisRowsValsMap = new HashMap();
            String whereClause = " WHERE ";
            
            for (String col : colNames){                
                thisRowsValsMap.put(col, rs_NEW.getString(colNames[colCount]));          
                colCount++;
            }
            
            //TODO we have to take into account deletetions in the new database. we have to find them in the old
            
            //At this point we have a row from the new databasse and we its values we now need to find the same 
            //record in the old database and compare to possible update or insert (if not found).
            int primaryKeyCount = 0;
            if (primaryKeys.size() > 0) {                     
                for (String primaryKey : primaryKeys) {
                    if (primaryKeyCount > 0) {
                        whereClause = whereClause + " AND ";
                    } else {whereClause = whereClause + " ";}
                    whereClause = whereClause + primaryKey + " = " + thisRowsValsMap.get(primaryKey);
                    primaryKeyCount++;
                }
            } else {//USE ALL COLUMNS AS A PRIMARY KEY but not the best solution as we dont know if it is an insert or a create.
                for (String primaryKey : colNames) {               
                    if (primaryKeyCount > 0) {
                        whereClause = whereClause + " AND ";
                    } else {whereClause = whereClause + " ";}
                    whereClause = whereClause + primaryKey + " = " + thisRowsValsMap.get(primaryKey);
                    primaryKeyCount++;                                    
                }
            }

            stmt_OLD = conn_OLD.createStatement();
            ResultSet rs_OLD = stmt_OLD.executeQuery(queryGetRows+whereClause);
            
        } catch (SQLException ex) {
            Logger.getLogger(MainClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
 
    
    private static List<String> getThisTablesPrimaryKey(String tablename) throws Exception {     
        
        List<String> keys = new ArrayList<String>();        
        String key = "";
        Statement stmt = null;
        String queryGetColumns = "SELECT COLUMN_LIST FROM INFORMATION_SCHEMA.CONSTRAINTS WHERE TABLE_NAME = '" + tablename + "' AND CONSTRAINT_TYPE='PRIMARY KEY'";

        try {
            
            stmt = conn_NEW.createStatement();
            ResultSet rs = stmt.executeQuery(queryGetColumns);
            while (rs.next()) {                
                key = rs.getString("COLUMN_LIST");  
            }
            
            if(key.length() > 0){
                String[] colNames = key.split("\\,");

                for (String col : colNames){                
                    keys.add(col);               
                }
            }
            
            if(keys.isEmpty()){
                System.out.println(ANSI_RED + "PRIMARY KEY: " + keys.toString() + ANSI_RESET);                
            }else{             
                System.out.println("PRIMARY KEY: "+ keys.toString());
            }     
            
            
        } catch (SQLException e ) {
            System.out.println(e.getMessage());
        } finally {
            if (stmt != null) { stmt.close(); }
        }
        
        return keys;     
    }
    
    
}

