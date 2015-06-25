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


/**
 *
 * @author cruz
 */
public class MainClient {
    
    static String dsname_OLD = "jdbc:h2:C:\\Users\\Cruz\\mpi_main_24062015"; //Our last release db
    static String dsname_NEW = "jdbc:h2:D:\\Projekte\\Software\\MMI-Service-Platform-v2\\trunk\\Java\\MMIServicePlatformV2\\db\\mpi_main"; //NEW database with changes 
    static String username = "sa";
    static String password = "b1l8u1b9b6e5r";
    static Connection conn_NEW = null;
    static Connection conn_OLD = null;
    static Map tablesAndCounts = new HashMap<Integer, String>();
    
    public static void main(String[] args) throws Exception {
       // Class.forName("org.h2.Driver"); Needed ?
        
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
                System.out.println(pair.getKey() + " = " + pair.getValue());                
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
                               
                System.out.println(tablename + " has " + rowcount + " rows.");
                tablesAndCounts.put(rowcount, tablename);
                             
            }
                        
        } catch (SQLException e ) {
            System.out.println(e.getMessage());
        } finally {
            if (stmt != null) { stmt.close(); }
        }
        
       
    }
    
    
    private static List<String> getThisTablesColumns(String tablename) throws Exception {     
        
        List<String> columns = new ArrayList<String>();
        Statement stmt = null;
        String queryGetColumns = "SELECT COLUMN_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tablename + "' ORDER BY ORDINAL_POSITION ASC";

        try {
            
            stmt = conn_NEW.createStatement();
            ResultSet rs = stmt.executeQuery(queryGetColumns);
            while (rs.next()) {
                
                String columnname = rs.getString("COLUMN_NAME");
                int ordinal = rs.getInt("ORDINAL_POSITION");               
                
                System.out.println("     columnname:" + columnname + " pos." + ordinal);
                
                columns.add(columnname);
                
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
        String csvColumns = thisTablesColumns.toString().replace("[", "").replace("]", "").replace(", ", ",");
        String queryGetRows = "SELECT " + csvColumns + " FROM " + tableName;
        
       
         try { 
            stmt = conn_OLD.createStatement();
            ResultSet rs = stmt.executeQuery(queryGetRows);
            while (rs.next()) {

                String columnname = rs.getString(thisTablesColumns.get(1));
                String ordinal = rs.getString(thisTablesColumns.get(2));

                System.out.println("     values:" + columnname + " ordi:" + ordinal);

            }
        } catch (SQLException e ) {
            System.out.println(e.getMessage());
        } finally {
            if (stmt != null) { stmt.close(); }
        }

    }
    
    
    
}

