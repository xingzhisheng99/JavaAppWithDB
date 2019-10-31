/*
 * Connect the tvShows.sqlite file to SQLiteJDBCDriver 
 * and handle data 
 * (for testing and debugging the queries only - results are shown in console rather than the UI)
 *
 */
package javaappwithdb;

import java.sql.*;
import sun.security.rsa.RSACore;

/**
 *
 * @author Yishuo Tang
 */
public class sqlConnection {

    public static Connection connect() {
        // SQLite connection string
        String url = "jdbc:sqlite:/Users/iris/PycharmProjects/untitled/tvshows.sqlite";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }

    public ResultSet getTable(String table_name) {
        String sql = "SELECT id, name, type, language, status, runtime, "
                + " premiered, schedule_day, rating FROM "+ table_name;
      
        
        try (Connection conn = this.connect();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            /*
            // parse col names automatically
            ResultSetMetaData md = rs.getMetaData();
            int colCount = md.getColumnCount();
            String[] col_name = new String[colCount];

            for (int i = 0; i < colCount; i++) {
                col_name[i] = md.getColumnName(i + 1);
                System.out.print(col_name[i] + "\t");
            }

            System.out.println();

            // print out data for every column
            while (rs.next()) {
                for (int i = 0; i < colCount; i++) {
                    System.out.print(rs.getString(col_name[i]) + "\t");
                }
                System.out.println();
               
            }
*/
            return rs;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }       
    }

    public void getAllEpisodes(String show_name) {
        String sql = "SELECT Episodes.name, Episodes.show_id, "
                + "Episodes.season, Episodes.number FROM Shows, Episodes "
                + "WHERE Episodes.show_id = Shows.id and Shows.name = ?";

        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the value
            pstmt.setString(1, show_name);
            //
            ResultSet rs = pstmt.executeQuery();

            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("show_id") + "\t"
                        + rs.getString("name") + "\t"
                        + rs.getInt("season") + "\t"
                        + rs.getInt("number")
                );
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void getSeason(String show_name, int season) {
        String sql = "SELECT Episodes.name, Episodes.show_id, "
                + "Episodes.season, Episodes.number FROM Shows, Episodes "
                + "WHERE Episodes.show_id = Shows.id and Shows.name = ? "
                + "and Episodes.season = ?";

        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the value
            pstmt.setString(1, show_name);
            pstmt.setInt(2, season);
            //
            ResultSet rs = pstmt.executeQuery();

            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("show_id") + "\t"
                        + rs.getString("name") + "\t"
                        + rs.getInt("season") + "\t"
                        + rs.getInt("number")
                );
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void getEpisode(String show_name, int season, int number) {
        String sql = "SELECT Episodes.name, Episodes.show_id, "
                + "Episodes.season, Episodes.number FROM Shows, Episodes "
                + "WHERE Episodes.show_id = Shows.id and Shows.name = ? "
                + "and Episodes.season = ? and Episodes.number = ?";

        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the value
            pstmt.setString(1, show_name);
            pstmt.setInt(2, season);
            pstmt.setInt(3, number);
            //
            ResultSet rs = pstmt.executeQuery();

            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("show_id") + "\t"
                        + rs.getString("name") + "\t"
                        + rs.getInt("season") + "\t"
                        + rs.getInt("number")
                );
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void getGenre(String show_name) {
        String sql = "SELECT Shows.name, Genres.genre_name, Genres.id "
                + "FROM Shows, Genres, Genre_Show  WHERE Shows.id = "
                + "Genre_Show.show_id and Genres.id = Genre_Show.genre_id "
                + "and Shows.name = ?";

        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the value
            pstmt.setString(1, show_name);
            //
            ResultSet rs = pstmt.executeQuery();

            System.out.println(rs.getString("name") + "Genre(s):");
            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getString("genre_name") + "\t"
                        + rs.getInt("id"));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    public void getScheduleDay(String show_name) {
        String sql = "SELECT name, schedule_day "
                + "FROM Shows "
                + "WHERE name = ?";

        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the value
            pstmt.setString(1, show_name);
            //
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                System.out.println(rs.getString("name") + "\t"
                        + rs.getString("schedule_day"));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    public void getShowFromScheduleDay(String schedule_day) {
        String sql = "SELECT name, schedule_day "
                + "FROM Shows "
                + "WHERE schedule_day = ?";

        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the value
            pstmt.setString(1, schedule_day);
            //
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                System.out.println(rs.getString("name") + "\t"
                        + rs.getString("schedule_day"));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    // get show from a certain premiere year
    public void getShowFromPremiereYear(String premiered_year) {
        String sql = "SELECT name, premiered "
                + "FROM Shows "
                + "WHERE premiered = ?";

        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {

            //String year = premiered_year + "%";
            // set the value
            //pstmt.setString(1, year);
            //
            pstmt.setString(1, premiered_year);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                System.out.println(rs.getString("name") + "\t"
                        + rs.getString("premiered"));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    public void getRatingLargerThan(double rating){
        String sql = "SELECT * FROM Shows WHERE rating >= " + rating;
        
        try (Connection conn = this.connect();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){
            
            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("id") +  "\t" + 
                                   rs.getString("name") + "\t" +
                                   rs.getDouble("rating"));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    public void getNetwork(){
        String sql = "SELECT Shows.id, Shows.name, Networks.id, Networks.name, "
                + "Networks.country_code, Countries.name, Countries.timezone "
                + "FROM Shows, Networks, Countries "
                + "WHERE Shows.network_id = Networks.id and "
                + "Networks.country_code = Countries.code";

        
        try (Connection conn = this.connect();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){
            
            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("timezone"));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

}
