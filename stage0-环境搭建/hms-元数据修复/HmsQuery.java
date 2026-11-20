import java.sql.*;

public class HmsQuery {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:derby:/home/BaiMenghui/hudi-bigdata/metastore_db";
        Connection c = DriverManager.getConnection(url);
        System.out.println("== HMS tables (ads_%) columns ==");
        String q1 = "SELECT t.TBL_NAME, col.COLUMN_NAME, col.TYPE_NAME"
                + " FROM APP.TBLS t"
                + " JOIN APP.SDS s ON t.SD_ID = s.SD_ID"
                + " JOIN APP.COLUMNS_V2 col ON s.CD_ID = col.CD_ID"
                + " WHERE t.TBL_NAME LIKE 'ads_%'"
                + " ORDER BY t.TBL_NAME, col.INTEGER_IDX";
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(q1)) {
            while (rs.next()) {
                System.out.println(rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3));
            }
        }
        System.out.println("== HMS partition keys (ads_%) ==");
        String q2 = "SELECT t.TBL_NAME, pk.PKEY_NAME, pk.PKEY_TYPE"
                + " FROM APP.TBLS t"
                + " JOIN APP.PARTITION_KEYS pk ON t.TBL_ID = pk.TBL_ID"
                + " WHERE t.TBL_NAME LIKE 'ads_%'"
                + " ORDER BY t.TBL_NAME, pk.INTEGER_IDX";
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(q2)) {
            while (rs.next()) {
                System.out.println(rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3));
            }
        }
        System.out.println("== HMS table locations (ads_%) ==");
        String q3 = "SELECT t.TBL_NAME, s.LOCATION FROM APP.TBLS t"
                + " JOIN APP.SDS s ON t.SD_ID = s.SD_ID"
                + " WHERE t.TBL_NAME LIKE 'ads_%' ORDER BY t.TBL_NAME";
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(q3)) {
            while (rs.next()) {
                System.out.println(rs.getString(1) + "\t" + rs.getString(2));
            }
        }
        c.close();
        System.out.println("HMS_QUERY_DONE");
    }
}
