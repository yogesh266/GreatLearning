import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {



    public static boolean tableExists(Connection connection, String tableName) throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet resultSet = meta.getTables(null, "test", tableName, new String[] {"TABLE"});

        return resultSet.next();
    }



    public static void main(String[] args) throws Exception {
        System.out.println("Application Started.....");
        Connection con = DriverManager.getConnection("jdbc:spark://amaaaaaav66vvniahdqqaywxhhftugrvy7cp7tbkmryxulf6sukrsv5g5cma.interactive.dataflowclusters.us-ashburn-1.oci.oraclecloud.com/default;SparkServerType=DFI;");


        System.out.println("Successfully connected");
        Statement stmt = con.createStatement();
        stmt.execute("use test");

        ResultSet rs = stmt.executeQuery("select LatD, LatM, City from City");
        //System.out.println(convert(rs));


        List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
        Map<String, Object> row = null;

        ResultSetMetaData metaData = rs.getMetaData();
        Integer columnCount = metaData.getColumnCount();

        while (rs.next()) {
            row = new HashMap<String, Object>();
            for (int i = 1; i <= columnCount; i++) {
                row.put(metaData.getColumnName(i), rs.getObject(i));
            }
            resultList.add(row);
        }

        System.out.println(resultList);
    }
    }
