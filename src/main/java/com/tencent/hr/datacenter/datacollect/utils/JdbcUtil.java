package com.tencent.hr.datacenter.datacollect.utils;


import com.tencent.hr.datacenter.datacollect.common.Constant;
import com.tencent.hr.datacenter.datacollect.entity.ProjectListBean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class JdbcUtil implements SourceFunction<JdbcUtil.TM> {


    public static Connection getJdbcConnection(String driver, String url, String user, String password) {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("你提供的驱动类错误, 请检查数据库连接器依赖是否导入, 或者驱动名字是否正确: " + driver);
        }
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("你提供的url或者user或者password 有误, 请检查:url=" + url + ", user=" + user + ",password=" + password);
        }

    }

    public static void closeConnection(Connection conn) {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static <T> List<T> queryList(Connection conn, String querySql, Object[] args, Class<T> tClass) {

        ArrayList<T> result = new ArrayList<>();

        try {
            PreparedStatement ps = conn.prepareStatement(querySql);
            // 给sql中的占位符赋值
            for (int i = 0; args != null && i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }

            ResultSet resultSet = ps.executeQuery();

            ResultSetMetaData metaData = resultSet.getMetaData(); // 元数据: 列名 列的类型 列的别名
            int columnCount = metaData.getColumnCount();  // 查询的结果有多少列
            // 遍历出结果集中的每一行
            while (resultSet.next()) {
                // 每一行数据, 封装到一个T类型的对象中, 然后放入到result这个集合中
                T t = tClass.newInstance(); // 利用反射创建一个T类型的对象

                // 给T中属性赋值, 属性的值从resultSet获取
                // 从resultSet里面查看有多少列, 每一列在T中应该对应一个属性
                for (int i = 1; i <= columnCount; i++) { // 列的索引应该从1开始,
                    // 列名
                    String columnName = metaData.getColumnLabel(i);
                    Object v = resultSet.getObject(i);

                    BeanUtils.setProperty(t, columnName, v);
                }

                result.add(t);

            }
            ps.close();


        } catch (Exception e) {
            e.printStackTrace();
        }


        return result;
    }


    @Override
    public void run(SourceContext<TM> ctx) throws Exception {
        String Sql = Constant.FTESql;
        List<TM> list = queryList(getJdbcConnection(Constant.PG_DRIVER, Constant.PG_URL, Constant.PG_USER, Constant.PG_PASSWORD),
                Sql,
                null,
                TM.class
        );


        for (TM obj : list) {
            ctx.collect(obj);

        }

    }

    @Override
    public void cancel() {

    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    public static class TM {

        private String rioid;
        private String FTE_number;

    }
}
