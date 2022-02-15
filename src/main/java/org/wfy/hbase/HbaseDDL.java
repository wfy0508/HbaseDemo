package org.wfy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Summer
 * @program: org.wfy.hadoop.hbase
 * @description Hbase操作练习
 * @create 2022-02-2022/2/14 13:38
 **/
public class HbaseDDL {
    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;

    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node1,node2,node3");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取admin
     */
    public static Admin getAdmin() throws IOException {
        admin = connection.getAdmin();
        return admin;
    }

    public static void main(String[] args) throws IOException {
        createNamespace("mydb3");
        //createTable("test1", "info", "name", "gender");
    }

    /**
     * 创建命名空间
     */
    private static void createNamespace(String ns) throws IOException {
        Admin admin = getAdmin();
        // 3 使用NamespaceDescriptor创建命名空间描述器
        NamespaceDescriptor build = NamespaceDescriptor.create(ns).build();
        // 4 执行创建操作
        try {
            admin.createNamespace(build);
        } catch (NamespaceExistException e) {
            System.out.println(ns + " 已存在！");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            admin.close();
            connection.close();
        }
    }

    /**
     * 创建表
     */
    public static void createTable(String tableName, String... columnFamily) throws IOException {
        if (isExists(tableName)) {
            System.out.println(tableName + " 已存在！");
        }

        if (columnFamily.length <= 0) {
            System.out.println("请添加列族！");
            return;
        }
        // 2 创建DDL操作对象
        Admin admin = getAdmin();
        // 创建Table描述器
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        // 循环添加列族
        for (String cf : columnFamily) {
            // 先构建列族描述器
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
            // 添加列族
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }
        TableDescriptor build = tableDescriptorBuilder.build();
        try {
            admin.createTable(build);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            admin.close();
            connection.close();
        }

    }

    /**
     * 判断表是否存在
     */
    public static boolean isExists(String tableName) throws IOException {
        // 2 创建DDL操作对象
        Admin admin = getAdmin();
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        connection.close();
        return exists;
    }
}