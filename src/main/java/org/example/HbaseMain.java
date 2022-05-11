package org.example;

import com.google.protobuf.ServiceException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import sun.plugin2.util.PojoUtil;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map;
import java.util.Properties;

public class HbaseMain{
    private static final Logger logger = LoggerFactory.getLogger(HbaseMain.class);
    public static void main(String[] args) {

        Connection connection = null;
        try {

            connection = getConnection();
            TableName tableName = TableName.valueOf("zhushengping:student");

            //删除DeviceState表
            deleteTable(connection, tableName);
            logger.info("删除表 {}", tableName.getNameAsString());

            //创建DeviceState表
            createTable(connection, tableName, "info", "score");

            logger.info("创建表 {}", tableName.getNameAsString());

            //写入数据
            put(connection, tableName, "Tom", "info", "student_id", "202100000001");
            put(connection, tableName, "Tom", "info", "class", "1");
            put(connection, tableName, "Tom", "score", "understanding", "75");
            put(connection, tableName, "Tom", "score", "programming", "82");
            put(connection, tableName, "Jerry", "info", "student_id", "202100000002");
            put(connection, tableName, "Jerry", "info", "class", "1");
            put(connection, tableName, "Jerry", "score", "understanding", "85");
            put(connection, tableName, "Jerry", "score", "programming", "67");
            put(connection, tableName, "Jack", "info", "student_id", "202100000003");
            put(connection, tableName, "Jack", "info", "class", "2");
            put(connection, tableName, "Jack", "score", "understanding", "80");
            put(connection, tableName, "Jack", "score", "programming", "80");
            put(connection, tableName, "Rose", "info", "student_id", "202100000004");
            put(connection, tableName, "Rose", "info", "class", "2");
            put(connection, tableName, "Rose", "score", "understanding", "60");
            put(connection, tableName, "Rose", "score", "programming", "61");
            put(connection, tableName, "zhushengping", "info", "student_id", "G20220735030062");
            put(connection, tableName, "zhushengping", "info", "class", "3");
            put(connection, tableName, "zhushengping", "score", "understanding", "75");
            put(connection, tableName, "zhushengping", "score", "programming", "82");
            logger.info("写入数据.");

            String value = getCell(connection, tableName, "zhushengping", "info", "student_id");
            logger.info("读取单元格-row1.state:{}", value);

            Map<String, String> row = getRow(connection, tableName, "zhushengping");

            logger.info("读取单元格-row2");

            List<Map<String, String>> dataList = scan(connection, tableName, null, null);
            logger.info("扫描表结果-");



            logger.info("操作完成.");
        } catch (Exception e) {
            logger.error("操作出错", e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    logger.error("error occurs", e);
                }
            }
        }


    }

    /**
     * 建立连接
     *
     * @return
     */
    public static Connection getConnection() {
        try {
            //获取配置

            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.property.clientPort", "2181");
            config.set("hbase.zookeeper.quorum", "emr-worker-2,emr-worker-1,emr-header-1");
            config.set("hbase.master","127.0.0.1:60000");
            return ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

/**
 * 创建表
 * @param connection
 * @param tableName
 * @param columnFamilies
 * @throws IOException
 */
public static void createTable(Connection connection, TableName tableName, String... columnFamilies) throws IOException {
        Admin admin = null;
        try {
        admin = connection.getAdmin();
        if (admin.tableExists(tableName)) {
        logger.warn("table:{} exists!", tableName.getName());
        } else {
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
        for (String columnFamily : columnFamilies) {
        builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily));
        }
        admin.createTable(builder.build());
        logger.info("create table:{} success!", tableName.getName());
        }
        } finally {
        if (admin != null) {
        admin.close();
        }
        }
        }


/**
 * 插入数据
 *
 * @param connection
 * @param tableName
 * @param rowKey
 * @param columnFamily
 * @param column
 * @param data
 * @throws IOException
 */
public static void put(Connection connection, TableName tableName,
        String rowKey, String columnFamily, String column, String data) throws IOException {

        Table table = null;
        try {
        table = connection.getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
        table.put(put);
        } finally {
        if (table != null) {
        table.close();
        }
        }
        }

/**
 * 根据row key、column 读取
 *
 * @param connection
 * @param tableName
 * @param rowKey
 * @param columnFamily
 * @param column
 * @throws IOException
 */
public static String getCell(Connection connection, TableName tableName, String rowKey, String columnFamily, String column) throws IOException {
        Table table = null;
        try {
        table = connection.getTable(tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));

        Result result = table.get(get);
        List<Cell> cells = result.listCells();

        if (CollectionUtils.isEmpty(cells)) {
        return null;
        }
        String value = new String(CellUtil.cloneValue(cells.get(0)), "UTF-8");
        return value;
        } finally {
        if (table != null) {
        table.close();
        }
        }
        }

/**
 * 根据rowkey 获取一行
 *
 * @param connection
 * @param tableName
 * @param rowKey
 * @return
 * @throws IOException
 */
public static Map<String, String> getRow(Connection connection, TableName tableName, String rowKey) throws IOException {
        Table table = null;
        try {
        table = connection.getTable(tableName);
        Get get = new Get(Bytes.toBytes(rowKey));

        Result result = table.get(get);
        List<Cell> cells = result.listCells();

        if (CollectionUtils.isEmpty(cells)) {
        return Collections.emptyMap();
        }
        Map<String, String> objectMap = new HashMap<>();
        for (Cell cell : cells) {
        String qualifier = new String(CellUtil.cloneQualifier(cell));
        String value = new String(CellUtil.cloneValue(cell), "UTF-8");
        objectMap.put(qualifier, value);
        }
        return objectMap;
        } finally {
        if (table != null) {
        table.close();
        }
        }
        }

/**
 * 扫描权标的内容
 *
 * @param connection
 * @param tableName
 * @param rowkeyStart
 * @param rowkeyEnd
 * @throws IOException
 */
public static List<Map<String, String>> scan(Connection connection, TableName tableName, String rowkeyStart, String rowkeyEnd) throws IOException {
        Table table = null;
        try {
        table = connection.getTable(tableName);
        ResultScanner rs = null;
        try {
        Scan scan = new Scan();
        if (!StringUtils.isEmpty(rowkeyStart)) {
        scan.withStartRow(Bytes.toBytes(rowkeyStart));
        }
        if (!StringUtils.isEmpty(rowkeyEnd)) {
        scan.withStopRow(Bytes.toBytes(rowkeyEnd));
        }
        rs = table.getScanner(scan);

        List<Map<String, String>> dataList = new ArrayList<>();
        for (Result r : rs) {
        Map<String, String> objectMap = new HashMap<>();
        for (Cell cell : r.listCells()) {
        String qualifier = new String(CellUtil.cloneQualifier(cell));
        String value = new String(CellUtil.cloneValue(cell), "UTF-8");
        objectMap.put(qualifier, value);
        }
        dataList.add(objectMap);
        }
        return dataList;
        } finally {
        if (rs != null) {
        rs.close();
        }
        }
        } finally {
        if (table != null) {
        table.close();
        }
        }
        }

/**
 * 删除表
 *
 * @param connection
 * @param tableName
 * @throws IOException
 */
public static void deleteTable(Connection connection, TableName tableName) throws IOException {
        Admin admin = null;
        try {
        admin = connection.getAdmin();
        if (admin.tableExists(tableName)) {
        //现执行disable
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        }
        } finally {
        if (admin != null) {
        admin.close();
        }
        }
        }
}
