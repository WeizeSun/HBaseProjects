package hbase;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.conf.Configuration;

public class Ngram{
    public static void main(String[] args) throws IOException{
        Configuration con = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(con);

        HTable hTable = new HTable(con, "ngram");
        Put p = new Put(Bytes.toBytes("-1"));
        p.add(Bytes.toBytes("cf"), Bytes.toBytes("bigram"), Bytes.toBytes("ierg4330"));
        p.add(Bytes.toBytes("cf"), Bytes.toBytes("year"), Bytes.toBytes("2017"));
        p.add(Bytes.toBytes("cf"), Bytes.toBytes("match_count"), Bytes.toBytes("100"));
        p.add(Bytes.toBytes("cf"), Bytes.toBytes("volume_count"), Bytes.toBytes("37"));
        hTable.put(p);
        
        String reg = "(?=[1-9]\\d\\d)(?!100)|\\d{4,}";
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("match_count"), CompareOp.EQUAL, new RegexStringComparator(reg));
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("year"), CompareOp.EQUAL, Bytes.toBytes("1671"));

        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        list.addFilter(filter1);
        list.addFilter(filter2);

        Scan scan = new Scan();
        scan.setFilter(list);
        ResultScanner scanner = hTable.getScanner(scan);

        PrintWriter writer = new PrintWriter("to_be_deleted.txt");

        for(Result result = scanner.next(); result != null; result = scanner.next()){
            Delete delete = new Delete(result.getRow());
            System.out.println(result);
            writer.println(result);
            delete.deleteFamily(Bytes.toBytes("cf"));
            hTable.delete(delete);
        }
        writer.close();
        hTable.close();
    }
}
