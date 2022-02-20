package sales;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class reducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{
    public void reduce(Text text,Iterator<IntWritable> data,OutputCollector<Text,IntWritable> values,Reporter r) throws IOException{
        int ans=0;
        while(data.hasNext()){
            ans=ans+data.next().get();
        }
        values.collect(text,new IntWritable(ans));
    }
}