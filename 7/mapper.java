package word;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
    public void map(LongWritable lineno,Text data,OutputCollector<Text,IntWritable> values,Reporter r) throws IOException{
        String words[] = data.toString().split(" ");
        for(String word:words){
            values.collect(new Text(word),new IntWritable(1));
        }
    }
}