package temp;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
    public void map(LongWritable lineno,Text line, OutputCollector<Text,IntWritable> values, Reporter r) throws IOException{
        String data = line.toString();
        String year = data.substring(15,19);
        int temp;
        if(data.charAt(87)=='+')
            temp = Integer.parseInt(data.substring(88,92));
        else
            temp = Integer.parseInt(data.substring(87,92));
        values.collect(new Text(year),new IntWritable(temp));
    }
}