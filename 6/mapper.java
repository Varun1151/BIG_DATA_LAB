package average;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,FloatWritable>{
    public void map(LongWritable lineno,Text data,OutputCollector<Text,FloatWritable> values,Reporter r) throws IOException{
        String arr[] = data.toString().split("\\t");
        
        String gender = arr[3];
        Float Salary = Float.parseFloat(arr[8]);
        
        values.collect(new Text(gender),new FloatWritable(Salary));
        
    }
}