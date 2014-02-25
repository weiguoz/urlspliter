package org.hadoop.urlspliter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created by weiguo on 14-2-20.
 */
public class URLMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        String [] tokens = value.toString().split("\\s+", 3); // space tabs...
        if (tokens.length != 3) return;
        String sld = SLDExtractor.getInstace().extract(tokens[1]);
        collector.collect(new Text(sld), value);
    }
}
