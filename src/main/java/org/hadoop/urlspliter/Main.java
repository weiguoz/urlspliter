package org.hadoop.urlspliter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

/**
 * Created by weiguo on 14-2-20.
 */
public class Main {
    static public void main(String[] args) throws Exception {
        String inDir  = checkArgs(args, 1);
        String outDir = checkArgs(args, 2);
        String dataBaseName = checkArgs(args, 3);
        String maxFileSize = checkArgs(args,4);

        JobConf cfg = new JobConf(Main.class);
        cfg.setJobName("MRJob_URL_SPLITER");

        cfg.setMapperClass(URLMapper.class);
        cfg.setReducerClass(URLReducer.class);

        cfg.setOutputKeyClass(Text.class);
        cfg.setOutputValueClass(Text.class);
        cfg.setInputFormat(TextInputFormat.class);
        cfg.setOutputFormat(NullOutputFormat.class);
        FileInputFormat.addInputPath(cfg, new Path(inDir));

        cfg.set("URL_PARENT_DIR", outDir); // used in reduce
        cfg.set("URL_DATA_BASE_NAME", dataBaseName);
        cfg.set("MAX_FILE_SIZE_KB", maxFileSize);
        JobClient.runJob(cfg);
    }

    static public String checkArgs(String[] args, int requiredNum) {
        return checkArgs(args, requiredNum, -requiredNum);
    }

    static public String checkArgs(String[] args, int requiredNum, int exitCode) {
        if (args.length < requiredNum || requiredNum < 0) {
            System.out.println("require more args, but given " + args.length);
            System.out.println("Usage: {hadoop jar urlspliter.xxx.jar}\t/DIR/TO/SRC_URL/\t/DIR/FOR/STORAGE/\tFILE_PREFIX_NAME\tFILE_SIZE_ALLOWED_KB");
            System.exit(exitCode);
        }
        return args[requiredNum-1];
    }
}
