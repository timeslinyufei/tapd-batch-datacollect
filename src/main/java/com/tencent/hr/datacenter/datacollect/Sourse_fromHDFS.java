package com.tencent.hr.datacenter.datacollect;

import com.tencent.hr.datacenter.flink.template.Application;
import com.tencent.hr.datacenter.flink.template.DataRow;
import com.tencent.hr.datacenter.flink.template.utils.HDFSUtil;

import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Sourse_fromHDFS {
    private static final Logger log = LoggerFactory.getLogger(Sourse_fromHDFS.class);
    public static void main(String[] args) throws Exception {
        String outPath = "hdfs://dwcluster/project/dw3/stg/projectlistTest";
        String hadoopConfPath = "/data/hadoop-2.9.2/etc/hadoop";


//        Configuration conf = new Configuration();
//        conf.setInteger("rest.port", 10001);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        FileSystem fileSystem = HDFSUtil.getFileSystem(hadoopConfPath,"hdfs://dwcluster/project/dw3/stg/projectlist/part-67953cf8-8f68-47a0-9606-a0f9cb00a102-0");
//        System.out.println(fileSystem);




        String result = HDFSUtil.readFile(HDFSUtil.getFileSystem(hadoopConfPath, outPath), "hdfs://dwcluster/project/dw3/stg/projectlist/part-67953cf8-8f68-47a0-9606-a0f9cb00a102-0", "UTF-8");




//        System.out.println(result);

//        log.info("data111=" + result);

//        TextInputFormat textInputFormat = new TextInputFormat(null);
// 可以过滤文件
//        textInputFormat.setFilesFilter(new FilePathFilter() {
//            @Override
//            public boolean filterPath(Path path) {
//                return path.getName().startsWith("2");//过滤掉2开头的文件
//            }
//
//        });

//        DataStreamSource<String> dataStreamSource = env.readFile(textInputFormat, "hdfs://dwcluster/project/dw3/stg/projectlist", FileProcessingMode.PROCESS_CONTINUOUSLY, 10000);


//        FileSink<String> sink = ((FileSink.DefaultRowFormatBuilder) ((FileSink.DefaultRowFormatBuilder) ((FileSink.DefaultRowFormatBuilder) FileSink.forRowFormat(new Path(outPath), new SimpleStringEncoder("UTF-8")).withRollingPolicy(OnCheckpointRollingPolicy.build())).withBucketAssigner(new BasePathBucketAssigner<DataRow>() {
//            public String getBucketId(DataRow row, BucketAssigner.Context context) {
//                return row.getAssigner() == null ? "" : row.getAssigner();
//            }
//        })).withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("part").build())).build();


//        FileSystem fs = null;
//
//        try {
//
//            fs = HDFSUtil.getFileSystem(hadoopConfPath, outPath);
//            HDFSUtil.moveToTrash(fs, new org.apache.hadoop.fs.Path(outPath));
//
//        } catch (IOException var19) {
//            throw var19;
//        } finally {
//            HDFSUtil.close(fs);
//        }

//        env.setParallelism(1);
//        dataStreamSource.sinkTo(sink);
//        dataStreamSource.print();
//        env.execute();
    }
}

