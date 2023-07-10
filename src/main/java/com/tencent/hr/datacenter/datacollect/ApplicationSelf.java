//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.tencent.hr.datacenter.datacollect;

import com.tencent.hr.datacenter.flink.template.*;

import com.tencent.hr.datacenter.flink.template.utils.HDFSUtil;

import java.io.IOException;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationSelf {
    private static final Logger log = LoggerFactory.getLogger(ApplicationSelf.class);

    public ApplicationSelf() {
    }

    public static void main(String[] args) throws Exception {
        start(args);
    }

    public static void start(String[] args) throws Exception {
//        log.info("client start");
//        ParameterTool argParams = ParameterTool.fromArgs(args);
//        String profile = argParams.getRequired("profile");
//        ParameterTool profileParams = ParameterTool.fromPropertiesFile(ApplicationSelf.class.getClassLoader().getResourceAsStream(String.format("application-%s.properties", profile)));
//        String outPath = argParams.getRequired("out.path");
//        String jobName = profileParams.getRequired("flink.job.name");
//        int checkpoint = profileParams.getInt("flink.checkpoint.seconds", 60);
//        String sourceClass = profileParams.getRequired("job.source.class");
//        String hadoopConfPath = profileParams.get("hadoop.conf.path", "/etc/hadoop/conf/");
//        boolean isPathDel = profileParams.getBoolean("job.path.delete", false);
//        log.info("profile=" + profile);
//        log.info("out.path=" + outPath);
//        log.info("jobName=" + jobName);
//        log.info("flink.checkpoint.seconds=" + checkpoint);
//        log.info("job.source.class=" + sourceClass);
//        log.info("job.path.delete=" + isPathDel);
//        log.info("hadoop.conf.path=" + hadoopConfPath);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        env.enableCheckpointing((long)(checkpoint * 1000), CheckpointingMode.EXACTLY_ONCE);
//        env.setRestartStrategy(RestartStrategies.noRestart());


//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        env.enableCheckpointing((long) (60 * 1000), CheckpointingMode.EXACTLY_ONCE);
//        env.setRestartStrategy(RestartStrategies.noRestart());
//
//
        FileSystem fs = null;
//
        try {
////                log.info("move hdfs path to trash:" + outPath);
            fs = HDFSUtil.getFileSystem("/data/hadoop-2.9.2/etc/hadoop", "hdfs://dwcluster/project/dw3/stg/projectlist");
////                HDFSUtil.moveToTrash(fs, new org.apache.hadoop.fs.Path(outPath));
//            log.info("delete succeed");
        } catch (IOException var19) {
            throw var19;
        } finally {
            HDFSUtil.close(fs);
        }


//        AbstractCustomSource customSource = (AbstractCustomSource)Class.forName(sourceClass).getConstructor().newInstance();
//        customSource.setParams(argParams, profileParams);
//        DataStream<DataRow> dataStream = env.addSource(customSource);
//        FileSink<DataRow> sink = ((FileSink.DefaultRowFormatBuilder)((FileSink.DefaultRowFormatBuilder)((FileSink.DefaultRowFormatBuilder)FileSink.forRowFormat(new Path(outPath), new SimpleStringEncoder("UTF-8")).withRollingPolicy(OnCheckpointRollingPolicy.build())).withBucketAssigner(new BasePathBucketAssigner<DataRow>() {
//            public String getBucketId(DataRow row, BucketAssigner.Context context) {
//                return row.getAssigner() == null ? "" : row.getAssigner();
//            }
//        })).withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("part").build())).build();
//        if (isPathDel) {
//            FileSystem fs = null;
//
//            try {
//                log.info("move hdfs path to trash:" + outPath);
//                fs = HDFSUtil.getFileSystem(hadoopConfPath, outPath);
//                HDFSUtil.moveToTrash(fs, new org.apache.hadoop.fs.Path(outPath));
//                log.info("delete succeed");
//            } catch (IOException var19) {
//                throw var19;
//            } finally {
//                HDFSUtil.close(fs);
//            }
//        }

//
//        dataStream.sinkTo(sink);
//        env.execute();
        log.info("client finish");
    }
}
