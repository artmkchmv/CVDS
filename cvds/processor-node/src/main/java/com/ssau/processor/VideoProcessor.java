package com.ssau.processor;

import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Properties;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import lombok.extern.slf4j.Slf4j;

import com.ssau.processor.model.VideoFrameData;
import com.ssau.processor.service.MotionDetector;
import com.ssau.processor.utils.ConfigLoader;

@Slf4j
public class VideoProcessor {

    public static void main(String[] args) throws Exception {

        Properties prop = ConfigLoader.loadDefault();

        SparkSession spark = SparkSession.builder()
            .appName("VideoProcessor")
            .master(prop.getProperty("spark.master.url"))
            .getOrCreate();

        final String processedImageDir = prop.getProperty("processed.output.dir");
        log.warn("Output directory for saving processed images: {}", processedImageDir);

        StructType schema = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("camId", DataTypes.StringType, true),
            DataTypes.createStructField("timestamp", DataTypes.TimestampType, true),
            DataTypes.createStructField("rows", DataTypes.IntegerType, true),
            DataTypes.createStructField("cols", DataTypes.IntegerType, true),
            DataTypes.createStructField("type", DataTypes.IntegerType, true),
            DataTypes.createStructField("data", DataTypes.StringType, true)
        });

        Dataset<VideoFrameData> ds = spark.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", prop.getProperty("kafka.bootstrap.servers"))
            .option("subscribe", prop.getProperty("kafka.topic"))
            .option("kafka.max.partition.fetch.bytes", prop.getProperty("kafka.max.partition.fetch.bytes"))
            .option("kafka.max.poll.records", prop.getProperty("kafka.max.poll.records"))
            .load()
            .selectExpr("CAST(value AS STRING) as json_str")
            .select(functions.from_json(functions.col("json_str"), schema).alias("json"))
            .select("json.*")
            .as(Encoders.bean(VideoFrameData.class));

        KeyValueGroupedDataset<String, VideoFrameData> kvDataset = ds.groupByKey(
            (MapFunction<VideoFrameData, String>) VideoFrameData::getCamId,
            Encoders.STRING());

        Dataset<VideoFrameData> processedDataset = kvDataset.mapGroupsWithState(
            new MapGroupsWithStateFunction<String, VideoFrameData, VideoFrameData, VideoFrameData>() {
                @Override
                public VideoFrameData call(String camId, Iterator<VideoFrameData> values, GroupState<VideoFrameData> state) throws Exception {
                    log.warn("Processing camId={} in partition {}", camId, TaskContext.getPartitionId());

                    VideoFrameData previousProcessed = state.exists() ? state.get() : null;
                    VideoFrameData processed = MotionDetector.detectMotion(camId, values, processedImageDir, previousProcessed);

                    if (processed != null) {
                        state.update(processed);
                        return processed;
                    } else if (state.exists()) {
                        return state.get();
                    } else {
                        return new VideoFrameData();
                    }
                }
            },
            Encoders.bean(VideoFrameData.class),
            Encoders.bean(VideoFrameData.class)
        );

        StreamingQuery query = processedDataset.writeStream()
            .outputMode("update")
            .format("console")
            .option("checkpointLocation", Paths.get(prop.getProperty("spark.checkpoint.dir")).toAbsolutePath().toString())
            .start();

        query.awaitTermination();
    }
}