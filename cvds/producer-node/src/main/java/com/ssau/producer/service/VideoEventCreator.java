package com.ssau.producer.service;

import java.time.Instant;
import java.util.Base64;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.bytedeco.opencv.global.opencv_imgproc;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.Size;
import org.bytedeco.opencv.opencv_videoio.VideoCapture;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ssau.producer.model.VideoFrameData;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class VideoEventCreator implements Runnable {

    private static final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final String cameraId;
    private final String url;
    private final Producer<String, String> producer;
    private final String topic;
    private final int frameWidth = 640;
    private final int frameHeight = 480;

    @Override
    public void run() {
        log.info("OpenCV loaded, version info not available as constant.");
        log.info("Processing cameraId {} with url {}", cameraId, url);
        try {
            createEvent();
        } catch (Exception e) {
            log.error("Error processing cameraId {}: ", cameraId, e);
        }
    }

    private void createEvent() throws Exception {
        VideoCapture camera = openCamera();
        if (!camera.isOpened()) {
            log.error("Camera not opened for cameraId={}, url={}", cameraId, url);
            return;
        }
        Mat mat = new Mat();

        if (!camera.read(mat)) {
            log.error("Failed to read first frame from cameraId={}, url={}", cameraId, url);
            return;
        }
        log.info("First frame read successfully");
        try {
            int frameCount = 0;
            while (camera.read(mat)) {
                frameCount++;
                log.info("Read frame #{} from cameraId={}", frameCount, cameraId);
                opencv_imgproc.resize(mat, mat, new Size(frameWidth, frameHeight), 0, 0, opencv_imgproc.INTER_CUBIC);
                String json = createJsonFromMat(mat);
                sendMessage(json);
                Thread.sleep(10);
            }
            log.info("Finished reading video for cameraId={}, total frames={}", cameraId, frameCount);
        } finally {
            mat.release();
            camera.release();
        }
    }

    private VideoCapture openCamera() throws Exception {
        VideoCapture camera;
        if (StringUtils.isNumeric(url)) {
            camera = new VideoCapture(Integer.parseInt(url));
        } else {
            camera = new VideoCapture(url);
        }

        if (!camera.isOpened()) {
            log.warn("Failed to open camera/video on first attempt for cameraId={}, url={}", cameraId, url);
            Thread.sleep(5000);
            if (!camera.isOpened()) {
                log.error("Failed to open camera/video after retry for cameraId={}, url={}", cameraId, url);
                throw new Exception("Error opening cameraId " + cameraId + " with url=" + url + ". Check camera URL/path.");
            } else {
                log.info("Camera/video opened successfully after retry for cameraId={}, url={}", cameraId, url);
            }
        } else {
            log.info("Camera/video opened successfully on first attempt for cameraId={}, url={}", cameraId, url);
        }

        return camera;
    }

    private String createJsonFromMat(Mat mat) throws Exception {
        int size = (int) (mat.total() * mat.channels());
        byte[] data = new byte[size];
        mat.data().get(data);

        VideoFrameData event = new VideoFrameData(
            cameraId,
            Instant.now(),
            mat.rows(),
            mat.cols(),
            mat.type(),
            Base64.getEncoder().encodeToString(data)
        );

        return objectMapper.writeValueAsString(event);
    }

    private void sendMessage(String json) {
        log.debug("Sending message for cameraId={}, json length={}", cameraId, json.length());
        producer.send(new ProducerRecord<>(topic, cameraId, json), (RecordMetadata rm, Exception ex) -> {
            if (rm != null) {
                log.info("cameraId={} partition={}", cameraId, rm.partition());
            }
            if (ex != null) {
                log.error("Error sending message for cameraId={}", cameraId, ex);
            }
        });
    }
}
