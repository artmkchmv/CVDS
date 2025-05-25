package com.ssau.processor.service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import org.bytedeco.opencv.global.opencv_core;
import org.bytedeco.opencv.global.opencv_imgcodecs;
import org.bytedeco.opencv.global.opencv_imgproc;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.MatVector;
import org.bytedeco.opencv.opencv_core.Rect;
import org.bytedeco.opencv.opencv_core.Scalar;
import org.bytedeco.opencv.opencv_core.Size;

import lombok.extern.slf4j.Slf4j;

import com.ssau.processor.model.VideoFrameData;

@Slf4j
public class MotionDetector {

    public static VideoFrameData detectMotion(String camId,
                                              Iterator<VideoFrameData> frames,
                                              String outputDir,
                                              VideoFrameData previousProcessedFrame) throws Exception {
        VideoFrameData currentProcessedFrame = null;

        Mat firstFrame = null;

        if (previousProcessedFrame != null) {
            log.info("Previous frame timestamp: {}", previousProcessedFrame.getTimestamp());
            Mat preFrame = decodeMat(previousProcessedFrame);
            Mat preGray = new Mat();
            opencv_imgproc.cvtColor(preFrame, preGray, opencv_imgproc.COLOR_BGR2GRAY);
            opencv_imgproc.GaussianBlur(preGray, preGray, new Size(3, 3), 0);
            firstFrame = preGray;
            preFrame.release();
        }

        List<VideoFrameData> sortedFrames = new ArrayList<>();
        frames.forEachRemaining(sortedFrames::add);
        sortedFrames.sort(Comparator.comparing(VideoFrameData::getTimestamp));
        log.info("CameraId={} total frames={}", camId, sortedFrames.size());

        for (VideoFrameData frameData : sortedFrames) {
            Mat frame = decodeMat(frameData);
            Mat copyFrame = frame.clone();

            Mat gray = new Mat();
            opencv_imgproc.cvtColor(frame, gray, opencv_imgproc.COLOR_BGR2GRAY);
            opencv_imgproc.GaussianBlur(gray, gray, new Size(3, 3), 0);

            if (firstFrame != null) {
                Mat delta = new Mat();
                opencv_core.absdiff(firstFrame, gray, delta);

                Mat thresh = new Mat();
                opencv_imgproc.threshold(delta, thresh, 20, 255, opencv_imgproc.THRESH_BINARY);

                List<Rect> motionAreas = detectContours(thresh);

                if (!motionAreas.isEmpty()) {
                    for (Rect rect : motionAreas) {
                        opencv_imgproc.rectangle(copyFrame, rect, new Scalar(0, 255, 0, 0));
                    }
                    log.info("Motion detected for camId={}, timestamp={}", camId, frameData.getTimestamp());
                    saveImage(copyFrame, frameData, outputDir);
                }

                delta.release();
                thresh.release();
            }

            if (firstFrame != null && firstFrame != gray) {
                firstFrame.release();
            }
            firstFrame = gray;

            copyFrame.release();
            frame.release();

            currentProcessedFrame = frameData;
        }

        if (firstFrame != null) {
            firstFrame.release();
        }

        return currentProcessedFrame;
    }

    private static Mat decodeMat(VideoFrameData data) {
        byte[] decoded = Base64.getDecoder().decode(data.getData());

        int expectedLength = (int) (data.getRows() * data.getCols() * 1 );
        int channels = opencv_core.CV_MAT_CN(data.getType());
        expectedLength = (int) (data.getRows() * data.getCols() * channels);

        if (decoded.length != expectedLength) {
            log.warn("Decoded data length {} does not match expected matrix size {} (rows={} cols={} channels={})",
                     decoded.length, expectedLength, data.getRows(), data.getCols(), channels);
        }

        Mat mat = new Mat(data.getRows(), data.getCols(), data.getType());
        mat.data().put(decoded);
        return mat;
    }

    private static List<Rect> detectContours(Mat binary) {
        MatVector contours = new MatVector();
        Mat hierarchy = new Mat();
        opencv_imgproc.findContours(binary.clone(), contours, hierarchy, opencv_imgproc.RETR_EXTERNAL, opencv_imgproc.CHAIN_APPROX_SIMPLE);

        List<Rect> result = new ArrayList<>();
        double minArea = 300;

        for (int i = 0; i < contours.size(); i++) {
            Mat contour = contours.get(i);
            double area = opencv_imgproc.contourArea(contour);
            if (area > minArea) {
                result.add(opencv_imgproc.boundingRect(contour));
            }
            contour.release();
        }
        hierarchy.release();
        contours.close();

        return result;
    }

    private static void saveImage(Mat mat, VideoFrameData frameData, String outputDir) {
        try {
            Path dirPath = Path.of(outputDir);
            Files.createDirectories(dirPath);
            String path = String.format("%s/%s-T-%d.png", outputDir, frameData.getCamId(), frameData.getTimestamp().toEpochMilli());
            log.info("Saving image to absolute path: {}", Path.of(path).toAbsolutePath());
            boolean saved = opencv_imgcodecs.imwrite(path, mat);
            if (!saved) {
                log.error("Failed to save image to path {}. Check if directory exists and writable.", outputDir);
            } else {
                log.info("Image saved to {}", path);
            }
        } catch (Exception e) {
            log.error("Exception while creating directories or saving image", e);
        }
    }
}
