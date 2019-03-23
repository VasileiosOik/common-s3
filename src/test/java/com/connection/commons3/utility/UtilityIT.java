package com.connection.commons3.utility;

import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.connection.commons3.Application;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;


@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@TestExecutionListeners({DependencyInjectionTestExecutionListener.class, DirtiesContextTestExecutionListener.class})
public class UtilityIT {

    private static final String DESTINATION_EXTRACT = "destination/extract/";
    private static final String TEST_FILE = "test_file_";
    private static final String STRING_DATA = "Name|Surname\nbill,eco\n";

    @Autowired
    private AmazonS3Util amazonS3Util;

    @Before
    @After
    public void setUp() {
        amazonS3Util.deleteAllFilesInBucket(DESTINATION_EXTRACT);
    }

    @Test
    public void uploadFile() throws IOException {
        Path file = Files.createTempFile(TEST_FILE, ".csv");
        Files.write(file, STRING_DATA.getBytes());
        String fileName = file.getFileName().toString();
        PutObjectResult putObjectResult;
        try {
            putObjectResult = amazonS3Util.uploadItemToS3(DESTINATION_EXTRACT + fileName, file.toFile());
            assertNotNull(putObjectResult);
            assertTrue(amazonS3Util.searchForFileInS3(DESTINATION_EXTRACT + fileName));
        } finally {
            Files.delete(file);
        }

    }

    @Test
    public void downloadFile() throws IOException {
        Path file = Files.createTempFile(TEST_FILE, ".csv");
        Files.write(file, STRING_DATA.getBytes());
        String fileName = file.getFileName().toString();
        S3ObjectInputStream s3Object;
        try {
            amazonS3Util.uploadItemToS3(DESTINATION_EXTRACT + fileName, file.toFile());
            s3Object = amazonS3Util.downloadItemFromS3(DESTINATION_EXTRACT, fileName);
            assertEquals(new String(Files.readAllBytes(file), StandardCharsets.UTF_8), IOUtils.toString(s3Object, StandardCharsets.UTF_8));
        } finally {
            Files.delete(file);
        }
    }

    @Test
    public void deleteFileFromS3() throws IOException {
        Path file = Files.createTempFile(TEST_FILE, ".csv");
        Files.write(file, STRING_DATA.getBytes());
        String fileName = file.getFileName().toString();
        try {
            amazonS3Util.uploadItemToS3(DESTINATION_EXTRACT + fileName, file.toFile());
            amazonS3Util.deleteFileFromS3(DESTINATION_EXTRACT + fileName);
            assertFalse(amazonS3Util.searchForFileInS3(DESTINATION_EXTRACT + fileName));
        } finally {
            Files.delete(file);
        }

    }
}
