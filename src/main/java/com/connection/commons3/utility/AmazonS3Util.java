package com.connection.commons3.utility;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;

@Component
public class AmazonS3Util {

    private static final Logger LOG = LoggerFactory.getLogger(AmazonS3Util.class);

    private final AmazonS3 amazonS3;

    @Value("${aws.bucket}")
    private String bucketName;

    @Autowired
    public AmazonS3Util(AmazonS3 amazonS3) {
        this.amazonS3 = amazonS3;
    }

    public PutObjectResult uploadItemToS3(String path, File file) throws IOException {
        try (InputStream targetStream = new FileInputStream(file)) {
            LOG.info("=============Uploading file========, {}", path);
            return amazonS3.putObject(bucketName, path, targetStream, getObjectMetadata(file));
        }
    }

    public S3ObjectInputStream downloadItemFromS3(String path, String name) {
        LOG.info("=============Downloading file========, {}", path);
        S3Object amazonS3Object = amazonS3.getObject(bucketName, path + name);
        return amazonS3Object.getObjectContent();

    }

    public void deleteFileFromS3(String path) {
        LOG.info("=============Deleting file========, {} ", path);
        amazonS3.deleteObject(bucketName, path);
    }

    public boolean searchForFileInS3(String path) {
        ObjectListing objectListing = amazonS3.listObjects(bucketName);
        for (S3ObjectSummary os : objectListing.getObjectSummaries()) {
            LOG.info("File is: {}", os.getKey());
            if (os.getKey().equals(path))
                return true;
        }
        return false;
    }

    public void deleteAllFilesInBucket(String folderPath) {
        for (S3ObjectSummary file : amazonS3.listObjects(bucketName, folderPath).getObjectSummaries()) {
            LOG.info("Deleting file: {}", file.getKey());
            amazonS3.deleteObject(bucketName, file.getKey());
        }
    }

    private ObjectMetadata getObjectMetadata(File file) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(file.length());
        return objectMetadata;
    }
}
