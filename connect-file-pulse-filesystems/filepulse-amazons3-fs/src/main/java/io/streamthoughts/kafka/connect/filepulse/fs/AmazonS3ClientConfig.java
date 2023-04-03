/*
 * Copyright 2019-2021 StreamThoughts.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.fs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.s3.model.StorageClass;
import io.streamthoughts.kafka.connect.filepulse.internal.StringUtils;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

/**
 * The Amazon S3 client's configuration.
 */
public class AmazonS3ClientConfig extends AbstractConfig {

    private static final String GROUP_AWS = "AWS";

    public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access.key.id";
    private static final String AWS_ACCESS_KEY_ID_DOC = "AWS Access Key ID";

    public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "aws.secret.access.key";
    private static final String AWS_SECRET_ACCESS_KEY_DOC = "AWS Secret Access Key";

    public static final String AWS_SECRET_SESSION_TOKEN_CONFIG = "aws.secret.session.token";
    private static final String AWS_SECRET_SESSION_TOKEN_DOC = "AWS Secret Session Token";

    public static final String AWS_S3_ENDPOINT_CONFIG = "aws.s3.service.endpoint";
    private static final String AWS_S3_ENDPOINT_DOC = "AWS S3 custom service endpoint.";

    public static final String AWS_S3_REGION_CONFIG = "aws.s3.region";
    private static final String AWS_S3_REGION_DOC = "The AWS S3 Region, e.g. us-east-1";
    public static final String AWS_S3_REGION_DEFAULT = Regions.DEFAULT_REGION.getName();

    public static final String AWS_S3_PATH_STYLE_ACCESS_ENABLED_CONFIG = "aws.s3.path.style.access.enabled";
    private static final String AWS_S3_PATH_STYLE_ACCESS_ENABLED_DOC = "Configures the client to use path-style access for all requests.";

    public final static String AWS_S3_BUCKET_NAME_CONFIG = "aws.s3.bucket.name";
    private final static String AWS_S3_BUCKET_NAME_DOC = "The name of the Amazon S3 bucket.";

    public final static String AWS_S3_BUCKET_PREFIX_CONFIG = "aws.s3.bucket.prefix";
    private final static String AWS_S3_BUCKET_PREFIX_DOC = "The prefix to be used for restricting the listing of the objects in the bucket";

    public static final String AWS_CREDENTIALS_PROVIDER_CLASS = "aws.credentials.provider.class";
    private static final String AWS_CREDENTIALS_PROVIDER_DOC = "The AWSCredentialsProvider to use if no access key id and secret access key is configured";
    public static final String AWS_CREDENTIALS_PROVIDER_DEFAULT = EnvironmentVariableCredentialsProvider.class.getName();

    public static final String AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG = "aws.s3.backoff.delay.ms";
    public static final String AWS_S3_RETRY_BACKOFF_DELAY_MS_DOC = "The base back-off time (milliseconds) before retrying a request.";
    // Default values from AWS SDK (see: PredefinedBackoffStrategies)
    public static final int AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT = 100;
    public static final String AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG = "aws.s3.backoff.max.delay.ms";
    public static final String AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DOC = "The maximum back-off time (in milliseconds) before retrying a request.";
    // Default values from AWS SDK (see: PredefinedBackoffStrategies)
    public static final int AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT = 20_000;
    public static final String AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG = "aws.s3.backoff.max.retries";
    public static final String AWS_S3_RETRY_BACKOFF_MAX_RETRIES_DOC = "The maximum number of retry attempts for failed retryable requests.";
    // Default values from AWS SDK (see: PredefinedBackoffStrategies)

    // Maximum retry limit. Avoids integer overflow issues.
    // NOTE: If the value is greater than 30, there can be integer overflow issues during delay calculation.
    public static final int AWS_S3_RETRY_BACKOFF_MAX_RETRIES_MAX_VALUE = 30;

    public static final int AWS_S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT = PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY;

    public static final String AWS_S3_OBJECT_STORAGE_CLASS_CONFIG =  "aws.s3.default.object.storage.class";
    public static final String AWS_S3_OBJECT_STORAGE_CLASS_DOC =  "The AWS storage class to associate with an S3 object when it is copied by the connector (e.g., during a move operation).";

    /**
     * Creates a new {@link AmazonS3ClientConfig} instance.
     *
     * @param originals the original configuration map.
     */
    public AmazonS3ClientConfig(final Map<String, ?> originals) {
        super(getConf(), originals, false);
    }

    public Password getAwsAccessKeyId() {
        return getPassword(AWS_ACCESS_KEY_ID_CONFIG);
    }

    public Password getAwsSecretAccessKey() {
        return getPassword(AWS_SECRET_ACCESS_KEY_CONFIG);
    }

    public Password getAwsSecretSessionToken() {
        return getPassword(AWS_SECRET_SESSION_TOKEN_CONFIG);
    }

    public String getAwsS3Region() {
        return getString(AWS_S3_REGION_CONFIG);
    }

    public String getAwsS3BucketName() {
        return getString(AWS_S3_BUCKET_NAME_CONFIG);
    }

    public String getAwsS3ServiceEndpoint() {
        return getString(AWS_S3_ENDPOINT_CONFIG);
    }

    public String getAwsS3BucketPrefix() {
        return getString(AWS_S3_BUCKET_PREFIX_CONFIG);
    }

    public boolean isAwsS3PathStyleAccessEnabled() {
        return getBoolean(AWS_S3_PATH_STYLE_ACCESS_ENABLED_CONFIG);
    }

    public AWSCredentialsProvider getAwsCredentialsProvider() {
        return getConfiguredInstance(AWS_CREDENTIALS_PROVIDER_CLASS, AWSCredentialsProvider.class);
    }

    public int getAwsS3RetryBackoffDelayMs() {
        return getInt(AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG);
    }

    public int getAwsS3RetryBackoffMaxDelayMs() {
        return getInt(AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG);
    }

    public int getAwsS3RetryBackoffMaxRetries() {
        return getInt(AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG);
    }

    public StorageClass getAwsS3DefaultStorageClass() {
        return Optional.ofNullable(getString(AWS_S3_OBJECT_STORAGE_CLASS_CONFIG))
                .map(String::toUpperCase)
                .map(StorageClass::fromValue)
                .orElse(null);
    }

    /**
     * @return the {@link ConfigDef}.
     */
    static ConfigDef getConf() {
        int awsGroupCounter = 0;

        return new ConfigDef()
                .define(
                        AWS_S3_BUCKET_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        AWS_S3_BUCKET_NAME_DOC,
                        GROUP_AWS,
                        awsGroupCounter++,
                        ConfigDef.Width.NONE,
                        AWS_S3_BUCKET_NAME_CONFIG
                )

                .define(
                        AWS_S3_BUCKET_PREFIX_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.MEDIUM,
                        AWS_S3_BUCKET_PREFIX_DOC,
                        GROUP_AWS,
                        awsGroupCounter++,
                        ConfigDef.Width.NONE,
                        AWS_S3_BUCKET_PREFIX_CONFIG
                )

                .define(
                        AWS_S3_ENDPOINT_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.MEDIUM,
                        AWS_S3_ENDPOINT_DOC,
                        GROUP_AWS,
                        awsGroupCounter++,
                        ConfigDef.Width.NONE,
                        AWS_S3_ENDPOINT_CONFIG
                )

                .define(
                        AWS_S3_REGION_CONFIG,
                        ConfigDef.Type.STRING,
                        AWS_S3_REGION_DEFAULT,
                        new AwsRegionValidator(),
                        ConfigDef.Importance.MEDIUM,
                        AWS_S3_REGION_DOC,
                        GROUP_AWS,
                        awsGroupCounter++,
                        ConfigDef.Width.NONE,
                        AWS_S3_REGION_CONFIG
                )

                .define(
                        AWS_S3_PATH_STYLE_ACCESS_ENABLED_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.MEDIUM,
                        AWS_S3_PATH_STYLE_ACCESS_ENABLED_DOC,
                        GROUP_AWS,
                        awsGroupCounter++,
                        ConfigDef.Width.NONE,
                        AWS_S3_PATH_STYLE_ACCESS_ENABLED_CONFIG
                )

                .define(
                        AWS_ACCESS_KEY_ID_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        new Password(null),
                        new NonEmptyPassword(),
                        ConfigDef.Importance.HIGH,
                        AWS_ACCESS_KEY_ID_DOC,
                        GROUP_AWS,
                        awsGroupCounter++,
                        ConfigDef.Width.NONE,
                        AWS_ACCESS_KEY_ID_CONFIG
                )

                .define(
                        AWS_SECRET_ACCESS_KEY_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        new Password(null),
                        new NonEmptyPassword(),
                        ConfigDef.Importance.HIGH,
                        AWS_SECRET_ACCESS_KEY_DOC,
                        GROUP_AWS,
                        awsGroupCounter++,
                        ConfigDef.Width.NONE,
                        AWS_SECRET_ACCESS_KEY_CONFIG
                )

                .define(
                        AWS_SECRET_SESSION_TOKEN_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        new Password(null),
                        new NonEmptyPassword(),
                        ConfigDef.Importance.HIGH,
                        AWS_SECRET_SESSION_TOKEN_DOC,
                        GROUP_AWS,
                        awsGroupCounter++,
                        ConfigDef.Width.NONE,
                        AWS_SECRET_SESSION_TOKEN_CONFIG
                )

                .define(
                        AWS_CREDENTIALS_PROVIDER_CLASS,
                        ConfigDef.Type.CLASS,
                        AWS_CREDENTIALS_PROVIDER_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        AWS_CREDENTIALS_PROVIDER_DOC,
                        GROUP_AWS,
                        awsGroupCounter++,
                        ConfigDef.Width.NONE,
                        AWS_CREDENTIALS_PROVIDER_CLASS
                )
                .define(
                        AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG,
                        ConfigDef.Type.INT,
                        AWS_S3_RETRY_BACKOFF_DELAY_MS_DEFAULT,
                        ConfigDef.Range.atLeast(1),
                        ConfigDef.Importance.MEDIUM,
                        AWS_S3_RETRY_BACKOFF_DELAY_MS_DOC,
                        GROUP_AWS,
                        awsGroupCounter++,
                        ConfigDef.Width.NONE,
                        AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG
                )
                .define(
                        AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG,
                        ConfigDef.Type.INT,
                        AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DEFAULT,
                        ConfigDef.Range.atLeast(1),
                        ConfigDef.Importance.MEDIUM,
                        AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_DOC,
                        GROUP_AWS,
                        awsGroupCounter++,
                        ConfigDef.Width.NONE,
                        AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG
                )
                .define(
                        AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG,
                        ConfigDef.Type.INT,
                        AWS_S3_RETRY_BACKOFF_MAX_RETRIES_DEFAULT,
                        ConfigDef.Range.between(1, AWS_S3_RETRY_BACKOFF_MAX_RETRIES_MAX_VALUE),
                        ConfigDef.Importance.MEDIUM,
                        AWS_S3_RETRY_BACKOFF_MAX_RETRIES_DOC,
                        GROUP_AWS,
                        awsGroupCounter++,
                        ConfigDef.Width.NONE,
                        AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG
                )
                .define(
                        AWS_S3_OBJECT_STORAGE_CLASS_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        new AwsStorageClassValidator(),
                        ConfigDef.Importance.LOW,
                        AWS_S3_OBJECT_STORAGE_CLASS_DOC,
                        GROUP_AWS,
                        awsGroupCounter++,
                        ConfigDef.Width.NONE,
                        AWS_S3_OBJECT_STORAGE_CLASS_CONFIG
                );
    }

    public static class NonEmptyPassword implements ConfigDef.Validator {

        @Override
        public void ensureValid(final String name, final Object value) {
            if (Objects.isNull(value) || ((Password) value).value() == null) {
                return;
            }
            final var pwd = (Password) value;
            if (StringUtils.isBlank(pwd.value())) {
                throw new ConfigException(name, pwd, "Password must be non-empty");
            }

        }
    }

    public static class AwsRegionValidator implements ConfigDef.Validator {
        private static final String SUPPORTED_AWS_REGIONS =
                Arrays.stream(Regions.values())
                        .map(Regions::getName)
                        .collect(Collectors.joining(", "));

        @Override
        public void ensureValid(final String name, final Object value) {
            if (Objects.nonNull(value)) {
                final String valueStr = (String) value;
                try {
                    Regions.fromName(valueStr);
                } catch (final IllegalArgumentException e) {
                    throw new ConfigException(
                            name, valueStr,
                            "supported values are: " + SUPPORTED_AWS_REGIONS);
                }
            }
        }
    }


    public static class AwsStorageClassValidator implements ConfigDef.Validator {
        private static final String SUPPORTED_AWS_STORAGE_CLASS =
                Arrays.stream(StorageClass.values())
                        .map(StorageClass::name)
                        .collect(Collectors.joining(", "));

        @Override
        public void ensureValid(final String name, final Object value) {
            if (Objects.nonNull(value)) {
                final String valueStr = (String) value;
                try {
                    StorageClass.fromValue(valueStr.toUpperCase(Locale.ROOT));
                } catch (final IllegalArgumentException e) {
                    throw new ConfigException(
                            name, valueStr,
                            "supported values are: " + SUPPORTED_AWS_STORAGE_CLASS);
                }
            }
        }
    }

}
