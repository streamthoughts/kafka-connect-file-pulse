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
package io.streamthoughts.kafka.connect.filepulse.storage.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import io.streamthoughts.kafka.connect.filepulse.internal.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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

    public String getAwsS3BucketPrefix() {
        return getString(AWS_S3_BUCKET_PREFIX_CONFIG);
    }

    public boolean isAwsS3PathStyleAccessEnabled() {
        return getBoolean(AWS_S3_PATH_STYLE_ACCESS_ENABLED_CONFIG);
    }

    public AWSCredentialsProvider getAwsCredentialsProvider() {
        return getConfiguredInstance(AWS_CREDENTIALS_PROVIDER_CLASS, AWSCredentialsProvider.class);
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
                        AWS_S3_BUCKET_NAME_CONFIG
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
                        null,
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
                        ConfigDef.Importance.HIGH,
                        AWS_CREDENTIALS_PROVIDER_DOC,
                        GROUP_AWS,
                        awsGroupCounter++,
                        ConfigDef.Width.NONE,
                        AWS_CREDENTIALS_PROVIDER_CLASS
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


}
