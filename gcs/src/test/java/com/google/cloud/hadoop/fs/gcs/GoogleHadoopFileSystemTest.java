/*
 * Copyright 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.FILES_DELETED;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.FILES_DELETE_REJECTED;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_READ_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GhfsStatistic.STREAM_WRITE_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.BLOCK_SIZE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopFileSystem;
import static com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage.getInMemoryGoogleCloudStorageOptions;
import static com.google.cloud.hadoop.util.interceptors.InvocationIdInterceptor.GCCL_INVOCATION_ID_PREFIX;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonError.ErrorInfo;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptions;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.Retryable;
import com.google.auth.ServiceAccountSigner;
import com.google.auth.mtls.CertificateSourceUnavailableException;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdentityPoolCredentialSource;
import com.google.auth.oauth2.IdentityPoolCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.StsRequestHandler;
import com.google.auth.oauth2.StsTokenExchangeRequest;
import com.google.cloud.gcs.analyticscore.client.GcsClientOptions;
import com.google.cloud.gcs.analyticscore.client.GcsClientOptions.Protocol;
import com.google.cloud.hadoop.fs.gcs.auth.DelegationTokenIOException;
import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageExceptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.ClientType;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.MethodOutcome;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AuthenticationType;
import com.google.cloud.hadoop.util.InvocationIdContext;
import com.google.cloud.hadoop.util.LoggingFormatter;
import com.google.cloud.hadoop.util.interceptors.LoggingInterceptor;
import com.google.cloud.hadoop.util.testing.TestingAccessTokenProvider;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Unit tests for {@link GoogleHadoopFileSystem} class. */
@RunWith(Parameterized.class)
public class GoogleHadoopFileSystemTest extends GoogleHadoopFileSystemIntegrationTest {

  private LoggingInterceptor mockInterceptor;
  private StreamHandler testLogHandler;
  private Logger ghfsLogger;

  @Before
  public void before() throws IOException {
    // Disable logging.
    // Normally you would need to keep a strong reference to any logger used for
    // configuration, but the "root" logger is always present.
    Logger.getLogger("").setLevel(Level.OFF);

    super.ghfs = createInMemoryGoogleHadoopFileSystem();

    postCreateInit();
  }

  @After
  public void after() throws IOException {
    if (mockInterceptor != null) {
      // Ensure handler is removed even if test fails, to not affect other tests
      Logger.getLogger("").removeHandler(mockInterceptor);
      mockInterceptor = null;
    }
    if (testLogHandler != null) {
      assertThat(ghfsLogger).isNotNull();
      ghfsLogger.removeHandler(testLogHandler);
      ghfsLogger.setLevel(Level.OFF);
      testLogHandler = null;
      ghfsLogger = null;
    }
  }

  @Test
  public void verifyHadoopPath() throws Exception {
    GoogleHadoopFileSystem eagerFs = createInMemoryGoogleHadoopFileSystem();
    String fileNameWithColon = "empty:file";
    eagerFs.create(new Path(/* schema= */ null, /* authority= */ null, fileNameWithColon)).close();
    FileStatus[] fileStatus =
        eagerFs.listStatus(new Path(GoogleHadoopFileSystemTestHelper.IN_MEMORY_TEST_BUCKET));
    assertThat(fileStatus).hasLength(1);
    assertThat(fileStatus[0].getPath().getName()).isEqualTo(fileNameWithColon);
  }

  @Test
  public void testVersionString() {
    assertThat(GoogleHadoopFileSystem.VERSION).isNotNull();
    assertThat(GoogleHadoopFileSystem.UNKNOWN_VERSION.equals(GoogleHadoopFileSystem.VERSION))
        .isFalse();
  }

  @Test
  @SuppressWarnings("CheckReturnValue")
  public void lazyInitialization_succeeds_withInvalidCredentialsConfiguration() throws Exception {
    new GoogleHadoopFileSystem();
    Configuration lazyConf = new Configuration();
    lazyConf.setBoolean("fs.gs.lazy.init.enable", true);
    lazyConf.set("fs.gs.auth.service.account.json.keyfile", "non-existent.json");
    lazyConf.setEnum(GCS_CLIENT_TYPE.toString(), storageClientType);
    GoogleHadoopFileSystem lazyFs = new GoogleHadoopFileSystem();

    lazyFs.initialize(new URI("gs://test-non-existent/"), lazyConf);
    lazyFs.close();
  }

  @Test
  public void lazyInitialization_deleteCall_fails_withInvalidCredentialsConfiguration()
      throws Exception {
    Configuration lazyConf = new Configuration();
    lazyConf.setBoolean("fs.gs.lazy.init.enable", true);
    lazyConf.setEnum("fs.gs.auth.type", AuthenticationType.SERVICE_ACCOUNT_JSON_KEYFILE);
    lazyConf.set("fs.gs.auth.service.account.json.keyfile", "non-existent.json");
    lazyConf.setEnum(GCS_CLIENT_TYPE.toString(), storageClientType);
    GoogleHadoopFileSystem lazyFs = new GoogleHadoopFileSystem();

    lazyFs.initialize(new URI("gs://test-non-existent"), lazyConf);

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> lazyFs.delete(new Path("gs://test-non-existent/dir"), false));

    assertThat(exception).hasMessageThat().isEqualTo("Failed to create GCS FS");
    assertThat(exception).hasCauseThat().isInstanceOf(FileNotFoundException.class);
    assertThat(exception)
        .hasCauseThat()
        .hasMessageThat()
        .isAnyOf(
            "non-existent.json (No such file or directory)",
            "non-existent.json (The system cannot find the file specified)");

    lazyFs.close();
  }

  @Test
  @SuppressWarnings("CheckReturnValue")
  public void eagerInitialization_fails_withInvalidCredentialsConfiguration() {
    new GoogleHadoopFileSystem();
    Configuration eagerConf = new Configuration();
    eagerConf.setBoolean("fs.gs.lazy.init.enable", false);
    eagerConf.setEnum("fs.gs.auth.type", AuthenticationType.SERVICE_ACCOUNT_JSON_KEYFILE);
    eagerConf.set("fs.gs.auth.service.account.json.keyfile", "non-existent.json");
    eagerConf.setEnum(GCS_CLIENT_TYPE.toString(), storageClientType);
    FileSystem eagerFs = new GoogleHadoopFileSystem();

    FileNotFoundException exception =
        assertThrows(
            FileNotFoundException.class,
            () -> eagerFs.initialize(new URI("gs://test-non-existent"), eagerConf));

    assertThat(exception)
        .hasMessageThat()
        .isAnyOf(
            "non-existent.json (No such file or directory)",
            "non-existent.json (The system cannot find the file specified)");
  }

  @Test
  public void read_throws_exception() throws Exception {
    String rootBucketName = ghfsHelper.getUniqueBucketName("read-throws-exception");
    URI initUri = new Path("gs://" + rootBucketName).toUri();
    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystemImpl(
            CustomInMemoryGoogleCloudStorage::new,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(getInMemoryGoogleCloudStorageOptions())
                .build());
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem(fakeGcsFs);
    fs.initialize(initUri, new Configuration());

    try (FSDataInputStream inputStream = fs.open(new Path("read-throws-exception-file"))) {
      IOException exception =
          assertThrows(IOException.class, () -> inputStream.read(new byte[2], 0, 1));
      assertThat(exception)
          .hasMessageThat()
          .isEqualTo("read_throws_exception test : read call throws exception");
    }
  }

  @Test
  public void read_single_byte_throws_exception() throws Exception {
    String rootBucketName = ghfsHelper.getUniqueBucketName("read-throws-exception");
    URI initUri = new Path("gs://" + rootBucketName).toUri();
    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystemImpl(
            CustomInMemoryGoogleCloudStorage::new,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(getInMemoryGoogleCloudStorageOptions())
                .build());
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem(fakeGcsFs);
    fs.initialize(initUri, new Configuration());

    try (FSDataInputStream inputStream = fs.open(new Path("read-throws-exception-file"))) {
      IOException exception = assertThrows(IOException.class, () -> inputStream.read());
      assertThat(exception)
          .hasMessageThat()
          .isEqualTo("read_throws_exception test : read call throws exception");
    }
  }

  // -----------------------------------------------------------------
  // Tests that exercise behavior defined in HdfsBehavior.
  // -----------------------------------------------------------------

  /** Validates {@link GoogleHadoopFileSystem#rename(Path, Path)}. */
  @Test
  @Override
  public void testRename() throws Exception {
    renameHelper(
        new HdfsBehavior() {
          /**
           * Returns the {@link MethodOutcome} of trying to rename an existing file into the root
           * directory.
           */
          @Override
          public MethodOutcome renameFileIntoRootOutcome() {
            return new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE);
          }
        });
  }

  @Test
  public void testCouldUseFlatGlob() throws IOException {
    try (GoogleHadoopFileSystem lazyFs = new InMemoryGoogleHadoopFileSystem()) {
      assertThat(lazyFs.couldUseFlatGlob(new Path("gs://**/test/"))).isFalse();
    }
  }

  @Test
  public void testTrimToPrefixWithoutGlob() {
    GoogleHadoopFileSystem lazyFs = new GoogleHadoopFileSystem();
    lazyFs.trimToPrefixWithoutGlob("gs://**/test");
    assertThat(lazyFs.trimToPrefixWithoutGlob("gs://**/test")).isEqualTo("gs://");
  }

  @Override
  @Test
  public void testGetGcsPath() throws URISyntaxException {
    GoogleHadoopFileSystem myghfs = (GoogleHadoopFileSystem) ghfs;
    URI gcsPath = new URI("gs://" + myghfs.getUri().getAuthority() + "/dir/obj");
    assertThat(myghfs.getGcsPath(new Path(gcsPath))).isEqualTo(gcsPath);
  }

  @Test
  public void testGetDefaultPortIndicatesPortsAreNotUsed() throws Exception {
    Configuration config = new Configuration();
    config.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    config.setClass(
        "fs.gs.auth.access.token.provider",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    config.setEnum(GCS_CLIENT_TYPE.toString(), storageClientType);
    URI gsUri = new URI("gs://foobar/");

    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gsUri, config);

    assertThat(ghfs.getDefaultPort()).isEqualTo(-1);
  }

  @Test
  public void testFileOpenWithStatus() throws Exception {
    URI bucketName = new URI("gs://read-test-bucket/");
    URI failureBucketName = new URI("gs://read-test-bucket-other/");

    FileInfo fileInfo =
        FileInfo.fromItemInfo(
            GoogleCloudStorageItemInfo.createObject(
                new StorageResourceId(bucketName.getAuthority(), "bar/test/object"),
                /* creationTime= */ 10L,
                /* modificationTime= */ 15L,
                /* size= */ 200L,
                "text/plain",
                /* contentEncoding= */ "lzma",
                /* metadata= */ ImmutableMap.of("foo-meta", new byte[] {5, 66, 56}),
                /* contentGeneration= */ 312432L,
                /* metaGeneration= */ 2L,
                /* verificationAttributes= */ null));

    GoogleHadoopFileStatus fileStatus =
        new GoogleHadoopFileStatus(
            fileInfo, new Path(fileInfo.getPath()), 1, 2, FsPermission.getFileDefault(), "foo");
    try (GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem()) {
      fs.initialize(bucketName, new Configuration());
      fs.open(fileStatus);

      fs.initialize(failureBucketName, new Configuration());

      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> fs.open(fileStatus));
      assertThat(exception.getMessage())
          .isEqualTo(
              "Wrong bucket: read-test-bucket, in path: gs://read-test-bucket/bar/test/object, expected bucket: read-test-bucket-other");
    }
  }

  @Test
  public void testFileOpenWithStatusInvalidType() throws Exception {
    try (GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem()) {
      fs.initialize(new URI("gs://read-test-bucket/"), new Configuration());

      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> fs.open(new FileStatus()));
      assertThat(exception.getMessage())
          .isEqualTo(
              "Expected status to be of type GoogleHadoopFileStatus, but found class org.apache.hadoop.fs.FileStatus");
    }
  }

  @Test
  public void testTotalTimeStatistics() throws IOException {
    GhfsGlobalStorageStatistics stats = new GhfsGlobalStorageStatistics();
    stats.updateStats(STREAM_READ_OPERATIONS, 10, 100, 200, 10, new Object());
    stats.addTotalTimeStatistic(STREAM_READ_OPERATIONS.getSymbol() + "_duration");
    assertThat(stats.getLong(STREAM_READ_OPERATIONS.getSymbol() + "_duration")).isEqualTo(200);

    stats.updateStats(STREAM_WRITE_OPERATIONS, 10, 100, 200, 10, new Object());
    stats.addTotalTimeStatistic(STREAM_WRITE_OPERATIONS.getSymbol() + "_duration");
    assertThat(stats.getLong(STREAM_WRITE_OPERATIONS.getSymbol() + "_duration")).isEqualTo(200);
  }

  @Test
  public void close_whenCloudLoggingEnabled_loggingInterceptorIsClosedAndRemoved()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(GoogleHadoopFileSystemConfiguration.GCS_CLOUD_LOGGING_ENABLE.getKey(), true);
    // Provide credentials config to satisfy initialize()
    conf.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    conf.setClass(
        "fs.gs.auth.access.token.provider",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    conf.setEnum(GCS_CLIENT_TYPE.toString(), storageClientType);

    LoggingInterceptor mockInterceptor = mock(LoggingInterceptor.class);
    // Anonymous subclass to inject the mock
    GoogleHadoopFileSystem fs =
        new GoogleHadoopFileSystem() {
          @Override
          LoggingInterceptor createLoggingInterceptor(
              GoogleCredentials credentials, String suffix) {
            // Return our mock instead of a real one
            return mockInterceptor;
          }
        };

    try {
      fs.initialize(new URI("gs://foobar/"), conf);

      // Verify handler was added
      Logger rootLogger = Logger.getLogger("");
      assertThat(Arrays.asList(rootLogger.getHandlers())).contains(mockInterceptor);

      fs.close();

      // Verify close was called and handler was removed
      verify(mockInterceptor, times(1)).close();
      assertThat(Arrays.asList(rootLogger.getHandlers())).doesNotContain(mockInterceptor);
    } finally {
      // Ensure handler is removed even if test fails, to not affect other tests
      Logger.getLogger("").removeHandler(mockInterceptor);
    }
  }

  @Test
  public void
      close_whenCloudLoggingEnabled_loggingInterceptorCloseFails_exceptionIsLoggedAndNotThrown()
          throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(GoogleHadoopFileSystemConfiguration.GCS_CLOUD_LOGGING_ENABLE.getKey(), true);
    // Provide credentials config to satisfy initialize()
    conf.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    conf.setClass(
        "fs.gs.auth.access.token.provider",
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
    conf.setEnum(GCS_CLIENT_TYPE.toString(), storageClientType);

    mockInterceptor = mock(LoggingInterceptor.class);
    doThrow(new RuntimeException("Close failed!")).when(mockInterceptor).close();

    // Setup log capturing
    ghfsLogger = Logger.getLogger(GoogleHadoopFileSystem.class.getName());
    ghfsLogger.setLevel(Level.ALL);
    ByteArrayOutputStream logOutput = new ByteArrayOutputStream();
    testLogHandler =
        new StreamHandler(
            logOutput,
            new Formatter() {
              @Override
              public String format(LogRecord record) {
                return record.getMessage() + "\n";
              }
            });
    ghfsLogger.addHandler(testLogHandler);

    // Anonymous subclass to inject the mock
    GoogleHadoopFileSystem fs =
        new GoogleHadoopFileSystem() {
          @Override
          LoggingInterceptor createLoggingInterceptor(
              GoogleCredentials credentials, String suffix) {
            // Return our mock instead of a real one
            return mockInterceptor;
          }
        };

    fs.initialize(new URI("gs://foobar/"), conf);

    // Verify handler was added
    Logger rootLogger = Logger.getLogger("");
    assertThat(Arrays.asList(rootLogger.getHandlers())).contains(mockInterceptor);

    // Close should not throw an exception, even if the interceptor's close fails.
    fs.close();

    // Verify close was called and handler was removed
    verify(mockInterceptor, times(1)).close();
    assertThat(Arrays.asList(rootLogger.getHandlers())).doesNotContain(mockInterceptor);

    // Verify exception was logged
    testLogHandler.flush();
    assertThat(logOutput.toString()).contains("Failed to stop cloud logging service");
  }

  @Test
  public void close_closesAndNullifiesInstrumentation() throws Exception {
    // Get the instrumentation field
    java.lang.reflect.Field instrumentationField =
        GoogleHadoopFileSystem.class.getDeclaredField("instrumentation");
    instrumentationField.setAccessible(true);

    // Assert that instrumentation is not null before close
    assertThat(instrumentationField.get(ghfs)).isNotNull();

    // Close the file system
    ghfs.close();

    // Assert that instrumentation is null after close
    assertThat(instrumentationField.get(ghfs)).isNull();
  }

  @Test
  public void blockSize_sizeSuffix() throws Exception {
    Configuration config = new Configuration();
    config.set(BLOCK_SIZE.getKey(), "128m");
    config.setBoolean("fs.gs.lazy.init.enable", true);
    config.setEnum(GCS_CLIENT_TYPE.toString(), storageClientType);
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();
    fs.initialize(new URI("gs://test-bucket/"), config);
    assertThat(fs.getDefaultBlockSize()).isEqualTo(128 * 1024 * 1024L);
    fs.close();
  }

  // -----------------------------------------------------------------
  // Inherited tests that we suppress because their behavior differs
  // from the base class.
  // -----------------------------------------------------------------
  @Override
  public void testInitializeSuccess() {}

  @Override
  public void testInitializeSucceedsWhenNoProjectIdConfigured() {}

  @Override
  public void testInitializeWithWorkingDirectory() {}

  @Override
  public void testIOExceptionIsThrowAfterClose() {}

  @Override
  public void testFileSystemIsRemovedFromCacheOnClose() {}

  @Override
  public void testConfigurablePermissions() {}

  @Override
  public void testFileStatusUser() {}

  @Override
  public void testCrc32cFileChecksum() {}

  @Override
  public void testMd5FileChecksum() {}

  @Override
  public void testConcurrentCreationWithoutOverwrite_onlyOneSucceeds() {}

  @Override
  public void testInvalidCredentialsFromAccessTokenProvider() {}

  @Override
  public void testImpersonationServiceAccountUsed() {}

  @Override
  public void testImpersonationUserNameIdentifierUsed() {}

  @Override
  public void testImpersonationGroupNameIdentifierUsed() {}

  @Override
  public void testImpersonationUserAndGroupNameIdentifiersUsed() {}

  @Override
  public void testImpersonationServiceAccountAndUserAndGroupNameIdentifierUsed() {}

  @Override
  public void testImpersonationInvalidUserNameIdentifierUsed() {}

  @Override
  public void unauthenticatedAccessToPublicBuckets_fsGsProperties() {}

  @Override
  public void unauthenticatedAccessToPublicBuckets_googleCloudProperties() {}

  @Override
  public void testInitializeCompatibleWithHadoopCredentialProvider() {}

  @Override
  public void testRenameHnBucket() {}

  @Override
  public void testRenameWithMoveDisabled() {}

  @Override
  public void testGcsJsonAPIMetrics() {}

  @Override
  public void testGcsThreadLocalMetrics() {}

  @Override
  public void multiThreadTest() {}

  /* Custom InMemoryGoogleCloudStorage object which throws exception when reading */
  private class CustomInMemoryGoogleCloudStorage extends InMemoryGoogleCloudStorage {
    private IOException exceptionThrown =
        new IOException("read_throws_exception test : read call throws exception");

    CustomInMemoryGoogleCloudStorage(GoogleCloudStorageOptions storageOptions) {
      super(storageOptions);
    }

    @Override
    public SeekableByteChannel open(
        GoogleCloudStorageItemInfo itemInfo, GoogleCloudStorageReadOptions readOptions) {
      return returnChannel();
    }

    public SeekableByteChannel returnChannel() {
      return new SeekableByteChannel() {
        private long position = 0;
        private boolean isOpen = true;

        @Override
        public long position() {
          return position;
        }

        @CanIgnoreReturnValue
        @Override
        public SeekableByteChannel position(long newPosition) {
          position = newPosition;
          return this;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
          throw exceptionThrown;
        }

        @Override
        public long size() throws IOException {
          throw exceptionThrown;
        }

        @Override
        public SeekableByteChannel truncate(long size) {
          throw new UnsupportedOperationException("Cannot mutate read-only channel");
        }

        @Override
        public int write(ByteBuffer src) {
          throw new UnsupportedOperationException("Cannot mutate read-only channel");
        }

        @Override
        public void close() {
          isOpen = false;
        }

        @Override
        public boolean isOpen() {
          return isOpen;
        }
      };
    }
  }

  @Override
  public void testHnBucketRecursiveDeleteOperationOnDirectory() {}

  @Override
  public void testHnBucketRecursiveDeleteOperationOnBucket() {}

  @Override
  public void testHnBucketNonRecursiveDeleteOperation() {}

  @Override
  public void testHnBucketDeleteOperationOnNonExistingFolder() {}

  @Override
  public void testGetFileStatusWithHint() {}

  @Test
  public void hasPathCapability_listStatusStartingFrom_onStandardBucket_returnsTrue()
      throws Exception {
    Path standardPath = new Path("gs://" + ghfs.getUri().getAuthority() + "/dir");
    assertThat(ghfs.hasPathCapability(standardPath, "fs.gs.capability.liststatus.starting.from"))
        .isTrue();
  }

  @Test
  public void hasPathCapability_listStatusStartingFrom_onHnsBucket_returnsFalse() throws Exception {
    URI initUri = new Path("gs://hns-bucket").toUri();
    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystemImpl(
            (gcsOptions) ->
                new InMemoryGoogleCloudStorage(gcsOptions) {
                  @Override
                  public boolean isHnBucket(URI src) {
                    return true;
                  }
                },
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(getInMemoryGoogleCloudStorageOptions())
                .build());
    try (GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem(fakeGcsFs)) {
      fs.initialize(initUri, new Configuration());

      Path hnsPath = new Path("gs://hns-bucket/dir");
      assertThat(fs.hasPathCapability(hnsPath, "fs.gs.capability.liststatus.starting.from"))
          .isFalse();
    }
  }

  @Test
  public void hasPathCapability_listStatusStartingFrom_fsClosed_returnsFalse() throws Exception {
    URI initUri = new Path("gs://hns-bucket").toUri();
    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystemImpl(
            (gcsOptions) ->
                new InMemoryGoogleCloudStorage(gcsOptions) {
                  @Override
                  public boolean isHnBucket(URI src) {
                    return false;
                  }
                },
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(getInMemoryGoogleCloudStorageOptions())
                .build());
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem(fakeGcsFs);
    fs.initialize(initUri, new Configuration());
    Path hnsPath = new Path("gs://non-hns-bucket/dir");
    fs.close();

    // even on non hns bucket, if file system is closed capability is disabled.
    assertThat(fs.hasPathCapability(hnsPath, "fs.gs.capability.liststatus.starting.from"))
        .isFalse();
  }

  @Test
  public void close_canBeCalledMultipleTimes() throws Exception {
    // Close the file system
    ghfs.close();
    // Close again - should not throw.
    ghfs.close();
  }

  @Test
  public void initialize_downscopedTokensAndAnalyticsCoreEnabled_throwsIOException()
      throws Exception {
    Configuration config = accessTokenProviderConfig(DownscopedAccessTokenProvider.class);
    config.setBoolean("fs.gs.analytics.core.enable", true);
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();

    IOException exception =
        assertThrows(IOException.class, () -> fs.initialize(new URI("gs://foobar/"), config));

    assertThat(exception).hasMessageThat().contains("fs.gs.analytics.core.enable");
  }

  @Test
  public void initialize_downscopedTokensAndAnalyticsCoreWriteEnabled_throwsIOException()
      throws Exception {
    Configuration config = accessTokenProviderConfig(DownscopedAccessTokenProvider.class);
    config.setBoolean("fs.gs.analytics.core.write.enable", true);
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();

    IOException exception =
        assertThrows(IOException.class, () -> fs.initialize(new URI("gs://foobar/"), config));

    assertThat(exception).hasMessageThat().contains("fs.gs.analytics.core.write.enable");
  }

  @Test
  public void initialize_downscopedTokensAndAnalyticsCoreEnabled_namesTheAccessTokenType()
      throws Exception {
    Configuration config = accessTokenProviderConfig(DownscopedAccessTokenProvider.class);
    config.setBoolean("fs.gs.analytics.core.enable", true);
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();

    IOException exception =
        assertThrows(IOException.class, () -> fs.initialize(new URI("gs://foobar/"), config));

    assertThat(exception).hasMessageThat().contains("DOWNSCOPED");
  }

  @Test
  public void initialize_downscopedTokensAndAnalyticsCoreDisabled_succeeds() throws Exception {
    Configuration config = accessTokenProviderConfig(DownscopedAccessTokenProvider.class);
    config.setBoolean("fs.gs.analytics.core.enable", false);
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();

    fs.initialize(new URI("gs://foobar/"), config);

    assertThat(fs.isAnalyticsCoreEnabled()).isFalse();
  }

  @Test
  public void initialize_genericTokensAndAnalyticsCoreEnabled_succeeds() throws Exception {
    Configuration config = accessTokenProviderConfig(TestingAccessTokenProvider.class);
    config.setBoolean("fs.gs.analytics.core.enable", true);
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();

    fs.initialize(new URI("gs://foobar/"), config);

    assertThat(fs.isAnalyticsCoreEnabled()).isTrue();
  }

  @Test
  public void initialize_analyticsCoreEnabled_defaultClientType_usesHttpProtocol()
      throws Exception {
    Configuration config = accessTokenProviderConfig(TestingAccessTokenProvider.class);
    config.setBoolean("fs.gs.analytics.core.enable", true);
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();

    fs.initialize(new URI("gs://foobar/"), config);

    assertThat(analyticsCoreClientOptions(fs).getProtocol()).isEqualTo(Protocol.HTTP);
    fs.close();
  }

  @Test
  public void initialize_analyticsCoreEnabled_storageClient_usesGrpcProtocol() throws Exception {
    Configuration config = accessTokenProviderConfig(TestingAccessTokenProvider.class);
    config.setBoolean("fs.gs.analytics.core.enable", true);
    config.setEnum(GCS_CLIENT_TYPE.getKey(), ClientType.STORAGE_CLIENT);
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();

    fs.initialize(new URI("gs://foobar/"), config);

    GcsClientOptions clientOptions = analyticsCoreClientOptions(fs);
    assertThat(clientOptions.getProtocol()).isEqualTo(Protocol.GRPC);
    assertThat(clientOptions.getGcsWriteOptions().isBidiWriteEnabled()).isFalse();
    fs.close();
  }

  @Test
  public void initialize_analyticsCoreEnabled_storageClientWithBidi_usesBidiProtocol()
      throws Exception {
    Configuration config = accessTokenProviderConfig(TestingAccessTokenProvider.class);
    config.setBoolean("fs.gs.analytics.core.write.enable", true);
    config.setEnum(GCS_CLIENT_TYPE.getKey(), ClientType.STORAGE_CLIENT);
    config.setBoolean("fs.gs.bidi.enable", true);
    config.setBoolean("fs.gs.bidi.finalize.on.close", true);
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();

    fs.initialize(new URI("gs://foobar/"), config);

    GcsClientOptions clientOptions = analyticsCoreClientOptions(fs);
    assertThat(clientOptions.getProtocol()).isEqualTo(Protocol.BIDI);
    assertThat(clientOptions.getGcsWriteOptions().isBidiWriteEnabled()).isTrue();
    assertThat(clientOptions.getGcsWriteOptions().isBidiFinalizeOnClose()).isTrue();
    fs.close();
  }

  private static GcsClientOptions analyticsCoreClientOptions(GoogleHadoopFileSystem fs) {
    assertThat(fs.getAnalyticsCoreGcsFs()).isNotNull();
    return fs.getAnalyticsCoreGcsFs().getFileSystemOptions().getGcsClientOptions();
  }

  private static Configuration accessTokenProviderConfig(
      Class<? extends AccessTokenProvider> provider) {
    Configuration config = new Configuration();
    config.setEnum("fs.gs.auth.type", AuthenticationType.ACCESS_TOKEN_PROVIDER);
    config.setClass("fs.gs.auth.access.token.provider", provider, AccessTokenProvider.class);
    return config;
  }

  @Test
  public void invocationId_isClearedAfterEveryFileSystemOperation() throws Exception {
    Path dir = new Path(ghfs.getWorkingDirectory(), "invocationId_isCleared");
    Path file = new Path(dir, "file");

    ghfs.mkdirs(dir);
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();

    try (FSDataOutputStream out = ghfs.create(file)) {
      out.write(new byte[] {1, 2, 3});
      assertThat(InvocationIdContext.getInvocationId()).isEmpty();
    }
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();

    ghfs.getFileStatus(file);
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
    ghfs.listStatus(dir);
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
    ghfs.globStatus(new Path(dir, "*"));
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();

    try (FSDataInputStream in = ghfs.open(file)) {
      assertThat(in.read()).isEqualTo(1);
      assertThat(InvocationIdContext.getInvocationId()).isEmpty();
    }
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();

    try (FSDataOutputStream out = ghfs.append(file)) {
      out.write(new byte[] {4});
      assertThat(InvocationIdContext.getInvocationId()).isEmpty();
    }
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();

    Path otherFile = new Path(dir, "other");
    createFile(otherFile, new byte[] {5});
    ghfs.concat(file, new Path[] {otherFile});
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();

    ghfs.delete(dir, /* recursive= */ true);
    assertThat(InvocationIdContext.getInvocationId()).isEmpty();
  }

  @Test
  public void inputStreamOperations_useInvocationIdOfOpenCall() throws Exception {
    Path dir = new Path(ghfs.getWorkingDirectory(), "inputStream_invocationId");
    Path file = new Path(dir, "file");
    Path otherFile = new Path(dir, "other");
    ghfs.mkdirs(dir);
    createFile(file, new byte[] {1, 2, 3, 4, 5});
    createFile(otherFile, new byte[] {9});

    try (LogCapture logs = new LogCapture(GoogleHadoopFSInputStream.class)) {
      FSDataInputStream in = ghfs.open(file);
      String openId = logs.invocationIdOf("GoogleHadoopFSInputStream(gcsPath:");
      assertThat(InvocationIdContext.getInvocationId()).isEmpty();

      ghfs.getFileStatus(otherFile);

      in.seek(2);
      assertThat(logs.invocationIdOf("seek(2)")).isEqualTo(openId);
      assertThat(in.read()).isEqualTo(3);
      assertThat(InvocationIdContext.getInvocationId()).isEmpty();

      in.close();
      assertThat(logs.invocationIdOf("close(): ")).isEqualTo(openId);
      assertThat(InvocationIdContext.getInvocationId()).isEmpty();
    }
  }

  @Test
  public void openFileWithStatus_streamCreatedOnPoolThread_usesInvocationIdOfOpenCall()
      throws Exception {
    Path dir = new Path(ghfs.getWorkingDirectory(), "openFileWithStatus_invocationId");
    Path file = new Path(dir, "file");
    ghfs.mkdirs(dir);
    createFile(file, new byte[] {1, 2, 3});
    FileStatus status = ghfs.getFileStatus(file);

    try (LogCapture fsLogs = new LogCapture(GoogleHadoopFileSystem.class);
        LogCapture streamLogs = new LogCapture(GoogleHadoopFSInputStream.class)) {
      FSDataInputStream in = ghfs.openFile(file).withFileStatus(status).build().get();
      String openId = fsLogs.invocationIdOf("Path to be opened:");

      assertThat(streamLogs.invocationIdOf("GoogleHadoopFSInputStream(gcsPath:")).isEqualTo(openId);
      assertThat(in.read()).isEqualTo(1);
      in.close();
      assertThat(streamLogs.invocationIdOf("close(): ")).isEqualTo(openId);
      assertThat(InvocationIdContext.getInvocationId()).isEmpty();
    }
  }

  @Test
  public void outputStreamOperations_useInvocationIdOfCreateCall() throws Exception {
    Path dir = new Path(ghfs.getWorkingDirectory(), "outputStream_invocationId");
    Path file = new Path(dir, "file");
    Path otherFile = new Path(dir, "other");
    ghfs.mkdirs(dir);
    createFile(otherFile, new byte[] {9});

    try (LogCapture logs = new LogCapture(GoogleHadoopOutputStream.class)) {
      FSDataOutputStream out = ghfs.create(file);
      String createId = logs.invocationIdOf("GoogleHadoopOutputStream(gcsPath:");
      assertThat(InvocationIdContext.getInvocationId()).isEmpty();

      ghfs.getFileStatus(otherFile);

      out.write(new byte[] {1, 2, 3});
      out.close();
      assertThat(logs.invocationIdOf("close(): temp tail file")).isEqualTo(createId);
      assertThat(InvocationIdContext.getInvocationId()).isEmpty();
    }
    assertThat(ghfs.getFileStatus(file).getLen()).isEqualTo(3);
  }

  private static final class LogCapture implements AutoCloseable {
    private static final Pattern INVOCATION_ID_LOG_LINE =
        Pattern.compile(
            "^\\[(" + Pattern.quote(GCCL_INVOCATION_ID_PREFIX) + "[0-9a-f]{8})\\]: (.*)$");

    private final Logger logger;
    private final Level previousLevel;
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final StreamHandler handler;

    LogCapture(Class<?> loggingClass) {
      this.logger = Logger.getLogger(loggingClass.getName());
      this.previousLevel = logger.getLevel();
      this.handler =
          new StreamHandler(
              output,
              new LoggingFormatter(
                  new Formatter() {
                    @Override
                    public String format(LogRecord logRecord) {
                      return logRecord.getMessage() + "\n";
                    }
                  }));
      handler.setLevel(Level.ALL);
      logger.setLevel(Level.ALL);
      logger.addHandler(handler);
    }

    String invocationIdOf(String part) {
      handler.flush();
      for (String line : output.toString(UTF_8).split("\n")) {
        Matcher matcher = INVOCATION_ID_LOG_LINE.matcher(line);
        if (matcher.matches() && matcher.group(2).contains(part)) {
          return matcher.group(1);
        }
      }
      throw new AssertionError(
          String.format("No log line with an invocation ID contains '%s':%n%s", part, output));
    }

    @Override
    public void close() {
      logger.removeHandler(handler);
      logger.setLevel(previousLevel);
    }
  }

  /** An {@link AccessTokenProvider} that reports issuing downscoped, per-request tokens. */
  public static final class DownscopedAccessTokenProvider implements AccessTokenProvider {

    private Configuration config;

    @Override
    public AccessTokenType getAccessTokenType() {
      return AccessTokenType.DOWNSCOPED;
    }

    @Override
    public AccessToken getAccessToken() {
      return new AccessToken("downscoped-access-token", Instant.now().plusSeconds(600));
    }

    @Override
    public void refresh() {
      // no-op
    }

    @Override
    public void setConf(Configuration config) {
      this.config = config;
    }

    @Override
    public Configuration getConf() {
      return config;
    }
  }

  @Test
  public void renameAndDelete_bareSocketTimeoutException_rethrows() throws Exception {
    Path src = new Path("gs://request-failure-test-bucket/src");
    Path dst = new Path("gs://request-failure-test-bucket/dst");

    try (GoogleHadoopFileSystem fs =
        createGhfsThrowingOnRenameAndDelete(new SocketTimeoutException("Read timed out"))) {
      assertThrows(SocketTimeoutException.class, () -> fs.rename(src, dst));
    }

    try (GoogleHadoopFileSystem fs =
        createGhfsThrowingOnRenameAndDelete(new SocketTimeoutException("Read timed out"))) {
      assertThrows(SocketTimeoutException.class, () -> fs.delete(src, /* recursive= */ false));
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETE_REJECTED.getSymbol()))
          .isEqualTo(1L);
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(0L);
    }
  }

  @Test
  public void renameAndDelete_nonSocketTransportAndTimeoutFailures_rethrow() throws Exception {
    Path src = new Path("gs://request-failure-test-bucket/src");
    Path dst = new Path("gs://request-failure-test-bucket/dst");

    ImmutableList<IOException> transportAndTimeoutFailures =
        ImmutableList.of(
            new EOFException("Unexpected end of stream"),
            new UnknownHostException("storage.googleapis.com"),
            new ClosedChannelException(),
            new AsynchronousCloseException(),
            new IOException("LRO timed out", new TimeoutException("Operation timed out")),
            new IOException(
                "Premature close",
                StorageException.translate(new IOException("Connection closed prematurely"))));

    for (IOException failure : transportAndTimeoutFailures) {
      try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(failure)) {
        IOException renameThrown = assertThrows(IOException.class, () -> fs.rename(src, dst));
        assertThat(renameThrown).isSameInstanceAs(failure);
      }
      try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(failure)) {
        IOException deleteThrown =
            assertThrows(IOException.class, () -> fs.delete(src, /* recursive= */ false));
        assertThat(deleteThrown).isSameInstanceAs(failure);
        assertThat(fs.getIOStatistics().counters().get(FILES_DELETE_REJECTED.getSymbol()))
            .isEqualTo(1L);
        assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(0L);
      }
    }
  }

  @Test
  public void renameAndDelete_http408And429QuotaOrPlain_rethrow() throws Exception {
    Path src = new Path("gs://request-failure-test-bucket/src");
    Path dst = new Path("gs://request-failure-test-bucket/dst");

    GoogleJsonError quotaError = new GoogleJsonError();
    quotaError.setCode(429);
    quotaError.setMessage("Quota exceeded");
    ErrorInfo errorInfo = new ErrorInfo();
    errorInfo.setDomain(ApiErrorExtractor.USAGE_LIMITS_DOMAIN);
    errorInfo.setReason(ApiErrorExtractor.QUOTA_EXCEEDED_REASON);
    quotaError.setErrors(Collections.singletonList(errorInfo));
    GoogleJsonResponseException quota429 =
        GoogleCloudStorageExceptions.createJsonResponseException(quotaError, new HttpHeaders());

    ImmutableList<IOException> httpFailures =
        ImmutableList.of(
            new HttpResponseException.Builder(408, "Request Timeout", new HttpHeaders()).build(),
            new HttpResponseException.Builder(429, "Too Many Requests", new HttpHeaders()).build(),
            quota429);

    for (IOException failure : httpFailures) {
      try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(failure)) {
        IOException renameThrown = assertThrows(IOException.class, () -> fs.rename(src, dst));
        assertThat(renameThrown).isSameInstanceAs(failure);
      }
      try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(failure)) {
        IOException deleteThrown =
            assertThrows(IOException.class, () -> fs.delete(src, /* recursive= */ false));
        assertThat(deleteThrown).isSameInstanceAs(failure);
        assertThat(fs.getIOStatistics().counters().get(FILES_DELETE_REJECTED.getSymbol()))
            .isEqualTo(1L);
        assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(0L);
      }
    }
  }

  @Test
  public void renameAndDelete_afterClose_throwsIOException() throws Exception {
    GoogleHadoopFileSystem fs = createInMemoryGoogleHadoopFileSystem();
    Path src = new Path(fs.getWorkingDirectory(), "src");
    Path dst = new Path(fs.getWorkingDirectory(), "dst");
    Path root = new Path(fs.getUri());
    fs.close();

    IOException renameThrown = assertThrows(IOException.class, () -> fs.rename(src, dst));
    assertThat(renameThrown)
        .hasMessageThat()
        .isEqualTo("GoogleHadoopFileSystem has been closed or not initialized.");

    IOException renameRootThrown = assertThrows(IOException.class, () -> fs.rename(root, dst));
    assertThat(renameRootThrown)
        .hasMessageThat()
        .isEqualTo("GoogleHadoopFileSystem has been closed or not initialized.");

    IOException deleteThrown =
        assertThrows(IOException.class, () -> fs.delete(src, /* recursive= */ false));
    assertThat(deleteThrown)
        .hasMessageThat()
        .isEqualTo("GoogleHadoopFileSystem has been closed or not initialized.");

    GoogleHadoopFileSystem uninitializedFs = new GoogleHadoopFileSystem();
    IOException uninitializedRenameThrown =
        assertThrows(IOException.class, () -> uninitializedFs.rename(src, dst));
    assertThat(uninitializedRenameThrown)
        .hasMessageThat()
        .isEqualTo("GoogleHadoopFileSystem has been closed or not initialized.");
    IOException uninitializedDeleteThrown =
        assertThrows(IOException.class, () -> uninitializedFs.delete(src, /* recursive= */ false));
    assertThat(uninitializedDeleteThrown)
        .hasMessageThat()
        .isEqualTo("GoogleHadoopFileSystem has been closed or not initialized.");
  }

  @Test
  public void renameAndDelete_storageExceptionCodes_rethrowOrReturnFalse() throws Exception {
    Path src = new Path("gs://request-failure-test-bucket/src");
    Path dst = new Path("gs://request-failure-test-bucket/dst");

    for (int failureCode : new int[] {401, 403, 429, 503}) {
      IOException failure =
          new IOException("storage failure", new StorageException(failureCode, "err"));
      try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(failure)) {
        IOException renameThrown = assertThrows(IOException.class, () -> fs.rename(src, dst));
        assertThat(renameThrown).isSameInstanceAs(failure);
      }
      try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(failure)) {
        IOException deleteThrown =
            assertThrows(IOException.class, () -> fs.delete(src, /* recursive= */ true));
        assertThat(deleteThrown).isSameInstanceAs(failure);
        assertThat(fs.getIOStatistics().counters().get(FILES_DELETE_REJECTED.getSymbol()))
            .isEqualTo(1L);
        assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(0L);
      }
    }

    for (int nonFailureCode : new int[] {404, 409, 412}) {
      IOException nonFailure =
          new IOException("storage semantic", new StorageException(nonFailureCode, "semantic"));
      try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(nonFailure)) {
        assertThat(fs.rename(src, dst)).isFalse();
        assertThat(fs.delete(src, /* recursive= */ true)).isFalse();
        assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(0L);
        assertThat(fs.getIOStatistics().counters().get(FILES_DELETE_REJECTED.getSymbol()))
            .isEqualTo(0L);
      }
    }
  }

  @Test
  public void renameAndDelete_apiExceptionCodes_rethrowOrReturnFalse() throws Exception {
    Path src = new Path("gs://request-failure-test-bucket/src");
    Path dst = new Path("gs://request-failure-test-bucket/dst");

    StatusCode.Code[] failureCodes = {
      StatusCode.Code.PERMISSION_DENIED,
      StatusCode.Code.UNAVAILABLE,
      StatusCode.Code.INVALID_ARGUMENT,
    };
    for (StatusCode.Code code : failureCodes) {
      IOException failure = new IOException("gax failure", createApiException(code));
      try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(failure)) {
        IOException renameThrown = assertThrows(IOException.class, () -> fs.rename(src, dst));
        assertThat(renameThrown).isSameInstanceAs(failure);
      }
      try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(failure)) {
        IOException deleteThrown =
            assertThrows(IOException.class, () -> fs.delete(src, /* recursive= */ false));
        assertThat(deleteThrown).isSameInstanceAs(failure);
        assertThat(fs.getIOStatistics().counters().get(FILES_DELETE_REJECTED.getSymbol()))
            .isEqualTo(1L);
        assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(0L);
      }
    }

    StatusCode.Code[] nonFailureCodes = {
      StatusCode.Code.NOT_FOUND,
      StatusCode.Code.ALREADY_EXISTS,
      StatusCode.Code.FAILED_PRECONDITION,
    };
    for (StatusCode.Code code : nonFailureCodes) {
      IOException nonFailure = new IOException("gax semantic", createApiException(code));
      try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(nonFailure)) {
        assertThat(fs.rename(src, dst)).isFalse();
        assertThat(fs.delete(src, /* recursive= */ false)).isFalse();
        assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(0L);
        assertThat(fs.getIOStatistics().counters().get(FILES_DELETE_REJECTED.getSymbol()))
            .isEqualTo(0L);
      }
    }
  }

  @Test
  public void renameAndDelete_statusRuntimeException_rethrowsOrReturnsFalse() throws Exception {
    Path src = new Path("gs://request-failure-test-bucket/src");
    Path dst = new Path("gs://request-failure-test-bucket/dst");

    IOException grpcFailure =
        new IOException("grpc unavailable", new StatusRuntimeException(Status.UNAVAILABLE));
    try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(grpcFailure)) {
      assertThrows(IOException.class, () -> fs.rename(src, dst));
      assertThrows(IOException.class, () -> fs.delete(src, /* recursive= */ false));
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETE_REJECTED.getSymbol()))
          .isEqualTo(1L);
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(0L);
    }

    IOException grpcNotFound =
        new IOException("grpc not found", new StatusRuntimeException(Status.NOT_FOUND));
    try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(grpcNotFound)) {
      assertThat(fs.rename(src, dst)).isFalse();
      assertThat(fs.delete(src, /* recursive= */ false)).isFalse();
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(0L);
    }
  }

  @Test
  public void renameAndDelete_compositeNotFoundAndForbidden_rethrows() throws Exception {
    Path src = new Path("gs://request-failure-test-bucket/src");
    Path dst = new Path("gs://request-failure-test-bucket/dst");

    IOException composite =
        GoogleCloudStorageExceptions.createCompositeException(
            ImmutableList.of(
                new FileNotFoundException("gs://request-failure-test-bucket/missing"),
                new IOException("Error deleting", new StorageException(403, "Forbidden"))));
    try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(composite)) {
      IOException renameThrown = assertThrows(IOException.class, () -> fs.rename(src, dst));
      assertThat(renameThrown).isSameInstanceAs(composite);
      IOException deleteThrown =
          assertThrows(IOException.class, () -> fs.delete(src, /* recursive= */ true));
      assertThat(deleteThrown).isSameInstanceAs(composite);
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETE_REJECTED.getSymbol()))
          .isEqualTo(1L);
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(0L);
    }

    FileNotFoundException primaryNotFound =
        new FileNotFoundException("gs://request-failure-test-bucket/missing");
    primaryNotFound.addSuppressed(new StorageException(403, "Forbidden"));
    try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(primaryNotFound)) {
      assertThrows(FileNotFoundException.class, () -> fs.rename(src, dst));
      assertThrows(FileNotFoundException.class, () -> fs.delete(src, /* recursive= */ true));
    }
  }

  @Test
  public void renameAndDelete_cyclicCauseAndSuppressedGraph_terminatesCleanly() throws Exception {
    Path src = new Path("gs://request-failure-test-bucket/src");
    Path dst = new Path("gs://request-failure-test-bucket/dst");

    IOException cycle1 = new IOException("cycle-1");
    IOException cycle2 = new IOException("cycle-2", cycle1);
    cycle1.initCause(cycle2);
    cycle1.addSuppressed(cycle2);
    cycle2.addSuppressed(cycle1);

    try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(cycle1)) {
      assertThat(fs.rename(src, dst)).isFalse();
      assertThat(fs.delete(src, /* recursive= */ false)).isFalse();
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(0L);
    }
  }

  @Test
  public void delete_nonEmptyDirectory_rethrowsDirectoryNotEmptyAndIncrementsRejected()
      throws Exception {
    try (GoogleHadoopFileSystem fs = createInMemoryGoogleHadoopFileSystem()) {
      Path dir = new Path(fs.getWorkingDirectory(), "non-empty-dir");
      Path child = new Path(dir, "child.txt");
      fs.mkdirs(dir);
      try (FSDataOutputStream out = fs.create(child)) {
        out.write(new byte[] {1, 2, 3});
      }

      assertThrows(DirectoryNotEmptyException.class, () -> fs.delete(dir, /* recursive= */ false));
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETE_REJECTED.getSymbol()))
          .isEqualTo(1L);
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(0L);
    }
  }

  @Test
  public void delete_missingFile_returnsFalseWithoutIncrementingFilesDeleted() throws Exception {
    try (GoogleHadoopFileSystem fs = createInMemoryGoogleHadoopFileSystem()) {
      Path missing = new Path(fs.getWorkingDirectory(), "does-not-exist.txt");

      assertThat(fs.delete(missing, /* recursive= */ false)).isFalse();
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(0L);
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETE_REJECTED.getSymbol()))
          .isEqualTo(0L);

      Path existing = new Path(fs.getWorkingDirectory(), "exists.txt");
      try (FSDataOutputStream out = fs.create(existing)) {
        out.write(new byte[] {42});
      }
      assertThat(fs.delete(existing, /* recursive= */ false)).isTrue();
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(1L);
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETE_REJECTED.getSymbol()))
          .isEqualTo(0L);
    }
  }

  @Test
  public void renameAndDelete_httpPathAuthAndTruncatedStreamFailures_rethrow() throws Exception {
    Path src = new Path("gs://request-failure-test-bucket/src");
    Path dst = new Path("gs://request-failure-test-bucket/dst");

    MockHttpTransport sts400Transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest(url) {
              @Override
              public LowLevelHttpResponse execute() {
                return new MockLowLevelHttpResponse()
                    .setStatusCode(400)
                    .setContentType("application/json; charset=UTF-8")
                    .setContent(
                        "{\"error\":\"invalid_grant\",\"error_description\":\"Bad token\"}");
              }
            };
          }
        };
    StsRequestHandler stsHandler =
        StsRequestHandler.newBuilder(
                "https://sts.googleapis.com/v1/token",
                StsTokenExchangeRequest.newBuilder(
                        "bad-token", "urn:ietf:params:oauth:token-type:jwt")
                    .build(),
                sts400Transport.createRequestFactory())
            .build();
    IOException nonRetryableOAuthEx = assertThrows(IOException.class, stsHandler::exchangeToken);
    assertThat(((Retryable) nonRetryableOAuthEx).isRetryable()).isFalse();

    ComputeEngineCredentials mds429Credentials =
        ComputeEngineCredentials.newBuilder()
            .setHttpTransportFactory(
                () ->
                    new MockHttpTransport() {
                      @Override
                      public LowLevelHttpRequest buildRequest(String method, String url) {
                        return new MockLowLevelHttpRequest(url) {
                          @Override
                          public LowLevelHttpResponse execute() {
                            return new MockLowLevelHttpResponse()
                                .setStatusCode(429)
                                .setContent("Too Many Requests");
                          }
                        };
                      }
                    })
            .build();
    IOException mds429Ex = assertThrows(IOException.class, mds429Credentials::refreshAccessToken);

    ImmutableList<IOException> failures =
        ImmutableList.of(
            nonRetryableOAuthEx,
            mds429Ex,
            new IOException(
                "Error code 404 trying to get security access token from Compute Engine metadata"
                    + " for the default service account"),
            new DelegationTokenIOException("token mismatch"),
            new CertificateSourceUnavailableException("mTLS cert missing"),
            new IOException(
                "signing failed",
                new ServiceAccountSigner.SigningException(
                    "Failed to sign", new RuntimeException("kms error"))),
            new IOException(
                "Error signing service account access token request with private key.",
                new GeneralSecurityException("Invalid key")),
            new IOException("Premature EOF"),
            new IOException("Connection closed prematurely"),
            new IOException("insufficient data written"),
            new IOException("Error writing request body to server"));

    for (IOException failure : failures) {
      try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(failure)) {
        IOException renameThrown = assertThrows(IOException.class, () -> fs.rename(src, dst));
        assertThat(renameThrown).isSameInstanceAs(failure);
      }
      try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(failure)) {
        IOException deleteThrown =
            assertThrows(IOException.class, () -> fs.delete(src, /* recursive= */ false));
        assertThat(deleteThrown).isSameInstanceAs(failure);
        assertThat(fs.getIOStatistics().counters().get(FILES_DELETE_REJECTED.getSymbol()))
            .isEqualTo(1L);
        assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(0L);
      }
    }

    FileNotFoundException notFoundWithMdsLikeObjectName =
        new FileNotFoundException("Error code 404 trying to get obj from Compute Engine metadata");
    try (GoogleHadoopFileSystem fs =
        createGhfsThrowingOnRenameAndDelete(notFoundWithMdsLikeObjectName)) {
      assertThat(fs.rename(src, dst)).isFalse();
      assertThat(fs.delete(src, /* recursive= */ false)).isFalse();
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(0L);
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETE_REJECTED.getSymbol()))
          .isEqualTo(0L);
    }
  }

  @Test
  public void
      renameAndDelete_workerErrorsFolderStallStorageZeroAndImpersonatedAuthFailures_rethrow()
          throws Exception {
    Path src = new Path("gs://request-failure-test-bucket/src");
    Path dst = new Path("gs://request-failure-test-bucket/dst");

    MockHttpTransport http404Transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest(url) {
              @Override
              public LowLevelHttpResponse execute() {
                return new MockLowLevelHttpResponse()
                    .setStatusCode(404)
                    .setContentType("application/json; charset=UTF-8")
                    .setContent("{\"error\":{\"code\":404,\"message\":\"Not found\"}}");
              }
            };
          }
        };
    ImpersonatedCredentials impersonatedCreds =
        ImpersonatedCredentials.create(
            GoogleCredentials.create(
                new AccessToken("src-token", new Date(System.currentTimeMillis() + 3_600_000L))),
            "missing-sa@test-project.iam.gserviceaccount.com",
            /* delegates= */ null,
            ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"),
            /* lifetime= */ 300,
            () -> http404Transport);
    IOException impersonated404Ex =
        assertThrows(IOException.class, impersonatedCreds::refreshAccessToken);

    IdentityPoolCredentials identityPoolUrl404Creds =
        IdentityPoolCredentials.newBuilder()
            .setAudience(
                "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/p/providers/pr")
            .setSubjectTokenType("urn:ietf:params:oauth:token-type:jwt")
            .setTokenUrl("https://sts.googleapis.com/v1/token")
            .setCredentialSource(
                new IdentityPoolCredentialSource(
                    ImmutableMap.of("url", "http://169.254.169.254/subject-token")))
            .setHttpTransportFactory(() -> http404Transport)
            .build();
    IOException identityPool404Ex =
        assertThrows(IOException.class, identityPoolUrl404Creds::refreshAccessToken);

    ImmutableList<IOException> failures =
        ImmutableList.of(
            new IOException("Error deleting object", new OutOfMemoryError("Java heap space")),
            new IOException("Error deleting object", new AssertionError("invariant failed")),
            new IOException(
                "Received IllegalStateException while deletion of folder resource : Deletion"
                    + " stalled: No active threads, but 1 folders remain.",
                new IllegalStateException(
                    "Deletion stalled: No active threads, but 1 folders remain.")),
            new IOException(
                "Error in shutting down batch executor : Failed to get result:"
                    + " java.lang.InterruptedException"),
            new IOException("task rejected", new RejectedExecutionException("pool shut down")),
            new IOException("client closed", new IllegalStateException("Storage client is closed")),
            new IOException(
                "Error creating empty object",
                new StorageException(
                    0, "onComplete without preceding onNext, unable to determine success.")),
            new IOException(
                "Error finalizing upload",
                new StorageException(
                    0, "Finalized upload, but object size less than expected.", "dataLoss", null)),
            impersonated404Ex,
            identityPool404Ex);

    for (IOException failure : failures) {
      try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(failure)) {
        IOException renameThrown = assertThrows(IOException.class, () -> fs.rename(src, dst));
        assertThat(renameThrown).isSameInstanceAs(failure);
      }
      try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(failure)) {
        IOException deleteThrown =
            assertThrows(IOException.class, () -> fs.delete(src, /* recursive= */ true));
        assertThat(deleteThrown).isSameInstanceAs(failure);
        assertThat(fs.getIOStatistics().counters().get(FILES_DELETE_REJECTED.getSymbol()))
            .isEqualTo(1L);
        assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(0L);
      }
    }

    // Concurrent close() racing between rename's outer checkOpen() and renameInternal's checkOpen()
    URI initUri = URI.create("gs://request-failure-test-bucket/");
    GoogleCloudStorageFileSystem backingFs =
        new GoogleCloudStorageFileSystemImpl(
            InMemoryGoogleCloudStorage::new,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(getInMemoryGoogleCloudStorageOptions())
                .build());
    GoogleHadoopFileSystem concurrentlyClosingFs =
        new GoogleHadoopFileSystem(backingFs) {
          @Override
          void renameInternal(Path s, Path d) throws IOException {
            close();
            super.renameInternal(s, d);
          }
        };
    concurrentlyClosingFs.initialize(initUri, new Configuration());
    IOException closedDuringRename =
        assertThrows(IOException.class, () -> concurrentlyClosingFs.rename(src, dst));
    assertThat(closedDuringRename)
        .hasMessageThat()
        .isEqualTo("GoogleHadoopFileSystem has been closed or not initialized.");

    // Negative check: semantic ApiException(NOT_FOUND) with suppressed AsyncTaskException -> false
    ApiException gaxNotFoundWithAsyncTask =
        assertThrows(
            ApiException.class,
            () ->
                ApiExceptions.callAndTranslateApiException(
                    ApiFutures.immediateFailedFuture(
                        createApiException(StatusCode.Code.NOT_FOUND))));
    IOException wrappedGaxNotFound = new IOException("not found", gaxNotFoundWithAsyncTask);
    try (GoogleHadoopFileSystem fs = createGhfsThrowingOnRenameAndDelete(wrappedGaxNotFound)) {
      assertThat(fs.rename(src, dst)).isFalse();
      assertThat(fs.delete(src, /* recursive= */ true)).isFalse();
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETED.getSymbol())).isEqualTo(0L);
      assertThat(fs.getIOStatistics().counters().get(FILES_DELETE_REJECTED.getSymbol()))
          .isEqualTo(0L);
    }
  }

  private static GoogleHadoopFileSystem createGhfsThrowingOnRenameAndDelete(IOException failure)
      throws IOException {
    URI initUri = URI.create("gs://request-failure-test-bucket/");
    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystemImpl(
            InMemoryGoogleCloudStorage::new,
            GoogleCloudStorageFileSystemOptions.builder()
                .setCloudStorageOptions(getInMemoryGoogleCloudStorageOptions())
                .build()) {
          @Override
          public void rename(URI src, URI dst) throws IOException {
            throw failure;
          }

          @Override
          public void delete(URI path, boolean recursive) throws IOException {
            throw failure;
          }
        };
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem(fakeGcsFs);
    fs.initialize(initUri, new Configuration());
    return fs;
  }

  private static ApiException createApiException(StatusCode.Code code) {
    StatusCode statusCode =
        new StatusCode() {
          @Override
          public Code getCode() {
            return code;
          }

          @Override
          public Object getTransportCode() {
            return code.getHttpStatusCode();
          }
        };
    return new ApiException("gax " + code, /* cause= */ null, statusCode, /* retryable= */ false);
  }
}
