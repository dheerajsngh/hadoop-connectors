/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonDataResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.Json;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.auth.Credentials;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.common.base.Equivalence;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StorageClientProviderTest {

  private static final Credentials TEST_CREDENTIALS = null;
  private static final FeatureHeaderGenerator TEST_FEATURE_HEADER_GENERATOR =
      new FeatureHeaderGenerator(GoogleCloudStorageFileSystemOptions.DEFAULT);

  private static final GoogleCloudStorageOptions TEST_STORAGE_OPTIONS =
      GoogleCloudStorageOptions.builder()
          .setHttpRequestHeaders((ImmutableMap.of("header-key", "header-value")))
          .setTraceLogEnabled(true)
          .setDirectPathPreferred(true)
          .setWriteChannelOptions(AsyncWriteChannelOptions.DEFAULT)
          .setStorageClientCachingEnabled(true)
          .build();

  private static final Function<List<AccessBoundary>, String> TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC =
      (accessBoundaries) -> "token";

  private static final String TEST_PROJECT_PREFIX = "test-project";

  private static final String PROJECT_ENV_VARIABLE = "GOOGLE_CLOUD_PROJECT";

  private StorageClientProvider storageClientProvider;

  private static String testProject = null;

  /** Returns number of times a storage object in cache is referenced among different clients. */
  private int getReferences(StorageClientWrapper storage) {
    return storageClientProvider.storageClientToReferenceMap.getOrDefault(storage, 0);
  }

  @BeforeClass
  public static void beforeClass() {
    if (System.getProperty(PROJECT_ENV_VARIABLE) == null) {
      // Project id is required for storage instantiation.
      testProject = TEST_PROJECT_PREFIX + UUID.randomUUID();
      System.setProperty(PROJECT_ENV_VARIABLE, testProject);
    }
  }

  @AfterClass
  public static void afterCLass() {
    if (Objects.equals(System.getProperty(PROJECT_ENV_VARIABLE), testProject)) {
      System.clearProperty(PROJECT_ENV_VARIABLE);
    }
  }

  @Before
  public void setUp() {
    storageClientProvider = new StorageClientProvider();
  }

  @Test
  public void getStorage_returnsAndCachesNewStorageObject() throws IOException {
    StorageClientWrapper storage =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            TEST_STORAGE_OPTIONS,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);

    assertNotNull(storage);
    assertEquals(storageClientProvider.cache.size(), 1);

    StorageClientWrapper cachedStorage =
        storageClientProvider.cache.getIfPresent(
            storageClientProvider.computeCacheKey(
                TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC));
    assertNotNull(cachedStorage);
    assertEquals(storage, cachedStorage);
  }

  @Test
  public void getStorage_cacheDisabled_doesNotCache() throws IOException {
    GoogleCloudStorageOptions disableCacheOptions =
        TEST_STORAGE_OPTIONS.toBuilder().setStorageClientCachingEnabled(false).build();
    StorageClientWrapper storage =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            disableCacheOptions,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);

    assertNotNull(storage);
    assertEquals(storageClientProvider.cache.size(), 0);
  }

  @Test
  public void getStorage_returnsCachedStorageObject() throws IOException {
    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            TEST_STORAGE_OPTIONS,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper storage2 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            TEST_STORAGE_OPTIONS,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);

    assertEquals(storageClientProvider.cache.size(), 1);
    // A single storage object should be shared across both the references.
    assertEquals(getReferences(storage1), 2);
    assertEquals(storage1, storage2);
  }

  @Test
  public void getStorage_cacheMiss_returnsNewObject() throws Exception {
    GoogleCloudStorageOptions testOptions =
        TEST_STORAGE_OPTIONS.toBuilder()
            .setTraceLogEnabled(false)
            .setDirectPathPreferred(false)
            .build();

    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            TEST_STORAGE_OPTIONS,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper storage2 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            testOptions,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);

    assertNotEquals(storage1, storage2);
    assertEquals(getReferences(storage1), 1);
    assertEquals(getReferences(storage2), 1);
  }

  @Test
  public void close_nonZeroReference_doesNotCloseObject() throws Exception {
    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            TEST_STORAGE_OPTIONS,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper storage2 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            TEST_STORAGE_OPTIONS,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);

    assertEquals(storageClientProvider.cache.size(), 1);
    // A single storage object should be shared across both the references.
    assertEquals(getReferences(storage1), 2);
    assertEquals(storage1, storage2);

    storageClientProvider.close(storage1);
    // Item is not removed from the cache but the reference is reduced.
    assertEquals(storageClientProvider.cache.size(), 1);
    assertEquals(getReferences(storage1), 1);
  }

  @Test
  public void close_zeroReference_closesStorageObject() throws Exception {
    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS,
            TEST_STORAGE_OPTIONS,
            null,
            null,
            TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
            TEST_FEATURE_HEADER_GENERATOR);

    assertEquals(storageClientProvider.cache.size(), 1);
    // A single storage object should be shared across both the references.
    assertEquals(getReferences(storage1), 1);

    storageClientProvider.close(storage1);
    // Item is removed from the cache.
    assertEquals(storageClientProvider.cache.size(), 0);
    assertEquals(getReferences(storage1), 0);
    assertFalse(storageClientProvider.storageClientToReferenceMap.containsKey(storage1));
  }

  @Test
  public void getStorageConcurrent_returnsStorage() throws Exception {
    int numThreads = 100;
    ExecutorService executorService = Executors.newFixedThreadPool(100);
    final CountDownLatch latch = new CountDownLatch(numThreads);

    AtomicInteger failures = new AtomicInteger();
    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          () -> {
            try {
              storageClientProvider.getStorage(
                  TEST_CREDENTIALS,
                  TEST_STORAGE_OPTIONS,
                  null,
                  null,
                  TEST_DOWNSCOPED_ACCESS_TOKEN_FUNC,
                  TEST_FEATURE_HEADER_GENERATOR);
            } catch (IOException e) {
              failures.getAndIncrement();
            } finally {
              latch.countDown();
            }
          });
    }

    boolean isComplete = latch.await(5000, TimeUnit.MILLISECONDS);
    executorService.shutdown();
    if (failures.get() > 0 || !isComplete) {
      fail("Storage creation failed");
    }
    assertEquals(storageClientProvider.cache.size(), 1);
  }

  private static final String EXTERNAL_ACCOUNT_CONFIG_JSON =
      "{"
          + "\"type\": \"external_account\","
          + "\"audience\": \"//iam.googleapis.com/projects/test/locations/global/workloadIdentityPools/test-pool/providers/tester\","
          + "\"subject_token_type\": \"urn:ietf:params:oauth:token-type:jwt\","
          + "\"service_account_impersonation_url\": \"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/tester@test.com:generateAccessToken\","
          + "\"token_url\": \"https://sts.googleapis.com/v1/token\","
          + "\"credential_source\": {"
          + "  \"url\": \"http://localhost:5000/token\","
          + "  \"format\": {\"type\": \"json\", \"subject_token_field_name\": \"id_token\"}"
          + "}"
          + "}";

  private static PrivateKey generatePrivateKey() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    return generator.generateKeyPair().getPrivate();
  }

  private static final class MockOAuthTokenTransportFactory implements HttpTransportFactory {
    @Override
    public HttpTransport create() {
      return new MockHttpTransport() {
        @Override
        public LowLevelHttpRequest buildRequest(String method, String url) {
          return new MockLowLevelHttpRequest(url) {
            @Override
            public LowLevelHttpResponse execute() {
              String body =
                  url.contains("iamcredentials.googleapis.com")
                      ? "{\"accessToken\":\"mock-impersonated-token\","
                          + "\"expireTime\":\"2099-01-01T00:00:00Z\"}"
                      : "{\"access_token\":\"mock-access-token\","
                          + "\"expires_in\":3600,\"token_type\":\"Bearer\"}";
              return new MockLowLevelHttpResponse()
                  .setContentType(Json.MEDIA_TYPE)
                  .setContent(body);
            }
          };
        }
      };
    }
  }

  private static final class MockExternalAccountTransportFactory implements HttpTransportFactory {
    @Override
    public HttpTransport create() {
      return new MockHttpTransport() {
        @Override
        public LowLevelHttpRequest buildRequest(String method, String url) {
          return new MockLowLevelHttpRequest(url) {
            @Override
            public LowLevelHttpResponse execute() {
              String body;
              if (url.contains("sts.googleapis.com")) {
                body =
                    "{\"access_token\":\"mock-sts-token\","
                        + "\"issued_token_type\":\"urn:ietf:params:oauth:token-type:access_token\","
                        + "\"token_type\":\"Bearer\",\"expires_in\":3600}";
              } else if (url.contains("iamcredentials.googleapis.com")) {
                body =
                    "{\"accessToken\":\"mock-impersonated-token\","
                        + "\"expireTime\":\"2099-01-01T00:00:00Z\"}";
              } else {
                body = "{\"id_token\":\"mock-subject-token\"}";
              }
              return new MockLowLevelHttpResponse()
                  .setContentType(Json.MEDIA_TYPE)
                  .setContent(body);
            }
          };
        }
      };
    }
  }

  private static ServiceAccountCredentials createServiceAccountCredentials(
      PrivateKey privateKey, String serviceAccountUser) {
    return ServiceAccountCredentials.newBuilder()
        .setClientId("client-id-1")
        .setClientEmail("sa@example.iam.gserviceaccount.com")
        .setPrivateKey(privateKey)
        .setPrivateKeyId("key-id-1")
        .setServiceAccountUser(serviceAccountUser)
        .setProjectId("proj-1")
        .setScopes(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"))
        .setHttpTransportFactory(new MockOAuthTokenTransportFactory())
        .build();
  }

  @Test
  public void getStorage_distinctDownscopedAccessTokenFunctions_missesCache() throws IOException {
    Function<List<AccessBoundary>, String> fn1 = boundaries -> "token-1";
    Function<List<AccessBoundary>, String> fn2 = boundaries -> "token-2";

    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, fn1, TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper storage2 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, fn2, TEST_FEATURE_HEADER_GENERATOR);

    try {
      assertNotEquals(storage1, storage2);
      assertEquals(2, storageClientProvider.cache.size());
      assertEquals(1, getReferences(storage1));
      assertEquals(1, getReferences(storage2));
    } finally {
      storageClientProvider.close(storage1);
      storageClientProvider.close(storage2);
    }
  }

  @Test
  public void getStorage_sameDownscopedAccessTokenFunction_hitsCache() throws IOException {
    Function<List<AccessBoundary>, String> fn = boundaries -> "token-shared";

    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, fn, TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper storage2 =
        storageClientProvider.getStorage(
            TEST_CREDENTIALS, TEST_STORAGE_OPTIONS, null, null, fn, TEST_FEATURE_HEADER_GENERATOR);

    try {
      assertEquals(storage1, storage2);
      assertEquals(1, storageClientProvider.cache.size());
      assertEquals(2, getReferences(storage1));
    } finally {
      storageClientProvider.close(storage1);
      storageClientProvider.close(storage2);
    }
  }

  @Test
  public void getStorage_nullVsNonNullDownscopedAccessTokenFunction_missesCache()
      throws IOException {
    GoogleCredentials credentials =
        GoogleCredentials.create(
            new AccessToken("token-value", Date.from(Instant.now().plusSeconds(3600))));
    Function<List<AccessBoundary>, String> fn = boundaries -> "downscoped-token";

    StorageClientProviderCacheKey keyWithoutDownscoping =
        storageClientProvider.computeCacheKey(credentials, TEST_STORAGE_OPTIONS, null);
    StorageClientProviderCacheKey keyWithDownscoping =
        storageClientProvider.computeCacheKey(credentials, TEST_STORAGE_OPTIONS, fn);
    assertFalse(keyWithoutDownscoping.getIsDownScopingEnabled());
    assertTrue(keyWithDownscoping.getIsDownScopingEnabled());

    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            credentials, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper storage2 =
        storageClientProvider.getStorage(
            credentials, TEST_STORAGE_OPTIONS, null, null, fn, TEST_FEATURE_HEADER_GENERATOR);

    try {
      assertNotEquals(storage1, storage2);
      assertEquals(2, storageClientProvider.cache.size());
    } finally {
      storageClientProvider.close(storage1);
      storageClientProvider.close(storage2);
    }
  }

  @Test
  public void getStorage_distinctBareGoogleCredentials_missesCache() throws IOException {
    Date expiry = Date.from(Instant.now().plusSeconds(3600));
    GoogleCredentials creds1 = GoogleCredentials.create(new AccessToken("token-a", expiry));
    GoogleCredentials creds2 = GoogleCredentials.create(new AccessToken("token-b", expiry));

    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            creds1, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper storage2 =
        storageClientProvider.getStorage(
            creds2, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);

    try {
      assertNotEquals(storage1, storage2);
      assertEquals(2, storageClientProvider.cache.size());
    } finally {
      storageClientProvider.close(storage1);
      storageClientProvider.close(storage2);
    }
  }

  @Test
  public void getStorage_distinctUserCredentials_missesCache() throws IOException {
    HttpTransportFactory transportFactory = new MockOAuthTokenTransportFactory();
    UserCredentials creds1 =
        UserCredentials.newBuilder()
            .setClientId("client-id")
            .setClientSecret("client-secret")
            .setRefreshToken("refresh-token-1")
            .setHttpTransportFactory(transportFactory)
            .build();
    UserCredentials creds2 =
        UserCredentials.newBuilder()
            .setClientId("client-id")
            .setClientSecret("client-secret")
            .setRefreshToken("refresh-token-1")
            .setHttpTransportFactory(transportFactory)
            .build();

    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            creds1, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper storage2 =
        storageClientProvider.getStorage(
            creds2, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);

    try {
      assertNotEquals(storage1, storage2);
      assertEquals(2, storageClientProvider.cache.size());
    } finally {
      storageClientProvider.close(storage1);
      storageClientProvider.close(storage2);
    }
  }

  @Test
  public void getStorage_distinctExternalAccountCredentials_missesCache() throws IOException {
    HttpTransportFactory transportFactory = new MockExternalAccountTransportFactory();
    ExternalAccountCredentials creds1 =
        ExternalAccountCredentials.fromStream(
            new ByteArrayInputStream(EXTERNAL_ACCOUNT_CONFIG_JSON.getBytes(UTF_8)),
            transportFactory);
    ExternalAccountCredentials creds2 =
        ExternalAccountCredentials.fromStream(
            new ByteArrayInputStream(EXTERNAL_ACCOUNT_CONFIG_JSON.getBytes(UTF_8)),
            transportFactory);

    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            creds1, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper storage2 =
        storageClientProvider.getStorage(
            creds2, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);

    try {
      assertNotEquals(storage1, storage2);
      assertEquals(2, storageClientProvider.cache.size());
    } finally {
      storageClientProvider.close(storage1);
      storageClientProvider.close(storage2);
    }
  }

  @Test
  public void getStorage_serviceAccountCredentialsWithDifferentUser_missesCache() throws Exception {
    PrivateKey privateKey = generatePrivateKey();
    ServiceAccountCredentials creds1 =
        createServiceAccountCredentials(privateKey, "alice@example.com");
    ServiceAccountCredentials creds2 =
        createServiceAccountCredentials(privateKey, "bob@example.com");

    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            creds1, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper storage2 =
        storageClientProvider.getStorage(
            creds2, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);

    try {
      assertNotEquals(storage1, storage2);
      assertEquals(2, storageClientProvider.cache.size());
    } finally {
      storageClientProvider.close(storage1);
      storageClientProvider.close(storage2);
    }
  }

  @Test
  public void getStorage_identicalServiceAccountCredentials_hitsCache() throws Exception {
    PrivateKey privateKey = generatePrivateKey();
    ServiceAccountCredentials creds1 =
        createServiceAccountCredentials(privateKey, "alice@example.com");
    ServiceAccountCredentials creds2 =
        createServiceAccountCredentials(privateKey, "alice@example.com");

    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            creds1, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper storage2 =
        storageClientProvider.getStorage(
            creds2, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);

    try {
      assertEquals(storage1, storage2);
      assertEquals(1, storageClientProvider.cache.size());
      assertEquals(2, getReferences(storage1));
    } finally {
      storageClientProvider.close(storage1);
      storageClientProvider.close(storage2);
    }
  }

  @Test
  public void close_userCredentialsAfterTokenRefresh_evictsCacheEntry() throws IOException {
    TokenResponse initialToken =
        new TokenResponse().setAccessToken("initial-token-value").setExpiresInSeconds(300L);
    TokenResponse refreshedToken =
        new TokenResponse().setAccessToken("refreshed-token-value").setExpiresInSeconds(300L);
    MockHttpTransport transport =
        mockTransport(jsonDataResponse(initialToken), jsonDataResponse(refreshedToken));

    UserCredentials userCredentials =
        UserCredentials.newBuilder()
            .setClientId("client-id")
            .setClientSecret("client-secret")
            .setRefreshToken("refresh-token")
            .setTokenServerUri(URI.create("http://localhost/token"))
            .setHttpTransportFactory(() -> transport)
            .build();

    StorageClientWrapper storage =
        storageClientProvider.getStorage(
            userCredentials, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);
    assertEquals(1, storageClientProvider.cache.size());

    int rawHashBeforeRefresh = userCredentials.hashCode();
    userCredentials.refresh();
    assertNotEquals(rawHashBeforeRefresh, userCredentials.hashCode());

    storageClientProvider.close(storage);

    assertEquals(0, storageClientProvider.cache.size());
    assertEquals(0, getReferences(storage));
    assertFalse(storageClientProvider.storageToCacheKeyMap.containsKey(storage));
    assertFalse(storageClientProvider.storageClientToReferenceMap.containsKey(storage));
  }

  @Test
  public void cacheKeyToString_hidesCredentialTokensAndHeaderValues() throws IOException {
    TokenResponse tokenResponse =
        new TokenResponse().setAccessToken("secret-access-token-123").setExpiresInSeconds(300L);
    MockHttpTransport transport = mockTransport(jsonDataResponse(tokenResponse));
    UserCredentials userCredentials =
        UserCredentials.newBuilder()
            .setClientId("client-id")
            .setClientSecret("secret-client-secret-123")
            .setRefreshToken("secret-refresh-token-123")
            .setTokenServerUri(URI.create("http://localhost/token"))
            .setHttpTransportFactory(() -> transport)
            .build();
    userCredentials.refresh();

    GoogleCloudStorageOptions optionsWithSecretHeaders =
        TEST_STORAGE_OPTIONS.toBuilder()
            .setHttpRequestHeaders(
                ImmutableMap.of(
                    "Authorization",
                    "Bearer secret-header-token-value",
                    "x-custom-header",
                    "secret-custom-header-value"))
            .build();

    Function<List<AccessBoundary>, String> fnWithSecretToString =
        new Function<List<AccessBoundary>, String>() {
          @Override
          public String apply(List<AccessBoundary> accessBoundaries) {
            return "downscoped-token";
          }

          @Override
          public String toString() {
            return "secret-function-state-value";
          }
        };

    StorageClientProviderCacheKey key =
        storageClientProvider.computeCacheKey(
            userCredentials, optionsWithSecretHeaders, fnWithSecretToString);

    String rendered = key.toString();
    assertThat(rendered).contains("UserCredentials");
    assertThat(rendered).contains("Authorization");
    assertThat(rendered).contains("x-custom-header");
    assertThat(rendered).doesNotContain("secret-access-token-123");
    assertThat(rendered).doesNotContain("secret-client-secret-123");
    assertThat(rendered).doesNotContain("secret-refresh-token-123");
    assertThat(rendered).doesNotContain("secret-header-token-value");
    assertThat(rendered).doesNotContain("secret-custom-header-value");
    assertThat(rendered).doesNotContain("secret-function-state-value");
  }

  @Test
  public void getStorage_impersonatedCredentialsOverUnsafeSources_missesCache() throws IOException {
    HttpTransportFactory oauthTransportFactory = new MockOAuthTokenTransportFactory();
    UserCredentials userSource =
        UserCredentials.newBuilder()
            .setClientId("client-id")
            .setClientSecret("client-secret")
            .setRefreshToken("refresh-token")
            .setHttpTransportFactory(oauthTransportFactory)
            .build();
    ImpersonatedCredentials userImp1 =
        ImpersonatedCredentials.newBuilder()
            .setSourceCredentials(userSource)
            .setTargetPrincipal("target@example.iam.gserviceaccount.com")
            .setScopes(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"))
            .setHttpTransportFactory(oauthTransportFactory)
            .build();
    ImpersonatedCredentials userImp2 =
        ImpersonatedCredentials.newBuilder()
            .setSourceCredentials(userSource)
            .setTargetPrincipal("target@example.iam.gserviceaccount.com")
            .setScopes(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"))
            .setHttpTransportFactory(oauthTransportFactory)
            .build();

    HttpTransportFactory externalTransportFactory = new MockExternalAccountTransportFactory();
    ExternalAccountCredentials externalSource =
        ExternalAccountCredentials.fromStream(
            new ByteArrayInputStream(EXTERNAL_ACCOUNT_CONFIG_JSON.getBytes(UTF_8)),
            externalTransportFactory);
    ImpersonatedCredentials externalImp1 =
        ImpersonatedCredentials.newBuilder()
            .setSourceCredentials(externalSource)
            .setTargetPrincipal("target@example.iam.gserviceaccount.com")
            .setScopes(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"))
            .setHttpTransportFactory(externalTransportFactory)
            .build();
    ImpersonatedCredentials externalImp2 =
        ImpersonatedCredentials.newBuilder()
            .setSourceCredentials(externalSource)
            .setTargetPrincipal("target@example.iam.gserviceaccount.com")
            .setScopes(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"))
            .setHttpTransportFactory(externalTransportFactory)
            .build();

    StorageClientWrapper userStorage1 =
        storageClientProvider.getStorage(
            userImp1, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper userStorage2 =
        storageClientProvider.getStorage(
            userImp2, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper externalStorage1 =
        storageClientProvider.getStorage(
            externalImp1, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper externalStorage2 =
        storageClientProvider.getStorage(
            externalImp2, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);

    try {
      assertNotEquals(userStorage1, userStorage2);
      assertNotEquals(externalStorage1, externalStorage2);
      assertEquals(4, storageClientProvider.cache.size());
    } finally {
      storageClientProvider.close(userStorage1);
      storageClientProvider.close(userStorage2);
      storageClientProvider.close(externalStorage1);
      storageClientProvider.close(externalStorage2);
    }
    assertEquals(0, storageClientProvider.cache.size());
  }

  @Test
  public void getStorage_impersonatedCredentialsWithDifferentSourceUser_missesCache()
      throws Exception {
    PrivateKey privateKey = generatePrivateKey();
    HttpTransportFactory transportFactory = new MockOAuthTokenTransportFactory();
    ServiceAccountCredentials sa1 =
        createServiceAccountCredentials(privateKey, "alice@example.com");
    ServiceAccountCredentials sa2 = createServiceAccountCredentials(privateKey, "bob@example.com");

    ImpersonatedCredentials imp1 =
        ImpersonatedCredentials.newBuilder()
            .setSourceCredentials(sa1)
            .setTargetPrincipal("target@example.iam.gserviceaccount.com")
            .setScopes(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"))
            .setHttpTransportFactory(transportFactory)
            .build();
    ImpersonatedCredentials imp2 =
        ImpersonatedCredentials.newBuilder()
            .setSourceCredentials(sa2)
            .setTargetPrincipal("target@example.iam.gserviceaccount.com")
            .setScopes(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"))
            .setHttpTransportFactory(transportFactory)
            .build();

    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            imp1, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);
    StorageClientWrapper storage2 =
        storageClientProvider.getStorage(
            imp2, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);

    try {
      assertNotEquals(storage1, storage2);
      assertEquals(2, storageClientProvider.cache.size());
    } finally {
      storageClientProvider.close(storage1);
      storageClientProvider.close(storage2);
    }
    assertEquals(0, storageClientProvider.cache.size());
  }

  @Test
  public void getStorage_impersonatedCredentialsAfterRefresh_hitsCacheAndCloseEvicts()
      throws Exception {
    PrivateKey privateKey = generatePrivateKey();
    HttpTransportFactory transportFactory = new MockOAuthTokenTransportFactory();
    ServiceAccountCredentials unscopedSa =
        ServiceAccountCredentials.newBuilder()
            .setClientId("client-id-1")
            .setClientEmail("sa@example.iam.gserviceaccount.com")
            .setPrivateKey(privateKey)
            .setPrivateKeyId("key-id-1")
            .setProjectId("proj-1")
            .setHttpTransportFactory(transportFactory)
            .build();
    ImpersonatedCredentials imp =
        ImpersonatedCredentials.newBuilder()
            .setSourceCredentials(unscopedSa)
            .setTargetPrincipal("target@example.iam.gserviceaccount.com")
            .setScopes(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"))
            .setHttpTransportFactory(transportFactory)
            .build();

    int rawHashBeforeGetStorage = imp.hashCode();

    StorageClientWrapper storage1 =
        storageClientProvider.getStorage(
            imp, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);
    assertEquals(1, storageClientProvider.cache.size());
    assertEquals(1, getReferences(storage1));

    imp.refresh();
    assertNotEquals(rawHashBeforeGetStorage, imp.hashCode());

    StorageClientWrapper storage2 =
        storageClientProvider.getStorage(
            imp, TEST_STORAGE_OPTIONS, null, null, null, TEST_FEATURE_HEADER_GENERATOR);
    assertEquals(storage1, storage2);
    assertEquals(1, storageClientProvider.cache.size());
    assertEquals(2, getReferences(storage1));

    storageClientProvider.close(storage1);
    assertEquals(1, storageClientProvider.cache.size());
    assertEquals(1, getReferences(storage1));

    storageClientProvider.close(storage2);
    assertEquals(0, storageClientProvider.cache.size());
    assertEquals(0, getReferences(storage1));
    assertFalse(storageClientProvider.storageToCacheKeyMap.containsKey(storage1));
    assertFalse(storageClientProvider.storageClientToReferenceMap.containsKey(storage1));
  }

  @Test
  public void cacheKey_wrappedNullDownscopedAccessTokenFn_normalizedToNull() {
    StorageClientProviderCacheKey keyWithNull =
        StorageClientProviderCacheKey.builder()
            .setIsTracingEnabled(false)
            .setIsDirectPathPreferred(true)
            .setDownscopedAccessTokenFn(null)
            .build();
    StorageClientProviderCacheKey keyWithWrappedNull =
        StorageClientProviderCacheKey.builder()
            .setIsTracingEnabled(false)
            .setIsDirectPathPreferred(true)
            .setDownscopedAccessTokenFn(Equivalence.identity().wrap(null))
            .build();
    StorageClientProviderCacheKey keyWithUnsetFn =
        StorageClientProviderCacheKey.builder()
            .setIsTracingEnabled(false)
            .setIsDirectPathPreferred(true)
            .build();

    assertNull(keyWithWrappedNull.getDownscopedAccessTokenFn());
    assertFalse(keyWithWrappedNull.getIsDownScopingEnabled());
    new EqualsTester()
        .addEqualityGroup(keyWithNull, keyWithWrappedNull, keyWithUnsetFn)
        .testEquals();

    String rendered = keyWithWrappedNull.toString();
    assertThat(rendered).contains("isDownScopingEnabled=false");
    assertThat(rendered).contains("downscopedAccessTokenFnIdentity=null");
    assertThat(rendered).doesNotContain("downscopedAccessTokenFnIdentity=0");
  }
}
