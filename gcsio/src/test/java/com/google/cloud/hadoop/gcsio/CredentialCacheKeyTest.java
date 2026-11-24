/*
 * Copyright 2026 Google LLC
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
import static org.mockito.Mockito.mock;

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
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdentityPoolCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CredentialCacheKeyTest {

  private static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";
  private static final String DEVSTORAGE_READ_ONLY_SCOPE =
      "https://www.googleapis.com/auth/devstorage.read_only";

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

  private static final HttpTransportFactory MOCK_AUTH_TRANSPORT_FACTORY =
      new MockAuthHttpTransportFactory();

  private static PrivateKey testPrivateKey;

  @BeforeClass
  public static void setUpKeys() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    testPrivateKey = generator.generateKeyPair().getPrivate();
  }

  private static final class MockAuthHttpTransportFactory implements HttpTransportFactory {
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
                      : "{\"access_token\":\"mock-oauth-token\","
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

  private static ServiceAccountCredentials newServiceAccountCredentials(
      String clientEmail, String serviceAccountUser, String projectId) {
    return newServiceAccountCredentials(
        clientEmail, serviceAccountUser, projectId, ImmutableList.of(CLOUD_PLATFORM_SCOPE), null);
  }

  private static ServiceAccountCredentials newServiceAccountCredentials(
      String clientEmail,
      @Nullable String serviceAccountUser,
      @Nullable String projectId,
      @Nullable Collection<String> scopes,
      @Nullable Collection<String> defaultScopes) {
    ServiceAccountCredentials.Builder builder =
        ServiceAccountCredentials.newBuilder()
            .setClientId("client-id-1")
            .setClientEmail(clientEmail)
            .setPrivateKey(testPrivateKey)
            .setPrivateKeyId("key-id-1")
            .setServiceAccountUser(serviceAccountUser)
            .setProjectId(projectId)
            .setHttpTransportFactory(MOCK_AUTH_TRANSPORT_FACTORY);
    if (defaultScopes != null) {
      builder.setScopes(scopes, defaultScopes);
    } else if (scopes != null) {
      builder.setScopes(scopes);
    }
    return builder.build();
  }

  private static ImpersonatedCredentials newImpersonatedCredentials(
      GoogleCredentials source, String targetPrincipal) {
    return ImpersonatedCredentials.newBuilder()
        .setSourceCredentials(source)
        .setTargetPrincipal(targetPrincipal)
        .setScopes(ImmutableList.of(CLOUD_PLATFORM_SCOPE))
        .setHttpTransportFactory(MOCK_AUTH_TRANSPORT_FACTORY)
        .build();
  }

  private static ExternalAccountCredentials newExternalAccountCredentials() throws IOException {
    return ExternalAccountCredentials.fromStream(
        new ByteArrayInputStream(EXTERNAL_ACCOUNT_CONFIG_JSON.getBytes(UTF_8)));
  }

  @Test
  public void of_null_returnsNull() {
    assertThat(CredentialCacheKey.of(null)).isNull();
    assertThat(CredentialCacheKey.isValueEqualitySafe(null)).isFalse();
  }

  @Test
  public void serviceAccountCredentials_equalInstancesMatchWithSameHashCode() {
    ServiceAccountCredentials creds1 =
        newServiceAccountCredentials(
            "sa@example.iam.gserviceaccount.com", "alice@example.com", "proj-1");
    ServiceAccountCredentials creds2 =
        newServiceAccountCredentials(
            "sa@example.iam.gserviceaccount.com", "alice@example.com", "proj-1");

    CredentialCacheKey key1 = CredentialCacheKey.of(creds1);
    CredentialCacheKey key2 = CredentialCacheKey.of(creds2);

    assertThat(CredentialCacheKey.isValueEqualitySafe(creds1)).isTrue();
    new EqualsTester().addEqualityGroup(key1, key2).testEquals();
  }

  @Test
  public void serviceAccountCredentials_stableEqualityAndHashCodeAcrossRefresh()
      throws IOException {
    ServiceAccountCredentials creds1 =
        newServiceAccountCredentials(
            "sa@example.iam.gserviceaccount.com", "alice@example.com", "proj-1");
    ServiceAccountCredentials creds2 =
        newServiceAccountCredentials(
            "sa@example.iam.gserviceaccount.com", "alice@example.com", "proj-1");

    CredentialCacheKey keyBefore = CredentialCacheKey.of(creds1);
    CredentialCacheKey keyTwin = CredentialCacheKey.of(creds2);

    creds1.refresh();

    CredentialCacheKey keyAfter = CredentialCacheKey.of(creds1);
    new EqualsTester().addEqualityGroup(keyBefore, keyAfter, keyTwin).testEquals();
  }

  @Test
  public void serviceAccountCredentials_differentServiceAccountUserAreNotEqual() {
    ServiceAccountCredentials creds1 =
        newServiceAccountCredentials(
            "sa@example.iam.gserviceaccount.com", "alice@example.com", "proj-1");
    ServiceAccountCredentials creds2 =
        newServiceAccountCredentials(
            "sa@example.iam.gserviceaccount.com", "bob@example.com", "proj-1");

    // Sanity check: upstream ServiceAccountCredentials.equals ignores serviceAccountUser.
    assertThat(creds1).isEqualTo(creds2);

    CredentialCacheKey key1 = CredentialCacheKey.of(creds1);
    CredentialCacheKey key2 = CredentialCacheKey.of(creds2);

    assertThat(key1).isNotEqualTo(key2);
    assertThat(key2).isNotEqualTo(key1);
  }

  @Test
  public void serviceAccountCredentials_differentProjectIdAreNotEqual() {
    ServiceAccountCredentials creds1 =
        newServiceAccountCredentials(
            "sa@example.iam.gserviceaccount.com", "alice@example.com", "proj-1");
    ServiceAccountCredentials creds2 =
        newServiceAccountCredentials(
            "sa@example.iam.gserviceaccount.com", "alice@example.com", "proj-2");

    // Sanity check: upstream ServiceAccountCredentials.equals ignores projectId.
    assertThat(creds1).isEqualTo(creds2);

    CredentialCacheKey key1 = CredentialCacheKey.of(creds1);
    CredentialCacheKey key2 = CredentialCacheKey.of(creds2);

    assertThat(key1).isNotEqualTo(key2);
    assertThat(key2).isNotEqualTo(key1);
  }

  @Test
  public void serviceAccountCredentials_differentClientEmailAreNotEqual() {
    ServiceAccountCredentials creds1 =
        newServiceAccountCredentials("sa1@example.iam.gserviceaccount.com", null, "proj-1");
    ServiceAccountCredentials creds2 =
        newServiceAccountCredentials("sa2@example.iam.gserviceaccount.com", null, "proj-1");

    assertThat(CredentialCacheKey.of(creds1)).isNotEqualTo(CredentialCacheKey.of(creds2));
  }

  @Test
  public void impersonatedCredentials_overServiceAccountCredentials_usesReferenceIdentity() {
    ServiceAccountCredentials sa1 =
        newServiceAccountCredentials(
            "sa@example.iam.gserviceaccount.com", "alice@example.com", "proj-1");
    ServiceAccountCredentials sa2 =
        newServiceAccountCredentials(
            "sa@example.iam.gserviceaccount.com", "alice@example.com", "proj-1");
    ServiceAccountCredentials saDifferentUser =
        newServiceAccountCredentials(
            "sa@example.iam.gserviceaccount.com", "bob@example.com", "proj-1");
    ServiceAccountCredentials saDifferentProject =
        newServiceAccountCredentials(
            "sa@example.iam.gserviceaccount.com", "alice@example.com", "proj-2");

    ImpersonatedCredentials imp1 =
        newImpersonatedCredentials(sa1, "target@example.iam.gserviceaccount.com");
    ImpersonatedCredentials imp2 =
        newImpersonatedCredentials(sa2, "target@example.iam.gserviceaccount.com");
    ImpersonatedCredentials impDifferentUser =
        newImpersonatedCredentials(saDifferentUser, "target@example.iam.gserviceaccount.com");
    ImpersonatedCredentials impDifferentProject =
        newImpersonatedCredentials(saDifferentProject, "target@example.iam.gserviceaccount.com");
    ImpersonatedCredentials impDifferentTarget =
        newImpersonatedCredentials(sa1, "target-2@example.iam.gserviceaccount.com");

    assertThat(CredentialCacheKey.isValueEqualitySafe(imp1)).isFalse();
    new EqualsTester()
        .addEqualityGroup(CredentialCacheKey.of(imp1), CredentialCacheKey.of(imp1))
        .addEqualityGroup(CredentialCacheKey.of(imp2))
        .addEqualityGroup(CredentialCacheKey.of(impDifferentUser))
        .addEqualityGroup(CredentialCacheKey.of(impDifferentProject))
        .addEqualityGroup(CredentialCacheKey.of(impDifferentTarget))
        .testEquals();
  }

  @Test
  public void impersonatedCredentials_refreshOverUnscopedServiceAccount_preservesContract()
      throws IOException {
    ServiceAccountCredentials unscopedSa =
        newServiceAccountCredentials(
            "sa@example.iam.gserviceaccount.com", "alice@example.com", "proj-1", null, null);
    ServiceAccountCredentials unscopedSaTwin =
        newServiceAccountCredentials(
            "sa@example.iam.gserviceaccount.com", "alice@example.com", "proj-1", null, null);
    ServiceAccountCredentials scopedSa =
        (ServiceAccountCredentials) unscopedSa.createScoped(ImmutableList.of(CLOUD_PLATFORM_SCOPE));

    ImpersonatedCredentials imp =
        newImpersonatedCredentials(unscopedSa, "target@example.iam.gserviceaccount.com");
    ImpersonatedCredentials impTwin =
        newImpersonatedCredentials(unscopedSaTwin, "target@example.iam.gserviceaccount.com");
    ImpersonatedCredentials impScoped =
        newImpersonatedCredentials(scopedSa, "target@example.iam.gserviceaccount.com");

    CredentialCacheKey keyBefore = CredentialCacheKey.of(imp);
    CredentialCacheKey keyTwin = CredentialCacheKey.of(impTwin);
    CredentialCacheKey keyScoped = CredentialCacheKey.of(impScoped);
    int rawHashBefore = imp.hashCode();
    int keyHashBefore = keyBefore.hashCode();

    imp.refresh();

    // Upstream ImpersonatedCredentials mutates its volatile sourceCredentials field during refresh.
    assertThat(imp.hashCode()).isNotEqualTo(rawHashBefore);

    CredentialCacheKey keyAfter = CredentialCacheKey.of(imp);
    assertThat(keyBefore.hashCode()).isEqualTo(keyHashBefore);
    assertThat(keyAfter.hashCode()).isEqualTo(keyHashBefore);
    new EqualsTester()
        .addEqualityGroup(keyBefore, keyAfter)
        .addEqualityGroup(keyTwin)
        .addEqualityGroup(keyScoped)
        .testEquals();
  }

  @Test
  public void impersonatedCredentials_refreshOverDevstorageAndDefaultScopes_preservesContract()
      throws IOException {
    List<ServiceAccountCredentials> sources =
        ImmutableList.of(
            newServiceAccountCredentials(
                "sa@example.iam.gserviceaccount.com",
                null,
                "proj-1",
                ImmutableList.of(DEVSTORAGE_READ_ONLY_SCOPE),
                null),
            newServiceAccountCredentials(
                "sa@example.iam.gserviceaccount.com",
                null,
                "proj-1",
                null,
                ImmutableList.of(DEVSTORAGE_READ_ONLY_SCOPE)));
    List<ServiceAccountCredentials> twinSources =
        ImmutableList.of(
            newServiceAccountCredentials(
                "sa@example.iam.gserviceaccount.com",
                null,
                "proj-1",
                ImmutableList.of(DEVSTORAGE_READ_ONLY_SCOPE),
                null),
            newServiceAccountCredentials(
                "sa@example.iam.gserviceaccount.com",
                null,
                "proj-1",
                null,
                ImmutableList.of(DEVSTORAGE_READ_ONLY_SCOPE)));

    for (int i = 0; i < sources.size(); i++) {
      ImpersonatedCredentials imp =
          newImpersonatedCredentials(sources.get(i), "target@example.iam.gserviceaccount.com");
      ImpersonatedCredentials impTwin =
          newImpersonatedCredentials(twinSources.get(i), "target@example.iam.gserviceaccount.com");

      CredentialCacheKey keyBefore = CredentialCacheKey.of(imp);
      CredentialCacheKey keyTwin = CredentialCacheKey.of(impTwin);
      int rawHashBefore = imp.hashCode();
      int keyHashBefore = keyBefore.hashCode();

      imp.refresh();

      assertThat(imp.hashCode()).isNotEqualTo(rawHashBefore);
      CredentialCacheKey keyAfter = CredentialCacheKey.of(imp);
      assertThat(keyBefore.hashCode()).isEqualTo(keyHashBefore);
      assertThat(keyAfter.hashCode()).isEqualTo(keyHashBefore);
      new EqualsTester()
          .addEqualityGroup(keyBefore, keyAfter)
          .addEqualityGroup(keyTwin)
          .testEquals();
    }
  }

  @Test
  public void impersonatedCredentials_mutatedDelegatesList_preservesHashCodeAndEquality() {
    ServiceAccountCredentials sa =
        newServiceAccountCredentials("sa@example.iam.gserviceaccount.com", null, "proj-1");
    List<String> mutableDelegates =
        new ArrayList<>(ImmutableList.of("delegate-1@example.iam.gserviceaccount.com"));

    ImpersonatedCredentials imp =
        ImpersonatedCredentials.newBuilder()
            .setSourceCredentials(sa)
            .setTargetPrincipal("target@example.iam.gserviceaccount.com")
            .setDelegates(mutableDelegates)
            .setScopes(ImmutableList.of(CLOUD_PLATFORM_SCOPE))
            .setHttpTransportFactory(MOCK_AUTH_TRANSPORT_FACTORY)
            .build();

    CredentialCacheKey keyBefore = CredentialCacheKey.of(imp);
    int rawHashBefore = imp.hashCode();
    int keyHashBefore = keyBefore.hashCode();

    mutableDelegates.add("delegate-2@example.iam.gserviceaccount.com");

    assertThat(imp.hashCode()).isNotEqualTo(rawHashBefore);
    CredentialCacheKey keyAfter = CredentialCacheKey.of(imp);
    assertThat(keyBefore.hashCode()).isEqualTo(keyHashBefore);
    assertThat(keyAfter.hashCode()).isEqualTo(keyHashBefore);
    new EqualsTester().addEqualityGroup(keyBefore, keyAfter).testEquals();
  }

  @Test
  public void impersonatedCredentials_overUnsafeSources_usesReferenceIdentity() throws IOException {
    UserCredentials userCreds =
        UserCredentials.newBuilder()
            .setClientId("client-id")
            .setClientSecret("client-secret")
            .setRefreshToken("refresh-token")
            .build();
    ExternalAccountCredentials externalCreds = newExternalAccountCredentials();
    GoogleCredentials bareCreds =
        GoogleCredentials.create(
            new AccessToken("bare-token", Date.from(Instant.now().plusSeconds(3600))));

    for (GoogleCredentials source : ImmutableList.of(userCreds, externalCreds, bareCreds)) {
      ImpersonatedCredentials imp1 =
          newImpersonatedCredentials(source, "target@example.iam.gserviceaccount.com");
      ImpersonatedCredentials imp2 =
          newImpersonatedCredentials(source, "target@example.iam.gserviceaccount.com");

      assertThat(CredentialCacheKey.isValueEqualitySafe(imp1)).isFalse();
      assertThat(CredentialCacheKey.of(imp1)).isEqualTo(CredentialCacheKey.of(imp1));
      assertThat(CredentialCacheKey.of(imp1).hashCode())
          .isEqualTo(CredentialCacheKey.of(imp1).hashCode());
      assertThat(CredentialCacheKey.of(imp1)).isNotEqualTo(CredentialCacheKey.of(imp2));
    }
  }

  @Test
  public void userCredentials_usesReferenceIdentityAndStableHashCodeAcrossRefresh()
      throws IOException {
    TokenResponse tokenResponse =
        new TokenResponse().setAccessToken("refreshed-access-token").setExpiresInSeconds(300L);
    MockHttpTransport transport = mockTransport(jsonDataResponse(tokenResponse));

    UserCredentials creds1 =
        UserCredentials.newBuilder()
            .setClientId("client-id")
            .setClientSecret("client-secret")
            .setRefreshToken("refresh-token")
            .setTokenServerUri(URI.create("http://localhost/token"))
            .setHttpTransportFactory(() -> transport)
            .build();
    UserCredentials creds2 =
        UserCredentials.newBuilder()
            .setClientId("client-id")
            .setClientSecret("client-secret")
            .setRefreshToken("refresh-token")
            .setTokenServerUri(URI.create("http://localhost/token"))
            .setHttpTransportFactory(() -> transport)
            .build();

    CredentialCacheKey keyBeforeRefresh = CredentialCacheKey.of(creds1);
    int rawHashBeforeRefresh = creds1.hashCode();
    int keyHashBeforeRefresh = keyBeforeRefresh.hashCode();

    assertThat(keyBeforeRefresh).isNotEqualTo(CredentialCacheKey.of(creds2));

    creds1.refresh();

    assertThat(creds1.hashCode()).isNotEqualTo(rawHashBeforeRefresh);
    CredentialCacheKey keyAfterRefresh = CredentialCacheKey.of(creds1);
    assertThat(keyBeforeRefresh.hashCode()).isEqualTo(keyHashBeforeRefresh);
    assertThat(keyAfterRefresh.hashCode()).isEqualTo(keyHashBeforeRefresh);
    assertThat(keyBeforeRefresh).isEqualTo(keyAfterRefresh);
  }

  @Test
  public void computeEngineCredentials_usesReferenceIdentity() {
    ComputeEngineCredentials creds1 = ComputeEngineCredentials.create();
    ComputeEngineCredentials creds2 = ComputeEngineCredentials.create();

    assertThat(CredentialCacheKey.isValueEqualitySafe(creds1)).isFalse();
    assertThat(CredentialCacheKey.of(creds1)).isEqualTo(CredentialCacheKey.of(creds1));
    assertThat(CredentialCacheKey.of(creds1).hashCode())
        .isEqualTo(CredentialCacheKey.of(creds1).hashCode());
    assertThat(CredentialCacheKey.of(creds1)).isNotEqualTo(CredentialCacheKey.of(creds2));
  }

  @Test
  public void externalAccountAndIdentityPoolCredentials_usesReferenceIdentity() throws IOException {
    ExternalAccountCredentials creds1 = newExternalAccountCredentials();
    ExternalAccountCredentials creds2 = newExternalAccountCredentials();

    assertThat(creds1).isInstanceOf(IdentityPoolCredentials.class);
    assertThat(CredentialCacheKey.isValueEqualitySafe(creds1)).isFalse();
    assertThat(CredentialCacheKey.of(creds1)).isEqualTo(CredentialCacheKey.of(creds1));
    assertThat(CredentialCacheKey.of(creds1).hashCode())
        .isEqualTo(CredentialCacheKey.of(creds1).hashCode());
    assertThat(CredentialCacheKey.of(creds1)).isNotEqualTo(CredentialCacheKey.of(creds2));
  }

  @Test
  public void bareGoogleCredentialsAndOAuth2Credentials_usesReferenceIdentity() {
    Date expiry = Date.from(Instant.now().plusSeconds(3600));
    GoogleCredentials google1 = GoogleCredentials.create(new AccessToken("token-1", expiry));
    GoogleCredentials google2 = GoogleCredentials.create(new AccessToken("token-2", expiry));
    GoogleCredentials googleSameToken =
        GoogleCredentials.create(new AccessToken("token-1", expiry));

    assertThat(CredentialCacheKey.of(google1)).isEqualTo(CredentialCacheKey.of(google1));
    assertThat(CredentialCacheKey.of(google1)).isNotEqualTo(CredentialCacheKey.of(google2));
    assertThat(CredentialCacheKey.of(google1)).isNotEqualTo(CredentialCacheKey.of(googleSameToken));

    OAuth2Credentials oauth1 = OAuth2Credentials.create(new AccessToken("token-1", expiry));
    OAuth2Credentials oauth2 = OAuth2Credentials.create(new AccessToken("token-1", expiry));
    assertThat(CredentialCacheKey.of(oauth1)).isEqualTo(CredentialCacheKey.of(oauth1));
    assertThat(CredentialCacheKey.of(oauth1)).isNotEqualTo(CredentialCacheKey.of(oauth2));
  }

  @Test
  public void customSubclassesAndMocks_useReferenceIdentity() {
    Date expiry = Date.from(Instant.now().plusSeconds(3600));
    GoogleCredentials custom1 = new CustomGoogleCredentials(new AccessToken("token-a", expiry));
    GoogleCredentials custom2 = new CustomGoogleCredentials(new AccessToken("token-b", expiry));

    assertThat(CredentialCacheKey.isValueEqualitySafe(custom1)).isFalse();
    assertThat(CredentialCacheKey.of(custom1)).isEqualTo(CredentialCacheKey.of(custom1));
    assertThat(CredentialCacheKey.of(custom1)).isNotEqualTo(CredentialCacheKey.of(custom2));

    Credentials mock1 = mock(Credentials.class);
    Credentials mock2 = mock(Credentials.class);
    assertThat(CredentialCacheKey.of(mock1)).isEqualTo(CredentialCacheKey.of(mock1));
    assertThat(CredentialCacheKey.of(mock1)).isNotEqualTo(CredentialCacheKey.of(mock2));

    ServiceAccountCredentials saMock1 = mock(ServiceAccountCredentials.class);
    ServiceAccountCredentials saMock2 = mock(ServiceAccountCredentials.class);
    assertThat(CredentialCacheKey.isValueEqualitySafe(saMock1)).isFalse();
    assertThat(CredentialCacheKey.of(saMock1)).isNotEqualTo(CredentialCacheKey.of(saMock2));
  }

  @Test
  public void toString_doesNotInvokeCredentialsToStringOrExposeTokens() throws IOException {
    TokenResponse tokenResponse =
        new TokenResponse().setAccessToken("secret-access-token-value").setExpiresInSeconds(300L);
    MockHttpTransport transport = mockTransport(jsonDataResponse(tokenResponse));
    UserCredentials userCreds =
        UserCredentials.newBuilder()
            .setClientId("client-id")
            .setClientSecret("secret-client-secret-value")
            .setRefreshToken("secret-refresh-token-value")
            .setTokenServerUri(URI.create("http://localhost/token"))
            .setHttpTransportFactory(() -> transport)
            .build();
    userCreds.refresh();

    String userKeyString = CredentialCacheKey.of(userCreds).toString();
    assertThat(userKeyString).contains("UserCredentials");
    assertThat(userKeyString).doesNotContain("secret-access-token-value");
    assertThat(userKeyString).doesNotContain("secret-client-secret-value");
    assertThat(userKeyString).doesNotContain("secret-refresh-token-value");

    GoogleCredentials bareCreds =
        GoogleCredentials.create(new AccessToken("secret-bare-token-value", null));
    String bareKeyString = CredentialCacheKey.of(bareCreds).toString();
    assertThat(bareKeyString).contains("GoogleCredentials");
    assertThat(bareKeyString).doesNotContain("secret-bare-token-value");

    Credentials throwingToStringCreds =
        new Credentials() {
          @Override
          public String getAuthenticationType() {
            return "Test";
          }

          @Override
          public Map<String, List<String>> getRequestMetadata(URI uri) {
            return ImmutableMap.of();
          }

          @Override
          public boolean hasRequestMetadata() {
            return false;
          }

          @Override
          public boolean hasRequestMetadataOnly() {
            return false;
          }

          @Override
          public void refresh() {}

          @Override
          public String toString() {
            throw new AssertionError("credentials.toString() must not be invoked");
          }
        };

    String customKeyString = CredentialCacheKey.of(throwingToStringCreds).toString();
    assertThat(customKeyString).startsWith("CredentialCacheKey{");
  }

  private static final class CustomGoogleCredentials extends GoogleCredentials {
    CustomGoogleCredentials(AccessToken token) {
      super(token);
    }
  }
}
