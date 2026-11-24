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

import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.getStandardOptionBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for GoogleCloudStorageClientImpl caching. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageJavaStorageClientCachingIntegrationTest {

  private static final Function<List<AccessBoundary>, String> TEST_DOWNSCOPED_ACCESS_TOKEN_FN =
      ignore -> "testDownscopedAccessToken";

  @Test
  public void reusesCachedStorageClient_cacheEnabled() throws IOException {
    GoogleCloudStorageClientImpl test_gcs1 = createGcsClient(null, true);
    GoogleCloudStorageClientImpl test_gcs2 = createGcsClient(null, true);

    assertEquals(test_gcs1.storageWrapper, test_gcs2.storageWrapper);
  }

  @Test
  public void createsNewClient_cacheEnabled() throws IOException {
    GoogleCloudStorageOptions.Builder testOptionsBuilder =
        getStandardOptionBuilder()
            .setTraceLogEnabled(true)
            .setHttpRequestHeaders((ImmutableMap.of("header-key", "header-value")));
    GoogleCloudStorageClientImpl test_gcs1 = createGcsClient(testOptionsBuilder, true);
    GoogleCloudStorageClientImpl test_gcs2 = createGcsClient(null, true);

    assertNotEquals(test_gcs1.storageWrapper, test_gcs2.storageWrapper);
  }

  @Test
  public void createsNewClient_cacheDisabled() throws IOException {
    GoogleCloudStorageClientImpl test_gcs1 = createGcsClient(null, false);
    GoogleCloudStorageClientImpl test_gcs2 = createGcsClient(null, false);

    assertNotEquals(test_gcs1.storageWrapper, test_gcs2.storageWrapper);
  }

  private GoogleCloudStorageClientImpl createGcsClient(
      @Nullable GoogleCloudStorageOptions.Builder optionsBuilder, boolean enableCache)
      throws IOException {
    if (optionsBuilder == null) {
      optionsBuilder = getStandardOptionBuilder();
    }
    optionsBuilder.setStorageClientCachingEnabled(enableCache);
    return GoogleCloudStorageClientImpl.builder()
        .setOptions(optionsBuilder.build())
        .setCredentials(null)
        .setDownscopedAccessTokenFn(TEST_DOWNSCOPED_ACCESS_TOKEN_FN)
        .build();
  }
}
