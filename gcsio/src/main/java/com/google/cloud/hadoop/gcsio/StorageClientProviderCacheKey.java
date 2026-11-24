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

import com.google.auto.value.AutoValue;
import com.google.cloud.hadoop.util.AccessBoundary;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.common.base.Equivalence;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nullable;

@AutoValue
public abstract class StorageClientProviderCacheKey {

  public static StorageClientProviderCacheKey.Builder builder() {
    return new AutoValue_StorageClientProviderCacheKey.Builder();
  }

  /** Whether tracing is requested. */
  public abstract boolean getIsTracingEnabled();

  /** Cache key for the GCS client credentials, or null if unauthenticated. */
  @Nullable
  public abstract CredentialCacheKey getCredentialCacheKey();

  /**
   * Identity-wrapped function providing downscoped access tokens for authenticating with the GCS
   * backend, or null when downscoping is disabled.
   */
  @Nullable
  public abstract Equivalence.Wrapper<Function<List<AccessBoundary>, String>>
      getDownscopedAccessTokenFn();

  /** Whether downscoped tokens are used for authenticating with the GCS backend. */
  public boolean getIsDownScopingEnabled() {
    Equivalence.Wrapper<Function<List<AccessBoundary>, String>> downscopedFn =
        getDownscopedAccessTokenFn();
    return downscopedFn != null && downscopedFn.get() != null;
  }

  @Nullable
  public abstract ImmutableMap<String, String> getHttpHeaders();

  @Nullable
  public abstract AsyncWriteChannelOptions getWriteChannelOptions();

  @Nullable
  public abstract String getProjectId();

  public abstract boolean getIsDirectPathPreferred();

  /** Universe domain to target, or null for the default Google universe (googleapis.com). */
  @Nullable
  public abstract String getUniverseDomain();

  public abstract StorageClientProviderCacheKey.Builder toBuilder();

  @Override
  public final String toString() {
    ImmutableMap<String, String> headers = getHttpHeaders();
    Equivalence.Wrapper<Function<List<AccessBoundary>, String>> downscopedFn =
        getDownscopedAccessTokenFn();
    Function<List<AccessBoundary>, String> downscopedFnValue =
        downscopedFn == null ? null : downscopedFn.get();
    return "StorageClientProviderCacheKey{"
        + "isTracingEnabled="
        + getIsTracingEnabled()
        + ", credentialCacheKey="
        + getCredentialCacheKey()
        + ", isDownScopingEnabled="
        + getIsDownScopingEnabled()
        + ", downscopedAccessTokenFnIdentity="
        + (downscopedFnValue == null
            ? "null"
            : Integer.toHexString(System.identityHashCode(downscopedFnValue)))
        + ", httpHeaderKeys="
        + (headers == null ? "null" : headers.keySet())
        + ", writeChannelOptions="
        + getWriteChannelOptions()
        + ", projectId="
        + getProjectId()
        + ", isDirectPathPreferred="
        + getIsDirectPathPreferred()
        + ", universeDomain="
        + getUniverseDomain()
        + "}";
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setIsTracingEnabled(boolean value);

    public abstract Builder setCredentialCacheKey(@Nullable CredentialCacheKey value);

    @Nullable
    abstract Equivalence.Wrapper<Function<List<AccessBoundary>, String>>
        getDownscopedAccessTokenFn();

    public abstract Builder setDownscopedAccessTokenFn(
        @Nullable Equivalence.Wrapper<Function<List<AccessBoundary>, String>> value);

    public abstract Builder setHttpHeaders(@Nullable ImmutableMap<String, String> value);

    public abstract Builder setWriteChannelOptions(@Nullable AsyncWriteChannelOptions value);

    public abstract Builder setProjectId(@Nullable String value);

    public abstract Builder setIsDirectPathPreferred(boolean value);

    public abstract Builder setUniverseDomain(@Nullable String value);

    abstract StorageClientProviderCacheKey autoBuild();

    public StorageClientProviderCacheKey build() {
      Equivalence.Wrapper<Function<List<AccessBoundary>, String>> downscopedFn =
          getDownscopedAccessTokenFn();
      if (downscopedFn != null && downscopedFn.get() == null) {
        setDownscopedAccessTokenFn(null);
      }
      return autoBuild();
    }
  }
}
