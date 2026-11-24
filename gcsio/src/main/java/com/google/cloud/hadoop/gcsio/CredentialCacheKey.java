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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Cache key wrapper for {@link Credentials} that preserves credential isolation and immutable hash
 * codes.
 *
 * <p>Value equality is permitted only for exact {@link ServiceAccountCredentials} instances, whose
 * equality fields are immutable. All other credential types use strict reference identity so cache
 * keys remain isolated and hash codes stay invariant across token refreshes and mutable state
 * updates.
 */
public final class CredentialCacheKey {

  private final Credentials credentials;
  private final boolean valueEqualitySafe;
  private final int cachedHashCode;

  private CredentialCacheKey(Credentials credentials) {
    this.credentials = checkNotNull(credentials, "credentials must not be null");
    this.valueEqualitySafe = isValueEqualitySafe(credentials);
    this.cachedHashCode = computeHashCode();
  }

  /**
   * Returns a {@link CredentialCacheKey} wrapping {@code credentials}, or {@code null} when {@code
   * credentials} is {@code null}.
   */
  @Nullable
  public static CredentialCacheKey of(@Nullable Credentials credentials) {
    return credentials == null ? null : new CredentialCacheKey(credentials);
  }

  @VisibleForTesting
  static boolean isValueEqualitySafe(@Nullable Credentials credentials) {
    return credentials != null && credentials.getClass() == ServiceAccountCredentials.class;
  }

  private int computeHashCode() {
    if (!valueEqualitySafe) {
      return System.identityHashCode(credentials);
    }
    ServiceAccountCredentials sa = (ServiceAccountCredentials) credentials;
    return Objects.hash(sa.hashCode(), sa.getServiceAccountUser(), sa.getProjectId());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof CredentialCacheKey)) {
      return false;
    }
    CredentialCacheKey other = (CredentialCacheKey) obj;
    if (this.credentials == other.credentials) {
      return true;
    }
    if (!this.valueEqualitySafe || !other.valueEqualitySafe) {
      return false;
    }
    ServiceAccountCredentials a = (ServiceAccountCredentials) this.credentials;
    ServiceAccountCredentials b = (ServiceAccountCredentials) other.credentials;
    return a.equals(b)
        && Objects.equals(a.getServiceAccountUser(), b.getServiceAccountUser())
        && Objects.equals(a.getProjectId(), b.getProjectId());
  }

  @Override
  public int hashCode() {
    return cachedHashCode;
  }

  @Override
  public String toString() {
    String typeName = credentials.getClass().getSimpleName();
    if (typeName.isEmpty()) {
      typeName = credentials.getClass().getName();
    }
    return "CredentialCacheKey{type="
        + typeName
        + ", hash="
        + Integer.toHexString(cachedHashCode)
        + ", identity="
        + Integer.toHexString(System.identityHashCode(credentials))
        + "}";
  }
}
