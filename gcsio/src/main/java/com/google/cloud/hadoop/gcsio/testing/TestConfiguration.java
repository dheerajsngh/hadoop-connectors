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

package com.google.cloud.hadoop.gcsio.testing;

/** Access to test configurations values. */
public abstract class TestConfiguration {
  public static final String GCS_TEST_PROJECT_ID = "GCS_TEST_PROJECT_ID";
  public static final String GCS_TEST_JSON_KEYFILE = "GCS_TEST_JSON_KEYFILE";

  public static final String GCS_TEST_DIRECT_PATH_PREFERRED = "GCS_TEST_DIRECT_PATH_PREFERRED";

  public static final String GCS_TEST_APPLICATION_DEFAULT_ENABLE =
      "GCS_TEST_APPLICATION_DEFAULT_ENABLE";

  /** Environment-based test configuration. */
  public static class EnvironmentBasedTestConfiguration extends TestConfiguration {
    @Override
    public String getProjectId() {
      return System.getenv(GCS_TEST_PROJECT_ID);
    }

    @Override
    public String getServiceAccountJsonKeyFile() {
      return System.getenv(GCS_TEST_JSON_KEYFILE);
    }

    @Override
    public boolean isApplicationDefaultModeEnabled() {
      String applicationDefaultModeEnable = System.getenv(GCS_TEST_APPLICATION_DEFAULT_ENABLE);
      if (applicationDefaultModeEnable == null) {
        return false;
      }
      return (Boolean.parseBoolean(applicationDefaultModeEnable));
    }

    @Override
    public boolean isDirectPathPreferred() {
      String envVar = System.getenv(GCS_TEST_DIRECT_PATH_PREFERRED);
      // if env variable is not configured default behaviour is to attempt directPath
      if (envVar == null) {
        return true;
      }
      return Boolean.parseBoolean(envVar);
    }
  }

  public static TestConfiguration getInstance() {
    return LazyHolder.INSTANCE;
  }

  private static class LazyHolder {
    private static final TestConfiguration INSTANCE = new EnvironmentBasedTestConfiguration();
  }

  public abstract String getProjectId();

  public abstract String getServiceAccountJsonKeyFile();

  public abstract boolean isApplicationDefaultModeEnabled();

  public abstract boolean isDirectPathPreferred();
}
