/*
 * Copyright 2026 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

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
import com.google.api.gax.rpc.AsyncTaskException;
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
import com.google.auth.oauth2.PluggableAuthCredentialSource;
import com.google.auth.oauth2.PluggableAuthCredentials;
import com.google.auth.oauth2.StsRequestHandler;
import com.google.auth.oauth2.StsTokenExchangeRequest;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.hadoop.fs.gcs.auth.DelegationTokenIOException;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageExceptions;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.storage.AsyncSessionClosedException;
import com.google.cloud.storage.AsyncStorageTaskException;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonParseException;
import com.google.gson.stream.MalformedJsonException;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.net.ConnectException;
import java.net.ProtocolException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.InterruptedByTimeoutException;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GhfsRequestFailures}. */
@RunWith(JUnit4.class)
public class GhfsRequestFailuresTest {

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void isRequestFailure_null_returnsFalse() {
    assertThat(GhfsRequestFailures.isRequestFailure(null)).isFalse();
  }

  @Test
  public void isRequestFailure_semanticNonFailures_returnFalse() {
    assertThat(GhfsRequestFailures.isRequestFailure(new FileNotFoundException("missing")))
        .isFalse();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new java.nio.file.FileAlreadyExistsException("exists")))
        .isFalse();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new org.apache.hadoop.fs.FileAlreadyExistsException("exists")))
        .isFalse();
    assertThat(GhfsRequestFailures.isRequestFailure(new DirectoryNotEmptyException("non-empty")))
        .isFalse();
    assertThat(GhfsRequestFailures.isRequestFailure(new IOException("Rename to self is forbidden")))
        .isFalse();
  }

  @Test
  public void isRequestFailure_accessDeniedException_returnsTrue() {
    AccessDeniedException accessDenied = new AccessDeniedException("gs://bucket/file");
    assertThat(GhfsRequestFailures.isRequestFailure(accessDenied)).isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new IOException("wrapped", accessDenied)))
        .isTrue();
  }

  @Test
  public void isRequestFailure_interruptsAndCancellations_returnTrue() {
    assertThat(GhfsRequestFailures.isRequestFailure(new InterruptedIOException("interrupted")))
        .isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new InterruptedException("interrupted")))
        .isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new ClosedByInterruptException())).isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new InterruptedByTimeoutException())).isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new CancellationException("cancelled")))
        .isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new TimeoutException("LRO timed out")))
        .isTrue();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new IOException(
                    "LRO failed", new ExecutionException(new TimeoutException("LRO timed out")))))
        .isTrue();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new StorageException(0, "LRO timed out", new TimeoutException("LRO timed out"))))
        .isTrue();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new IOException("wrapped", new InterruptedException("interrupted"))))
        .isTrue();
  }

  @Test
  public void isRequestFailure_socketAndSslErrors_returnTrue() {
    assertThat(GhfsRequestFailures.isRequestFailure(new SocketTimeoutException("Read timed out")))
        .isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new ConnectException("Connection refused")))
        .isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new SocketException("Socket closed"))).isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new SSLException("TLS handshake failed")))
        .isTrue();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new SSLException("SSL connection reset", new EOFException("eof"))))
        .isTrue();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new IOException("wrapped", new SocketTimeoutException("Connect timed out"))))
        .isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new EOFException("Unexpected end of stream")))
        .isTrue();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new IOException("wrapped", new EOFException("Unexpected end of stream"))))
        .isTrue();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new UnknownHostException("storage.googleapis.com")))
        .isTrue();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new IOException("wrapped", new UnknownHostException("storage.googleapis.com"))))
        .isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new ClosedChannelException())).isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new AsynchronousCloseException())).isTrue();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new IOException("wrapped", new ClosedChannelException())))
        .isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new ProtocolException("Unexpected status")))
        .isTrue();
    IOException retryableAuthEx = new RetryableAuthIOException(/* retryable= */ true);
    assertThat(GhfsRequestFailures.isRequestFailure(retryableAuthEx)).isTrue();
    IOException nonRetryableAuthEx = new RetryableAuthIOException(/* retryable= */ false);
    assertThat(GhfsRequestFailures.isRequestFailure(nonRetryableAuthEx)).isTrue();
  }

  @Test
  public void isRequestFailure_storageExceptionStatusCodes_classifiedCorrectly() {
    for (int failureCode : new int[] {400, 401, 403, 408, 429, 500, 502, 503, 504}) {
      StorageException se = new StorageException(failureCode, "failure " + failureCode);
      assertThat(GhfsRequestFailures.isRequestFailure(se)).isTrue();
      assertThat(GhfsRequestFailures.isRequestFailure(new IOException("wrapped", se))).isTrue();
    }

    for (int nonFailureCode : new int[] {404, 409, 412}) {
      StorageException se = new StorageException(nonFailureCode, "semantic " + nonFailureCode);
      assertThat(GhfsRequestFailures.isRequestFailure(se)).isFalse();
      assertThat(GhfsRequestFailures.isRequestFailure(new IOException("wrapped", se))).isFalse();
    }

    // StorageException(0) wrapping SocketTimeoutException continues traversal to cause -> true
    StorageException transportWrapped =
        new StorageException(0, "Read timed out", new SocketTimeoutException("Read timed out"));
    assertThat(GhfsRequestFailures.isRequestFailure(transportWrapped)).isTrue();

    // StorageException.translate(IOException) code-0 retryable reasons -> true
    StorageException prematureClose =
        StorageException.translate(new IOException("Connection closed prematurely"));
    assertThat(prematureClose.getCode()).isEqualTo(0);
    assertThat(prematureClose.getReason()).isEqualTo("connectionClosedPrematurely");
    assertThat(GhfsRequestFailures.isRequestFailure(prematureClose)).isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new IOException("wrapped", prematureClose)))
        .isTrue();

    StorageException prematureEof = StorageException.translate(new IOException("Premature EOF"));
    assertThat(GhfsRequestFailures.isRequestFailure(prematureEof)).isTrue();

    StorageException internalErrorReason =
        new StorageException(0, "Internal error", "internalError", new IOException("backend"));
    assertThat(GhfsRequestFailures.isRequestFailure(internalErrorReason)).isTrue();

    StorageException insufficientDataWritten =
        StorageException.translate(new IOException("insufficient data written"));
    assertThat(GhfsRequestFailures.isRequestFailure(insufficientDataWritten)).isTrue();

    // Non-retryable StorageException(0) stream-truncation, protocol, and dataLoss errors -> true
    StorageException truncatedStream =
        new StorageException(
            0, "onComplete without preceding onNext, unable to determine success.");
    assertThat(truncatedStream.isRetryable()).isFalse();
    assertThat(truncatedStream.getCause()).isNull();
    assertThat(GhfsRequestFailures.isRequestFailure(truncatedStream)).isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new IOException("wrapped", truncatedStream)))
        .isTrue();

    StorageException nonJsonResponse = new StorageException(0, "Response not application/json.");
    assertThat(nonJsonResponse.isRetryable()).isFalse();
    assertThat(GhfsRequestFailures.isRequestFailure(nonJsonResponse)).isTrue();

    StorageException dataLossNullCause =
        new StorageException(
            0, "Finalized upload, but object size less than expected.", "dataLoss", null);
    assertThat(dataLossNullCause.isRetryable()).isFalse();
    assertThat(GhfsRequestFailures.isRequestFailure(dataLossNullCause)).isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new IOException("wrapped", dataLossNullCause)))
        .isTrue();

    // UploadFailureScenario.SCENARIO_5 wraps OutOfRangeException with reason = "dataLoss" -> true
    StorageException dataLossWithOutOfRangeCause =
        new StorageException(
            0,
            "Client side data loss detected. Attempt to append to a upload session with an offset"
                + " higher than the backend has",
            "dataLoss",
            apiException(StatusCode.Code.OUT_OF_RANGE, null));
    assertThat(GhfsRequestFailures.isRequestFailure(dataLossWithOutOfRangeCause)).isTrue();

    StorageException invalidUploadReason =
        new StorageException(
            0, "Attempt to append to already finalized upload session.", "invalid", null);
    assertThat(invalidUploadReason.isRetryable()).isFalse();
    assertThat(GhfsRequestFailures.isRequestFailure(invalidUploadReason)).isTrue();

    // StorageException(0) wrapping semantic FileNotFoundException -> false
    StorageException translatedNotFound =
        StorageException.translate(new FileNotFoundException("missing"));
    assertThat(GhfsRequestFailures.isRequestFailure(translatedNotFound)).isFalse();

    // StorageException(409) wrapping ApiException(ABORTED) continues traversal to cause -> true
    StorageException abortedWrapped =
        new StorageException(409, "Aborted", apiException(StatusCode.Code.ABORTED, null));
    assertThat(GhfsRequestFailures.isRequestFailure(abortedWrapped)).isTrue();

    // StorageException(400) wrapping ApiException(FAILED_PRECONDITION / OUT_OF_RANGE) -> false
    StorageException failedPreconditionWrapped =
        new StorageException(
            400, "Failed precondition", apiException(StatusCode.Code.FAILED_PRECONDITION, null));
    assertThat(GhfsRequestFailures.isRequestFailure(failedPreconditionWrapped)).isFalse();
  }

  @Test
  public void isRequestFailure_apiException_classifiedByStatusCodeNotHttpCode() {
    StatusCode.Code[] failureCodes = {
      StatusCode.Code.INVALID_ARGUMENT,
      StatusCode.Code.UNAUTHENTICATED,
      StatusCode.Code.PERMISSION_DENIED,
      StatusCode.Code.RESOURCE_EXHAUSTED,
      StatusCode.Code.INTERNAL,
      StatusCode.Code.UNAVAILABLE,
      StatusCode.Code.DEADLINE_EXCEEDED,
      StatusCode.Code.UNKNOWN,
      StatusCode.Code.DATA_LOSS,
      StatusCode.Code.CANCELLED,
      StatusCode.Code.ABORTED,
      StatusCode.Code.UNIMPLEMENTED,
    };
    for (StatusCode.Code code : failureCodes) {
      ApiException apiEx = apiException(code, null);
      assertThat(GhfsRequestFailures.isRequestFailure(apiEx)).isTrue();
      assertThat(GhfsRequestFailures.isRequestFailure(new IOException("wrapped", apiEx))).isTrue();
    }

    StatusCode.Code[] nonFailureCodes = {
      StatusCode.Code.NOT_FOUND,
      StatusCode.Code.ALREADY_EXISTS,
      StatusCode.Code.FAILED_PRECONDITION,
      StatusCode.Code.OUT_OF_RANGE,
      StatusCode.Code.OK,
    };
    for (StatusCode.Code code : nonFailureCodes) {
      ApiException apiEx = apiException(code, null);
      assertThat(GhfsRequestFailures.isRequestFailure(apiEx)).isFalse();
      assertThat(GhfsRequestFailures.isRequestFailure(new IOException("wrapped", apiEx))).isFalse();
    }

    // FAILED_PRECONDITION maps to HTTP 400 in GAX; verify even an HttpJson-style carrier
    // HttpResponseException(400) underneath ApiException(FAILED_PRECONDITION) returns false.
    assertThat(StatusCode.Code.FAILED_PRECONDITION.getHttpStatusCode()).isEqualTo(400);
    HttpResponseException http400Carrier =
        new HttpResponseException.Builder(400, "Failed Precondition", new HttpHeaders()).build();
    ApiException httpJsonFailedPrecondition =
        apiException(StatusCode.Code.FAILED_PRECONDITION, http400Carrier);
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new IOException("wrapped", httpJsonFailedPrecondition)))
        .isFalse();
  }

  @Test
  public void isRequestFailure_grpcStatusExceptions_classifiedCorrectly() {
    Status.Code[] failureCodes = {
      Status.Code.INVALID_ARGUMENT,
      Status.Code.UNAUTHENTICATED,
      Status.Code.PERMISSION_DENIED,
      Status.Code.RESOURCE_EXHAUSTED,
      Status.Code.INTERNAL,
      Status.Code.UNAVAILABLE,
      Status.Code.DEADLINE_EXCEEDED,
      Status.Code.UNKNOWN,
      Status.Code.DATA_LOSS,
      Status.Code.CANCELLED,
      Status.Code.ABORTED,
      Status.Code.UNIMPLEMENTED,
    };
    for (Status.Code code : failureCodes) {
      Status status = code.toStatus().withDescription("grpc failure");
      assertThat(GhfsRequestFailures.isRequestFailure(new StatusRuntimeException(status))).isTrue();
      assertThat(GhfsRequestFailures.isRequestFailure(new StatusException(status))).isTrue();
      assertThat(
              GhfsRequestFailures.isRequestFailure(
                  new IOException("wrapped", new StatusRuntimeException(status))))
          .isTrue();
    }

    Status.Code[] nonFailureCodes = {
      Status.Code.NOT_FOUND,
      Status.Code.ALREADY_EXISTS,
      Status.Code.FAILED_PRECONDITION,
      Status.Code.OUT_OF_RANGE,
      Status.Code.OK,
    };
    for (Status.Code code : nonFailureCodes) {
      Status status = code.toStatus().withDescription("grpc semantic");
      assertThat(GhfsRequestFailures.isRequestFailure(new StatusRuntimeException(status)))
          .isFalse();
      assertThat(GhfsRequestFailures.isRequestFailure(new StatusException(status))).isFalse();
      assertThat(
              GhfsRequestFailures.isRequestFailure(
                  new IOException("wrapped", new StatusRuntimeException(status))))
          .isFalse();
    }
  }

  @Test
  public void isRequestFailure_jsonHttpResponseExceptions_classifiedCorrectly() {
    for (int code : new int[] {400, 401, 403, 408, 429, 500, 503}) {
      assertThat(GhfsRequestFailures.isRequestFailure(jsonException(code, null, null))).isTrue();
      HttpResponseException plainHttp =
          new HttpResponseException.Builder(code, "HTTP " + code, new HttpHeaders()).build();
      assertThat(GhfsRequestFailures.isRequestFailure(plainHttp)).isTrue();
      assertThat(GhfsRequestFailures.isRequestFailure(new IOException("wrapped", plainHttp)))
          .isTrue();
    }
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                jsonException(
                    429,
                    ApiErrorExtractor.USAGE_LIMITS_DOMAIN,
                    ApiErrorExtractor.RATE_LIMITED_REASON)))
        .isTrue();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                jsonException(
                    429,
                    ApiErrorExtractor.USAGE_LIMITS_DOMAIN,
                    ApiErrorExtractor.QUOTA_EXCEEDED_REASON)))
        .isTrue();

    for (int code : new int[] {404, 409, 412}) {
      assertThat(GhfsRequestFailures.isRequestFailure(jsonException(code, null, null))).isFalse();
      HttpResponseException plainHttp =
          new HttpResponseException.Builder(code, "HTTP " + code, new HttpHeaders()).build();
      assertThat(GhfsRequestFailures.isRequestFailure(plainHttp)).isFalse();
    }
  }

  @Test
  public void isRequestFailure_compositeExceptionWithNotFoundAndRequestFailure_returnsTrue() {
    IOException composite =
        GoogleCloudStorageExceptions.createCompositeException(
            ImmutableList.of(
                new FileNotFoundException("gs://bucket/missing"),
                new IOException("Error deleting object", new StorageException(403, "Forbidden"))));
    assertThat(GhfsRequestFailures.isRequestFailure(composite)).isTrue();

    FileNotFoundException primaryNotFound = new FileNotFoundException("gs://bucket/missing");
    primaryNotFound.addSuppressed(new StorageException(403, "Forbidden"));
    assertThat(GhfsRequestFailures.isRequestFailure(primaryNotFound)).isTrue();

    java.nio.file.FileAlreadyExistsException primaryExists =
        new java.nio.file.FileAlreadyExistsException("gs://bucket/existing");
    primaryExists.addSuppressed(new SocketTimeoutException("Read timed out"));
    assertThat(GhfsRequestFailures.isRequestFailure(primaryExists)).isTrue();

    IOException compositeOnlyNonFailures =
        GoogleCloudStorageExceptions.createCompositeException(
            ImmutableList.of(
                new FileNotFoundException("gs://bucket/missing-1"),
                new IOException("not found", new StorageException(404, "Not Found"))));
    assertThat(GhfsRequestFailures.isRequestFailure(compositeOnlyNonFailures)).isFalse();
  }

  @Test
  public void isRequestFailure_cyclicCauseAndSuppressedGraph_terminatesCleanly() {
    // Mutual cause cycle: e1 -> e2 -> e1 (no request failure)
    IOException causeCycle1 = new IOException("cycle-1");
    IOException causeCycle2 = new IOException("cycle-2", causeCycle1);
    causeCycle1.initCause(causeCycle2);
    assertThat(GhfsRequestFailures.isRequestFailure(causeCycle1)).isFalse();

    // Mutual suppressed cycle: s1 <-> s2 (no request failure)
    IOException suppCycle1 = new FileNotFoundException("supp-1");
    IOException suppCycle2 = new FileNotFoundException("supp-2");
    suppCycle1.addSuppressed(suppCycle2);
    suppCycle2.addSuppressed(suppCycle1);
    assertThat(GhfsRequestFailures.isRequestFailure(suppCycle1)).isFalse();

    // Mixed cause + suppressed cycle where a reachable node is a 503 StorageException
    IOException mixedRoot = new FileNotFoundException("root");
    IOException mixedChild = new IOException("child", mixedRoot);
    mixedRoot.initCause(mixedChild);
    mixedChild.addSuppressed(mixedRoot);
    mixedChild.addSuppressed(new StorageException(503, "Service Unavailable"));
    assertThat(GhfsRequestFailures.isRequestFailure(mixedRoot)).isTrue();

    // Cyclic cause chain on HttpResponseException(408) and HttpResponseException(429) -> true
    HttpResponseException http408Cycle =
        new HttpResponseException.Builder(408, "Request Timeout", new HttpHeaders()).build();
    IOException http408Wrap = new IOException("wrap-408", http408Cycle);
    http408Cycle.initCause(http408Wrap);
    assertThat(GhfsRequestFailures.isRequestFailure(http408Wrap)).isTrue();

    HttpResponseException http429Cycle =
        new HttpResponseException.Builder(429, "Too Many Requests", new HttpHeaders()).build();
    IOException http429Wrap = new IOException("wrap-429", http429Cycle);
    http429Cycle.initCause(http429Wrap);
    assertThat(GhfsRequestFailures.isRequestFailure(http429Wrap)).isTrue();
  }

  @Test
  public void isRequestFailure_httpPathCredentialAndMetadataServerFailures_returnTrue()
      throws Exception {
    // 1. Workload Identity Federation OAuthException (isRetryable == false, cause == null)
    IOException oauthEx = createStsOAuthException();
    assertThat(oauthEx.getClass().getName()).endsWith(".OAuthException");
    assertThat(((Retryable) oauthEx).isRetryable()).isFalse();
    assertThat(oauthEx.getCause()).isNull();
    assertThat(GhfsRequestFailures.isRequestFailure(oauthEx)).isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(StorageException.translate(oauthEx))).isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new IOException("wrapped", oauthEx))).isTrue();

    // 2. PluggableAuthException (isRetryable == false, cause == null)
    IOException pluggableEx = createPluggableAuthException();
    assertThat(pluggableEx.getClass().getName()).endsWith(".PluggableAuthException");
    assertThat(((Retryable) pluggableEx).isRetryable()).isFalse();
    assertThat(pluggableEx.getCause()).isNull();
    assertThat(GhfsRequestFailures.isRequestFailure(pluggableEx)).isTrue();

    // 3. Non-retryable GoogleAuthException (isRetryable == false)
    IOException nonRetryableGoogleAuthEx = createNonRetryableGoogleAuthException();
    assertThat(nonRetryableGoogleAuthEx.getClass().getName()).endsWith(".GoogleAuthException");
    assertThat(((Retryable) nonRetryableGoogleAuthEx).isRetryable()).isFalse();
    assertThat(GhfsRequestFailures.isRequestFailure(nonRetryableGoogleAuthEx)).isTrue();

    // 4. DelegationTokenIOException, CertificateSourceUnavailableException, SigningException,
    //    and GeneralSecurityException
    DelegationTokenIOException delegationTokenEx = new DelegationTokenIOException("token mismatch");
    assertThat(GhfsRequestFailures.isRequestFailure(delegationTokenEx)).isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new IOException("wrapped", delegationTokenEx)))
        .isTrue();

    CertificateSourceUnavailableException certSourceEx =
        new CertificateSourceUnavailableException("mTLS certificate unavailable");
    assertThat(GhfsRequestFailures.isRequestFailure(certSourceEx)).isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new IOException("wrapped", certSourceEx)))
        .isTrue();

    ServiceAccountSigner.SigningException signingEx =
        new ServiceAccountSigner.SigningException(
            "Failed to sign", new RuntimeException("KMS unavailable"));
    assertThat(GhfsRequestFailures.isRequestFailure(signingEx)).isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new IOException("wrapped", signingEx)))
        .isTrue();

    GeneralSecurityException securityEx = new GeneralSecurityException("Invalid PKCS8 key");
    assertThat(GhfsRequestFailures.isRequestFailure(securityEx)).isTrue();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new IOException(
                    "Error signing service account access token request with private key.",
                    securityEx)))
        .isTrue();

    // 5. Real ComputeEngineCredentials non-503 GCE Metadata Server HTTP error IOExceptions
    for (int mdsCode : new int[] {400, 401, 403, 404, 408, 429, 500, 502, 504}) {
      IOException mdsEx = createComputeEngineMetadataTokenException(mdsCode);
      assertThat(mdsEx.getClass()).isEqualTo(IOException.class);
      assertThat(mdsEx.getCause()).isNull();
      assertThat(mdsEx).isNotInstanceOf(Retryable.class);
      assertThat(GhfsRequestFailures.isRequestFailure(mdsEx)).isTrue();
      assertThat(GhfsRequestFailures.isRequestFailure(StorageException.translate(mdsEx))).isTrue();
      assertThat(
              GhfsRequestFailures.isRequestFailure(
                  new IOException("Error accessing bucket", mdsEx)))
          .isTrue();
    }

    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new IOException(
                    "Error code 429 trying to get security access token from Compute Engine"
                        + " metadata for the default service account")))
        .isTrue();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new IOException(
                    "Unexpected Error code 404 trying to get security access token from Compute"
                        + " Engine metadata for the default service account")))
        .isTrue();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new IOException("Empty content from metadata token server request.")))
        .isTrue();

    // 6. Semantic non-failure subclasses or messages containing arbitrary object names -> false
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new FileNotFoundException(
                    "Error code 404 trying to get obj from Compute Engine metadata")))
        .isFalse();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new java.nio.file.FileAlreadyExistsException(
                    "Error code 409 trying to get obj from Compute Engine metadata")))
        .isFalse();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new org.apache.hadoop.fs.FileAlreadyExistsException(
                    "Error code 409 trying to get obj from Compute Engine metadata")))
        .isFalse();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new IOException(
                    "Cannot overwrite an existing file: gs://bucket/Error code 429 trying to get"
                        + " obj from Compute Engine metadata")))
        .isFalse();
  }

  @Test
  public void isRequestFailure_unwrappedHttpUrlConnectionTruncatedStreamMessages_returnTrue() {
    ImmutableList<String> transportMessages =
        ImmutableList.of(
            "Premature EOF",
            "Connection closed prematurely",
            "insufficient data written",
            "Error writing request body to server");
    for (String message : transportMessages) {
      IOException plainTransportEx = new IOException(message);
      assertThat(GhfsRequestFailures.isRequestFailure(plainTransportEx)).isTrue();
      assertThat(
              GhfsRequestFailures.isRequestFailure(
                  new IOException("Error deleting object", plainTransportEx)))
          .isTrue();
    }

    // Generic plain IOExceptions and semantic exceptions with matching object names remain false
    assertThat(GhfsRequestFailures.isRequestFailure(new IOException("rename failed"))).isFalse();
    assertThat(GhfsRequestFailures.isRequestFailure(new FileNotFoundException("Premature EOF")))
        .isFalse();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new IOException("Cannot overwrite an existing file: gs://bucket/Premature EOF")))
        .isFalse();
  }

  @Test
  public void isRequestFailure_workerErrorsExecutorLifecycleAndClosedFs_classifiedCorrectly()
      throws Exception {
    // 1. Worker-thread java.lang.Error wrapped in IOException / ExecutionException /
    //    StorageException
    for (Error err :
        new Error[] {
          new OutOfMemoryError("Java heap space"),
          new AssertionError("invariant violated"),
          new LinkageError("incompatible class"),
          new IOError(new IOException("disk failure"))
        }) {
      assertThat(GhfsRequestFailures.isRequestFailure(err)).isTrue();
      assertThat(
              GhfsRequestFailures.isRequestFailure(
                  new IOException("Error deleting gs://bucket/obj, stage 1", err)))
          .isTrue();
      assertThat(
              GhfsRequestFailures.isRequestFailure(
                  new IOException("Failed to get result: " + err, new ExecutionException(err))))
          .isTrue();
      assertThat(
              GhfsRequestFailures.isRequestFailure(new StorageException(0, "coalesced error", err)))
          .isTrue();
    }

    // 2. DeleteFolderOperation stall IllegalStateException and cause-stripped shutdown IOException
    IOException folderDeleteStall =
        new IOException(
            "Received IllegalStateException while deletion of folder resource : Deletion stalled:"
                + " No active threads, but 2 folders remain.",
            new IllegalStateException(
                "Deletion stalled: No active threads, but 2 folders remain."));
    assertThat(GhfsRequestFailures.isRequestFailure(folderDeleteStall)).isTrue();

    IOException strippedBatchShutdownInterrupt =
        new IOException(
            "Error in shutting down batch executor : Failed to get result:"
                + " java.lang.InterruptedException");
    assertThat(strippedBatchShutdownInterrupt.getCause()).isNull();
    assertThat(GhfsRequestFailures.isRequestFailure(strippedBatchShutdownInterrupt)).isTrue();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                GoogleCloudStorageExceptions.createCompositeException(
                    ImmutableList.of(strippedBatchShutdownInterrupt))))
        .isTrue();

    // 3. Closed-FS IOException and executor/lifecycle runtime exceptions
    IOException closedFsEx =
        new IOException("GoogleHadoopFileSystem has been closed or not initialized.");
    assertThat(GhfsRequestFailures.isRequestFailure(closedFsEx)).isTrue();

    Constructor<AsyncSessionClosedException> asyncClosedCtor =
        AsyncSessionClosedException.class.getDeclaredConstructor(String.class);
    asyncClosedCtor.setAccessible(true);
    AsyncSessionClosedException asyncSessionClosed =
        asyncClosedCtor.newInstance("Session already closed");

    for (Throwable runtimeFailure :
        new Throwable[] {
          new RejectedExecutionException("Executor shut down"),
          new IllegalStateException("Storage client is closed"),
          new IllegalArgumentException("Invalid bucket argument"),
          new SecurityException("Access denied by SecurityManager"),
          new UnsupportedOperationException("Operation unsupported"),
          new NullPointerException("generation can not be null"),
          asyncSessionClosed,
          new StorageException(0, "Parent stream shutdown", asyncSessionClosed),
          new JsonParseException("Malformed JSON"),
          new MalformedJsonException("Expected BEGIN_OBJECT")
        }) {
      assertThat(GhfsRequestFailures.isRequestFailure(runtimeFailure)).isTrue();
      assertThat(GhfsRequestFailures.isRequestFailure(new IOException("wrapped", runtimeFailure)))
          .isTrue();
    }

    // 4. Negative check: semantic ApiException(NOT_FOUND) / StorageException(412) with suppressed
    //    GAX AsyncTaskException, java-storage AsyncStorageTaskException, or direct Throwable
    //    diagnostic comments (ApiExceptionErrorDetailsComment / BackoffComment) MUST remain false.
    ApiException gaxNotFound =
        assertThrows(
            ApiException.class,
            () ->
                ApiExceptions.callAndTranslateApiException(
                    ApiFutures.immediateFailedFuture(
                        apiException(StatusCode.Code.NOT_FOUND, null))));
    assertThat(
            Arrays.stream(gaxNotFound.getSuppressed())
                .anyMatch(s -> s instanceof AsyncTaskException))
        .isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(gaxNotFound)).isFalse();
    assertThat(GhfsRequestFailures.isRequestFailure(new IOException("wrapped", gaxNotFound)))
        .isFalse();

    Constructor<AsyncStorageTaskException> asyncStorageTaskCtor =
        AsyncStorageTaskException.class.getDeclaredConstructor();
    asyncStorageTaskCtor.setAccessible(true);
    AsyncStorageTaskException asyncStorageTaskEx = asyncStorageTaskCtor.newInstance();
    ApiException notFoundWithAsyncStorageTask = apiException(StatusCode.Code.NOT_FOUND, null);
    notFoundWithAsyncStorageTask.addSuppressed(asyncStorageTaskEx);
    assertThat(GhfsRequestFailures.isRequestFailure(notFoundWithAsyncStorageTask)).isFalse();
    assertThat(
            GhfsRequestFailures.isRequestFailure(
                new IOException("wrapped", notFoundWithAsyncStorageTask)))
        .isFalse();

    StorageException preconditionWithThrowableComment =
        new StorageException(412, "Precondition Failed");
    Throwable diagnosticComment =
        new Throwable("BackoffComment: retry budget", null, true, false) {};
    preconditionWithThrowableComment.addSuppressed(diagnosticComment);
    preconditionWithThrowableComment.addSuppressed(asyncStorageTaskCtor.newInstance());
    assertThat(GhfsRequestFailures.isRequestFailure(preconditionWithThrowableComment)).isFalse();
  }

  @Test
  public void isRequestFailure_impersonatedAndIdentityPoolCredentialRefreshFailures_returnTrue()
      throws Exception {
    GoogleCredentials sourceCreds =
        GoogleCredentials.create(
            new AccessToken("source-token", new Date(System.currentTimeMillis() + 3_600_000L)));

    // 1. ImpersonatedCredentials HTTP 404 from IAM :generateAccessToken -> plain IOException
    //    ("Error requesting access token", HttpResponseException(404))
    ImpersonatedCredentials impersonated404 =
        ImpersonatedCredentials.create(
            sourceCreds,
            "missing-sa@test-project.iam.gserviceaccount.com",
            /* delegates= */ null,
            ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"),
            /* lifetime= */ 300,
            () -> mockJsonTransport(404, "{\"error\":{\"code\":404,\"message\":\"Not found\"}}"));
    IOException impersonated404Ex =
        assertThrows(IOException.class, impersonated404::refreshAccessToken);
    assertThat(impersonated404Ex.getClass()).isEqualTo(IOException.class);
    assertThat(impersonated404Ex).hasMessageThat().isEqualTo("Error requesting access token");
    assertThat(impersonated404Ex.getCause()).isInstanceOf(HttpResponseException.class);
    assertThat(((HttpResponseException) impersonated404Ex.getCause()).getStatusCode())
        .isEqualTo(404);
    assertThat(GhfsRequestFailures.isRequestFailure(impersonated404Ex)).isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(StorageException.translate(impersonated404Ex)))
        .isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(new IOException("wrapped", impersonated404Ex)))
        .isTrue();

    // ImpersonatedCredentials malformed 200 responses (missing accessToken / expireTime / bad date)
    for (String badIamPayload :
        ImmutableList.of(
            "{}",
            "{\"accessToken\":\"tok\"}",
            "{\"accessToken\":\"tok\",\"expireTime\":\"not-a-timestamp\"}")) {
      ImpersonatedCredentials impersonatedBadBody =
          ImpersonatedCredentials.create(
              sourceCreds,
              "sa@test-project.iam.gserviceaccount.com",
              /* delegates= */ null,
              ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"),
              /* lifetime= */ 300,
              () -> mockJsonTransport(200, badIamPayload));
      IOException badBodyEx =
          assertThrows(IOException.class, impersonatedBadBody::refreshAccessToken);
      assertThat(badBodyEx.getClass()).isEqualTo(IOException.class);
      assertThat(GhfsRequestFailures.isRequestFailure(badBodyEx)).isTrue();
    }

    // 2. Workload Identity Federation (IdentityPoolCredentials) subject-token supplier failures
    File missingTokenFile = new File(tempFolder.getRoot(), "nonexistent-subject-token.jwt");
    IdentityPoolCredentials missingFileCreds =
        createIdentityPoolCredentials(
            new IdentityPoolCredentialSource(
                ImmutableMap.of("file", missingTokenFile.getAbsolutePath())),
            () -> mockJsonTransport(200, "{}"));
    IOException missingFileEx =
        assertThrows(IOException.class, missingFileCreds::refreshAccessToken);
    assertThat(missingFileEx.getClass()).isEqualTo(IOException.class);
    assertThat(missingFileEx.getMessage()).startsWith("Invalid credential location. The file at ");
    assertThat(GhfsRequestFailures.isRequestFailure(missingFileEx)).isTrue();

    File directoryAsCredentialFile = tempFolder.newFolder("cred-dir");
    IdentityPoolCredentials unreadableFileCreds =
        createIdentityPoolCredentials(
            new IdentityPoolCredentialSource(
                ImmutableMap.of("file", directoryAsCredentialFile.getAbsolutePath())),
            () -> mockJsonTransport(200, "{}"));
    IOException unreadableFileEx =
        assertThrows(IOException.class, unreadableFileCreds::refreshAccessToken);
    assertThat(unreadableFileEx.getClass()).isEqualTo(IOException.class);
    assertThat(unreadableFileEx)
        .hasMessageThat()
        .isEqualTo("Error when attempting to read the subject token from the credential file.");
    assertThat(GhfsRequestFailures.isRequestFailure(unreadableFileEx)).isTrue();

    File missingFieldJson = tempFolder.newFile("missing-subject-token-field.json");
    Files.write(missingFieldJson.toPath(), "{}".getBytes(UTF_8));
    IdentityPoolCredentials missingFieldCreds =
        createIdentityPoolCredentials(
            new IdentityPoolCredentialSource(
                ImmutableMap.of(
                    "file",
                    missingFieldJson.getAbsolutePath(),
                    "format",
                    ImmutableMap.of("type", "json", "subject_token_field_name", "id_token"))),
            () -> mockJsonTransport(200, "{}"));
    IOException missingFieldEx =
        assertThrows(IOException.class, missingFieldCreds::refreshAccessToken);
    assertThat(missingFieldEx.getClass()).isEqualTo(IOException.class);
    assertThat(missingFieldEx)
        .hasMessageThat()
        .isEqualTo("Error when attempting to read the subject token from the credential file.");
    assertThat(missingFieldEx.getCause())
        .hasMessageThat()
        .isEqualTo("Invalid subject token field name. No subject token was found.");
    assertThat(GhfsRequestFailures.isRequestFailure(missingFieldEx)).isTrue();
    assertThat(GhfsRequestFailures.isRequestFailure(missingFieldEx.getCause())).isTrue();

    IdentityPoolCredentials url404Creds =
        createIdentityPoolCredentials(
            new IdentityPoolCredentialSource(
                ImmutableMap.of("url", "http://169.254.169.254/subject-token")),
            () -> mockJsonTransport(404, "subject token endpoint not found"));
    IOException url404Ex = assertThrows(IOException.class, url404Creds::refreshAccessToken);
    assertThat(url404Ex.getClass()).isEqualTo(IOException.class);
    assertThat(url404Ex.getMessage())
        .startsWith("Error getting subject token from metadata server: ");
    assertThat(url404Ex.getCause()).isInstanceOf(HttpResponseException.class);
    assertThat(((HttpResponseException) url404Ex.getCause()).getStatusCode()).isEqualTo(404);
    assertThat(GhfsRequestFailures.isRequestFailure(url404Ex)).isTrue();

    // Additional plain IOException messages thrown by source-credential / mTLS / AWS / GCE
    // suppliers
    for (String refreshFailureMessage :
        ImmutableList.of(
            "Unable to refresh sourceCredentials",
            "Unable to refresh the provided source credential.",
            "Expected to find an accessToken",
            "Expected to find an expireTime",
            "Leaf certificate file not found: /etc/certs/leaf.pem",
            "Trust chain file not found: /etc/certs/chain.pem",
            "Failed to read leaf certificate file: /etc/certs/leaf.pem",
            "Failed to read trust chain file: /etc/certs/chain.pem",
            "Failed to retrieve AWS security credentials.",
            "Unable to determine the AWS region. The credential source does not contain the region"
                + " URL.",
            "ComputeEngineCredentials cannot find the metadata server. This is likely because code"
                + " is not running on Google Compute Engine.")) {
      IOException plainAuthEx =
          new IOException(refreshFailureMessage, new FileNotFoundException("underlying missing"));
      assertThat(GhfsRequestFailures.isRequestFailure(plainAuthEx)).isTrue();
    }
  }

  private static IdentityPoolCredentials createIdentityPoolCredentials(
      IdentityPoolCredentialSource credentialSource,
      com.google.auth.http.HttpTransportFactory transportFactory) {
    return IdentityPoolCredentials.newBuilder()
        .setAudience(
            "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/p/providers/pr")
        .setSubjectTokenType("urn:ietf:params:oauth:token-type:jwt")
        .setTokenUrl("https://sts.googleapis.com/v1/token")
        .setCredentialSource(credentialSource)
        .setHttpTransportFactory(transportFactory)
        .build();
  }

  private IOException createPluggableAuthException() throws IOException {
    File badOutputFile = tempFolder.newFile("malformed-pluggable-output.json");
    Files.write(badOutputFile.toPath(), "{}".getBytes(UTF_8));
    PluggableAuthCredentials credentials =
        PluggableAuthCredentials.newBuilder()
            .setAudience(
                "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/p/providers/pr")
            .setSubjectTokenType("urn:ietf:params:oauth:token-type:jwt")
            .setTokenUrl("https://sts.googleapis.com/v1/token")
            .setCredentialSource(
                new PluggableAuthCredentialSource(
                    ImmutableMap.of(
                        "executable",
                        ImmutableMap.of(
                            "command", "unused", "output_file", badOutputFile.getAbsolutePath()))))
            .build();
    return assertThrows(IOException.class, credentials::retrieveSubjectToken);
  }

  private static IOException createStsOAuthException() {
    MockHttpTransport stsTransport =
        mockJsonTransport(
            400, "{\"error\":\"invalid_grant\",\"error_description\":\"Bad subject token\"}");
    StsTokenExchangeRequest tokenExchangeRequest =
        StsTokenExchangeRequest.newBuilder("bad-token", "urn:ietf:params:oauth:token-type:jwt")
            .build();
    StsRequestHandler handler =
        StsRequestHandler.newBuilder(
                "https://sts.googleapis.com/v1/token",
                tokenExchangeRequest,
                stsTransport.createRequestFactory())
            .build();
    return assertThrows(IOException.class, handler::exchangeToken);
  }

  private static IOException createNonRetryableGoogleAuthException() {
    MockHttpTransport tokenEndpointTransport = mockJsonTransport(404, "not found");
    UserCredentials credentials =
        UserCredentials.newBuilder()
            .setClientId("client-id")
            .setClientSecret("client-secret")
            .setRefreshToken("refresh-token")
            .setHttpTransportFactory(() -> tokenEndpointTransport)
            .build();
    return assertThrows(IOException.class, credentials::refreshAccessToken);
  }

  private static IOException createComputeEngineMetadataTokenException(int statusCode) {
    MockHttpTransport mdsTransport = mockJsonTransport(statusCode, "metadata error " + statusCode);
    ComputeEngineCredentials credentials =
        ComputeEngineCredentials.newBuilder().setHttpTransportFactory(() -> mdsTransport).build();
    return assertThrows(IOException.class, credentials::refreshAccessToken);
  }

  private static MockHttpTransport mockJsonTransport(int statusCode, String body) {
    return new MockHttpTransport() {
      @Override
      public LowLevelHttpRequest buildRequest(String method, String url) {
        return new MockLowLevelHttpRequest(url) {
          @Override
          public LowLevelHttpResponse execute() {
            return new MockLowLevelHttpResponse()
                .setStatusCode(statusCode)
                .setContentType("application/json; charset=UTF-8")
                .setContent(body);
          }
        };
      }
    };
  }

  private static final class RetryableAuthIOException extends IOException implements Retryable {
    private final boolean retryable;

    RetryableAuthIOException(boolean retryable) {
      super("auth error");
      this.retryable = retryable;
    }

    @Override
    public boolean isRetryable() {
      return retryable;
    }

    @Override
    public int getRetryCount() {
      return 1;
    }
  }

  private static ApiException apiException(StatusCode.Code code, Throwable cause) {
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
    return new ApiException("gax " + code, cause, statusCode, /* retryable= */ false);
  }

  private static GoogleJsonResponseException jsonException(
      int status, String domain, String reason) {
    GoogleJsonError jsonError = new GoogleJsonError();
    jsonError.setCode(status);
    jsonError.setMessage("HTTP " + status);
    if (domain != null || reason != null) {
      ErrorInfo errorInfo = new ErrorInfo();
      errorInfo.setDomain(domain);
      errorInfo.setReason(reason);
      jsonError.setErrors(Collections.singletonList(errorInfo));
    }
    return GoogleCloudStorageExceptions.createJsonResponseException(jsonError, new HttpHeaders());
  }
}
