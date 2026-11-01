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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpResponseException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.Retryable;
import com.google.auth.ServiceAccountSigner;
import com.google.auth.mtls.CertificateSourceUnavailableException;
import com.google.cloud.hadoop.fs.gcs.auth.DelegationTokenIOException;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.IoExceptionHelper;
import com.google.cloud.storage.AsyncSessionClosedException;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Strings;
import com.google.gson.JsonParseException;
import com.google.gson.stream.MalformedJsonException;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ProtocolException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.InterruptedByTimeoutException;
import java.nio.file.AccessDeniedException;
import java.security.GeneralSecurityException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;

/**
 * Classifies exceptions thrown during {@link GoogleHadoopFileSystem#rename} and {@link
 * GoogleHadoopFileSystem#delete} to distinguish genuine request/transport/auth failures (which must
 * propagate as {@link java.io.IOException}) from semantic filesystem preconditions such as
 * not-found, already-exists, or precondition-not-met (which return {@code false}).
 */
final class GhfsRequestFailures {

  // ComputeEngineCredentials surfaces non-503 metadata server HTTP errors as a plain IOException
  // with the status code only in the message (matching RetryHttpInitializer).
  private static final Pattern GCE_METADATA_ERROR_PATTERN =
      Pattern.compile("^(?:Unexpected )?Error code \\d{3} trying to get .+ from Compute Engine");

  private GhfsRequestFailures() {}

  /**
   * Returns {@code true} if any node in the causal or suppressed exception graph of {@code t}
   * represents a genuine request, authentication, permission, interruption, or transport failure.
   */
  static boolean isRequestFailure(@Nullable Throwable t) {
    if (t == null) {
      return false;
    }

    ArrayDeque<Throwable> worklist = new ArrayDeque<>();
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    visited.add(t);
    worklist.addLast(t);

    while (!worklist.isEmpty()) {
      Throwable curr = worklist.removeFirst();
      if (isNodeRequestFailure(curr)) {
        return true;
      }

      Throwable cause = curr.getCause();
      if (curr instanceof ApiException
          && !isApiExceptionRequestFailure((ApiException) curr)
          && cause instanceof HttpResponseException) {
        // An ApiException with a semantic non-failure StatusCode.Code (such as FAILED_PRECONDITION
        // or OUT_OF_RANGE over HttpJson) may carry an underlying HttpResponseException(400). Skip
        // HTTP status-code classification on that immediate carrier while still traversing its
        // nested cause and suppressed exceptions.
        if (visited.add(cause)) {
          enqueueChildren(cause, visited, worklist);
        }
      } else if (cause != null && visited.add(cause)) {
        worklist.addLast(cause);
      }

      enqueueSuppressed(curr, visited, worklist);
    }

    return false;
  }

  private static void enqueueChildren(
      Throwable node, Set<Throwable> visited, ArrayDeque<Throwable> worklist) {
    Throwable nestedCause = node.getCause();
    if (nestedCause != null && visited.add(nestedCause)) {
      worklist.addLast(nestedCause);
    }
    enqueueSuppressed(node, visited, worklist);
  }

  private static void enqueueSuppressed(
      Throwable node, Set<Throwable> visited, ArrayDeque<Throwable> worklist) {
    Throwable[] suppressed = node.getSuppressed();
    if (suppressed != null) {
      for (Throwable s : suppressed) {
        if (s != null && visited.add(s)) {
          worklist.addLast(s);
        }
      }
    }
  }

  private static boolean isNodeRequestFailure(Throwable curr) {
    if (curr instanceof Error) {
      return true;
    }

    boolean acyclicCause = !hasCauseCycle(curr);

    if (curr instanceof AccessDeniedException) {
      return true;
    }

    if (curr instanceof InterruptedIOException
        || curr instanceof InterruptedException
        || curr instanceof ClosedByInterruptException
        || curr instanceof InterruptedByTimeoutException
        || curr instanceof CancellationException
        || curr instanceof TimeoutException
        || (acyclicCause && IoExceptionHelper.isInterrupted(curr))) {
      return true;
    }

    if (curr instanceof RejectedExecutionException
        || curr instanceof IllegalStateException
        || curr instanceof IllegalArgumentException
        || curr instanceof SecurityException
        || curr instanceof UnsupportedOperationException
        || curr instanceof NullPointerException
        || curr instanceof AsyncSessionClosedException
        || curr instanceof JsonParseException
        || curr instanceof MalformedJsonException) {
      return true;
    }

    if (curr instanceof SocketException
        || curr instanceof SocketTimeoutException
        || curr instanceof SSLException
        || curr instanceof EOFException
        || curr instanceof UnknownHostException
        || curr instanceof ProtocolException
        || curr instanceof ClosedChannelException
        || (acyclicCause && IoExceptionHelper.isSocketError(curr))) {
      return true;
    }

    if (curr instanceof Retryable
        || curr instanceof DelegationTokenIOException
        || curr instanceof CertificateSourceUnavailableException
        || curr instanceof ServiceAccountSigner.SigningException
        || curr instanceof GeneralSecurityException) {
      return true;
    }

    if (curr instanceof StorageException) {
      if (isStorageExceptionRequestFailure((StorageException) curr)) {
        return true;
      }
    }

    if (curr instanceof ApiException) {
      if (isApiExceptionRequestFailure((ApiException) curr)) {
        return true;
      }
    }

    if (curr instanceof StatusRuntimeException) {
      Status status = ((StatusRuntimeException) curr).getStatus();
      if (status != null && isGrpcCodeRequestFailure(status.getCode())) {
        return true;
      }
    }

    if (curr instanceof StatusException) {
      Status status = ((StatusException) curr).getStatus();
      if (status != null && isGrpcCodeRequestFailure(status.getCode())) {
        return true;
      }
    }

    if (curr instanceof HttpResponseException) {
      HttpResponseException httpEx = (HttpResponseException) curr;
      if (isSingleNodeHttpRequestFailure(httpEx)
          || (acyclicCause && ApiErrorExtractor.INSTANCE.requestFailure(httpEx))) {
        return true;
      }
    }

    if (curr instanceof IOException && isPlainIoExceptionRequestFailure((IOException) curr)) {
      return true;
    }

    return false;
  }

  private static boolean isPlainIoExceptionRequestFailure(IOException ioEx) {
    if (ioEx.getClass() != IOException.class) {
      return false;
    }
    String message = Strings.nullToEmpty(ioEx.getMessage());
    if (message.isEmpty()) {
      return false;
    }
    return message.startsWith("Premature EOF")
        || message.startsWith("Connection closed prematurely")
        || "insufficient data written".equals(message)
        || "Error writing request body to server".equals(message)
        || message.startsWith("Error in shutting down batch executor : ")
        || "GoogleHadoopFileSystem has been closed or not initialized.".equals(message)
        || isPlainAuthRefreshIoExceptionMessage(message);
  }

  private static boolean isPlainAuthRefreshIoExceptionMessage(String message) {
    return "Empty content from metadata token server request.".equals(message)
        || message.startsWith("ComputeEngineCredentials cannot find the metadata server.")
        || GCE_METADATA_ERROR_PATTERN.matcher(message).find()
        || "Error requesting access token".equals(message)
        || "Unable to refresh sourceCredentials".equals(message)
        || "Unable to refresh the provided source credential.".equals(message)
        || message.startsWith("Expected to find an accessToken")
        || message.startsWith("Expected to find an expireTime")
        || message.startsWith("Error parsing expireTime:")
        || message.startsWith("Error parsing token response.")
        || "Error occurred when attempting to retrieve source credential universe domain."
            .equals(message)
        || message.startsWith("Error getting subject token from metadata server: ")
        || "Error when attempting to read the subject token from the credential file."
            .equals(message)
        || message.startsWith("Invalid credential location. The file at ")
        || "Invalid subject token field name. No subject token was found.".equals(message)
        || "Failed to initialize IdentityPoolCredentials from certificate source due to an I/O error."
            .equals(message)
        || message.startsWith("Leaf certificate file not found: ")
        || message.startsWith("Trust chain file not found: ")
        || message.startsWith("Failed to read leaf certificate file: ")
        || message.startsWith("Failed to read trust chain file: ")
        || message.startsWith("Failed to parse leaf certificate from file: ")
        || message.startsWith("Failed to parse certificate(s) from trust chain file: ")
        || message.startsWith("Trust chain misconfiguration:")
        || message.startsWith("Failed to retrieve AWS ")
        || message.startsWith("Unable to determine the AWS ")
        || message.startsWith("Scopes and uri are not configured for service account.")
        || message.startsWith("Service Account user is configured for the credential.");
  }

  private static boolean isStorageExceptionRequestFailure(StorageException storageException) {
    String reason = storageException.getReason();
    if ("connectionClosedPrematurely".equals(reason)
        || "internalError".equals(reason)
        || "dataLoss".equals(reason)
        || "invalid".equals(reason)) {
      return true;
    }
    Throwable cause = storageException.getCause();
    if (cause instanceof ApiException && !isApiExceptionRequestFailure((ApiException) cause)) {
      return false;
    }
    int code = storageException.getCode();
    if (code == 400 || code == 401 || code == 403 || code == 408 || code == 429 || code >= 500) {
      return true;
    }
    return code == 0 && (storageException.isRetryable() || cause == null);
  }

  private static boolean isApiExceptionRequestFailure(ApiException apiException) {
    StatusCode statusCode = apiException.getStatusCode();
    StatusCode.Code code = statusCode != null ? statusCode.getCode() : null;
    if (code == null) {
      return false;
    }
    switch (code) {
      case NOT_FOUND:
      case ALREADY_EXISTS:
      case FAILED_PRECONDITION:
      case OUT_OF_RANGE:
      case OK:
        return false;
      default:
        return true;
    }
  }

  private static boolean isGrpcCodeRequestFailure(@Nullable Status.Code grpcCode) {
    if (grpcCode == null) {
      return false;
    }
    switch (grpcCode) {
      case NOT_FOUND:
      case ALREADY_EXISTS:
      case FAILED_PRECONDITION:
      case OUT_OF_RANGE:
      case OK:
        return false;
      default:
        return true;
    }
  }

  private static boolean isSingleNodeHttpRequestFailure(HttpResponseException httpEx) {
    int code = httpEx.getStatusCode();
    return code == 400
        || code == 401
        || code == 403
        || code == 408
        || code == 429
        || code / 100 == 5
        || (httpEx instanceof GoogleJsonResponseException
            && (ApiErrorExtractor.INSTANCE.rateLimited(httpEx)
                || ApiErrorExtractor.INSTANCE.quotaExceeded(httpEx)));
  }

  private static boolean hasCauseCycle(Throwable start) {
    Throwable slow = start;
    Throwable fast = start.getCause();
    while (fast != null && fast.getCause() != null) {
      if (slow == fast) {
        return true;
      }
      slow = slow.getCause();
      fast = fast.getCause().getCause();
    }
    return false;
  }
}
