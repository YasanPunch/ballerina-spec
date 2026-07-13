# Introduce Azure Files Connector

- Authors
  - Yasan Punchihewa
- Reviewed by
- Created date
  - 2026-07-13
- Updated date
  - 2026-07-13
- Issue
  - [#1457](https://github.com/ballerina-platform/ballerina-spec/issues/1457)
- State
  - Submitted

## Summary

Ballerina's current support for Azure Files lives inside `ballerinax/azure_storage_service`, a combined package that re-implements the Azure Storage REST protocol (Shared Key signing, chunked transfer, error handling) by hand and is pinned to the 2019-12-12 API version. This proposal introduces **ballerinax/azure.storage.files**, a standalone connector for [Azure Files](https://learn.microsoft.com/en-us/azure/storage/files/storage-files-introduction) built on Microsoft's official `com.azure:azure-storage-file-share` Java SDK. It provides a two-tier client (`AdminClient` for account-level operations, `Client` bound to one share), a polling `Listener` for event-driven services, a union-typed authentication model, and a consistent error hierarchy. It is the sibling of `ballerinax/azure.storage.blob` and shares its design conventions.

## Motivation

The existing `azure_storage_service.files` module has accumulated several problems:

1. **Hand-written protocol layer:** Shared Key signing, chunked upload, and response parsing are implemented in Ballerina and pinned to the 2019-12-12 REST API version. Every protocol fix and every new service capability must be re-implemented by hand.
2. **Fully in-memory transfers:** `getFile` buffers the entire file before writing to disk, and the hand-rolled chunked upload both swallows failures in a logging side effect and carries a chunk-size discrepancy between its two size constants.
3. **Ambiguous configuration:** the auth record makes every field optional (`accessKeyOrSAS?`, `accountName?`), so misconfiguration surfaces as a runtime failure instead of a compile error.
4. **Inconsistent error handling:** error subtypes are defined but applied inconsistently, and an empty directory listing is raised as an error even though it is a valid state.
5. **No event-driven support:** there is no listener, so applications that react to files arriving in a share must hand-roll polling.
6. **No tests in CI:** the legacy tests are live-only and CI skips them entirely.
7. **Combined packaging:** file support is a submodule of a package that also covers Blob, so users pull one large artifact for one service, against the prevailing one-package-per-service pattern of the Azure ecosystem.

Microsoft's own SDKs solve the protocol problems once, centrally: `azure-storage-file-share` encapsulates signing, SAS construction, retry policies, parallel chunked transfer, connection-string parsing, and parity with new REST API versions. Wrapping the SDK instead of the REST API means the connector inherits all of this and Microsoft remains responsible for maintaining it.

## Goals

* Provide an idiomatic Ballerina API for Azure Files with a focused core surface: share lifecycle, directory and file CRUD, upload/download (disk, in-memory, stream), listing, properties and metadata, rename/move, copy, and byte ranges.
* Match Microsoft's two-tier mental model: `AdminClient` for account-level operations and `Client(shareName)` for everything inside one share.
* Provide a polling `Listener` (there is no Event Grid source for Azure Files) with a `Caller` so handlers can act on the event's file without constructing a separate client.
* Provide a union-typed authentication model where each member is exactly one real-world credential artifact and misconfiguration is a compile error.
* Provide a consistent, pattern-matchable error hierarchy keyed on the Azure error code.
* Be a strict superset of the file surface of the existing `azure_storage_service` connector, so existing users can migrate with no loss of functionality.

## Non-Goals

- **No Blob, Queue, or Table support.** Blob is the sibling package `azure.storage.blob`; Queue and Table would be their own future packages.
- **No re-implementation of the wire protocol.** Authentication, signing, retry, and chunked transfer are delegated to the official SDK.
- **Microsoft Entra ID authentication, resilience/transport configuration, and the power-user operations** (snapshots, leases, SAS generation, SMB handles, SDDL permissions, NFS links) are part of the advanced surface that follows the initial release; the design accounts for them so they arrive as additive methods (see the advanced surface summary below).

## Design

### 1. Module overview

The module is `ballerinax/azure.storage.files`. The hierarchical name groups it with its sibling `azure.storage.blob`, following the pattern of `azure.openai.chat` and `azure.openai.responses`. The package has two parts: a `ballerina/` module holding the public API and a `native/` Java subproject that adapts it onto `com.azure:azure-storage-file-share`.

Microsoft's SDK is structured as a chain of four clients (`ShareServiceClient` account scope, `ShareClient` share scope, `ShareDirectoryClient`, `ShareFileClient`). The connector exposes the two scopes users actually think in:

- **`AdminClient`**: account level. List, create, delete, and restore shares. Used by admin tooling and applications that work with multiple shares.
- **`Client`**: bound to one share at `init`. All directory, file, transfer, copy, and range operations, plus share-level properties and metadata. This is the client most applications instantiate.

The lower SDK levels are not surfaced as classes. Directory and file operations are methods on `Client` taking a single slash-delimited, share-relative `path` (for example `"/reports/2026/q4.pdf"`); the Java adaptor splits the path into the segments the SDK requires. Where two paths co-occur they are named `sourcePath` and `destinationPath` in logical source-first order.

Because `azure-storage-blob` and `azure-storage-file-share` depend on the same `azure-storage-common` artifact, splitting Blob and Files into two Ballerina packages duplicates nothing at the JVM level: the auth, signing, and retry layer is shared by Microsoft's own packaging.

### 2. Authentication

#### 2.1 The `AuthConfig` union

Each union member is exactly one real-world credential artifact, the thing the portal, CLI, or IaC tooling actually hands the user:

```ballerina
# The authentication configuration: exactly one credential-artifact record.
public type AuthConfig SharedKeyConfig|SasConfig|SasUrlConfig|ConnectionStringConfig;
```

The advanced surface extends the union with `EntraIdConfig` (DefaultAzureCredential, managed identity, client secret, client certificate, workload identity).

#### 2.2 Credential records

```ballerina
# Authenticates with the storage account name and key (Shared Key).
public type SharedKeyConfig record {|
    # Storage account name, the signing identity
    string accountName;
    # Storage account access key (base64)
    string accountKey;
    # Overrides where requests are sent (sovereign clouds, private endpoints,
    # local test endpoints); defaults to `https://{accountName}.file.core.windows.net`
    string serviceUrl?;
|};

# Authenticates with a bare shared access signature (SAS) token.
public type SasConfig record {|
    # Storage account name, used to derive the service URL
    string accountName;
    # The SAS token, e.g. `sv=...&sig=...`
    string sasToken;
|};

# Authenticates with a fused SAS URL as generated by the Azure portal
# (endpoint and token in one string).
public type SasUrlConfig record {|
    # The full File service SAS URL
    string sasUrl;
|};

# Authenticates with a storage account connection string. The endpoint is
# part of the string, so no separate account name is needed.
public type ConnectionStringConfig record {|
    # The connection string as shown in the portal
    string connectionString;
|};

public type ClientConfiguration record {|
    # The authentication configuration
    AuthConfig auth;
|};
```

Every member has a unique required field, so both the compiler and `Config.toml` select the right member by structural matching, with no discriminator field:

```toml
# The fields present select the union member:
[myapp.filesConfig]
auth = {accountName = "myacct", accountKey = "..."}               # SharedKeyConfig
# auth = {accountName = "myacct", sasToken = "sv=..."}            # SasConfig
# auth = {sasUrl = "https://myacct.file.core.windows.net/?sv=..."}# SasUrlConfig
# auth = {connectionString = "..."}                               # ConnectionStringConfig
```

Every auth mode is validated at `init` with local computation and no call to Azure: connection strings run the SDK's own strict parser plus a file-endpoint check, and the explicit records get non-empty, base64, and URL-scheme checks. A malformed credential surfaces a specific error at `init` rather than an opaque failure at first use.

### 3. The `AdminClient`

The core `AdminClient` surface is the share lifecycle plus an existence check:

```ballerina
# `config` is an included record parameter; callers pass its fields as named
# arguments, e.g. `new (auth = {accountName, accountKey})`.
public function init(*ClientConfiguration config) returns Error?;
remote function close() returns Error?;

# Returns false only when Azure confirms absence (404); an Error means the
# check itself could not complete, so an auth problem is never misreported
# as a missing share.
remote function shareExists(string shareName) returns boolean|Error;

# deleteShare is soft under the account's soft-delete retention policy and
# restorable via undeleteShare.
remote function listShares(ShareListOptions? options = ()) returns ShareInfo[]|Error;
remote function createShare(string shareName, ShareCreateOptions? options = ()) returns Error?;
remote function deleteShare(string shareName, ShareDeleteOptions? options = ()) returns Error?;
remote function undeleteShare(string shareName, string version) returns Error?;
```

### 4. The `Client`

```ballerina
public function init(string shareName, *ClientConfiguration config) returns Error?;
remote function close() returns Error?;
```

Binding is lazy: `init` makes no call to Azure, so the first operation against a nonexistent share fails with `NotFoundError`; the up-front check is `AdminClient.shareExists`. Every operation on all four public classes is an `isolated remote function` on an `isolated` class holding only immutable configuration, so concurrent invocation from parallel strands is safe (the qualifier is omitted below for brevity).

A method name carries the `File` token either to disambiguate a verb that also exists for directories (`createFile` next to `createDirectory`) or to keep a bare verb from implying it handles directories when it is file-only (`uploadFile`, `downloadFile`, `copyFile`). Verbs whose object is already explicit stay bare (`uploadContent`, `uploadRange`, `setContentHeaders`), and `list` is deliberately tier-neutral because it returns files and directories in one stream.

#### 4.1 Share-level operations

```ballerina
remote function getShareProperties() returns ShareProperties|Error;
remote function setShareMetadata(map<string> metadata) returns Error?;
remote function getShareUsage() returns int|Error;   // approximate stored bytes
```

#### 4.2 Directory operations

```ballerina
remote function createDirectory(string directoryPath, DirectoryCreateOptions? options = ()) returns Error?;
remote function deleteDirectory(string directoryPath) returns Error?;
remote function directoryExists(string directoryPath) returns boolean|Error;
remote function getDirectoryProperties(string directoryPath) returns DirectoryProperties|Error;
remote function setDirectoryMetadata(string directoryPath, map<string> metadata) returns Error?;

# Lists a directory's entries, files and subdirectories, as one stream. Every
# Entry carries its full share-relative path, so entries feed directly into
# the path-taking operations.
remote function list(string directoryPath, ListOptions? options = ()) returns stream<Entry, Error?>|Error;

# Rename doubles as move: the destination is a full share-relative path, so
# "/X/A" to "/Y/A" re-parents within the same share.
remote function renameDirectory(string sourcePath, string destinationPath, RenameOptions? options = ()) returns Error?;
```

#### 4.3 File operations

```ballerina
# Metadata is read via `getFileProperties().metadata`; only a setter is exposed.
remote function createFile(string path, int sizeInBytes, CreateOptions? options = ()) returns Error?;
remote function deleteFile(string path) returns Error?;
remote function fileExists(string path) returns boolean|Error;
remote function getFileProperties(string path) returns FileProperties|Error;
remote function setFileMetadata(string path, map<string> metadata) returns Error?;
# Replaces the complete content-header set: any header omitted is cleared.
remote function setContentHeaders(string path, ContentHeaders headers) returns Error?;
remote function renameFile(string sourcePath, string destinationPath, RenameOptions? options = ()) returns Error?;
```

#### 4.4 Transfer operations

```ballerina
# uploadFile/downloadFile move a local file on disk; both paths are full paths
# including the file name. uploadContent takes in-memory content (byte[]/string
# written as-is, xml serialized, map<json> serialized as JSON). uploadFromStream
# requires contentLength because Azure Files pre-allocates a file at a fixed
# size before content is written into its ranges. The transfer methods chunk
# internally; downloads stream rather than buffer.
remote function uploadFile(string sourcePath, string destinationPath, UploadOptions? options = ()) returns Error?;
remote function uploadContent(byte[]|string|xml|map<json> content, string destinationPath, UploadOptions? options = ()) returns Error?;
remote function uploadFromStream(stream<byte[], error?> content, int contentLength, string destinationPath, UploadOptions? options = ()) returns Error?;
remote function downloadFile(string sourcePath, string destinationPath, DownloadOptions? options = ()) returns Error?;
remote function getFileContent(string path, DownloadOptions? options = ()) returns stream<byte[], Error?>|Error;
```

#### 4.5 Copy operations

```ballerina
# Copies are asynchronous; observe an in-flight copy with checkCopyStatus,
# which returns () when the file has never been a copy destination.
remote function copyFile(string sourcePath, string destinationPath, CopyOptions? options = ()) returns CopyInfo|Error;
remote function copyFileFromUrl(string sourceUrl, string destinationPath, CopyOptions? options = ()) returns CopyInfo|Error;
remote function checkCopyStatus(string path) returns CopyStatusInfo?|Error;
remote function abortCopy(string path, string copyId) returns Error?;
```

#### 4.6 Range operations

```ballerina
# uploadRange is a single Put Range (at most 4 MiB); the transfer methods
# above chunk internally.
remote function uploadRange(string path, int offset, byte[] content) returns Error?;
remote function clearRange(string path, int offset, int length) returns Error?;
remote function listRanges(string path, RangeListOptions? options = ()) returns Range[]|Error;
```

### 5. The `Listener` and `Caller`

Azure Files is not exposed as an Event Grid source, so the listener polls, following the model of `ballerina/ftp`. The core mode is **stateless dispatch**: each polling tick lists the watched path and fires `onFile` for every file present. No per-file state is kept, so the contract is that handlers consume files by processing them and then deleting or moving them out of the watched path; an unprocessed file fires again on the next poll. This mode is trivially restart-safe.

```ballerina
public type ListenerConfiguration record {|
    # The authentication configuration
    AuthConfig auth;
    # The watched path within the share; root by default
    string path = "/";
    # Polling interval in seconds
    decimal pollingInterval = 60;
    # Whether each tick lists the whole tree under `path`
    boolean recursive = true;
    # Regex on the file name; non-matching files are never dispatched
    string fileNamePattern?;
|};

public isolated class Listener {
    public isolated function init(string shareName, *ListenerConfiguration config) returns Error?;
    public isolated function attach(Service serviceRef, string[]|string? name = ()) returns error?;
    public isolated function 'start() returns error?;
    public isolated function gracefulStop() returns error?;
    public isolated function immediateStop() returns error?;
    public isolated function detach(Service serviceRef) returns error?;
}

# The service type declares the mandatory handler, so a missing or misspelled
# `onFile` is a compile error.
public type Service distinct service object {
    remote function onFile(FileInfo file, Caller caller) returns error?;
};

# Carries what a directory listing provides, the Listener's data source.
public type FileInfo record {|
    # Share-relative path, e.g. "/dir1/dir2/file.ext"
    string path;
    # File name only
    string name;
    # Size in bytes
    int sizeBytes;
    # Entity tag of the file
    string eTag;
    # Last-modified time
    time:Utc lastModified;
|};
```

A `Caller` is passed to each handler so it can act on the event's file without constructing a separate client. It forwards a curated share-scoped subset of `Client` (`downloadFile`, `getFileContent`, `uploadFile`, `uploadContent`, `deleteFile`, `copyFile`, `abortCopy`, `renameFile`, `createDirectory`, `deleteDirectory`, `list`) plus a non-remote `getShareName()`. Handlers pass the event's path explicitly, e.g. `caller->deleteFile(file.path)`.

A poll failure applies exponential backoff to the polling interval, capped at five minutes.

### 6. Errors

A distinct error hierarchy allows pattern-matching on specific failures:

```ballerina
public type ErrorDetail record {|
    # HTTP status; absent on client-side failures with no server exchange
    int httpStatus?;
    # The Azure error code, or a connector-defined identifier for client-side failures
    string errorCode;
|};

public type Error                    distinct error<ErrorDetail>;
public type NotFoundError            distinct Error;
public type ConflictError            distinct Error;
public type AuthorizationError       distinct Error;
public type PreconditionFailedError  distinct Error;
public type RangeNotSatisfiableError distinct Error;
public type QuotaExceededError       distinct Error;
public type ProcessingError          distinct Error;
```

Mapping keys on the Azure error code string, not the HTTP status alone: `ShareSizeLimitReached` (403) maps to `QuotaExceededError`, distinct from auth failures (also 403) mapping to `AuthorizationError`. The human-readable message becomes the Ballerina error's `message()` rather than being duplicated into the detail record.

### 7. Advanced surface (additive)

The design accounts for the full Azure Files capability set; the following arrive as additive methods and configuration on the same classes, kept out of the core surface so the common path stays small:

* **Authentication:** `EntraIdConfig` union (DefaultAzureCredential, managed identity, client secret, client certificate, workload identity). The connector sets the required `ShareTokenIntent.BACKUP` request intent implicitly.
* **Resilience and transport configuration:** a retry record mirroring the SDK's `RequestRetryOptions` (with the SDK's own defaults) plus proxy, TLS, and connection-pool settings.
* **`AdminClient`:** `getServiceProperties`, `setServiceProperties`, `getUserDelegationKey`, `generateAccountSas`.
* **`Client`:** share snapshots (`createShareSnapshot`, `listShareSnapshots`, `deleteShareSnapshot`, `listRangesDiff`); share and file leases (acquire, renew, release, break, change); SMB handle enumeration and force-close; post-create property updates (`setShareProperties` quota/tier, `setFileProperties` including resize, `setDirectoryProperties`); SAS generation (`generateShareSas`, `generateSas`) and user-delegation SAS; stored access policies; SDDL permission get/create; NFS hard and symbolic links.
* **`Listener`:** a snapshot-diff mode that compares each poll against an in-memory `path` to etag snapshot and fires `onFileAdd`, `onFileDelete`, and `onFileModify` once per change; typed-content handlers (`onFileJson`, `onFileXml`, `onFileCsv`, `onFileText`); a generic `onFileChange` batch handler; an `onError` hook; and additional filters (`minFileAgeSeconds`, `maxBatchSize`).

### 8. Usage

Working with files in a share:

```ballerina
import ballerinax/azure.storage.files;

configurable files:ClientConfiguration filesConfig = ?;

public function main() returns error? {
    files:Client fileShare = check new ("invoices", filesConfig);

    check fileShare->uploadFile("./invoice-2026-07.pdf", "/2026/07/invoice.pdf");

    stream<files:Entry, files:Error?> entries = check fileShare->list("/2026/07");
    check entries.forEach(function(files:Entry entry) {
        // ...
    });

    check fileShare->close();
}
```

Reacting to files arriving in a share:

```ballerina
import ballerinax/azure.storage.files;

listener files:Listener invoiceListener = check new ("invoices",
    auth = {accountName: "myacct", accountKey: "..."},
    path = "/incoming",
    pollingInterval = 30
);

service on invoiceListener {
    remote function onFile(files:FileInfo file, files:Caller caller) returns error? {
        check caller->downloadFile(file.path, "./processed/" + file.name);
        // Consume the file so it does not fire again on the next poll.
        check caller->deleteFile(file.path);
    }
}
```

Share administration:

```ballerina
files:AdminClient admin = check new (auth = {accountName: "myacct", accountKey: "..."});
if !(check admin->shareExists("invoices")) {
    check admin->createShare("invoices", {quotaInGb: 100});
}
```

## Alternatives

* **Revamp `azure_storage_service` in place.** Rejected: the hand-written protocol layer remains the maintenance burden, and the combined Blob-plus-Files packaging contradicts the one-package-per-service pattern of the rest of the Azure ecosystem.
* **Generate a client from the REST/OpenAPI definition.** Rejected: the generated client would still leave Shared Key signing, SAS construction, retry, and chunked transfer to be implemented and maintained by hand; the official SDK already encapsulates all of it and tracks new API versions.
* **Surface the SDK's four-client chain directly** (`ShareServiceClient`, `ShareClient`, `ShareDirectoryClient`, `ShareFileClient`). Rejected: navigating a client chain to reach a file is SDK ergonomics, not Ballerina ergonomics. Two clients plus a combined `path` parameter keeps the common case to a single object and small signatures.
* **One combined package for Blob and Files.** Rejected: users pull one artifact per service everywhere else in the ecosystem, and Microsoft's own packaging already shares the common auth/retry layer between the two SDK artifacts, so separate Ballerina packages duplicate nothing.

## Testing

Azure Files has no local emulator (Azurite covers Blob, Queue, and Table only), so the test strategy is two-layered:

1. **Mock test group** (every PR, credential-free): the full surface tested against a localhost mock of the File REST service, wired through the `serviceUrl` override on the auth config. This includes canned error responses covering the Azure error-code to typed-error mapping.
2. **Live test group** (gated): the same surface against a real storage account, using organization secrets; skipped on fork PRs.

## Dependencies

* Azure SDK for Java: `com.azure:azure-storage-file-share` (and `com.azure:azure-identity` for the advanced Entra ID support)

## References

* [Azure Files documentation](https://learn.microsoft.com/en-us/azure/storage/files/storage-files-introduction)
* [Azure Files REST API](https://learn.microsoft.com/en-us/rest/api/storageservices/file-service-rest-api)
* [Azure SDK for Java, File Share client library](https://learn.microsoft.com/en-us/java/api/overview/azure/storage-file-share-readme)
* [Existing connector: `ballerinax/azure_storage_service`](https://central.ballerina.io/ballerinax/azure_storage_service/latest)
* [`ballerina/ftp`, the polling-listener precedent](https://central.ballerina.io/ballerina/ftp/latest)
