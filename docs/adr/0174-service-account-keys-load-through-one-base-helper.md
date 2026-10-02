<!--
Copyright 2026 The flink-gcp authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# ADR-0174: Service-account keys load through one base helper

- Status: Accepted
- Date: 2026-10-03
- Issues: [#1562](https://github.com/flink-gcp/flink-connector-gcp/issues/1562) (before
  [#1542](https://github.com/flink-gcp/flink-connector-gcp/issues/1542))
- Modules: base (`ServiceAccountKeys`), bigquery, bigtable, cloudtasks, firestore, pubsub, spanner
- Current behavior: the `ServiceAccountKeys` javadoc and `.agents/references/modules/flink-connector-gcp-base.md`

## Context

Every connector accepts an optional path to a service-account key file and loads it on the component that opens the client.
Six modules carried their own loader, and the six copies had the same core.
A `null` path leaves application-default credentials in effect.
The file is parsed with `ServiceAccountCredentials.fromStream` and scoped with `createScoped`.
Any `IOException` or `RuntimeException` becomes `IOException("Failed to load the configured <Product> service-account key file.")`, with no path and no cause.

The failure rule is the part that matters for security.
A path can expose a mounted secret's name, and a parser exception can echo credential material, so neither reaches the message or the cause chain.
Each copy held that rule with its own tests (three cases in each connector's credentials test), and the Datastore sink ([#1542](https://github.com/flink-gcp/flink-connector-gcp/issues/1542)) would have added a seventh copy.

The copies differed in two ways only.
Their scopes come from the product: most use the client's default service scopes, BigQuery uses the cloud-platform scope for BigQuery and Cloud Storage together, and Bigtable unions the scopes of up to three client families.
Their return shape follows the client builders: Spanner, Firestore and BigQuery's REST and Cloud Storage clients take `GoogleCredentials`, while Pub/Sub, Cloud Tasks, Bigtable and BigQuery's three gRPC client sites take a gax `CredentialsProvider` and wrap the credentials in a `FixedCredentialsProvider`.

## Decision

**`base.auth.ServiceAccountKeys.load(path, scopes, product)` holds the shared core and the failure rule.**
It returns `GoogleCredentials`, or `null` for a `null` path.
The caller passes the scopes it needs and the product name the message carries, so each connector's message is unchanged byte for byte.

**Each connector keeps its own `*Credentials` entry point as a thin caller.**
The entry point decides the scopes and, where its client builder takes a provider, wraps the result.
The connector tests keep what is the connector's own: the scopes, the wrapping, and the product name in the message.
`ServiceAccountKeysTest` holds the failure rule once, including the `RuntimeException` arm, which two measured inputs reach (see Evidence).

The rule's rationale lives in the helper's Javadoc; each `*Credentials` class points there instead of restating it, on its loader's Javadoc or, for Bigtable's three loaders, on the class.

**Base declares `google-auth-library-oauth2-http` directly.**
`gax` already brings it in transitively, so the runtime dependency tree of every module and SQL jar is unchanged.

## Evidence

Measured on 2026-10-03 against `google-auth-library-oauth2-http` 1.51.0, the version libraries-bom 26.87.0 resolves, by feeding inputs to `ServiceAccountCredentials.fromStream`:

- `{}`, `{"type":"service_account"}` and malformed PKCS#8 material throw `IOException`.
- `[]`, a bare string and a number throw `IllegalArgumentException`; a non-string `type` (`{"type":1}`) throws `ClassCastException`, because the parser casts the field to `String`.
- `Path.of` on a path containing NUL throws `InvalidPathException`, whose message echoes the path. `Path.of` runs inside the `try`-with-resources header, so the catch sees it.

Both `RuntimeException` inputs carry a precondition in `ServiceAccountKeysTest` that asserts the raw exception type, so a library change that turns either into an `IOException` fails the test rather than silently leaving the arm unexercised.

## Alternatives declined

- **Return a gax `CredentialsProvider`.**
  Spanner, Firestore and BigQuery's REST and Cloud Storage clients take `GoogleCredentials`, so they would unwrap what base wrapped.
  Credentials are the shape every caller can use, and wrapping is one expression where a gRPC client needs it.
- **Move the `*Credentials` classes into base whole.**
  The scopes are product knowledge (Bigtable's union reads three client settings classes), and base would gain a dependency on every client library to hold them.
- **Take the scopes as a supplier evaluated inside the sanitized region, and only for a non-null path.**
  Four copies evaluated `getDefaultServiceScopes()` inside their `try`, after the `null` check; they now evaluate it on every call, ADC included, before the helper runs.
  These are static getters over a constant list that do not throw, and Bigtable's scope lookup already ran on every call outside its `try`, so the supplier would guard nothing.

## Consequences

- A new connector, starting with the Datastore root of [#1542](https://github.com/flink-gcp/flink-connector-gcp/issues/1542), calls the helper instead of copying the loader.
- A change to the failure rule is made and tested in one place.
  Each connector's credentials test still pins its product's message, and so do 21 other test classes on the connectors' runtime paths (at least one per connector, counted 2026-10-03), so a caller passing the wrong name fails.
