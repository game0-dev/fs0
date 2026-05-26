# fs0

## Workspace

This repository is a Rust 2024 workspace for a distributed filesystem, split into small crates:

- `crates/fs0-core`: shared protocol types, constants, hashes, compression, and typed errors.
- `crates/fs0-transport`: frame encoding, endpoint helpers, and RPC helpers.
- `crates/fs0-volume`: local volume metadata and byte storage.
- `crates/fs0-central`: central metadata and placement.
- `crates/fs0-storage`: storage server and data plane.
- `crates/fs0-client`: client library.
- `crates/fs0-cli`: command-line entrypoint.

Changes should live in the crate that owns the behavior. Do not move code into `fs0-core` casually, unless the type, constant, or helper is shared by multiple crates or is part of the wire protocol.

## Rust Conventions

- Modules are private by default; public exports should be explicit.
- Use `pub(crate)` unless an item is clearly part of the crate API.
- For ambiguous booleans or positional `Option` parameters, prefer enums, newtypes, or named structs.
- Inline format arguments when possible: `format!("volume {volume_id}")`.
- Use method references instead of redundant closures when the method form is clearer.
- Collapse `if` statements when possible to avoid unnecessary nesting.
- Prefer exhaustive `match` statements; avoid wildcard arms when adding a future variant should force a compile error.
- Prefer types that express meaning. Use enums for closed states, and use newtypes or dedicated structs for IDs, names, and paths that require validation instead of passing raw `String`/`u64` everywhere.
- Do not create many “one-line function” helpers. A one-line function that is called only once and does not express an important invariant adds navigation cost; write it directly at the call site.
- Helpers should extract real complexity, reuse logic, or name key invariants, not mechanically split code just to make functions appear shorter.
- Keep one blank line between different logical regions so readers can see phase boundaries. For example: argument validation, state loading, core computation, persistence writes, and response construction should be separated by blank lines.
- Do not insert extra blank lines between tightly related short statements, such as consecutive field initializers, consecutive simple assertions, or consecutive builder calls.
- If a function contains multiple logical phases, prefer blank lines and clear variable names to express structure; extract a helper only when a phase is complex enough or reusable.
- Avoid abstraction for abstraction’s sake. Make the current behavior clear first, then extract when duplication and complexity actually appear.
- Do not leave migration notes, dead code, commented-out old implementations, or temporary debug output.

## API Design

- Public APIs should make call sites as self-explanatory as possible. Avoid forcing callers to write hard-to-read calls like `foo(false, None, 0)`.
- If a small local change cannot remove positional bare booleans, `None`, or numeric literals, add precise parameter comments at the call site, for example `/*create*/ false`; the comment name must match the parameter name.
- New traits should have documentation explaining the trait’s role and the guarantees implementors must uphold.
- Do not use `#[async_trait]` or `#[allow(async_fn_in_trait)]` in traits as the default approach. When an async trait is needed, prefer an explicit signature returning `impl Future + Send`.
- Do not expose public struct fields casually. If invariants may need to be maintained later, prefer private fields with constructors/accessors.

## Errors And Results

- Use `fs0_core::Fs0Error` for all protocol-boundary and library errors.
- Use `fs0_core::Fs0Result<T>` directly; do not define crate-local `Result<T>` aliases.
- Do not use `anyhow` in library crates or wire-protocol paths.
- Keep errors structured so callers can match on variants.
- Convert external errors at crate boundaries into the most meaningful `Fs0Error` variant.
- Avoid `unwrap` and `expect` outside tests, examples, and truly impossible invariants.
- Error messages should include debugging context, such as path, volume id, chunk id, and expected/actual values.
- Do not put semantically different failures into `Internal`. Errors that callers can recover from or branch on should have explicit variants.
- When using `ok_or_else`/`map_err`, keep error construction simple if it is not expensive; do not create noisy code for stylistic uniformity.

## Protocol Rules

- Types in `fs0-core/src/protocol.rs` are wire contracts.
- Any protocol shape change must update the postcard roundtrip tests in `crates/fs0-core/tests/core.rs`.
- Registration and authentication should belong to protocol entrypoint enums, not temporary side channels.
- Do not add capability, lease, migration, or auth fields unless the current protocol model requires them.
- Public constants belong in `fs0-core/src/lib.rs`; downstream crates should not duplicate shared constants.
- Protocol entrypoint enums are the main communication entrypoints. When adding a request/response, first decide whether it belongs to the control plane, data plane, or session push.
- Do not create meaningless wrapper structs around enum variants. Extract a struct only when it is reused, has many fields, or needs independent documentation/validation.
- When changing protocol field names, also check client, storage, central, transport, and core roundtrip tests.

## Storage And Hashing

- Raw content hashes identify raw bytes.
- Compressed hashes validate compressed bytes.
- Storage should not silently rewrite or reinterpret content identity.
- Volume metadata format constants belong in `fs0-core`.
- Do not add database migrations unless explicitly requested.
- SQLite schema changes should be intentional and keep metadata invariants clear.

## Formatting And Linting

- Do not use `cargo fmt` casually. If formatting is needed, use Cargo or package-scoped formatting. Do not invoke bare `rustfmt` without workspace edition context.
- When the workspace is loadable, run `cargo check` or `cargo test` for the changed crate before handoff.
- If Cargo cannot load the workspace because of unrelated missing members or local state, report the blocker clearly instead of editing unrelated files.
- Do not perform unrelated formatting. Only format the crate or files touched by the current change.

## Dependencies

- Do not add dependencies unless necessary.
- Put shared or likely-to-be-reused dependencies in the root workspace `Cargo.toml`.
- Dependency changes should be committed together with the code that requires them.

## Git Hygiene

- Inspect the worktree before modifying files that may contain user changes.
- Do not revert changes you did not make unless explicitly requested.
- Keep commits focused by crate or behavior.
- Use concise commit messages:
  - `core: update protocol auth API`
  - `core: split protocol errors`
  - `transport: align with core API`
  - `volume: align with core constants`

## Documentation

- Prefer clear API names over comments that compensate for unclear names.
- Add comments for invariants, protocol compatibility, data layout, or non-obvious safety constraints.
- Do not write comments that merely restate the code.
- README and docs updates should be scoped to user-visible behavior.
