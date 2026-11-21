# bughound

Bughound is a pi-studio extension that takes routine test selection, reruns, and failure reporting off LLM agents. It watches enabled workspaces, waits for edits to settle, and runs the checks related to changed files. Its goal is to keep a live broken-tests card so agents can start with the failures already in view and spend fewer turns and tokens finding them.

Today, each workspace can opt into Python or JavaScript/TypeScript checks. The goal is to discover supported suites and offer them in settings, keep the test-to-source index current as files appear, and clear each reported failure when its check passes. Bughound is an extension today; a standalone daemon is a possible future form. See the [project testament](docs/testament.md) for the full purpose and scope.

## Branching, versioning, and tags

- Use `dev` for integration and `master` for releases. `master` is this repository's default branch and serves the role of pi-studio's `main`.
- Branch from the latest `dev` as `<type>/<slug>-<thread-id>`, using `feat`, `fix`, `refactor`, `docs`, `test`, or `chore`. Rebase on the latest `dev` before merging. Preserve a merge commit (`--no-ff`) with the subject `merge: <what the branch delivers>`. Do not commit directly to either trunk. Merge, tag, or publish only when the human asks.
- Before a merge, run `cargo test --locked`, `cargo build --release --locked`, and the two [pi-studio integration checks](README.md#check-it). For the integration checks, set `PI_STUDIO_DIR` to a pi-studio source checkout with its dependencies installed and point `BUGHOUND_BIN` at the release binary.
- Use ZeroVer (`0.MINOR.PATCH`). Bump minor and reset patch for new features or breaking changes; bump patch for fixes, documentation, and maintenance. Call out breaking changes and any required migration in the release notes. Only bump versions when preparing a release.
- Prepare the release on a branch from the latest `dev`. Pick the next unused version above the current version and all existing release tags. Update `Cargo.toml` (the source of truth) and `plugin.json` together, and let Cargo refresh the root package version in `Cargo.lock` before running the locked checks. Commit all three as `chore(release): bump version to <version>`.
- Merge the release branch into `dev`, then merge `dev` into `master` with `--no-ff` and the subject `merge: dev into master for <version>`. If `dev` moves before the release merge, refresh the release candidate and rerun the checks. Create an annotated `v<version>` tag on the `master` release merge commit. When authorized to publish, push `master` before the tag, then bring `master` back into `dev`. Never move a published tag or reuse a published version. Bughound has no tag-triggered packaging workflow.
