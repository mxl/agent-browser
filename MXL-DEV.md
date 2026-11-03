# mxl-dev fork

This fork keeps local dashboard-input fixes in separate branches and combines them in `mxl-dev`. The upstream base is agent-browser v0.41.1, commit `5868f7c05020e047c00e68e4abf9253cce864f41`.

## Branches

| Branch | Scope | Base |
| --- | --- | --- |
| `fix/stream-socket-tempdir` | Respect `TMPDIR` in Unix socket test fixtures | Upstream |
| `fix/stream-frame-geometry` | Preserve actual CDP frame geometry and map input to drawn pixels | Upstream |
| `fix/stream-binding-epoch` | Reject input from obsolete target/client bindings | Frame geometry branch |
| `feat/dashboard-plain-text-paste` | Bounded plain-text paste and keyboard handling | Binding epoch branch |
| `mxl-dev` | Integration branch with explicit merges | Upstream plus all fix branches |

The stacked branches make dependencies explicit. Compare a dependent branch against its immediate base when reviewing that change, rather than comparing every branch against upstream.

## Input behavior and limits

The dashboard pairs input geometry with the frame it has drawn. The stream server checks the binding token returned with human input before sending it to CDP. Older stream clients that omit this token cannot control the protected input path without adapting to the fork's protocol.

Plain-text paste uses the user's paste event rather than polling the clipboard or granting clipboard-read permissions. Text is limited to 65,536 UTF-8 bytes and inserted with CDP `Input.insertText`. Oversize input is rejected rather than truncated. Rich text, files, images, and remote-to-local clipboard synchronization are outside this change.

A binding token identifies the target/client binding, not a particular document. Same-target navigation needs separate qualification. Releases for an obsolete binding are dropped instead of being routed to a new target, so cleanup of held keys in the former target is not guaranteed. These checks do not hide pasted text from trusted local processes, the dashboard, CDP, or the destination page.

## Prepared Chrome stream visibility fix

`fix/stream-hidden-page-visibility` prepares a viewer-scoped focus-emulation change; it is not evidence that the integration branch or an installed binary contains the fix. While stream viewers are connected, Chrome requests focus emulation on the active CDP binding before screencasting and retains it across viewport restarts. Last-viewer disconnect, rebinding, and stream shutdown attempt cleanup on the exact old binding. Unsupported methods leave streaming available, and visibility is not guaranteed. This can trigger page focus/visibility handlers and background work. Other engines and existing binding-epoch/navigation/held-key limits are unchanged.

Validation must cover viewer lifecycle, viewport restarts, rebinding, unsupported-method fallback, and cleanup. Backend mock tests can verify CDP ordering and binding ownership, but do not establish real-browser visibility or application behavior. Browser validation and installation remain separate coordinated steps; this preparation does not authorize either.

## Build from the integration branch

Use Node.js 24 or later, Rust, and the pnpm version pinned in `package.json`.

```sh
git switch mxl-dev
pnpm install --frozen-lockfile --ignore-scripts
pnpm run build:dashboard
pnpm run build:native
```

Build the dashboard before Rust so the native binary embeds the built dashboard rather than a placeholder. The native build copies the result into the platform-specific file under `bin/`.

For macOS ARM64, verify the result before registering it globally:

```sh
test -x bin/agent-browser-darwin-arm64
bin/agent-browser-darwin-arm64 --version
```

The package version alone does not identify the fork. Record the branch commit and compare the installed binary's SHA-256 with the locally built binary.

## Test scope

Run the focused stream and dashboard-input tests before installation. The documentation checks also cover the changed protocol examples.

The broad upstream Rust suite includes `native::parity_tests::test_all_documented_actions_are_handled`, which launches a real Chrome instance despite not being marked ignored. It did not complete in local validation. A browser-free broad run must explicitly skip that test and use a disposable `HOME` and `TMPDIR`; preserve the intended Cargo/Rust toolchain cache locations separately. Do not describe that excluded run as a complete browser E2E qualification.

The fork does not change or disable the upstream parity test. Real-browser tests and replacement of running browser sessions require their own coordinated run.

## Installation precautions

A Git URL or package checkout without the built platform binary is insufficient: the upstream `postinstall` script can download an unmodified upstream release. Register or package only after the native build, and verify the installed executable's provenance.

With pnpm 11, local global registration uses `pnpm add --global .`, not the older `pnpm link --global` workflow. Lifecycle scripts are not needed once the local binary exists. A separate pnpm home keeps this registration apart from other globally installed tools:

```sh
PNPM_HOME="$HOME/.local/share/agent-browser-mxl-dev/pnpm" \
PATH="$HOME/.local/share/agent-browser-mxl-dev/pnpm/bin:$PATH" \
pnpm add --global --ignore-scripts .
```

The global command must resolve to that registration's entrypoint even if another package manager previously installed agent-browser earlier on `PATH`. The entrypoint can point directly to the locally built native binary, as the upstream installer does, to avoid a JavaScript launcher. Preserve the previous entrypoint before replacing a conflicting symlink, and verify the selected executable and native-binary hash afterward. Merely adding a package to a global directory does not establish which command your shell invokes.

Do not run `agent-browser upgrade` to update this fork: that command follows the package-manager release channel, not this Git branch. Update `mxl-dev`, rebuild it, and reinstall the verified local artifact.

Installing the CLI does not replace already-running daemons or dashboards. Coordinate their task ownership and any human handoff before a separate restart. A new CLI on disk is not proof that an existing browser session is using the new code.
