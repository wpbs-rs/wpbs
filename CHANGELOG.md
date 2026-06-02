# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

#### Core

- Configuration system supporting system services, as well as per-plugin environment
variables, custom settings, and permissions.
- Registry support for resolving, downloading, and caching plugins.
- Key-value database ([`fjall`](https://github.com/fjall-rs/fjall)) for persistent
plugin state storage.

#### Runtime

- WebAssembly Component Model-based plugin system (using [`wasmtime`](https://github.com/bytecodealliance/wasmtime)).

#### Services

- Discord service supporting Gateway events, HTTP requests, and interaction registration.
- Job scheduler service for executing cron-based tasks.

[Unreleased]: https://github.com/wpbs-rs/wpbs/compare/main...feat/new-plugin-api
