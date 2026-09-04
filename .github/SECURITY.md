# Security Policy

## Reporting a vulnerability

Please don't open public issues for security vulnerabilities.

Report privately through either channel:

- **GitHub Security Advisories**: use "Report a vulnerability" on this
  repository (preferred), or
- **Email**: support@vaelii.com with "SECURITY" in the subject line.

Include what you can: affected version or commit, reproduction steps, and
impact. Please practice coordinated disclosure — report privately first and
allow time for a fix to land before publishing details.

## Supported versions

Only the latest release (and `main`) receive security fixes.

## Scope

`vaelii-sqlite` is an Apache-2.0 adapter on the vaelii snapshot protocol: it writes a
KB image to a single SQLite file and reads it back (`vaelii.sqlite.snapshot`). It
depends on the [vaelii](https://github.com/vaelii/vaelii) engine and is never
depended on by it; it has no network surface of its own — the image is a local
file. A running deployment's exposed surface (the browser `/eval` endpoint, the
headless API daemon) belongs to core — see core's full
[security policy](https://github.com/vaelii/vaelii/blob/main/.github/SECURITY.md),
which also tracks third-party advisories for the shared dependency stack.

Adapter-specific reports worth sending:

- anything that lets untrusted KB content reach the SQLite (xerial) driver as
  executable SQL/DDL rather than a bound parameter;
- an image that reads back as trusted when its records fingerprint or index-layout
  version does not match — the validate-or-discard check is the whole safety
  property (`snapshot/decision`);
- anything that corrupts or leaks image sections across `image` names, or that
  reads a snapshot file from an untrusted source as if it were the store's own.
