# Security Policy

Pulsar Lite is a local development and testing broker. It is not intended to
provide production-grade authentication, authorization, tenant isolation, or
network hardening.

## Reporting a Vulnerability

Please report security issues privately by opening a GitHub security advisory
for this repository. If advisories are not available, contact the repository
maintainers through the GitHub project.

Do not open a public issue for a suspected vulnerability until the report has
been triaged.

## Supported Versions

The project currently supports the `main` branch. No long-term support release
branches are maintained.

## Scope

Security reports are most useful when they affect:

- Unexpected remote code execution.
- Unauthorized broker control or data access beyond documented limitations.
- Dependency vulnerabilities in the shipped broker or Python package.
- Unsafe default behavior that affects local development environments.

Reports about missing production hardening are usually considered out of scope
unless they contradict documented behavior.
