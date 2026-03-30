# Security Policy

## Supported Versions

We currently only support the latest version of the project.

| Version | Supported          |
| ------- | ------------------ |
| latest  | :white_check_mark: |

## Security Measures

- **Static Analysis**: [gosec](https://github.com/securego/gosec) runs on every PR and push to main
- **Vulnerability Scanning**: [govulncheck](https://pkg.go.dev/golang.org/x/vuln/cmd/govulncheck) checks for known Go vulnerabilities
- **Fuzz Testing**: [ClusterFuzzLite](https://google.github.io/clusterfuzzlite/) continuously fuzzes codec and store operations
- **Code Scanning**: GitHub CodeQL analysis on every PR
- **Dependency Pinning**: All CI actions and tools are pinned by hash
- **Dependency Updates**: Dependabot monitors for outdated and vulnerable dependencies

## Reporting a Vulnerability

We use GitHub's **[Private Vulnerability Reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)**.

Please **do not** open a public issue for security bugs. Instead:

1. Go to the **[Security](https://github.com/rbaliyan/config/security)** tab of this repository.
2. Click on **[Advisories](https://github.com/rbaliyan/config/security/advisories)** on the left sidebar.
3. Click **Report a vulnerability**.

This allows you to share the details privately with the maintainers.

## Response Timeline

- **Acknowledgment**: Within 48 hours
- **Initial assessment**: Within 1 week
- **Fix or mitigation**: Depends on severity, targeting 30 days for critical issues
