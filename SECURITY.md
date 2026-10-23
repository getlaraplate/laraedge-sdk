# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately, not in public issues or pull requests.

Use GitHub private vulnerability reporting for this repository: open the **Security** tab of
`getlaraplate/laraedge-sdk` and choose **Report a vulnerability**. Include the affected version or commit, a
description of the impact and, if you can, steps to reproduce.

You will get an acknowledgement, and we will keep you informed while the issue is investigated and fixed. Please allow
time for a fix before disclosing the problem publicly.

## Supported versions

The SDK has no stable release yet. Fixes are made on `main`.

## Scope

This repository holds the plugin protocol, the handshake (cookie check, socket and named pipe creation and
permissions), the manifest and the Go SDK. Vulnerabilities in the AGPL agent belong to `getlaraplate/laraedge` and are
reported the same way there. Vulnerabilities in a third-party plugin belong to its author.
