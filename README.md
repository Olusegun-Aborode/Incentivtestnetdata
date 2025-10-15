# Goldsky CLI Reference

This repository contains documentation and examples for using the Goldsky CLI.

## Installation

To install the Goldsky CLI, you can use the following command:

```bash
curl https://goldsky.com | sh
```

Note: If the installation requires sudo privileges, you may need to provide your administrator password.

## Basic Usage

```bash
goldsky <cmd> args
```

## Available Commands

| Command | Description |
|---------|-------------|
| `goldsky` | Get started with Goldsky |
| `goldsky login` | Log in to Goldsky to enable authenticated CLI commands |
| `goldsky logout` | Log out of Goldsky on this computer |
| `goldsky subgraph` | Commands related to subgraphs |
| `goldsky project` | Commands related to project management |
| `goldsky pipeline` | Commands related to Goldsky pipelines |
| `goldsky dataset` | Commands related to Goldsky datasets |
| `goldsky indexed` | Analyze blockchain data with indexed.xyz |
| `goldsky secret` | Commands related to secret management |
| `goldsky telemetry` | Commands related to CLI telemetry |

## Options

| Option | Description | Type | Default |
|--------|-------------|------|---------|
| `--token` | CLI Auth Token | string | "" |
| `--color` | Colorize output | boolean | true |
| `-v, --version` | Show version number | boolean | |
| `-h, --help` | Show help | boolean | |

## Getting Started

To get started with Goldsky, follow these steps:

1. Install the Goldsky CLI
2. Log in to your Goldsky account: `goldsky login`
3. Create or manage your projects: `goldsky project`

For more detailed information, check the examples directory in this repository.