# LeaderLessStateMachine

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
  - [Prerequisites](#prerequisites)
  - [Clone the Repository](#clone-the-repository)
  - [Build the Project](#build-the-project)
- [Configuration](#configuration)
<!-- - [Usage](#usage)
  - [Running the State Machine](#running-the-state-machine)
  - [Example Usage](#example-usage)
- [Testing](#testing)
  - [Unit Tests](#unit-tests)
  - [Integration Tests](#integration-tests)
  - [Performance Tests](#performance-tests)
  - [Benchmarking with RocksDB](#benchmarking-with-rocksdb)
- [Comparison with RocksDB](#comparison-with-rocksdb) -->
- [Contributing](#contributing)
  - [How to Contribute](#how-to-contribute)
  - [Contribution Guidelines](#contribution-guidelines)
- [License](#license)
- [Contact](#contact)
- [Acknowledgements](#acknowledgements)

## Introduction

**LeaderLessStateMachine** is a leaderless state machine implementation based on the [EPaxos](https://www.cs.cornell.edu/projects/epaxos/) protocol, designed for the [TemplateDB](https://github.com/devillove084/TemplateDB) project. This project aims to deliver a highly available and fault-tolerant distributed state machine solution by eliminating the single leader node, thereby enhancing concurrency and reducing latency.

## Features

- **Leaderless Architecture**: Utilizes the EPaxos consensus protocol to remove the single leader bottleneck, increasing fault tolerance and scalability.
- **Storage Abstraction with OpenDAL**: Integrates [OpenDAL](https://github.com/opendal/opendal) to provide a unified storage interface supporting multiple backend storage systems.
- **Custom RPC Framework**: Implements a lightweight RPC framework tailored for fine-grained network control and simulation of network failures, facilitating robust testing and debugging.
- **Comprehensive Testing Suite**: Includes extensive unit tests, integration tests, and performance benchmarks to ensure system reliability and optimal performance.
- **Benchmarking with RocksDB**: Incorporates [RocksDB](https://github.com/facebook/rocksdb) as a benchmark group for comparative performance and functionality testing.

## Architecture

The architecture of **LeaderLessStateMachine** is composed of the following key components:

1. **EPaxos State Machine**: The core component that implements the EPaxos protocol to achieve leaderless distributed consensus.
2. **OpenDAL Storage Layer**: Provides an abstracted storage interface, allowing flexibility to switch between different storage backends seamlessly.
3. **Custom RPC Framework**: Manages inter-node communication with the ability to simulate network conditions such as latency and partitioning.
4. **Testing Suite**: Encompasses unit tests for individual components, integration tests for module interactions, and performance tests to evaluate system throughput and latency.
5. **RocksDB Benchmark Group**: Serves as a baseline for performance comparison, highlighting the advantages and potential trade-offs of the leaderless approach.

## Installation

### Prerequisites

Ensure you have the following installed on your system:

- [Rust](https://www.rust-lang.org/) version 1.85 or higher
- [Cargo](https://doc.rust-lang.org/cargo/) package manager

### Clone the Repository

```bash
git clone https://github.com/yourusername/LeaderLessStateMachine.git
cd LeaderLessStateMachine
```

### Build the Project

Build the project in release mode for optimized performance:

```bash
cargo build --release
```

### Configuration

The project uses a configuration file config.toml to manage settings. A default configuration is provided, which can be customized as needed.

```bash
[storage]
backend = "OpenDAL" # Options: "TemplateDB", "RocksDB"

[rpc]
port = 8080
simulate_fault = true # Enable network fault simulation

[testing]
enable_rocksdb = true # Enable RocksDB benchmark
```

You can modify the config.toml to suit your environment and requirements. Additional configuration options can be added to fine-tune the behavior of the state machine, RPC framework, and storage layer.

### Contributing

Contributions are welcome! Whether it's bug fixes, feature enhancements, or documentation improvements, your input is valuable to the project.
How to Contribute

### License

This project is licensed under the MIT License. You are free to use, modify, and distribute this software as per the terms of the license.
