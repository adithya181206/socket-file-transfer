# Socket File Transfer System

> A production-grade TCP/IP socket file transfer system featuring multi-threaded client concurrency, custom 4-byte length-prefixed binary protocol framing, and end-to-end SHA-256 data integrity verification engineered in pure Python 3.

[![Python](https://img.shields.io/badge/Python-3.9%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Dependencies](https://img.shields.io/badge/Dependencies-Zero%20Mandatory-success?style=for-the-badge)](requirements.txt)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge)](LICENSE)

---

## Overview & Key Engineering Highlights

This project implements a high-performance, resilient client–server file transfer suite built directly on raw TCP streams using Python's standard library. Designed to demonstrate systems-level network engineering concepts, it handles simultaneous bidirectional file transfers across independent worker threads with explicit packet boundary framing, automatic collision resolution, and cryptographic verification.

- **Raw TCP Stream Handling**: Implements direct stream socket operations (`socket.AF_INET`, `socket.SOCK_STREAM`) with manual 1024-byte chunk buffering (`CHUNK_SIZE = 1024`) and byte-exact buffer reassembly (`_recv_exact()`), bypassing high-level HTTP/FTP abstractions.
- **Multi-Threaded Server Concurrency (`threading.Thread` & `threading.Lock`)**: Spawns an isolated daemon worker thread per incoming client connection (`threading.Thread(target=handle_client, daemon=True)`). Active client tracking, server start timestamps, and runtime statistics are coordinated using a synchronized `threading.Lock`.
- **4-Byte Big-Endian Length-Prefixed Framing**: Solves TCP message boundary ambiguity ("sticky packets" and chunk fragmentation) at the application layer by prepending every JSON control message with a 4-byte big-endian unsigned integer (`struct.pack('>I', len)`).
- **End-to-End Cryptographic Integrity**: Computes SHA-256 checksums in streaming, memory-efficient blocks (`hashlib.sha256`) on the client prior to sending and on the server after receipt, detecting transmission corruption with zero buffer exhaustion.
- **Defensive Edge-Case Engineering**:
  - **Path Traversal Protection**: `sanitize_filename()` strips directory paths, replaces directory separators (`/`, `\\`), and rejects relative parent lookups (`..`).
  - **Duplicate Handling**: Server automatically detects existing files and appends collision suffixes (`file_1.ext`, `file_2.ext`) instead of overwriting data.
  - **Aborted Transfer Recovery**: Interrupted transfers automatically trigger clean-up that renames incomplete writes to `.partial` files.
  - **Port Reuse**: Configured with `socket.SO_REUSEADDR` to avoid lingering `TIME_WAIT` lockups and provide actionable startup diagnostics.

---

## System Architecture & Protocol Flow

### Concurrency Model

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SINGLE MACHINE (localhost)                      │
│                                                                         │
│  ┌──────────────┐  TCP:5000   ┌──────────────────────────────────────┐ │
│  │  Client 1   │◄───────────►│                                      │ │
│  └──────────────┘             │  SERVER  (server/server.py)          │ │
│                               │  ┌────────────────────────────────┐  │ │
│  ┌──────────────┐  TCP:5000   │  │  Main Thread: accept() loop    │  │ │
│  │  Client 2   │◄───────────►│  ├────────────────────────────────┤  │ │
│  └──────────────┘             │  │  Thread-1 → Client 1 handler   │  │ │
│                               │  │  Thread-2 → Client 2 handler   │  │ │
│  ┌──────────────┐  TCP:5000   │  │  Thread-N → Client N handler   │  │ │
│  │  Client N   │◄───────────►│  └────────────────────────────────┘  │ │
│  └──────────────┘             │                                      │ │
│                               │  Storage: server/received_files/     │ │
│                               └──────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

### Custom Wire Protocol Specification

Every control message in the application layer uses **length-prefixed framing**:

```
[ 4-byte big-endian integer (length) ] [ UTF-8 JSON payload of exact length ]
```

- **Header**: 4 bytes, `>I` format (`struct.pack('>I', len(encoded))`).
- **Control Payload**: Valid JSON dictionary specifying commands, statuses, filenames, or checksums.
- **Binary Data Stream**: Raw byte stream transferred in discrete 1024-byte blocks (`CHUNK_SIZE`) immediately following the metadata handshake.

### Command Table

| Command | Direction | Purpose | Payload Details |
| :--- | :--- | :--- | :--- |
| `UPLOAD` | Client → Server | Initiates a file upload | `{"command": "UPLOAD"}` |
| `DOWNLOAD` | Client → Server | Requests a file download | `{"command": "DOWNLOAD", "filename": "<name>"}` |
| `LIST` | Client → Server | Queries server file inventory | `{"command": "LIST"}` |
| `STATUS` | Client → Server | Queries server telemetry/health | `{"command": "STATUS"}` |
| `EXIT` | Client → Server | Gracefully terminates session | `{"command": "EXIT"}` |

### Step-by-Step Handshake Sequences

#### Upload Handshake Sequence

```
Client                                                  Server
  │                                                        │
  ├─────── [4B Header] {"command": "UPLOAD"} ─────────────►│
  │◄────── [4B Header] {"status": "READY_FOR_METADATA"} ──┤
  │                                                        │
  ├─────── [4B Header] {filename, filesize, checksum} ────►│ (Validates path & disk)
  │◄────── [4B Header] {"status": "READY", "stored_as"} ──┤
  │                                                        │
  ├═══════ [1024-byte Raw Binary File Chunks] ════════════►│ (Writes to disk)
  │                                                        │ (Computes SHA-256)
  │◄────── [4B Header] {"status": "UPLOAD_SUCCESS", ...} ─┤
  │                                                        │
```

1. **Initiation**: Client transmits `{"command": "UPLOAD"}`.
2. **Metadata Request**: Server replies with `{"status": "READY_FOR_METADATA"}`.
3. **Metadata Transmission**: Client sends target filename, size in bytes, and pre-computed SHA-256 checksum.
4. **Validation**: Server sanitizes filename, prevents path traversal, resolves collisions, and replies with `{"status": "READY"}`.
5. **Data Streaming**: Client streams the file in 1024-byte chunks with real-time progress updates.
6. **Integrity Check**: Server calculates SHA-256 over received bytes and confirms `{"status": "UPLOAD_SUCCESS"}` or flags `{"status": "CHECKSUM_MISMATCH"}`.

#### Download Handshake Sequence

```
Client                                                  Server
  │                                                        │
  ├─────── [4B Header] {"command": "DOWNLOAD", filename} ─►│ (Inspects received_files/)
  │◄────── [4B Header] {"status": "FOUND", filesize, chk} ─┤
  │                                                        │
  ├─────── [4B Header] {"status": "READY"} ───────────────►│
  │                                                        │
  │◄══════ [1024-byte Raw Binary File Chunks] ═════════════┤ (Streams file bytes)
  │                                                        │
  ▼ (Client computes SHA-256 and verifies locally)
```

1. **Request**: Client queries `{"command": "DOWNLOAD", "filename": "<filename>"}`.
2. **Lookup**: Server validates file presence. If missing, responds with `{"status": "NOT_FOUND"}`; if present, responds with `{"status": "FOUND", "filesize": N, "checksum": "<sha256>"}`.
3. **Ready Signal**: Client checks local storage path (auto-renaming if conflict exists) and returns `{"status": "READY"}`.
4. **Data Streaming**: Server transmits raw binary chunks to the client socket.
5. **Verification**: Client calculates local SHA-256 digest upon transfer completion and verifies byte parity against server metadata.

---

## Sample Terminal Output (Demo)

### Interactive Client Menu & Upload Session

```text
==================================================
  Multi-Threaded File Transfer Client
==================================================
Server IP   [default: 127.0.0.1]: 
Server Port [default: 5000]: 

[*] Targeting server at 127.0.0.1:5000

╔══════════════════════════════════════════════╗
║      Socket File Transfer Client v1.0       ║
╠══════════════════════════════════════════════╣
║  1. Upload a file                           ║
║  2. Download a file                         ║
║  3. List files on server                    ║
║  4. Server status                           ║
║  5. Exit                                    ║
╚══════════════════════════════════════════════╝

Choose an option [1-5]: 1
Enter path to file to upload: ../test-files/test1.txt
[*] Computing SHA-256 checksum of 'test1.txt' ...
[*] Checksum: a5600c32b508f7dd...  Size: 22 B

[*] Uploading 'test1.txt' ...
Uploading: [=============================>] 100.0%  22 B/22 B  2.4 KB/s   

[✓] Upload successful!
    File stored as : test1.txt
    Bytes          : 22
    Elapsed        : 0.01 s
    Speed          : 0.15 MB/s
    Integrity      : SHA-256 verified ✓
```

### File Listing & Download Session

```text
Choose an option [1-5]: 2
[*] Fetching file list from server ...

#     Filename                            Size  Modified
──────────────────────────────────────────────────────────────────────
1     largefile.bin                    5.00 MB  2026-09-26 10:54:12
2     test1.txt                           22 B  2026-09-26 10:15:30
3     test2.txt                        3.24 KB  2026-09-26 10:15:44

Enter filename to download (exact name): test1.txt

[*] File found: 'test1.txt'  Size: 22 B
[*] Downloading 'test1.txt' ...
Downloading: [=============================>] 100.0%  22 B/22 B  3.1 KB/s   

[✓] Download successful!
    Saved to   : C:\Users\...\client\downloaded_files\test1.txt
    Bytes      : 22
    Elapsed    : 0.01 s
    Speed      : 0.18 MB/s
    Integrity  : SHA-256 verified ✓
```

### Server Telemetry Output

```text
Choose an option [1-5]: 4

╔══════════════════════════════════════╗
║          SERVER STATUS               ║
╠══════════════════════════════════════╣
║  Host          : 0.0.0.0             ║
║  Port          : 5000                ║
║  Uptime        : 00h 14m 32s         ║
║  Active clients: 1                   ║
║  Files stored  : 3                   ║
╚══════════════════════════════════════╝
```

---

## Project Structure

```text
socket-file-transfer/
├── client/
│   ├── downloaded_files/       # Storage directory for retrieved files
│   ├── files_to_send/          # Local files prepared for dispatch (test1.txt, test2.txt)
│   └── client.py               # Interactive CLI client implementation
├── dashboard/                  # Optional web monitoring dashboard (Flask)
│   ├── app.py                  # Dashboard application backend
│   ├── check_syntax.py         # Syntax verification script
│   └── patch_html.py           # Dashboard UI styling patcher
├── logs/                       # Automated application runtime logs
│   ├── client.log              # Client-side execution and error records
│   └── server.log              # Server-side connection and transfer logs
├── server/
│   ├── received_files/         # Storage repository for incoming uploaded files
│   ├── file_meta.json          # Server metadata registry for tracking
│   └── server.py               # Multi-threaded TCP socket server
├── shared/
│   └── protocol.py             # Protocol framing, network constants, and utility helpers
├── test-files/
│   ├── generate_large_file.py  # Utility generator for 5 MB pseudo-random binary files
│   ├── test1.txt               # Lightweight text test fixture (22 B)
│   └── test2.txt               # Medium text test fixture (~3.3 KB)
├── .gitignore                  # Git tracking exclusion configuration
├── demo_run.py                 # Automated end-to-end operation demonstration script
├── README.md                   # System documentation and protocol specification
├── requirements.txt            # Project dependencies (zero mandatory dependencies)
├── run_client.bat / .sh        # Convenience launch scripts for client
├── run_dashboard.bat / .sh     # Convenience launch scripts for dashboard
├── run_server.bat / .sh        # Convenience launch scripts for server
└── test_e2e.py                 # Automated integration test script
```

---

## Quick Start & Installation

### Prerequisites

| Requirement | Minimum Version | Notes |
| :--- | :--- | :--- |
| **Python** | $\ge$ 3.9 | Standard library only (`socket`, `threading`, `struct`, `hashlib`, `json`). |
| **Dependencies** | *None* | Zero mandatory external packages required. |
| **Optional Enhancements** | `tqdm >= 4.64.0` | Enables high-fidelity terminal progress meters. |

```bash
# Optional: install tqdm for enhanced progress bars
pip install -r requirements.txt
```

### Step 1: Clone the Repository

```bash
git clone https://github.com/adithya181206/socket-file-transfer.git
cd socket-file-transfer
```

### Step 2: Start Server & Client Instances

#### Option A: One-Click Launch Scripts

**On Windows:**
```bat
:: Terminal 1: Launch Server
run_server.bat

:: Terminal 2+: Launch One or More Clients (open separate windows)
run_client.bat
```

**On Linux / macOS:**
```bash
chmod +x run_server.sh run_client.sh

# Terminal 1: Launch Server
./run_server.sh

# Terminal 2+: Launch One or More Clients (open separate windows)
./run_client.sh
```

#### Option B: Manual Execution via Python CLI

**Terminal 1 — Server:**
```bash
cd server
python server.py
# (On Linux / macOS: python3 server.py)
```

**Terminal 2 — Client:**
```bash
cd client
python client.py
# (On Linux / macOS: python3 client.py)
```

*Note: When prompted for Server IP and Port, press `Enter` to use default values (`127.0.0.1:5000`).*

### Step 3: Generate the 5 MB Benchmark Test File

```bash
cd test-files
python generate_large_file.py
# (On Linux / macOS: python3 generate_large_file.py)
```

This generates `test-files/largefile.bin` containing 5 MB of cryptographically uncompressible bytes (`os.urandom`) written in 1 MB chunks to benchmark raw socket throughput.

---

## Verification & Edge-Case Testing

### Test 1: Single Upload and Download Verification
Verify fundamental protocol transactions using the provided test fixtures:
1. Launch `server.py` and `client.py`.
2. From the client menu, select `1` (Upload) and provide path `test-files/test1.txt`.
3. Confirm console displays `[✓] Upload successful!` and `Integrity : SHA-256 verified ✓`.
4. From the client menu, select `2` (Download) and choose `test1.txt`.
5. Confirm local file is placed in `client/downloaded_files/test1.txt` and digests match.

### Test 2: Concurrent Multi-Client Stress Test
Verify thread safety, socket isolation, and server stability under concurrent workloads:
1. Keep the server instance active in Terminal 1.
2. Launch 5 distinct client terminals (`run_client.bat` or `python client.py`).
3. Simultaneously trigger downloads of `largefile.bin` across 3 client windows.
4. Concurrently initiate file uploads across the remaining 2 client terminals.
5. Verify that no thread deadlocks occur, CPU/memory usage remains flat, and all 5 clients complete with verified SHA-256 checksums.

<details>
<summary><strong>View Full 10-Point Verification & Edge-Case Checklist</strong></summary>

| # | Test Scenario | Execution Step | Expected Behavior |
| :---: | :--- | :--- | :--- |
| **1** | **Small File Upload** | Upload `test-files/test1.txt` (22 B) | Server acknowledges payload; SHA-256 confirmed instantly. |
| **2** | **Download Parity** | Download `test1.txt` back to client | File placed in `client/downloaded_files/`; digests match. |
| **3** | **Medium File Transfer** | Upload and download `test-files/test2.txt` (~3.3 KB) | Multi-chunk stream verifies smoothly across chunk boundaries. |
| **4** | **Large File Benchmark** | Transfer `test-files/largefile.bin` (5 MB) | Progress meter tracks in real time; memory stays flat. |
| **5** | **Concurrent Stress Test** | 5 clients active simultaneously (3 downloading, 2 uploading) | Dedicated worker threads handle requests concurrently without cross-talk. |
| **6** | **Duplicate Filenames** | Upload `test1.txt` consecutively | Server renames to `test1_1.txt` and alerts client; zero overwrites. |
| **7** | **Non-Existent Download** | Query download for `ghost.bin` | Server returns `NOT_FOUND`; client cleanly reports missing file. |
| **8** | **Mid-Transfer Drop** | Terminate client (`Ctrl+C`) during active upload | Server renames partial write to `<file>.partial`; thread cleanly terminates. |
| **9** | **Port Conflict Handling** | Launch second server instance on port 5000 | Caught via `OSError`; friendly diagnostic printed instead of raw traceback. |
| **10** | **Live STATUS Telemetry** | Execute option `4` during active transfers | Server returns live uptime, stored file count, and exact active client count. |

</details>

---

## Implementation Details & Logging

### Engineering Concerns & Solutions

| Technical Concern | Architectural Solution |
| :--- | :--- |
| **TCP Stream Boundaries** | Implemented 4-byte big-endian prefix framing (`struct.pack('>I', len)`), allowing the socket receiver to read the exact payload length without relying on delimiters. |
| **Client Concurrency** | Configured `threading.Thread(target=handle_client, daemon=True)` for non-blocking per-connection lifecycles, keeping the main `accept()` loop free. |
| **State Thread-Safety** | Enforced mutex synchronization (`threading.Lock`) over client connection registries and file-access tracking. |
| **Payload Integrity** | Streaming SHA-256 hashing (`hashlib.sha256`) computed incrementally in 1024-byte chunks on both sender and receiver. |
| **Path Traversal Attacks** | Implemented `sanitize_filename()` to strip directory trees (`os.path.basename`), replace separators (`/`, `\\`), and disallow relative parent paths (`..`). |
| **Silent Data Overwrites** | Automatic duplicate resolution appends incrementing integer suffixes (`<name>_1.<ext>`) to preserve existing files. |
| **Interrupted Transfers** | Connection drops mid-stream trigger exception handling that isolates truncated data as `<name>.partial` files. |
| **Socket TIME_WAIT Lockup** | Configured `socket.SO_REUSEADDR` flag on the listening socket to allow immediate restarts without waiting for OS socket release. |
| **Minimal Dependencies** | Implemented terminal progress meter using carriage returns (`\r`) and `sys.stdout.flush()`, with optional fallback to `tqdm`. |

### Structured Log Files

All runtime events are tracked in the `logs/` directory using standardized timestamp formats:

| Log File | Format & Level | Purpose |
| :--- | :--- | :--- |
| `logs/server.log` | `%(asctime)s [%(levelname)s] %(threadName)s — %(message)s` (`DEBUG`) | Captures connection handshakes, thread IDs, transfer throughput, checksum evaluations, and network exceptions. |
| `logs/client.log` | `%(asctime)s [%(levelname)s] — %(message)s` (`DEBUG`) | Records connection attempts, transfer status, diagnostic errors, and checksum results while keeping stdout CLI uncluttered. |

---

## Future Roadmap

| Milestone | Upgrade Feature | Implementation Strategy |
| :---: | :--- | :--- |
| **01** | **TLS/SSL Encryption** | Wrap client and server sockets with Python's standard `ssl` context (`ssl.wrap_socket()`) to secure transfers in transit. |
| **02** | **Resumable Transfers** | Implement byte-offset negotiation (`Range` headers) to resume interrupted `.partial` transfers from last received chunk. |
| **03** | **Streaming Compression** | Integrate inline `zlib` stream compression/decompression to maximize transfer speeds across bandwidth-constrained networks. |
| **04** | **Token Authentication** | Implement challenge-response handshakes with token validation before granting access to server commands. |
| **05** | **Web Telemetry Dashboard** | Expand the included Flask dashboard (`dashboard/app.py`) with real-time WebSocket telemetry, transfer charts, and browser uploads. |

---

## Author & License

- **Author**: **Etikala Adithya**
- **Institution**: B.Tech Computer Science and Engineering — Computer Networks, B V Raju Institute of Technology (BVRIT)
- **Profile**: [github.com/adithya181206](https://github.com/adithya181206)
- **License**: Released under the [MIT License](LICENSE).
