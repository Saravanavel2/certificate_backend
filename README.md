# CertLock Advanced Backend Engine v2.0

CertLock is a high-performance, production-ready certification SaaS engine designed to handle massive batch generation, secure document verification, and integrated quiz assessments.

## 🚀 Technology Stack
- **Core Framework**: [Fastify](https://www.fastify.io/) (High-performance Node.js framework)
- **Database**: [Neon PostgreSQL](https://neon.tech/) (Serverless Postgres)
- **Task Queuing**: [BullMQ](https://docs.bullmq.io/) with [Redis](https://redis.io/) (Persistent job management)
- **Offloading**: [Cloudflare Workers](https://workers.cloudflare.com/) (Distributed high-speed PDF generation)
- **PDF Manipulation**: [pdf-lib](https://pdf-lib.js.org/) (Metadata embedding & local processing)
- **Storage**: [Cloudinary](https://cloudinary.com/) (Template hosting & optimization)
- **Email Delivery**: [Nodemailer](https://nodemailer.com/) (Gmail SMTP)
- **Real-time Updates**: [Server-Sent Events (SSE)](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events) via `fastify-sse-v2`

---

## 🛠️ Global Architecture & Flow

### 1. The Generation Pipeline (High-Speed Engine)
The generation process is designed for scalability and speed:
1. **Request**: The frontend sends a batch request to `/generate`.
2. **Queuing**: The job is added to a **BullMQ** queue (backed by Redis). If Redis is unavailable, it gracefully falls back to an internal memory queue.
3. **Worker Execution**: A dedicated worker pulls the job and initializes an `archiver` ZIP stream.
4. **Cloudflare Offloading**: To prevent CPU bottlenecks, the backend offloads the actual PDF drawing to a **Cloudflare Worker**. This allows parallel generation of hundreds of certificates in seconds.
5. **Security Logging**: Each generated certificate is assigned a **UUID** and its **SHA-256 hash** is stored in the Neon database for future verification.
6. **Automated Dispatch**: If "Auto-Email Delivery" is enabled, the system uses **Nodemailer** to send the PDF directly to the participant.
7. **Finalization**: The ZIP is finalized, and the user receives a completion signal via SSE to trigger the `/download`.

### 2. The Verification System (Zero-Trust)
CertLock uses a multi-layer verification strategy:
- **Metadata Extraction**: It reads internal PDF "Subject" and "Keywords" fields where the unique ID is hidden.
- **Integrity Check**: It re-hashes the uploaded file and compares it against the original SHA-256 fingerprint stored during generation.
- **Registry Query**: It validates the ID against the official issuance registry in the Postgres database.
- **Tamper Detection**: If even a single pixel or metadata byte is changed, the SHA-256 check fails, alerting the user to a tampered document.

### 3. The Quiz Ecosystem
A fully integrated assessment platform that feeds directly into the certification engine:
- **Creation**: Supports MCQ and text-based questions with custom point values.
- **Access Control**: Features **Access Keys** and **Email Whitelisting** (only specific students can enter).
- **Proctoring Readiness**: Includes attempt limits (max 2) and scheduling (start/end times).
- **Analytics**: Provides detailed performance distribution and question-by-question correctness rates.
- **Direct Export**: Quiz results can be "exported" directly into the Generation Studio with a single click.

---

## 📡 API Endpoints (Summary)

### Core Generation
| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/upload-csv` | POST | Parses CSV and returns structured participant data. |
| `/upload-template`| POST | Uploads certificate designs to Cloudinary. |
| `/preview-pdf` | POST | Generates a single, watermarked PDF preview. |
| `/generate` | POST | **The Engine.** Queues a batch generation job. |
| `/progress` | GET | SSE stream for real-time progress and queue status. |
| `/download` | GET | Streams the final ZIP and triggers auto-cleanup. |
| `/stop-generate` | POST | Cancels a running or queued job. |

### Verification
| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/verify-pdf` | POST | Performs hash-matching and registry validation. |

### Quiz Pro
| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/quiz/quizzes` | GET/POST| List all quizzes or create a new one. |
| `/quiz/attempts` | POST | Start a new timed attempt with access validation. |
| `/quiz/attempts/:id/submit` | POST | Submit answers for auto-scoring and storage. |
| `/quiz/leaderboard/:id` | GET | Retrieve top-performing participants. |
| `/quiz/quizzes/:id/export` | GET | Format results for direct certificate generation. |

---

## 🔐 Security & Optimization
- **Rate Limiting**: Global limit of 60 req/min; `/generate` is strictly capped at 5 req/min to prevent SMTP abuse.
- **Auto-Cleanup**: Temporary ZIP files and Cloudinary fallbacks are deleted immediately after download or on job cancellation.
- **Streaming Architecture**: Uses Node.js streams for file handling to keep memory usage low even during large batch processing.
- **Concurrency Control**: BullMQ ensures only one massive batch is processed at a time per worker instance, preventing server crashes.

---
*Generated and Secured by CertLock Advanced Backend Engine.*
