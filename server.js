import Fastify from 'fastify';
import crypto from 'crypto';
import multipart from '@fastify/multipart';
import cors from '@fastify/cors';
import fastifyStatic from '@fastify/static';
import { FastifySSEPlugin } from 'fastify-sse-v2';
import rateLimit from '@fastify/rate-limit';
import archiver from 'archiver';
import { PDFDocument, rgb } from 'pdf-lib';
import { v2 as cloudinary } from 'cloudinary';
import https from 'https';
import http from 'http';
import dotenv from 'dotenv';
import fetch from "node-fetch";
import nodemailer from 'nodemailer';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import fontkit from "@pdf-lib/fontkit";
import { v4 as uuidv4 } from 'uuid';
import { Queue, Worker } from 'bullmq';
import IORedis from 'ioredis';
import { Redis as UpstashRedis } from '@upstash/redis';
import { EventEmitter } from 'events';
import { PassThrough } from 'stream';
import jwt from 'jsonwebtoken';
import dns from 'dns';
import XLSX from 'xlsx';
import QRCode from 'qrcode';

// Force Node.js to prefer IPv4 over IPv6
// This prevents ENETUNREACH errors on environments like Render where IPv6 might be unstable or unsupported.
dns.setDefaultResultOrder('ipv4first');

const sseEvents = new EventEmitter();
sseEvents.setMaxListeners(100);

dotenv.config();
const JWT_SECRET = process.env.JWT_SECRET;
if (!JWT_SECRET) {
    console.error("CRITICAL ERROR: JWT_SECRET is not defined in environment variables!");
    process.exit(1);
}

// Helper: Verify Token and return user payload
const verifyToken = (request) => {
    const authHeader = request.headers.authorization;
    console.log(`🔍 [AUTH] Verifying token for: ${request.url} | Header: ${authHeader ? 'Present' : 'MISSING'}`);
    if (!authHeader) return null;
    const token = authHeader.split(' ')[1];
    try {
        return jwt.verify(token, JWT_SECRET);
    } catch (err) {
        return null;
    }
};

// UUID and ID Validation Helpers
const isValidUUID = (id) => /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id);
const isValidInt = (id) => /^\d+$/.test(id);
console.log("=================================================");
console.log("🚀 CERTIFYPRO ADVANCED ENGINE v2.0 STARTING...");
console.log("🔑 Nodemailer Gmail Service Detected:", !!process.env.GMAIL_USER);
console.log("=================================================");

import { neon } from '@neondatabase/serverless';
const sql = process.env.DATABASE_URL ? neon(process.env.DATABASE_URL) : null;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Global Error Protection
process.on('unhandledRejection', (reason, promise) => {
    console.error('⚠️ UNHANDLED REJECTION:', reason);
});
process.on('uncaughtException', (err) => {
    console.error('‼️ UNCAUGHT EXCEPTION:', err);
});

// ====== CONFIGURE BULLMQ & UPSTASH ======
let bullEnabled = false;
let certQueue = null;
let redisConnection = null;

const upstash = new UpstashRedis({
    url: process.env.UPSTASH_REDIS_REST_URL,
    token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

(async () => {
    try {
        await upstash.set("foo", "bar");
        const val = await upstash.get("foo");
        console.log("✅ Upstash HTTP Demo: foo =", val);
    } catch (e) {
        console.warn("⚠️ Upstash HTTP Demo error:", e.message);
    }
})();

const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const isRemote = redisUrl.includes('upstash.io') || redisUrl.includes('rediss://');

try {
    const redisOpts = {
        maxRetriesPerRequest: null,
        connectTimeout: 10000,
        enableOfflineQueue: false,
        tls: isRemote ? { rejectUnauthorized: false } : undefined
    };

    redisConnection = new IORedis(redisUrl, { ...redisOpts, lazyConnect: true });

    let warned = false;

    redisConnection.on('error', (e) => {
        if (!warned) {
          console.warn("⚠️ Redis not found. Falling back to internal memory queue.");
          warned = true;
        }
        bullEnabled = false;
    });

    redisConnection.on('connect', () => {
        bullEnabled = true;
        console.log("✅ Redis Connected - BullMQ Persistence Enabled");
    });

    certQueue = new Queue('certify-pro-tasks', { 
        connection: redisConnection,
        defaultJobOptions: { removeOnComplete: true, removeOnFail: 100 }
    });
    
    new Worker('certify-pro-tasks', async (job) => {
        const { body, key } = job.data;
        console.log(`⚙️ [BULLMQ-WORKER] Picking up job: ${key}`);
        
        if (!workerContexts[key]) {
            const zipPath = path.join(__dirname, `temp_${key}.zip`);
            workerContexts[key] = {
              body, key, zipPath, processedCount: 0,
              emailSuccessCount: 0, emailFailCount: 0,
              total: body.participants?.length || 0,
              archive: null, output: null, baseTemplate: null, fontBytes: null, scales: null, startTime: Date.now(),
              abortController: new AbortController()
            };
        }
        
        currentJobKey = key;
        activeJob = {
            key: key,
            progress: { stage: "processing", task: "🚀 Starting generation engine...", percent: 0 },
            startTime: Date.now()
        };
        
        console.log(`🚀 [BULLMQ] Beginning generation for ${key}`);
        await broadcastQueueUpdate(); 
        
        try {
            await generateHandlerFair(workerContexts[key]);
        } catch (workerErr) {
            console.error(`❌ [BULLMQ-WORKER-ERROR] ${key}:`, workerErr);
        } finally {
            console.log(`🏁 [BULLMQ] Job ${key} loop finished.`);
            activeJob = null;
            currentJobKey = null;
            await broadcastQueueUpdate();
        }
    }, { 
        connection: redisConnection,
        concurrency: 1,
        lockDuration: 60000
    });

    redisConnection.connect().catch(() => {});

} catch (e) {
    console.warn("❌ BullMQ Setup failed completely:", e.message);
}

// ====== NODEMAILER GMAIL CONFIG ======
const GMAIL_USER = process.env.GMAIL_USER;
const GMAIL_PASS = process.env.GMAIL_PASS;
const GMAIL_SENDER_NAME = process.env.GMAIL_SENDER_NAME || "CertLock Service";

const transporter = nodemailer.createTransport({
    host: 'smtp.gmail.com',
    port: 587,
    secure: false,
    auth: {
        user: GMAIL_USER,
        pass: GMAIL_PASS
    },
    tls: {
        rejectUnauthorized: false
    },
    // Force IPv4 lookup for this transporter specifically
    // This is the most reliable fix for ENETUNREACH IPv6 errors on Render
    lookup: (hostname, options, callback) => {
        return dns.lookup(hostname, { family: 4 }, callback);
    }
});

if (GMAIL_USER && GMAIL_PASS) {
    console.log(`✅ Nodemailer Gmail Service Connected (${GMAIL_USER})`);
} else {
    console.warn("⚠️ Gmail credentials not found. Email features will be disabled.");
}

async function sendEmail(toEmail, toName, attachmentBuffer, attachmentName, certId) {
    if (!GMAIL_USER || !GMAIL_PASS) {
        console.warn("⚠️ Cannot send email: Gmail credentials missing from environment variables.");
        return false;
    }
    console.log(`📧 Dispatching certificate email to: ${toEmail} (${toName})`);
    
    const encodedName = encodeURIComponent("Professional Certificate via CertLock");
    const encodedUrl = encodeURIComponent(`https://certifypro.vsgrps.com/verify?id=${certId}`);
    const encodedId = encodeURIComponent(certId);
    const issueYear = new Date().getFullYear();
    const issueMonth = new Date().getMonth() + 1;
    const tweetText = encodeURIComponent('I am thrilled to share my newly earned certificate from CertLock! 🎓✨ #Achievement #CertLock');

    const mailOptions = {
        from: `"${GMAIL_SENDER_NAME}" <${GMAIL_USER}>`,
        to: toEmail,
        subject: "Your Official Certificate is Ready! - CertLock",
        html: `
            <div style="font-family: 'Inter', 'Helvetica Neue', Helvetica, Arial, sans-serif; padding: 20px 10px; background-color: #f8fafc; color: #1e293b; line-height: 1.5;">
                <div style="max-width: 600px; margin: 0 auto; background: white; border-radius: 16px; padding: 32px 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); border: 1px solid #e2e8f0;">
                    <div style="text-align: center; margin-bottom: 24px;">
                        <div style="display: inline-block; width: 48px; height: 48px; background: linear-gradient(135deg, #3b82f6, #a855f7); border-radius: 12px; margin-bottom: 12px; line-height: 48px; color: white; font-weight: bold; font-size: 24px;">✨</div>
                        <h1 style="margin: 0; font-size: 22px; font-weight: 800; color: #0f172a; letter-spacing: -0.02em;">CertLock</h1>
                    </div>
                    
                    <h2 style="font-size: 18px; font-weight: 700; color: #0f172a; margin-top: 0; margin-bottom: 16px;">Congratulations, ${toName}!</h2>
                    
                    <p style="font-size: 14px; color: #475569; margin-bottom: 20px;">
                        We are pleased to inform you that your official accomplishment certificate has been generated. Your document has been permanently secured and digitally signed by our system to guarantee authenticity.
                    </p>

                    <div style="background: #f1f5f9; border-radius: 12px; padding: 16px; margin-bottom: 24px; text-align: center; border: 1px solid #e2e8f0;">
                        <p style="margin: 0; font-size: 12px; font-weight: 700; color: #3b82f6; text-transform: uppercase; letter-spacing: 0.05em;">Document Secured</p>
                        <p style="margin: 4px 0 0; font-size: 14px; color: #1e293b; font-weight: 600;">Your certificate is attached as a high-resolution PDF.</p>
                    </div>

                    <div style="margin-bottom: 24px;">
                        <h3 style="font-size: 13px; font-weight: 700; color: #0f172a; margin-bottom: 12px; text-transform: uppercase; letter-spacing: 0.05em;">Best Practices for Sharing:</h3>
                        
                        <div style="background: #ecfdf5; border-left: 3px solid #10b981; padding: 12px; margin-bottom: 8px; border-radius: 0 8px 8px 0;">
                            <strong style="font-size: 13px; color: #065f46; display: block; margin-bottom: 4px;">✓ DO: Upload to Google Drive</strong>
                            <p style="margin: 0; font-size: 12px; color: #047857;">For maximum security, upload this attached PDF to your personal Google Drive. Sharing a public Drive link is the easiest and most professional way for recruiters to view and verify your credential.</p>
                        </div>
                        
                        <div style="background: #eff6ff; border-left: 3px solid #3b82f6; padding: 12px; margin-bottom: 8px; border-radius: 0 8px 8px 0;">
                            <strong style="font-size: 13px; color: #1e40af; display: block; margin-bottom: 4px;">✓ DO: Share on LinkedIn</strong>
                            <p style="margin: 0; font-size: 12px; color: #1d4ed8;">Showcase your achievement by adding this credential to the "Licenses & certifications" section of your LinkedIn profile.</p>
                        </div>

                        <div style="background: #fef2f2; border-left: 3px solid #ef4444; padding: 12px; border-radius: 0 8px 8px 0;">
                            <strong style="font-size: 13px; color: #991b1b; display: block; margin-bottom: 4px;">✕ DON'T: Modify the File</strong>
                            <p style="margin: 0; font-size: 12px; color: #b91c1c;">Do not attempt to compress, edit, or alter the PDF file. Any modifications will instantly break the cryptographic seal and invalidate the certificate.</p>
                        </div>
                    </div>

                    <div style="margin-top: 32px; padding: 24px; background: linear-gradient(135deg, #eff6ff 0%, #f5f3ff 100%); border-radius: 16px; text-align: center; border: 1px solid #e0e7ff;">
                        <h4 style="margin: 0 0 8px; font-size: 14px; color: #1e40af; font-weight: 800;">🎉 Proud of your achievement?</h4>
                        <p style="margin: 0 0 20px; font-size: 12px; color: #475569;">Share it with your professional network and inspire others!</p>
                        
                        <a href="https://www.linkedin.com/profile/add?startTask=CERTIFICATION_NAME&name=${encodedName}&certId=${encodedId}&certUrl=${encodedUrl}&issueYear=${issueYear}&issueMonth=${issueMonth}" style="background: #0077b5; color: white; padding: 12px 20px; border-radius: 10px; text-decoration: none; font-size: 13px; font-weight: 700; display: inline-block; margin: 4px;">
                            Add to LinkedIn
                        </a>
                        
                        <a href="https://twitter.com/intent/tweet?text=${tweetText}&url=${encodedUrl}" style="background: #000000; color: white; padding: 12px 20px; border-radius: 10px; text-decoration: none; font-size: 13px; font-weight: 700; display: inline-block; margin: 4px;">
                            Share on X
                        </a>
                        <p style="margin: 16px 0 0; font-size: 11px; color: #64748b;"><strong>Tip:</strong> For the best look on LinkedIn, click "Add Media" and upload the PDF attached to this email!</p>
                    </div>

                    <hr style="border: none; border-top: 1px solid #e2e8f0; margin: 24px 0;" />

                    <p style="font-size: 11px; color: #94a3b8; text-align: center; margin: 0;">
                        This is an automated message from CertLock.<br/>
                        &copy; ${new Date().getFullYear()} CertLock. All rights reserved.
                    </p>
                </div>
            </div>
        `,
        attachments: [
            {
                filename: attachmentName,
                content: attachmentBuffer
            }
        ]
    };

    try {
        await transporter.sendMail(mailOptions);
        console.log(`✅ Email sent successfully to ${toEmail}`);
        return true;
    } catch (err) {
        console.error(`❌ Email send failed [${toEmail}]:`, err.message);
        return false;
    }
}



cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
  secure: true
});

const fastify = Fastify({ 
  logger: true,
  connectionTimeout: 120000,
  requestTimeout: 120000
});

fastify.register(cors, {
  origin: ["http://localhost:5173", "http://localhost:3000", "http://localhost:5000", "https://certifypro.vsgrps.com", "https://certify-vsgrps.onrender.com"],
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: [
    'Content-Type', 
    'Authorization', 
    'Accept', 
    'Cache-Control', 
    'cache-control',
    'Last-Event-ID', 
    'X-Requested-With'
  ]
});

fastify.register(rateLimit, {
  max: 60,
  timeWindow: '1 minute'
});


fastify.register(multipart, {
  limits: {
    fileSize: 50 * 1024 * 1024
  }
});

fastify.register(FastifySSEPlugin);

fastify.register(fastifyStatic, {
  root: path.join(__dirname, 'public'),
  prefix: '/public/',
});

// Font Mapping
const FONT_MAP = {
  'NotoSans': {
    '300': 'https://github.com/googlefonts/noto-fonts/raw/main/hinted/ttf/NotoSans/NotoSans-Light.ttf',
    '400': 'https://github.com/googlefonts/noto-fonts/raw/main/hinted/ttf/NotoSans/NotoSans-Regular.ttf',
    '500': 'https://github.com/googlefonts/noto-fonts/raw/main/hinted/ttf/NotoSans/NotoSans-Medium.ttf',
    '700': 'https://github.com/googlefonts/noto-fonts/raw/main/hinted/ttf/NotoSans/NotoSans-Bold.ttf',
    '900': 'https://github.com/googlefonts/noto-fonts/raw/main/hinted/ttf/NotoSans/NotoSans-Black.ttf'
  },
  'Roboto': {
    '300': 'https://github.com/google/fonts/raw/main/apache/roboto/static/Roboto-Light.ttf',
    '400': 'https://github.com/google/fonts/raw/main/apache/roboto/static/Roboto-Regular.ttf',
    '500': 'https://github.com/google/fonts/raw/main/apache/roboto/static/Roboto-Medium.ttf',
    '700': 'https://github.com/google/fonts/raw/main/apache/roboto/static/Roboto-Bold.ttf',
    '900': 'https://github.com/google/fonts/raw/main/apache/roboto/static/Roboto-Black.ttf'
  },
  'Montserrat': {
    '300': 'https://github.com/google/fonts/raw/main/ofl/montserrat/static/Montserrat-Light.ttf',
    '400': 'https://github.com/google/fonts/raw/main/ofl/montserrat/static/Montserrat-Regular.ttf',
    '500': 'https://github.com/google/fonts/raw/main/ofl/montserrat/static/Montserrat-Medium.ttf',
    '700': 'https://github.com/google/fonts/raw/main/ofl/montserrat/static/Montserrat-Bold.ttf',
    '900': 'https://github.com/google/fonts/raw/main/ofl/montserrat/static/Montserrat-Black.ttf'
  },
  'PlayfairDisplay': {
    '400': 'https://github.com/google/fonts/raw/main/ofl/playfairdisplay/static/PlayfairDisplay-Regular.ttf',
    '700': 'https://github.com/google/fonts/raw/main/ofl/playfairdisplay/static/PlayfairDisplay-Bold.ttf',
    '900': 'https://github.com/google/fonts/raw/main/ofl/playfairdisplay/static/PlayfairDisplay-Black.ttf'
  },
  'DancingScript': {
    '400': 'https://github.com/google/fonts/raw/main/ofl/dancingscript/static/DancingScript-Regular.ttf',
    '700': 'https://github.com/google/fonts/raw/main/ofl/dancingscript/static/DancingScript-Bold.ttf'
  }
};

const WEIGHT_MAP = {
  'light': '300',
  'regular': '400',
  'normal': '400',
  'medium': '500',
  'bold': '700',
  'black': '900'
};

const fontCache = {};
const FONT_DIR = path.join(__dirname, 'fonts');
if (!fs.existsSync(FONT_DIR)) fs.mkdirSync(FONT_DIR);

async function getFontBytes(family = 'NotoSans', weight = '400') {
  const numericWeight = WEIGHT_MAP[weight.toLowerCase()] || weight;
  const url = (FONT_MAP[family] && FONT_MAP[family][numericWeight]) || FONT_MAP['NotoSans']['400'];
  if (fontCache[url]) return fontCache[url];
  
  const fontFilename = path.basename(url);
  const localPath = path.join(FONT_DIR, fontFilename);

  if (fs.existsSync(localPath)) {
    const bytes = fs.readFileSync(localPath);
    fontCache[url] = bytes;
    return bytes;
  }

  try {
    console.log(`📥 Cloud Font Load: ${family} (${numericWeight}) from GitHub...`);
    const resp = await fetch(url, { timeout: 15000 });
    if (!resp.ok) throw new Error(`HTTP ${resp.status} - ${resp.statusText}`);
    
    const buf = Buffer.from(await resp.arrayBuffer());
    if (buf.length < 1000) throw new Error(`Received invalid font buffer (size: ${buf.length})`);
    
    fs.writeFileSync(localPath, buf);
    fontCache[url] = buf;
    return buf;
  } catch (err) {
    console.error(`❌ Font Fetch Critical Error [${family}/${weight}]:`, err.message);
    if (family !== 'NotoSans') {
      console.warn(`⚠️ Falling back to NotoSans Regular for [${family}]`);
      return getFontBytes('NotoSans', '400');
    }
    throw err;
  }
}

function getBufferFromUrl(url) {
  if (typeof url === 'string' && (url.includes('localhost') || url.includes('127.0.0.1') || url.includes('/public/uploads/'))) {
    try {
      const parts = url.split('/public/uploads/');
      if (parts.length > 1) {
        const filename = parts[1];
        const filePath = path.join(__dirname, 'public', 'uploads', filename);
        if (fs.existsSync(filePath)) {
          return Promise.resolve(fs.readFileSync(filePath));
        }
      }
    } catch (e) {
      console.error("❌ Local template read error:", e);
    }
  }

  return new Promise((resolve, reject) => {
    const client = url.startsWith('https') ? https : http;
    const req = client.get(url, (res) => {
      if (res.statusCode !== 200) {
        reject(new Error(`Failed to fetch image: ${res.statusCode}`));
        return;
      }
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => resolve(Buffer.concat(chunks)));
    });
    req.on('error', reject);
    req.setTimeout(10000, () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });
  });
}

async function deleteTemplate(publicId) {
    if (!publicId) return;
    const isCloudinaryConfigured = process.env.CLOUDINARY_CLOUD_NAME && process.env.CLOUDINARY_API_KEY && process.env.CLOUDINARY_API_SECRET;
    if (isCloudinaryConfigured) {
        try {
            await cloudinary.uploader.destroy(publicId);
            console.log(`🗑️ Auto-deleted Cloudinary template: ${publicId}`);
        } catch (e) {
            console.error(`⚠️ Cloudinary delete failed:`, e.message);
        }
    } else {
        try {
            const localPath = path.join(__dirname, 'public', 'uploads', publicId);
            if (fs.existsSync(localPath)) {
                fs.unlinkSync(localPath);
                console.log(`🗑️ Auto-deleted local template file: ${publicId}`);
            }
        } catch (e) {
            console.error(`⚠️ Local template delete failed:`, e.message);
        }
    }
}

// Global state - IMPROVED TRACKING
let clients = [];
let isGenerating = false;
let queue = []; // Queue list (waiting users)
let activeJob = null; // Active Job (currently processing)
let stopRequestedStore = {};
let zipStore = {};
let cloudinaryStore = {};
let progressStore = {};
let workerContexts = {};
let currentJobKey = null;
let downloadedStore = {};
let downloadStartedStore = {};
let lastSeenClients = {};

// Heartbeat for continuous sync
setInterval(() => {
    if (activeJob || queue.length > 0) {
        broadcastQueueUpdate();
    }
}, 5000);

// Core function: Get complete queue state (BullMQ or Internal)
async function getQueueState() {
    let currentQueue = [];
    if (bullEnabled && certQueue) {
        try {
            // Fetch waiting jobs from BullMQ
            const waitingJobs = await certQueue.getWaiting();
            currentQueue = waitingJobs.map(job => ({
                key: job.data.key,
                addedAt: job.timestamp
            }));
        } catch (e) {
            console.error("⚠️ BullMQ getWaiting failed:", e.message);
            currentQueue = queue; // Fallback
        }
    } else {
        currentQueue = queue;
    }

    return {
        activeJob: activeJob ? {
            key: activeJob.key,
            progress: activeJob.progress,
            elapsedTime: activeJob.startTime ? Math.floor((Date.now() - activeJob.startTime) / 1000) : 0
        } : null,
        queue: currentQueue.map((item, index) => ({
            key: typeof item === 'string' ? item : item.key,
            position: index + 1,
            waitingTime: item.addedAt ? Math.floor((Date.now() - item.addedAt) / 1000) : 0
        })),
        timestamp: Date.now()
    };
}

// Core function: Broadcast complete queue state to all users
async function broadcastQueueUpdate() {
    const queueState = await getQueueState();
    
    // Broadcast to all active SSE streams via EventEmitter
    sseEvents.emit('progress', { key: null, data: { type: "queue_update", data: queueState } });
    
    // Also send individual position updates to each queued user
    queueState.queue.forEach((item) => {
        const targetKey = item.key;
        const position = item.position;
        const isNext = position === 1;
        
        const updateData = { 
            stage: "queued", 
            task: isNext ? "You are NEXT in line!" : `Position #${position} in queue`,
            position: position,
            totalInQueue: queueState.queue.length,
            message: isNext ? "🎯 Ready to start! Waiting for current job to complete..." : `⏳ Please wait. ${position} ${position === 1 ? 'person is' : 'people are'} ahead of you.`,
            activeJob: queueState.activeJob ? {
                status: queueState.activeJob.progress,
                estimatedRemaining: queueState.activeJob.progress?.estimatedTimeRemaining || "Calculating..."
            } : null,
            estimatedWaitTime: position * 30 
        };

        // Broadcast individual position update via EventEmitter
        sseEvents.emit('progress', { key: targetKey, data: updateData });
    });
}

function sendProgress(key, data) {
    if (key && data) {
        // Only include fields if they aren't 'undefined'
        const cleanData = Object.fromEntries(Object.entries(data || {}).filter(([_, v]) => v !== undefined));
        progressStore[key] = { ...progressStore[key], ...cleanData, timestamp: Date.now() };
        
        // Update active job progress if this is the current job
        if (key === currentJobKey) {
            if (activeJob) {
                activeJob.progress = { ...activeJob.progress, ...cleanData };
                activeJob.lastUpdate = Date.now();
            }
            broadcastQueueUpdate();
        }
        
        console.log(`📡 [SSE] Broadcast to ${key}: ${cleanData.stage || 'ping'} - ${cleanData.task || 'no-task'}`);
        
        // Emit to all active SSE streams
        sseEvents.emit('progress', { key, data: cleanData });
    }
}

// Routes
fastify.get('/', async (request, reply) => {
  return { message: "Fastify Certificate Generator API" };
});

fastify.post("/save-user", async (request, reply) => {
  try {
    const { sub, name, email, picture, user_type } = request.body;
    if (!sub || !email) {
      return reply.code(400).send({ error: "Missing required fields" });
    }
    
    const [savedUser] = await sql`
      INSERT INTO users (google_id, name, email, picture, user_type)
      VALUES (${sub}, ${name}, ${email}, ${picture}, ${user_type || 'User'})
      ON CONFLICT (email) DO UPDATE SET 
        name = EXCLUDED.name, 
        picture = EXCLUDED.picture,
        user_type = CASE 
          WHEN users.user_type = 'User' OR users.user_type IS NULL THEN EXCLUDED.user_type 
          ELSE users.user_type 
        END
      RETURNING *;
    `;

    const token = jwt.sign({ 
      id: savedUser.google_id, 
      sub: savedUser.google_id, 
      email: savedUser.email, 
      name: savedUser.name, 
      picture: savedUser.picture, 
      user_type: savedUser.user_type 
    }, JWT_SECRET, { expiresIn: '30d' });

    return reply.send({ 
      success: true, 
      message: "User stored successfully", 
      token,
      user_type: savedUser.user_type,
      user: savedUser
    });
  } catch (err) {
    console.error("Save User Error:", err);
    return reply.code(500).send({ error: "Failed to save user" });
  }
});

fastify.post('/upload-csv', async (request, reply) => {
  const data = await request.file();
  if (!data) return reply.code(400).send({ error: "No file received" });
  try {
    const buffer = await data.toBuffer();
    const filename = data.filename.toLowerCase();
    
    let columns = [];
    let participants = [];

    if (filename.endsWith('.xlsx') || filename.endsWith('.xls')) {
      const workbook = XLSX.read(buffer, { type: 'buffer' });
      const sheetName = workbook.SheetNames[0];
      const sheet = workbook.Sheets[sheetName];
      const jsonData = XLSX.utils.sheet_to_json(sheet, { defval: "" });
      
      if (jsonData.length === 0) {
        return reply.code(400).send({ error: "Excel sheet is empty" });
      }
      
      columns = Object.keys(jsonData[0]);
      participants = jsonData;
    } else {
      const content = buffer.toString("utf8");
      const lines = content.split(/\r?\n/).filter(l => l.trim());
      if (lines.length < 2) {
        return reply.code(400).send({ error: "CSV must have headers and at least one row" });
      }
      columns = lines[0].split(',').map(h => h.trim().replace(/^"(.*)"$/, '$1'));
      participants = lines.slice(1).map(line => {
        const values = line.match(/("(?:[^"]|"")*"|[^,]*),?/g)
          ?.map(v => v.replace(/,$/, '').trim().replace(/^"(.*)"$/, '$1').replace(/""/g, '"'))
          || line.split(',').map(v => v.trim());
        const obj = {};
        columns.forEach((h, i) => {
          obj[h] = values[i] || '';
        });
        return obj;
      });
    }

    if (participants.length > 1001) {
        return reply.code(400).send({ error: "Limit Exceeded: Maximum 1001 participants allowed per batch." });
    }
    
    return { columns, participants };
  } catch (e) {
    fastify.log.error("Data File Parse Error:", e);
    return reply.code(500).send({ error: "Failed to parse data file" });
  }
});

fastify.post("/upload-template", async (request, reply) => {
  const data = await request.file();
  if (!data) return reply.code(400).send({ error: "No file" });
  try {
    const buffer = await data.toBuffer();
    
    const isCloudinaryConfigured = process.env.CLOUDINARY_CLOUD_NAME && process.env.CLOUDINARY_API_KEY && process.env.CLOUDINARY_API_SECRET;
    
    if (isCloudinaryConfigured) {
      fastify.log.info("Got template buffer, uploading to Cloudinary...");
      const result = await new Promise((resolve, reject) => {
        const uploadStream = cloudinary.uploader.upload_stream(
          { resource_type: 'image', folder: 'certificates' },
          (error, result) => {
            if (error) reject(error);
            else resolve(result);
          }
        );
        uploadStream.end(buffer);
      });
      fastify.log.info("Cloudinary upload success:", result.secure_url);
      return { templateUrl: result.secure_url, publicId: result.public_id };
    } else {
      // Local fallback logic
      const uploadsDir = path.join(__dirname, 'public', 'uploads');
      if (!fs.existsSync(uploadsDir)) {
          fs.mkdirSync(uploadsDir, { recursive: true });
      }
      const ext = path.extname(data.filename) || '.jpg';
      const fileName = `${uuidv4()}${ext}`;
      const filePath = path.join(uploadsDir, fileName);
      fs.writeFileSync(filePath, buffer);
      
      const localUrl = `http://localhost:${process.env.PORT || 5000}/public/uploads/${fileName}`;
      fastify.log.info("Cloudinary not configured. Saved template locally:", localUrl);
      return { templateUrl: localUrl, publicId: fileName };
    }
  } catch (err) {
    fastify.log.error("FULL TEMPLATE UPLOAD ERROR:", err);
    return reply.code(500).send({ error: "Image upload failed", details: err.message });
  }
});

fastify.post('/preview-pdf', async (request, reply) => {
  try {
    const { participant, templateUrl, fields, customDimensions } = request.body;
    const pdfDoc = await PDFDocument.create();
    pdfDoc.registerFontkit(fontkit);
    
    let imageBytes = null;
    let img = null;
    let width = customDimensions?.width || 600;
    let height = customDimensions?.height || 400;

    if (templateUrl && templateUrl.startsWith("http")) {
      const isCloudinary = templateUrl.includes("res.cloudinary.com");
      const finalUrl = isCloudinary ? templateUrl.replace("/upload/", "/upload/q_auto,f_auto/") : templateUrl;
      imageBytes = await getBufferFromUrl(finalUrl);
      const lowerUrl = templateUrl.toLowerCase();
      if (lowerUrl.includes(".png")) {
        img = await pdfDoc.embedPng(imageBytes);
      } else {
        img = await pdfDoc.embedJpg(imageBytes);
      }
      if (!customDimensions && img) {
         width = img.width;
         height = img.height;
      }
    }

    const page = pdfDoc.addPage([width, height]);
    if (img) {
      page.drawImage(img, { x: 0, y: 0, width: width, height: height });
    }
    
    const scaleX = width / 600;
    const scaleY = height / 400;
    const scaleAvg = (scaleX + scaleY) / 2;

    for (const f of fields) {
      let value = participant[f.field] ? String(participant[f.field]) : "";
      value = value.trim();
      if (!value) continue;

      const fBytes = await getFontBytes(f.fontFamily, f.fontWeight);
      const customFont = await pdfDoc.embedFont(fBytes);

      let hex = (f.color || "#000000").replace("#", "");
      if (hex.length !== 6) hex = "000000";

      const r = parseInt(hex.substring(0, 2), 16) / 255;
      const g = parseInt(hex.substring(2, 4), 16) / 255;
      const b = parseInt(hex.substring(4, 6), 16) / 255;

      const fontSize = f.size * scaleAvg;
      const textWidth = customFont.widthOfTextAtSize(value, fontSize);
      const paddingX = 16 * scaleX;
      let xPos = f.x * scaleX + paddingX;

      if (f.textAlign === 'center') {
          const visualBoxWidth = Math.max(60, (textWidth / scaleAvg) + 32);
          xPos = (f.x + (visualBoxWidth / 2)) * scaleX - (textWidth / 2);
      } else if (f.textAlign === 'right') {
          const visualBoxWidth = Math.max(60, (textWidth / scaleAvg) + 32);
          xPos = (f.x + visualBoxWidth - 16) * scaleX - textWidth;
      }

      page.drawText(value, {
        x: xPos,
        y: (height - (f.y * scaleY)) - (fontSize * 0.8) - (8 * scaleY),
        size: fontSize,
        font: customFont,
        color: rgb(r, g, b),
        characterSpacing: f.letterSpacing || 0
      });
    }

    const certUniqueId = uuidv4();
    pdfDoc.setSubject(certUniqueId);
    pdfDoc.setKeywords([certUniqueId]);

    const watermarkText = "Generated by CertLock";
    const watermarkFontSize = 10 * scaleAvg;
    const watermarkFontBytes = await getFontBytes('NotoSans', '400');
    const watermarkFont = await pdfDoc.embedFont(watermarkFontBytes);
    const watermarkWidth = watermarkFont.widthOfTextAtSize(watermarkText, watermarkFontSize);
    
    const watermarkY = Math.max(35, Math.min(100, Math.round(height * 0.045)));
    page.drawText(watermarkText, {
        x: (width - watermarkWidth) / 2,
        y: watermarkY,
        size: watermarkFontSize,
        font: watermarkFont,
        color: rgb(0.6, 0.6, 0.6),
        opacity: 0.5
    });

    let pdfBytes = await pdfDoc.save();
    pdfBytes = await embedQRCodeOnPDF(pdfBytes, certUniqueId);
    reply.type('application/pdf')
         .header('Content-Disposition', 'inline; filename=preview.pdf')
         .send(Buffer.from(pdfBytes));
  } catch (err) {
    console.error("❌ CRITICAL PREVIEW PDF ERROR:", err);
    reply.code(500).send({ error: "Preview engine failure", details: err.message });
  }
});

fastify.get("/progress", async (request, reply) => {
  const key = request.query.key ? request.query.key.toString().trim() : null;
  console.log(`📡 [SSE-CONNECTION] Incoming key: [${key}]`);
  if (!key) return reply.code(400).send("Missing key");

  const clientId = uuidv4();
  
  // Track client for metadata/audit purposes
  clients.push({ id: clientId, key });
  
  const stream = new PassThrough({ objectMode: true });
  
  // Clean up on disconnect
  const onProgress = (evt) => {
    if (!request.raw.destroyed && (evt.key === key || evt.key === null)) {
      const messageData = evt.data.type === 'queue_update' ? evt.data : evt.data;
      stream.write({ data: JSON.stringify(messageData) });
    }
  };

  sseEvents.on('progress', onProgress);
  
  request.raw.on('close', () => {
    clients = clients.filter(c => c.id !== clientId);
    sseEvents.off('progress', onProgress);
    stream.end();
  });

  // Start SSE
  reply.sse(stream);

  // Send "connected" signal
  stream.write({ data: "connected" });

  // 1. Initial Queue State (BullMQ Aware)
  const queueState = await getQueueState();
  if (queueState.activeJob || queueState.queue.length > 0) {
      stream.write({ data: JSON.stringify({ type: "queue_update", data: queueState, forUser: key }) });
      
      // Also send specific position if user is currently in queue
      const myEntry = queueState.queue.find(item => item.key === key);
      if (myEntry) {
          const position = myEntry.position;
          const isNext = position === 1;
          stream.write({ data: JSON.stringify({
              stage: "queued",
              task: isNext ? "You are NEXT in line!" : `Position #${position} in queue`,
              position: position,
              totalInQueue: queueState.queue.length,
              message: isNext ? "🎯 Ready to start! Waiting for current job to complete..." : `⏳ Please wait. ${position} people ahead of you.`
          })});
      }
  }

  // 2. Latched Progress (if already started)
  if (progressStore[key]) {
      console.log(`📡 [SSE] Sending latched progress for ${key}`);
      stream.write({ data: JSON.stringify(progressStore[key]) });
  } else if (currentJobKey === key) {
      stream.write({ data: JSON.stringify({ stage: "started", task: "Processing your certificates..." }) });
  }

  // 3. Heartbeat
  const heartbeat = setInterval(() => {
    if (!request.raw.destroyed) {
      stream.write({ data: JSON.stringify({ type: "ping", timestamp: Date.now() }) });
    } else {
      clearInterval(heartbeat);
    }
  }, 5000);

  stream.on('finish', () => clearInterval(heartbeat));
});


fastify.post("/generate", {
  config: {
    rateLimit: {
      max: 5,
      timeWindow: '1 minute',
      errorResponseBuilder: (request, context) => ({
        statusCode: 429,
        error: 'Too Many Requests',
        message: `You've reached the generation limit. Please wait ${context.after} before trying again.`
      })
    }
  }
}, async (request, reply) => {
  const generationKey = "gen_" + Date.now() + "_" + Math.random().toString(36).substring(2,10);
  console.log(`📡 [API] /generate POST. Key: ${generationKey}`);

  // Turnstile human verification bypassed for local development


  console.log(`📡 [API] KEY LIST RECEIVED:`, Object.keys(request.body));
  console.log(`📡 [API] force_mass_email value:`, request.body.force_mass_email);
  if (request.body.participants && request.body.participants.length > 0) {
      console.log(`📡 [API] SAMPLE DATA (Row 1):`, JSON.stringify(request.body.participants[0]).substring(0, 200));
  }
  
  if (request.body.participants && request.body.participants.length > 1001) {
      return reply.code(400).send({ error: "Batch size too large. Only 1001 participants allowed." });
  }

  reply.send({ success: true, key: generationKey, message: "Tracking initiated." });

  const zipPath = path.join(__dirname, `temp_${generationKey}.zip`);
  
  workerContexts[generationKey] = {
      body: request.body,
      key: generationKey,
      zipPath,
      processedCount: 0,
      emailSuccessCount: 0,
      emailFailCount: 0,
      total: request.body.participants?.length || 0,
      archive: null,
      output: null,
      baseTemplate: null,
      fontBytes: null,
      scales: null,
      startTime: Date.now(),
      addedAt: Date.now()
  };
  
  if (request.body.publicId) {
      cloudinaryStore[generationKey] = request.body.publicId;
  }
  
  if (bullEnabled) {
      console.log(`📡 [API] BullMQ Enabled. Adding job ${generationKey} to Redis...`);
      await certQueue.add('generate-certs', { body: request.body, key: generationKey });
      await broadcastQueueUpdate();
      return;
  }

  // Add to queue with timestamp
  console.log(`📡 [API] BullMQ Disabled. Adding job ${generationKey} to internal memory queue...`);
  queue.push({ key: generationKey, addedAt: Date.now() });
  await broadcastQueueUpdate();

  console.log(`📡 [API] Current isGenerating status: ${isGenerating}`);
  if (!isGenerating) {
      processNextFairJob();
  }
});

async function embedQRCodeOnPDF(pdfBytes, certUniqueId) {
    try {
        const pdfDoc = await PDFDocument.load(pdfBytes);
        const pages = pdfDoc.getPages();
        if (pages.length === 0) return pdfBytes;

        const firstPage = pages[0];
        const { width, height } = firstPage.getSize();

        // The verification URL
        const verifyUrl = `http://localhost:5173/verify?id=${certUniqueId}`;
        
        // Generate QR code buffer (PNG format)
        const qrBuffer = await QRCode.toBuffer(verifyUrl, {
            type: 'png',
            width: 400,
            margin: 1,
            color: {
                dark: '#000000',
                light: '#FFFFFF'
            }
        });

        // Embed the QR code image into the PDF
        const qrImage = await pdfDoc.embedPng(qrBuffer);

        // Place the QR code in the bottom right corner of the certificate
        const qrSize = Math.max(120, Math.min(300, Math.round(width * 0.10)));
        const marginRight = Math.max(70, Math.min(300, Math.round(width * 0.08)));
        const marginBottom = Math.max(70, Math.min(300, Math.round(height * 0.08)));

        firstPage.drawImage(qrImage, {
            x: width - qrSize - marginRight,
            y: marginBottom,
            width: qrSize,
            height: qrSize
        });

        // Save the modified PDF
        return Buffer.from(await pdfDoc.save());
    } catch (e) {
        console.error("❌ Error embedding QR code on PDF:", e);
        return pdfBytes;
    }
}

async function generateSinglePDFLocal(participant, templateUrl, fields, customDimensions) {
    const pdfDoc = await PDFDocument.create();
    pdfDoc.registerFontkit(fontkit);
    
    let imageBytes = null;
    let img = null;
    let width = customDimensions?.width || 600;
    let height = customDimensions?.height || 400;

    if (templateUrl && templateUrl.startsWith("http")) {
      const isCloudinary = templateUrl.includes("res.cloudinary.com");
      const finalUrl = isCloudinary ? templateUrl.replace("/upload/", "/upload/q_auto,f_auto/") : templateUrl;
      imageBytes = await getBufferFromUrl(finalUrl);
      const lowerUrl = templateUrl.toLowerCase();
      if (lowerUrl.includes(".png")) {
        img = await pdfDoc.embedPng(imageBytes);
      } else {
        img = await pdfDoc.embedJpg(imageBytes);
      }
      if (!customDimensions && img) {
         width = img.width;
         height = img.height;
      }
    }

    const page = pdfDoc.addPage([width, height]);
    if (img) {
      page.drawImage(img, { x: 0, y: 0, width: width, height: height });
    }
    
    const scaleX = width / 600;
    const scaleY = height / 400;
    const scaleAvg = (scaleX + scaleY) / 2;

    for (const f of fields) {
      let value = participant[f.field] ? String(participant[f.field]) : "";
      value = value.trim();
      if (!value) continue;

      const fBytes = await getFontBytes(f.fontFamily, f.fontWeight);
      const customFont = await pdfDoc.embedFont(fBytes);

      let hex = (f.color || "#000000").replace("#", "");
      if (hex.length !== 6) hex = "000000";

      const r = parseInt(hex.substring(0, 2), 16) / 255;
      const g = parseInt(hex.substring(2, 4), 16) / 255;
      const b = parseInt(hex.substring(4, 6), 16) / 255;

      const fontSize = f.size * scaleAvg;
      const textWidth = customFont.widthOfTextAtSize(value, fontSize);
      const paddingX = 16 * scaleX;
      let xPos = f.x * scaleX + paddingX;

      if (f.textAlign === 'center') {
          const visualBoxWidth = Math.max(60, (textWidth / scaleAvg) + 32);
          xPos = (f.x + (visualBoxWidth / 2)) * scaleX - (textWidth / 2);
      } else if (f.textAlign === 'right') {
          const visualBoxWidth = Math.max(60, (textWidth / scaleAvg) + 32);
          xPos = (f.x + visualBoxWidth - 16) * scaleX - textWidth;
      }

      page.drawText(value, {
        x: xPos,
        y: (height - (f.y * scaleY)) - (fontSize * 0.8) - (8 * scaleY),
        size: fontSize,
        font: customFont,
        color: rgb(r, g, b),
        characterSpacing: f.letterSpacing || 0
      });
    }

    const certUniqueId = uuidv4();
    pdfDoc.setSubject(certUniqueId);
    pdfDoc.setKeywords([certUniqueId]);

    const watermarkText = "Generated by CertLock";
    const watermarkFontSize = 10 * scaleAvg;
    const watermarkFontBytes = await getFontBytes('NotoSans', '400');
    const watermarkFont = await pdfDoc.embedFont(watermarkFontBytes);
    const watermarkWidth = watermarkFont.widthOfTextAtSize(watermarkText, watermarkFontSize);
    
    const watermarkY = Math.max(35, Math.min(100, Math.round(height * 0.045)));
    page.drawText(watermarkText, {
        x: (width - watermarkWidth) / 2,
        y: watermarkY,
        size: watermarkFontSize,
        font: watermarkFont,
        color: rgb(0.6, 0.6, 0.6),
        opacity: 0.5
    });

    let pdfBytes = await pdfDoc.save();
    pdfBytes = await embedQRCodeOnPDF(pdfBytes, certUniqueId);
    return { pdfBytes, certUniqueId };
}

async function processNextFairJob() {
  console.log(`⚙️ [WORKER-LOOP] Attempting to process next job. isGenerating: ${isGenerating}, Queue Length: ${queue.length}`);
  if (isGenerating) return;
  isGenerating = true;

  try {
      while (queue.length > 0) {
          const queueItem = queue.shift();
          const key = typeof queueItem === 'string' ? queueItem : queueItem.key;
          const ctx = workerContexts[key];
          
          await broadcastQueueUpdate(); // Update queue positions after shift

          if (!ctx) {
              console.warn(`⚠️ Context missing for ${key}. Skipping.`);
              continue;
          }

          console.log(`🚀 Starting generation for: ${key}`);
          currentJobKey = key;
          
          // Set active job
          activeJob = {
              key: key,
              progress: { stage: "started", task: "Initializing...", percent: 0 },
              startTime: Date.now()
          };
          
          await broadcastQueueUpdate(); // Show active job to all users
          
          await generateHandlerFair(ctx);
          
          delete workerContexts[key];
          currentJobKey = null;
          activeJob = null;
          await broadcastQueueUpdate(); // Clear active job and update queue
      }
  } catch (err) {
      console.error("❌ Worker Loop Error:", err);
  } finally {
      isGenerating = false;
      currentJobKey = null;
      activeJob = null;
      await broadcastQueueUpdate();
  }
}

async function generateHandlerFair(ctx) {
  const { body, key, zipPath, total } = ctx;
  const { participants, templateUrl, fields, customDimensions } = body;
  console.log(`⚙️ [WORKER] Starting job ${key}. Email requested: ${body.force_mass_email}`);
  
  currentJobKey = key;
  ctx.abortController = new AbortController();


  if (stopRequestedStore[key]) {
      sendProgress(key, { stage: "cancelled", task: "Generation stopped by user" });
      delete workerContexts[key];
      delete stopRequestedStore[key];
      currentJobKey = null;
      activeJob = null;
      await broadcastQueueUpdate();
      return;
  }

  // Initialization
  if (!ctx.archive) {
      console.log(`🔨 Initializing New Archive for [${key}]`);
      try {
          ctx.archive = archiver("zip", { zlib: { level: 1 } });
          ctx.output = fs.createWriteStream(zipPath);
          ctx.archive.pipe(ctx.output);
          zipStore[key] = zipPath;
          sendProgress(key, { stage: "started", task: "🛠️ Setting up environment...", current: 0, total, percent: 0 });

          ctx.heartbeat = setInterval(() => {
            sendProgress(key, { type: "ping", ts: Date.now() });
          }, 3000);

          if (templateUrl) {
               console.log(`🖼️ Loading Template: ${templateUrl}`);
               sendProgress(key, { stage: "processing", task: "🖼️ Fetching certificate template...", current: 0, total, percent: 5 });
               const isCloudinary = templateUrl.includes("res.cloudinary.com");
               const finalUrl = isCloudinary ? templateUrl.replace("/upload/", "/upload/q_auto,f_auto/") : templateUrl;
               const buf = await getBufferFromUrl(finalUrl);
               
               let actualWidth = customDimensions?.width || 600;
               let actualHeight = customDimensions?.height || 400;

               sendProgress(key, { stage: "processing", task: "📑 Pre-processing PDF template...", current: 0, total, percent: 10 });
               if (!customDimensions) {
                   if (buf.slice(0, 4).toString() === "%PDF") {
                     const base = await PDFDocument.load(buf);
                     actualWidth = base.getPage(0).getSize().width;
                     actualHeight = base.getPage(0).getSize().height;
                     ctx.baseTemplate = base;
                   } else {
                     const tmpPdf = await PDFDocument.create();
                     const lowerUrl = templateUrl.toLowerCase();
                     const img = lowerUrl.includes(".png") ? await tmpPdf.embedPng(buf) : await tmpPdf.embedJpg(buf);
                     actualWidth = img.width;
                     actualHeight = img.height;
                     const page = tmpPdf.addPage([actualWidth, actualHeight]);
                     page.drawImage(img, { x: 0, y: 0, width: actualWidth, height: actualHeight });
                     ctx.baseTemplate = await PDFDocument.load(await tmpPdf.save());
                   }
               } else {
                   const tmpPdf = await PDFDocument.create();
                   const lowerUrl = templateUrl.toLowerCase();
                   const img = lowerUrl.includes(".png") ? await tmpPdf.embedPng(buf) : await tmpPdf.embedJpg(buf);
                   const page = tmpPdf.addPage([actualWidth, actualHeight]);
                   page.drawImage(img, { x: 0, y: 0, width: actualWidth, height: actualHeight });
                   ctx.baseTemplate = await PDFDocument.load(await tmpPdf.save());
               }
                const scaleX = actualWidth / 600;
                const scaleY = actualHeight / 400;
                ctx.scales = { scaleX, scaleY, scaleAvg: (scaleX + scaleY) / 2, actualWidth, actualHeight };
           }
           console.log(`🔤 Loading Font...`);
           sendProgress(key, { stage: "processing", task: "🔤 Loading high-resolution fonts...", current: 0, total, percent: 15 });
           const fontResp = await fetch("https://github.com/googlefonts/noto-fonts/raw/main/hinted/ttf/NotoSans/NotoSans-Regular.ttf", { signal: ctx.abortController.signal });
           ctx.fontBytes = Buffer.from(await fontResp.arrayBuffer());
           sendProgress(key, { stage: "processing", task: "🚀 Starting generation engine...", current: 0, total, percent: 20 });
       } catch (e) { 
          if (e.name === 'AbortError') {
              console.log(`🛑 Job [${key}] aborted during initialization.`);
          } else {
              console.error(`❌ Fair Init Failed for [${key}]:`, e.message); 
              sendProgress(key, { stage: "error", task: "⚠️ Initialization failed", message: e.message });
              delete workerContexts[key];
              currentJobKey = null;
              activeJob = null;
              await broadcastQueueUpdate();
              if (!bullEnabled) setTimeout(processNextFairJob, 500);
          }
          return;
      }
  }

  // --- ROBUST BATCH-PROCESSING LOOP ---
  try {
      while (ctx.processedCount < total && !stopRequestedStore[key]) {
          const p = participants[ctx.processedCount];
          const targetName = (p[Object.keys(p)[0]] || "Participant").toString();
          const percent = Math.round((ctx.processedCount / total) * 100);
          
          sendProgress(key, { 
              stage: "processing", 
              task: `🖋️ Processing # ${ctx.processedCount + 1} [${targetName}]`, 
              current: ctx.processedCount, 
              total, 
              percent,
              emailSuccessCount: ctx.emailSuccessCount,
              emailFailCount: ctx.emailFailCount,
              name: targetName
          });

          // Generate certificate locally
          const { pdfBytes, certUniqueId } = await generateSinglePDFLocal(p, templateUrl, fields, customDimensions);

          const file_hash = crypto.createHash('sha256').update(pdfBytes).digest('hex');
          const file_size = pdfBytes.length;

          if (sql) {
              try {
                  await sql`
                      INSERT INTO certificates (unique_id, participant_name, template_url, file_hash, file_size)
                      VALUES (${certUniqueId}, ${targetName}, ${ctx.body.templateUrl || ""}, ${file_hash}, ${file_size})
                  `;
               } catch (dbErr) {
                   console.error("❌ DATABASE INSERT ERROR:", dbErr.message);
               }
          }

          const safeName = targetName.replace(/[^a-z0-9_.-]/gi, "_").toLowerCase();
          const attachmentName = `${safeName}.pdf`;
          ctx.archive.append(pdfBytes, { name: attachmentName });

          const explicitFlag = String(body.force_mass_email) === "true" || body.force_mass_email === true;
          const emailKey = Object.keys(p).find(k => {
              const kl = k.toLowerCase();
              return kl.includes('email') || kl.includes('mail');
          });
          const targetEmail = p[emailKey]?.toString().trim();

          if (explicitFlag && targetEmail && targetEmail.includes('@')) {
              try {
                  const success = await sendEmail(targetEmail, targetName, pdfBytes, attachmentName, certUniqueId);
                  if (success) {
                      ctx.emailSuccessCount++;
                      sendProgress(key, { stage: "processing", task: `📧 Email Sent to ${targetEmail}`, emailSuccessCount: ctx.emailSuccessCount });
                  } else {
                      ctx.emailFailCount++;
                      sendProgress(key, { stage: "processing", task: `⚠️ Email FAILED to ${targetEmail}`, emailFailCount: ctx.emailFailCount });
                  }
              } catch (mailErr) {
                  console.error(`❌ Mail Error for ${targetEmail}:`, mailErr.message);
                  ctx.emailFailCount++;
              }
          }

          ctx.processedCount++;
          const finalPercent = Math.round((ctx.processedCount / total) * 100);
          sendProgress(key, { 
              stage: "processing", 
              task: `🖋️ # ${ctx.processedCount} Processed [${targetName}]`, 
              current: ctx.processedCount, 
              total, 
              percent: finalPercent,
              emailSuccessCount: ctx.emailSuccessCount,
              emailFailCount: ctx.emailFailCount,
              name: targetName
          });
      }
      console.log(`🏁 [ENGINE] Generation completed. Total: ${ctx.processedCount}/${total}`);
  } catch (e) {
      if (e.name === 'AbortError') {
          console.log(`🛑 Job [${key}] fetch aborted by user.`);
      } else {
          console.error(`❌ [WORKER-FATAL] Service failure for [${key}]:`, e.message);
          sendProgress(key, { stage: "error", task: "⚠️ High-speed engine failure", message: e.message });
          throw e; 
      }
  }


  // Finalization
  try {
      if (ctx.processedCount >= total || stopRequestedStore[key]) {
          console.log(`✅ Job Finished or Stopped for [${key}]`);
          if (stopRequestedStore[key]) {
              console.log(`🗑️ Job [${key}] Stopped.`);
              clearInterval(ctx.heartbeat);
              try { if (fs.existsSync(zipPath)) fs.unlinkSync(zipPath); } catch (e) {}
              
              if (cloudinaryStore[key]) {
                  await deleteTemplate(cloudinaryStore[key]);
              }
              delete cloudinaryStore[key];
              delete zipStore[key];
              progressStore[key] = { stage: "cancelled", task: "🚫 Generation Stopped" };
              sendProgress(key, { ...progressStore[key], current: ctx.processedCount, total });
          } else {
              sendProgress(key, { stage: "processing", task: "📦 Finalizing ZIP archive...", current: total, total, percent: 99 });
              await ctx.archive.finalize();
              await new Promise(r => ctx.output.on("close", r));
              clearInterval(ctx.heartbeat);
              progressStore[key] = { 
                   stage: "completed", 
                   downloadUrl: `/download?key=${key}`,
                   emailSuccessCount: ctx.emailSuccessCount,
                   emailFailCount: ctx.emailFailCount
               };
              sendProgress(key, { ...progressStore[key], task: "🎉 All certificates generated! Ready for download.", current: ctx.processedCount, total });
              
              // Wait for download
              console.log(`⏸️ Job [${key}] Finalized. Waiting for download...`);
              const downloadStage = { stage: "completed", task: "📥 Ready for download! Click the download button.", current: total, total, percent: 100, emailSuccessCount: ctx.emailSuccessCount, emailFailCount: ctx.emailFailCount, downloadUrl: `/download?key=${key}` };
              progressStore[key] = downloadStage;
              sendProgress(key, downloadStage);
              
              let waitSecs = 0;
              while (!downloadedStore[key]) {
                  await new Promise(r => setTimeout(r, 1000));
                  waitSecs++;

                  if (!downloadStartedStore[key]) {
                      const isConnected = clients.some(c => c.key === key);
                      if (!isConnected && lastSeenClients[key]) {
                          const disconnectedDuration = (Date.now() - lastSeenClients[key]) / 1000;
                          if (disconnectedDuration > 5) {
                              console.warn(`🛑 Job [${key}] cancelled: User disconnected before downloading.`);
                              break;
                          }
                      }
                      if (waitSecs > 30) {
                          console.warn(`⏰ User failed to start download within 30s. Timing out.`);
                          break;
                      }
                  } else {
                      if (waitSecs > 300) {
                          console.warn(`⏰ Download stuck for >5 mins. Forcing release.`);
                          break;
                      }
                  }
                  if (waitSecs % 5 === 0) await broadcastQueueUpdate();
              }
              console.log(downloadedStore[key] ? `✅ User [${key}] completed download.` : `⏰ Timeout waiting for download [${key}].`);
          
              if (!downloadedStore[key]) {
                  try { if (fs.existsSync(zipPath)) fs.unlinkSync(zipPath); } catch (e) {}
                  if (cloudinaryStore[key]) {
                      await deleteTemplate(cloudinaryStore[key]);
                  }
                  delete cloudinaryStore[key];
                  delete zipStore[key];
                  delete progressStore[key];
              }
          }

          delete workerContexts[key];
          delete stopRequestedStore[key];
          delete downloadedStore[key]; 
          delete downloadStartedStore[key];
          ctx.baseTemplate = null; 
          ctx.fontBytes = null;
          currentJobKey = null;
          activeJob = null;
          await broadcastQueueUpdate();
      }
  } catch (err) {
      console.error(`❌ Finalization error for [${key}]:`, err);
  }
}

fastify.get("/download", async (request, reply) => {
  const key = request.query.key;
  downloadStartedStore[key] = true;
  
  const filePath = zipStore[key];
  if (!filePath || !fs.existsSync(filePath)) return reply.code(404).send("File not found");
  
  const stream = fs.createReadStream(filePath);
  
  stream.on('close', () => {
    setTimeout(async () => {
        try {
            if (fs.existsSync(filePath)) {
                fs.unlinkSync(filePath);
                delete zipStore[key];
                delete progressStore[key];
                console.log(`🗑️ Auto-deleted ZIP ${key} after download.`);
                if (cloudinaryStore[key]) {
                    await deleteTemplate(cloudinaryStore[key]);
                }
                delete cloudinaryStore[key];
            }
        } catch (err) {}
    }, 1000);
  });

  reply.raw.on('finish', () => {
    console.log(`📥 ZIP Download Signal Received: ${key}`);
    downloadedStore[key] = true;
    broadcastQueueUpdate();
  });

  reply.header('Content-Disposition', `attachment; filename=certificates_${key}.zip`);
  return reply.send(stream);
});

fastify.post("/stop-generate", async (request, reply) => {
  const { key } = request.body;
  if (!key) return reply.code(400).send({ error: "Missing job key" });
  
  stopRequestedStore[key] = true;
  if (workerContexts[key] && workerContexts[key].abortController) {
      workerContexts[key].abortController.abort();
  }
  queue = queue.filter(item => (typeof item === 'string' ? item !== key : item.key !== key));
  await broadcastQueueUpdate();
  sendProgress(key, { stage: "cancelled", task: "🚫 Stopping generation..." });
  return { success: true };
});

fastify.post("/cleanup", async (request, reply) => {
  const { key, publicId } = request.body;
  
  if (key && zipStore[key]) {
    const filePath = zipStore[key];
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    delete zipStore[key];
    delete progressStore[key];
  }

  if (publicId) {
      try {
          await cloudinary.uploader.destroy(publicId);
          fastify.log.info(`🗑️ Deleted Cloudinary Template ${publicId}`);
      } catch (e) {
          fastify.log.error("Cloudinary Cleanup Error:", e);
      }
  }

  return { success: true };
});

fastify.post('/verify-pdf', async (request, reply) => {
    const data = await request.file();
    if (!data) return reply.code(400).send({ error: "No file uploaded" });

    const verifyKey = data.fields?.verifyKey?.value;
    console.log(`🔍 [VERIFY] Request received. Key: ${verifyKey || 'NONE'}`);

    const sleep = (ms) => new Promise(r => setTimeout(r, ms));

    // comparisons = [{label, expected?, got, match}]
    const emit = async (step, status, message, comparisons = []) => {
        console.log(`  [${status.toUpperCase()}] ${step}: ${message}`);
        if (verifyKey) {
            sendProgress(verifyKey, { type: 'verify_progress', step, status, message, comparisons });
            await sleep(700);
        }
    };

    try {
        // ── Step 1: Hashing ──
        await emit('hashing', 'working', 'Computing SHA-256 fingerprint of the uploaded file...');
        const buffer = await data.toBuffer();
        const uploadedHash = crypto.createHash('sha256').update(buffer).digest('hex');
        const uploadedSize = buffer.length;
        await emit('hashing', 'pass', 'Cryptographic hash computed successfully.', [
            { label: 'Algorithm', got: 'SHA-256', match: true },
            { label: 'File Hash', got: uploadedHash, match: true },
            { label: 'File Size', got: `${(uploadedSize / 1024).toFixed(2)} KB`, match: true },
        ]);

        // ── Step 2: Parsing ──
        await emit('parsing', 'working', 'Decoding PDF binary and extracting CertLock metadata...');
        let pdfDoc, subjectId, keywordId;
        try {
            pdfDoc    = await PDFDocument.load(buffer, { ignoreEncryption: true });
            subjectId = pdfDoc.getSubject();
            keywordId = pdfDoc.getKeywords();
            
            if (subjectId) {
                subjectId = subjectId.replace(/[^a-fA-F0-9-]/g, '').trim();
            }
            if (keywordId) {
                keywordId = keywordId.replace(/[^a-fA-F0-9-]/g, '').trim();
            }
        } catch (parseErr) {
            await emit('parsing', 'fail', 'PDF could not be decoded.', [
                { label: 'PDF Valid',    expected: 'true', got: 'false', match: false },
                { label: 'Parse Error', got: parseErr.message, match: false },
            ]);
            return reply.send({ verified: false, message: "This file could not be read as a valid PDF." });
        }
        if (!subjectId) {
            await emit('parsing', 'fail', 'No CertLock metadata found inside this PDF.', [
                { label: 'PDF Valid',     got: 'true',       match: true  },
                { label: 'Subject Field', expected: '<UUID>', got: '(empty)', match: false },
                { label: 'Diagnosis',     got: 'Not a CertLock certificate, or metadata was stripped.', match: false },
            ]);
            return reply.send({ verified: false, message: "No verification data found in this PDF." });
        }
        await emit('parsing', 'pass', 'Metadata block found and decoded.', [
            { label: 'PDF Valid',     got: 'true',    match: true },
            { label: 'Subject Field', got: subjectId, match: true },
            { label: 'Keywords',      got: keywordId ? keywordId.substring(0, 40) + '...' : '(none)', match: !!keywordId },
        ]);

        // ── Step 3: Registry ──
        await emit('registry', 'working', 'Querying the official CertLock issuance registry...');
        const uniqueId = subjectId;
        console.log("🔍 [VERIFY-DEBUG] uniqueId length:", uniqueId.length, "chars:", [...uniqueId].map(c => c.charCodeAt(0)));
        if (!sql) return reply.send({ verified: false, message: "Database not configured." });

        await emit('registry', 'working', 'Authenticating document against official registry...');
        const records = await sql`SELECT * FROM certificates WHERE unique_id = ${uniqueId}`;

        if (!records || records.length === 0) {
            await emit('registry', 'fail', 'No matching record found in the official registry.', [
                { label: 'Cert ID',      got: uniqueId,         match: false },
                { label: 'Registry Hit', expected: '1 record',  got: '0 records', match: false },
                { label: 'Diagnosis',    got: 'Never issued by CertLock, or the ID was fabricated.', match: false },
            ]);
            return reply.send({ verified: false, message: "Certificate ID not found in our records." });
        }

        const dbRecord = records[0];

        if (dbRecord.file_hash && dbRecord.file_hash !== uploadedHash) {
            await emit('registry', 'fail', 'Integrity check failed — document was modified after issuance.', [
                { label: 'Registry Check', got: 'Match Found ✓', match: true },
                { label: 'Integrity',      expected: 'STABLE', got: 'TAMPERED', match: false },
                { label: 'Fingerprint',    expected: dbRecord.file_hash, got: uploadedHash, match: false },
                { label: 'Audit Result',   got: '🚨 DOCUMENT MODIFIED AFTER ISSUANCE', match: false },
            ]);
            return reply.send({ verified: false, message: "SECURITY ALERT: This PDF has been modified after issuance. Authenticity cannot be confirmed." });
        }

        if (dbRecord.file_size && parseInt(dbRecord.file_size) !== uploadedSize) {
            await emit('registry', 'fail', 'Integrity check failed — document size mismatch.', [
                { label: 'Registry Check', got: 'Match Found ✓', match: true },
                { label: 'Integrity',      expected: 'STABLE', got: 'RESIZED', match: false },
                { label: 'File Size',       expected: `${dbRecord.file_size} B`, got: `${uploadedSize} B`, match: false },
                { label: 'Audit Result',   got: '🚨 DOCUMENT SIZE HAS BEEN ALTERED', match: false },
            ]);
            return reply.send({ verified: false, message: "SECURITY ALERT: File size discrepancy detected. This document may have been tampered with." });
        }

        await emit('registry', 'pass', 'Registry record found — all integrity checks passed.', [
            { label: 'Cert ID',      got: uniqueId,                                                              match: true },
            { label: 'Registry Hit', got: '1 record found',                                                     match: true },
            { label: 'Hash Check',   expected: dbRecord.file_hash ? dbRecord.file_hash.substring(0,20)+'...' : 'N/A', got: uploadedHash.substring(0,20)+'...', match: true },
            { label: 'Size Check',   expected: dbRecord.file_size ? `${(parseInt(dbRecord.file_size)/1024).toFixed(1)} KB` : 'N/A', got: `${(uploadedSize/1024).toFixed(1)} KB`, match: true },
            { label: 'Issued To',    got: dbRecord.participant_name,                                            match: true },
        ]);

        return reply.send({
            verified: true,
            data: { name: dbRecord.participant_name, date: dbRecord.issue_date, id: uniqueId }
        });

    } catch (e) {
        console.error("❌ Verification Error:", e.message);
        if (verifyKey) sendProgress(verifyKey, { type: 'verify_progress', step: 'error', status: 'fail', message: 'An unexpected server error occurred.', comparisons: [{ label: 'Error', got: e.message, match: false }] });
        return reply.code(500).send({ error: "Failed to process PDF" });
    }
});

// ROUTE: GET /verify-id (Instant Registry Lookup)
fastify.get('/verify-id', async (request, reply) => {
  const { id } = request.query;
  if (!id) return reply.code(400).send({ error: "Certificate ID is required" });
  if (!sql) return reply.code(503).send({ error: "Database not configured" });

  try {
    const result = await sql`SELECT * FROM certificates WHERE unique_id = ${id}`;
    if (result.length === 0) {
      return reply.code(404).send({ error: "Certificate not found in official registry" });
    }

    return { 
      verified: true, 
      data: { 
        id: result[0].unique_id, 
        name: result[0].participant_name, 
        date: result[0].created_at 
      } 
    };
  } catch (err) {
    return reply.code(500).send({ error: "Database lookup failed" });
  }
});







// ================================================================
// ██████╗ ██╗   ██╗██╗███████╗    ██████╗  ██████╗ ██╗   ██╗████████╗███████╗███████╗
// ██╔═══██╗██║   ██║██║╚══███╔╝    ██╔══██╗██╔═══██╗██║   ██║╚══██╔══╝██╔════╝██╔════╝
// ██║   ██║██║   ██║██║  ███╔╝     ██████╔╝██║   ██║██║   ██║   ██║   █████╗  ███████╗
// ██║▄▄ ██║██║   ██║██║ ███╔╝      ██╔══██╗██║   ██║██║   ██║   ██║   ██╔══╝  ╚════██║
// ╚██████╔╝╚██████╔╝██║███████╗    ██║  ██║╚██████╔╝╚██████╔╝   ██║   ███████╗███████║
//  ╚══▀▀═╝  ╚═════╝ ╚═╝╚══════╝    ╚═╝  ╚═╝ ╚═════╝  ╚═════╝   ╚═╝   ╚══════╝╚══════╝
//
// ✅ SAFE ADD-ON — Does NOT touch any existing CertLock routes or logic
// ✅ All quiz routes are prefixed with /quiz/ to avoid conflicts
// ================================================================


// ----------------------------------------------------------------
// ROUTE: POST /quiz/users
// Create or retrieve a quiz user by email
// Body: { email, name }
// ----------------------------------------------------------------
// ----------------------------------------------------------------
// AUTO-MIGRATION: Ensure 'type' column exists in quiz_questions
// ----------------------------------------------------------------
if (sql) {
  (async () => {
    try {
      // 0. User type migration for main users table
      await sql`ALTER TABLE users ADD COLUMN IF NOT EXISTS user_type VARCHAR(50) DEFAULT 'User'`;

      // 1. Existing type column migration
      await sql`ALTER TABLE quiz_questions ADD COLUMN IF NOT EXISTS type VARCHAR(50) DEFAULT 'mcq'`;
      
      // 2. Quiz Metadata (Description, Duration, Scheduling)
      await sql`ALTER TABLE quiz_quizzes ADD COLUMN IF NOT EXISTS description TEXT`;
      await sql`ALTER TABLE quiz_quizzes ADD COLUMN IF NOT EXISTS duration_minutes INT DEFAULT 30`;
      await sql`ALTER TABLE quiz_quizzes ADD COLUMN IF NOT EXISTS start_time TIMESTAMP`;
      await sql`ALTER TABLE quiz_quizzes ADD COLUMN IF NOT EXISTS end_time TIMESTAMP`;
      await sql`ALTER TABLE quiz_quizzes ADD COLUMN IF NOT EXISTS access_key VARCHAR(50)`;

      // 3. Allowed Students (Whitelist)
      await sql`
        CREATE TABLE IF NOT EXISTS quiz_allowed_students (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          quiz_id UUID REFERENCES quiz_quizzes(id) ON DELETE CASCADE,
          email VARCHAR(255) NOT NULL,
          created_at TIMESTAMP DEFAULT NOW(),
          UNIQUE(quiz_id, email)
        )
      `;

      // 4. Relax Constraint: Allow creators from main system without mandatory quiz_users entry
      await sql`ALTER TABLE quiz_quizzes DROP CONSTRAINT IF EXISTS quiz_quizzes_created_by_fkey`;

      console.log("✅ Database migration: Quiz Pro features ensured");
    } catch (err) {
      console.warn("⚠️ Migration warning:", err.message);
    }
  })();
}

fastify.post('/quiz/users', async (request, reply) => {
  try {
    if (!sql) return reply.code(503).send({ error: "Database not configured" });

    const { email, name } = request.body;
    if (!email || !name) return reply.code(400).send({ error: "email and name are required" });

    // Upsert: insert if not exists, return existing if conflict
    const existing = await sql`
      SELECT * FROM quiz_users WHERE email = ${email}
    `;

    if (existing.length > 0) {
      const token = jwt.sign({ 
        id: existing[0].id, 
        email: existing[0].email, 
        name: existing[0].name,
        user_type: 'Student' 
      }, JWT_SECRET, { expiresIn: '7d' });
      return reply.send({ success: true, user: existing[0], token, created: false });
    }

    const result = await sql`
      INSERT INTO quiz_users (email, name)
      VALUES (${email}, ${name})
      RETURNING *
    `;

    const token = jwt.sign({ 
      id: result[0].id, 
      email: result[0].email, 
      name: result[0].name,
      user_type: 'Student'
    }, JWT_SECRET, { expiresIn: '7d' });
    return reply.code(201).send({ success: true, user: result[0], token, created: true });
  } catch (err) {
    fastify.log.error("Quiz /users error:", err);
    return reply.code(500).send({ error: "Failed to create user", details: err.message });
  }
});

// ROUTE: GET /quiz/refresh-token
fastify.get('/quiz/refresh-token', async (request, reply) => {
  const user = verifyToken(request);
  if (!user) return reply.code(401).send({ error: "Invalid or expired token" });
  
  // Create fresh token with same payload but new expiry
  const { iat, exp, ...payload } = user;
  const token = jwt.sign(payload, JWT_SECRET, { expiresIn: '7d' });
  
  return { success: true, token };
});


// ----------------------------------------------------------------
// ROUTE: GET /quiz/users/:id
// Get a specific quiz user by UUID
// ----------------------------------------------------------------
fastify.get('/quiz/users/:id', async (request, reply) => {
  try {
    if (!sql) return reply.code(503).send({ error: "Database not configured" });

    const { id } = request.params;
    const result = await sql`SELECT * FROM quiz_users WHERE id = ${id}`;

    if (result.length === 0) return reply.code(404).send({ error: "User not found" });
    return reply.send({ success: true, user: result[0] });
  } catch (err) {
    fastify.log.error("Quiz GET /users/:id error:", err);
    return reply.code(500).send({ error: "Failed to fetch user", details: err.message });
  }
});


// ----------------------------------------------------------------
// ROUTE: POST /quiz/quizzes
// Create a new quiz
// Body: { title, created_by } (created_by = quiz_users UUID)
// ----------------------------------------------------------------
fastify.post('/quiz/quizzes', async (request, reply) => {
  try {
    if (!sql) return reply.code(503).send({ error: "Database not configured" });

    const user = verifyToken(request);
    if (!user) {
      return reply.code(401).send({ error: "Authentication required to create quiz" });
    }

    const { title, description, duration_minutes, start_time, end_time, access_key } = request.body;
    if (!title) return reply.code(400).send({ error: "title is required" });

    // Prefer Google sub if available, otherwise use Quiz User ID
    const dbCreatedBy = user.sub || user.id;
    const dbDesc = description && description !== "" ? description : null;
    const dbDuration = parseInt(duration_minutes) || 30;
    const dbStart = start_time && start_time !== "" ? start_time : null;
    const dbEnd = end_time && end_time !== "" ? end_time : null;
    const dbKey = access_key && access_key !== "" ? access_key : null;

    const result = await sql`
      INSERT INTO quiz_quizzes (title, created_by, description, duration_minutes, start_time, end_time, access_key)
      VALUES (${title}, ${dbCreatedBy}, ${dbDesc}, ${dbDuration}, ${dbStart}, ${dbEnd}, ${dbKey})
      RETURNING *
    `;

    return reply.code(201).send({ success: true, quiz: result[0] });
  } catch (err) {
    console.error("❌ Quiz POST /quizzes error:", err);
    console.error("Payload received:", request.body);
    return reply.code(500).send({ 
      error: "Failed to create quiz", 
      details: err.message,
      payload: request.body 
    });
  }
});

// ----------------------------------------------------------------
// ROUTE: DELETE /quiz/quizzes/:id
// Delete a quiz (Only if creator)
// ----------------------------------------------------------------
fastify.delete('/quiz/quizzes/:id', async (request, reply) => {
  try {
    if (!sql) return reply.code(503).send({ error: "Database not configured" });

    const { id } = request.params;
    const user = verifyToken(request);

    if (!user) {
      return reply.code(401).send({ error: "Authentication required" });
    }

    const quiz = await sql`SELECT created_by FROM quiz_quizzes WHERE id = ${id}`;
    if (quiz.length === 0) return reply.code(404).send({ error: "Quiz not found" });

    // Match against user.id (Quiz User) or user.sub (Google User)
    const ownerId = String(quiz[0].created_by);
    if (ownerId !== String(user.id) && ownerId !== String(user.sub)) {
      return reply.code(403).send({ error: "You are not authorized to delete this quiz" });
    }

    await sql`DELETE FROM quiz_quizzes WHERE id = ${id}`;
    return reply.send({ success: true, message: "Quiz deleted successfully" });
  } catch (err) {
    fastify.log.error("Quiz DELETE error:", err);
    return reply.code(500).send({ error: "Failed to delete quiz" });
  }
});

// ROUTE: POST /quiz/quizzes/:id/whitelist
// Add allowed emails
fastify.post('/quiz/quizzes/:id/whitelist', async (request, reply) => {
  try {
    const { id } = request.params;
    const user = verifyToken(request);
    if (!user) return reply.code(401).send({ error: "Auth required" });

    const quiz = await sql`SELECT created_by FROM quiz_quizzes WHERE id = ${id}`;
    if (quiz.length === 0) return reply.code(404).send({ error: "Quiz not found" });
    if (String(quiz[0].created_by) !== String(user.id) && String(quiz[0].created_by) !== String(user.sub)) {
      return reply.code(403).send({ error: "Unauthorized" });
    }

    const { emails } = request.body;
    if (!emails || !Array.isArray(emails)) return reply.code(400).send({ error: "emails array required" });

    // HTTP-based Neon driver doesn't support .begin() transaction. 
    // We execute sequentially.
    await sql`DELETE FROM quiz_allowed_students WHERE quiz_id = ${id}`;
    
    for (const email of emails) {
      if (email.trim()) {
        await sql`INSERT INTO quiz_allowed_students (quiz_id, email) VALUES (${id}, ${email.trim().toLowerCase()})`;
      }
    }

    return { success: true };
  } catch (err) {
    fastify.log.error("Whitelist sync error:", err);
    return reply.code(500).send({ error: "Sync failed", details: err.message });
  }
});

// ROUTE: PUT /quiz/quizzes/:id
// Update quiz metadata
fastify.put('/quiz/quizzes/:id', async (request, reply) => {
  try {
    const { id } = request.params;
    const user = verifyToken(request);
    if (!user) return reply.code(401).send({ error: "Auth required" });

    const quizCheck = await sql`SELECT created_by FROM quiz_quizzes WHERE id = ${id}`;
    if (quizCheck.length === 0) return reply.code(404).send({ error: "Quiz not found" });
    if (String(quizCheck[0].created_by) !== String(user.id) && String(quizCheck[0].created_by) !== String(user.sub)) {
      return reply.code(403).send({ error: "Unauthorized" });
    }

    const { title, description, duration_minutes, start_time, end_time, access_key } = request.body;

    // Sanitize inputs
    const dbDesc = description && description !== "" ? description : null;
    const dbDuration = parseInt(duration_minutes) || 30;
    const dbStart = start_time && start_time !== "" ? start_time : null;
    const dbEnd = end_time && end_time !== "" ? end_time : null;
    const dbKey = access_key && access_key !== "" ? access_key : null;

    const result = await sql`
      UPDATE quiz_quizzes
      SET title = ${title},
          description = ${dbDesc},
          duration_minutes = ${dbDuration},
          start_time = ${dbStart},
          end_time = ${dbEnd},
          access_key = ${dbKey}
      WHERE id = ${id}
      RETURNING *
    `;

    if (result.length === 0) return reply.code(404).send({ error: "Quiz not found" });
    return { success: true, quiz: result[0] };
  } catch (err) {
    return reply.code(500).send({ error: "Update failed", details: err.message });
  }
});


// ROUTE: GET /quiz/quizzes/:id/analytics
// Get detailed performance analytics for a quiz
fastify.get('/quiz/quizzes/:id/analytics', async (request, reply) => {
  try {
    if (!sql) return reply.code(503).send({ error: "Database not configured" });
    const { id } = request.params;
    const user = verifyToken(request);
    if (!user) return reply.code(401).send({ error: "Auth required" });

    const quiz = await sql`SELECT created_by FROM quiz_quizzes WHERE id = ${id}`;
    if (quiz.length === 0) return reply.code(404).send({ error: "Quiz not found" });
    if (String(quiz[0].created_by) !== String(user.id) && String(quiz[0].created_by) !== String(user.sub)) {
      return reply.code(403).send({ error: "Unauthorized" });
    }

    // 1. Overall Stats
    const stats = await sql`
      SELECT 
        COUNT(a.id) as total_attempts,
        COUNT(DISTINCT a.user_id) as unique_participants,
        AVG(s.total_score) as avg_score,
        MAX(s.total_score) as top_score
      FROM quiz_attempts a
      JOIN quiz_scores s ON a.id = s.attempt_id
      WHERE a.quiz_id = ${id} AND a.submitted_at IS NOT NULL
    `;

    // 2. Score Distribution (Grouped by 10% ranges)
    const distribution = await sql`
      SELECT 
        floor(s.total_score * 10 / NULLIF((SELECT SUM(points) FROM quiz_questions WHERE quiz_id = ${id}), 0)) * 10 as range,
        COUNT(*) as count
      FROM quiz_attempts a
      JOIN quiz_scores s ON a.id = s.attempt_id
      WHERE a.quiz_id = ${id} AND a.submitted_at IS NOT NULL
      GROUP BY range
      ORDER BY range
    `;

    // 3. Question Performance (Correctness rate per question)
    const questions = await sql`
      SELECT 
        q.id, q.question,
        COUNT(r.id) as total_responses,
        SUM(CASE WHEN r.is_correct THEN 1 ELSE 0 END) as correct_responses
      FROM quiz_questions q
      LEFT JOIN quiz_responses r ON q.id = r.question_id
      WHERE q.quiz_id = ${id}
      GROUP BY q.id, q.question
    `;

    return reply.send({
      success: true,
      stats: stats[0],
      distribution,
      questions: questions.map(q => ({
        ...q,
        rate: q.total_responses > 0 ? Math.round((q.correct_responses / q.total_responses) * 100) : 0
      }))
    });
  } catch (err) {
    fastify.log.error("Quiz Analytics Error:", err);
    return reply.code(500).send({ error: "Analytics failed", details: err.message });
  }
});


// ROUTE: GET /quiz/quizzes/:id/export
// Get quiz results formatted for the certificate studio
fastify.get('/quiz/quizzes/:id/export', async (request, reply) => {
  try {
    if (!sql) return reply.code(503).send({ error: "Database not configured" });
    const { id } = request.params;
    const user = verifyToken(request);
    if (!user) return reply.code(401).send({ error: "Auth required" });

    const quiz = await sql`SELECT created_by FROM quiz_quizzes WHERE id = ${id}`;
    if (quiz.length === 0) return reply.code(404).send({ error: "Quiz not found" });
    if (String(quiz[0].created_by) !== String(user.id) && String(quiz[0].created_by) !== String(user.sub)) {
      return reply.code(403).send({ error: "Unauthorized" });
    }

    // Fetch quiz total possible points
    const quizMetadata = await sql`
      SELECT COALESCE(SUM(points), 0) as total FROM quiz_questions WHERE quiz_id = ${id}
    `;
    const totalPoints = parseInt(quizMetadata[0].total);

    // Get unique participants with their BEST score
    const results = await sql`
      SELECT DISTINCT ON (u.email)
        u.name AS "Name", 
        u.email AS "Email", 
        s.total_score AS "Score",
        ${totalPoints} AS "Total"
      FROM quiz_users u
      JOIN quiz_attempts a ON u.id = a.user_id
      JOIN quiz_scores s ON a.id = s.attempt_id
      WHERE a.quiz_id = ${id} AND a.submitted_at IS NOT NULL
      ORDER BY u.email, s.total_score DESC
    `;
    
    return reply.send({
      success: true,
      columns: ["Name", "Email", "Score", "Total"],
      participants: results
    });
  } catch (err) {
    fastify.log.error("Quiz Export Error:", err);
    return reply.code(500).send({ error: "Export failed", details: err.message });
  }
});


// ----------------------------------------------------------------
// ROUTE: GET /quiz/quizzes
// List all quizzes (with creator name)
// ----------------------------------------------------------------
fastify.get('/quiz/quizzes', async (request, reply) => {
  try {
    if (!sql) return reply.code(503).send({ error: "Database not configured" });

    const result = await sql`
      SELECT q.*, u.name AS creator_name
      FROM quiz_quizzes q
      LEFT JOIN users u ON q.created_by = u.google_id
      ORDER BY q.created_at DESC
    `;

    return reply.send({ success: true, quizzes: result });
  } catch (err) {
    fastify.log.error("Quiz GET /quizzes error:", err);
    return reply.code(500).send({ 
      error: "Failed to fetch quizzes", 
      details: err.message,
      stack: process.env.NODE_ENV === 'development' ? err.stack : undefined 
    });
  }
});


// ----------------------------------------------------------------
// ROUTE: GET /quiz/quizzes/:id
// Get a specific quiz along with all its questions
// ----------------------------------------------------------------
fastify.get('/quiz/quizzes/:id', async (request, reply) => {
  try {
    if (!sql) return reply.code(503).send({ error: "Database not configured" });

    const { id } = request.params;

    const quizResult = await sql`
      SELECT q.*, u.name AS creator_name
      FROM quiz_quizzes q
      LEFT JOIN users u ON q.created_by = u.google_id
      WHERE q.id = ${id}
    `;

    if (quizResult.length === 0) return reply.code(404).send({ error: "Quiz not found" });

    const questions = await sql`
      SELECT id, question, options, points, type FROM quiz_questions
      WHERE quiz_id = ${id}
      ORDER BY id
    `;

    // Strip correct_answer from the response (don't leak answers to takers)
    const sanitizedQuestions = questions.map(({ correct_answer, ...q }) => q);

    return reply.send({
      success: true,
      quiz: quizResult[0],
      questions: sanitizedQuestions
    });
  } catch (err) {
    fastify.log.error("Quiz GET /quizzes/:id error:", err);
    return reply.code(500).send({ error: "Failed to fetch quiz", details: err.message });
  }
});


// ----------------------------------------------------------------
// ROUTE: POST /quiz/quizzes/:id/questions
// Add a question to a quiz
// Body: { question, options, correct_answer, points }
//   options format: ["Option A", "Option B", "Option C", "Option D"]
// ----------------------------------------------------------------
fastify.post('/quiz/quizzes/:id/questions', async (request, reply) => {
  try {
    if (!sql) return reply.code(503).send({ error: "Database not configured" });

    const quizId = parseInt(request.params.id);
    const { question, options, correct_answer, points, type = 'mcq' } = request.body;

    if (!question || !correct_answer) {
      return reply.code(400).send({ error: "question and correct_answer are required" });
    }

    // Verify quiz exists
    const quizCheck = await sql`SELECT id FROM quiz_quizzes WHERE id = ${quizId}`;
    if (quizCheck.length === 0) return reply.code(404).send({ error: "Quiz not found" });

    const result = await sql`
      INSERT INTO quiz_questions (quiz_id, question, options, correct_answer, points, type)
      VALUES (
        ${quizId},
        ${question},
        ${JSON.stringify(Array.isArray(options) ? options : [])}::jsonb,
        ${correct_answer},
        ${parseInt(points) || 1},
        ${type}
      )
      RETURNING *
    `;

    return reply.code(201).send({ success: true, question: result[0] });
  } catch (err) {
    fastify.log.error("Quiz POST /questions error:", err);
    return reply.code(500).send({ error: "Failed to add question", details: err.message });
  }
});


// ----------------------------------------------------------------
// ROUTE: DELETE /quiz/quizzes/:quizId/questions/:questionId
// Delete a specific question from a quiz
// ----------------------------------------------------------------
fastify.delete('/quiz/quizzes/:quizId/questions/:questionId', async (request, reply) => {
  try {
    if (!sql) return reply.code(503).send({ error: "Database not configured" });

    const { quizId, questionId } = request.params;

    const result = await sql`
      DELETE FROM quiz_questions
      WHERE id = ${questionId} AND quiz_id = ${quizId}
      RETURNING id
    `;

    if (result.length === 0) return reply.code(404).send({ error: "Question not found" });
    return reply.send({ success: true, deleted: result[0].id });
  } catch (err) {
    fastify.log.error("Quiz DELETE /questions/:id error:", err);
    return reply.code(500).send({ error: "Failed to delete question", details: err.message });
  }
});


// ----------------------------------------------------------------
// ROUTE: POST /quiz/attempts
// Start a new quiz attempt
// Body: { quiz_id, user_id }
// ----------------------------------------------------------------
fastify.post('/quiz/attempts', {
  config: {
    rateLimit: {
      max: 10,
      timeWindow: '1 minute'
    }
  }
}, async (request, reply) => {
  try {
    if (!sql) return reply.code(503).send({ error: "Database not configured" });

    const { quiz_id, user_id, access_key } = request.body;
    if (!quiz_id || !user_id) return reply.code(400).send({ error: "quiz_id and user_id are required" });

    // Verify quiz and user exist
    const quizCheck = await sql`SELECT * FROM quiz_quizzes WHERE id = ${quiz_id}`;
    if (quizCheck.length === 0) return reply.code(404).send({ error: "Quiz not found" });

    // ACCESS KEY CHECK
    if (quizCheck[0].access_key && quizCheck[0].access_key !== access_key) {
      return reply.code(401).send({ error: "Invalid Quiz Access Key. Please contact the administrator." });
    }

    // SCHEDULING CHECK
    const now = new Date();
    if (quizCheck[0].start_time && now < new Date(quizCheck[0].start_time)) {
      return reply.code(403).send({ 
        error: "This quiz hasn't started yet.", 
        startTime: quizCheck[0].start_time 
      });
    }
    if (quizCheck[0].end_time && now > new Date(quizCheck[0].end_time)) {
      return reply.code(403).send({ error: "This quiz has already ended." });
    }

    const userCheck = await sql`SELECT * FROM quiz_users WHERE id = ${user_id}`;
    if (userCheck.length === 0) return reply.code(404).send({ error: "User not found" });

    // ACCESS CONTROL: Check whitelist if it exists
    const whitelist = await sql`SELECT 1 FROM quiz_allowed_students WHERE quiz_id = ${quiz_id}`;
    if (whitelist.length > 0) {
      const allowed = await sql`
        SELECT 1 FROM quiz_allowed_students 
        WHERE quiz_id = ${quiz_id} AND LOWER(email) = LOWER(${userCheck[0].email})
      `;
      if (allowed.length === 0) {
        return reply.code(403).send({ error: "Your email is not authorized to take this quiz. Contact admin." });
      }
    }

    // ATTEMPT LIMIT: Max 2 attempts
    const existingAttempts = await sql`
      SELECT count(*) FROM quiz_attempts 
      WHERE user_id = ${user_id} AND quiz_id = ${quiz_id}
    `;
    const attemptCount = parseInt(existingAttempts[0].count);
    if (attemptCount >= 2) {
      return reply.code(403).send({ error: "Attempt limit reached. You have already submitted this quiz twice." });
    }

    const result = await sql`
      INSERT INTO quiz_attempts (quiz_id, user_id)
      VALUES (${quiz_id}, ${user_id})
      RETURNING *
    `;

    return reply.code(201).send({ success: true, attempt: result[0] });
  } catch (err) {
    fastify.log.error("Quiz POST /attempts error:", err);
    return reply.code(500).send({ error: "Failed to start attempt", details: err.message });
  }
});


// ----------------------------------------------------------------
// ROUTE: POST /quiz/attempts/:id/submit
// Submit answers for an attempt. Auto-scores and stores results.
// Body: { answers: [{ question_id, answer }] }
// Returns: { score, total, percent, responses }
// ----------------------------------------------------------------
fastify.post('/quiz/attempts/:id/submit', {
  config: {
    rateLimit: {
      max: 10,
      timeWindow: '1 minute'
    }
  }
}, async (request, reply) => {
  try {
    if (!sql) return reply.code(503).send({ error: "Database not configured" });

    const attemptId = parseInt(request.params.id);
    if (isNaN(attemptId)) return reply.code(400).send({ error: "Invalid attempt ID format" });

    // Verify attempt exists and is not already submitted
    const attemptResult = await sql`
      SELECT * FROM quiz_attempts WHERE id = ${attemptId}
    `;
    if (attemptResult.length === 0) return reply.code(404).send({ error: "Attempt not found" });

    const attempt = attemptResult[0];
    if (attempt.submitted_at) {
      return reply.code(409).send({ error: "This attempt has already been submitted" });
    }

    // Fetch all questions for this quiz to check answers
    const questions = await sql`
      SELECT id, correct_answer, points FROM quiz_questions
      WHERE quiz_id = ${attempt.quiz_id}
    `;

    const questionMap = {};
    questions.forEach(q => { questionMap[q.id] = q; });

    let totalScore = 0;
    const responseInserts = [];

    const { answers = [] } = request.body;
    for (const ans of answers) {
      const { question_id, answer } = ans;
      const question = questionMap[question_id];

      if (!question) continue; // Skip unknown question IDs silently

      const isCorrect = String(question.correct_answer || '').trim().toLowerCase() === String(answer || '').trim().toLowerCase();
      const pointsEarned = isCorrect ? (question.points || 1) : 0;
      totalScore += pointsEarned;

      responseInserts.push({ question_id, answer: answer || "", is_correct: isCorrect, points: pointsEarned });
    }

    // Insert all responses and update attempt in a single block
    try {
        if (responseInserts.length > 0) {
            for (const r of responseInserts) {
              await sql`
                INSERT INTO quiz_responses (attempt_id, question_id, answer, is_correct, points)
                VALUES (${attemptId}, ${r.question_id}, ${r.answer}, ${r.is_correct}, ${r.points})
              `;
            }
        }

        // Mark attempt as submitted
        await sql`
          UPDATE quiz_attempts
          SET submitted_at = NOW()
          WHERE id = ${attemptId}
        `;
    } catch (dbErr) {
        fastify.log.error("Database error during submission detail storage:", dbErr);
        // We continue to at least record the score if possible, or fail gracefully
    }

    // Upsert score into quiz_scores
    await sql`
      INSERT INTO quiz_scores (attempt_id, total_score)
      VALUES (${attemptId}, ${totalScore})
      ON CONFLICT (attempt_id) DO UPDATE SET total_score = ${totalScore}
    `;

    const maxPossible = questions.reduce((sum, q) => sum + (q.points || 1), 0);
    const percent = maxPossible > 0 ? Math.round((totalScore / maxPossible) * 100) : 0;

    return reply.send({
      success: true,
      result: {
        attempt_id: attemptId,
        score: totalScore,
        max_possible: maxPossible,
        percent,
        total_questions: questions.length,
        answered: responseInserts.length
      }
    });
  } catch (err) {
    fastify.log.error("Quiz POST /attempts/:id/submit error:", err);
    return reply.code(500).send({ error: "Failed to submit attempt", details: err.message });
  }
});


// ----------------------------------------------------------------
// ROUTE: GET /quiz/attempts/:id/result
// Get detailed result for a submitted attempt
// ----------------------------------------------------------------
fastify.get('/quiz/attempts/:id/result', async (request, reply) => {
  try {
    if (!sql) return reply.code(503).send({ error: "Database not configured" });

    const { id } = request.params;

    const attemptResult = await sql`
      SELECT a.*, u.name AS user_name, u.email AS user_email,
             q.title AS quiz_title, s.total_score
      FROM quiz_attempts a
      LEFT JOIN quiz_users u ON a.user_id = u.id
      LEFT JOIN quiz_quizzes q ON a.quiz_id = q.id
      LEFT JOIN quiz_scores s ON s.attempt_id = a.id
      WHERE a.id = ${id}
    `;

    if (attemptResult.length === 0) return reply.code(404).send({ error: "Attempt not found" });

    const responses = await sql`
      SELECT r.*, qq.question, qq.correct_answer, qq.options, qq.points AS max_points, qq.type
      FROM quiz_responses r
      LEFT JOIN quiz_questions qq ON r.question_id = qq.id
      WHERE r.attempt_id = ${id}
    `;

    return reply.send({
      success: true,
      attempt: attemptResult[0],
      responses
    });
  } catch (err) {
    fastify.log.error("Quiz GET /attempts/:id/result error:", err);
    return reply.code(500).send({ error: "Failed to fetch result", details: err.message });
  }
});


// ----------------------------------------------------------------
// ROUTE: GET /quiz/leaderboard/:quizId
// Get top scores for a quiz (sorted by score desc, then by time)
// Query: ?limit=10 (default 10)
// ----------------------------------------------------------------
fastify.get('/quiz/leaderboard/:quizId', async (request, reply) => {
  try {
    if (!sql) return reply.code(503).send({ error: "Database not configured" });

    const { quizId } = request.params;
    const limit = Math.min(parseInt(request.query.limit) || 10, 100);

    // Verify quiz exists
    const quizCheck = await sql`SELECT id, title FROM quiz_quizzes WHERE id = ${quizId}`;
    if (quizCheck.length === 0) return reply.code(404).send({ error: "Quiz not found" });

    const leaderboard = await sql`
      SELECT
        s.total_score,
        u.name AS user_name,
        a.submitted_at,
        a.started_at,
        EXTRACT(EPOCH FROM (a.submitted_at - a.started_at))::INT AS time_taken_seconds
      FROM quiz_scores s
      JOIN quiz_attempts a ON s.attempt_id = a.id
      JOIN quiz_users u ON a.user_id = u.id
      WHERE a.quiz_id = ${quizId}
        AND a.submitted_at IS NOT NULL
      ORDER BY s.total_score DESC, a.submitted_at ASC
      LIMIT ${limit}
    `;

    return reply.send({
      success: true,
      quiz: quizCheck[0],
      leaderboard
    });
  } catch (err) {
    fastify.log.error("Quiz GET /leaderboard/:quizId error:", err);
    return reply.code(500).send({ error: "Failed to fetch leaderboard", details: err.message });
  }
});


// ----------------------------------------------------------------
// ROUTE: GET /quiz/users/:userId/history
// Get all quiz attempts for a user
// ----------------------------------------------------------------
fastify.get('/quiz/users/:userId/history', async (request, reply) => {
  try {
    if (!sql) return reply.code(503).send({ error: "Database not configured" });

    const { userId } = request.params;

    const history = await sql`
      SELECT
        a.id AS attempt_id,
        a.started_at,
        a.submitted_at,
        q.id AS quiz_id,
        q.title AS quiz_title,
        s.total_score,
        EXTRACT(EPOCH FROM (a.submitted_at - a.started_at))::INT AS time_taken_seconds
      FROM quiz_attempts a
      JOIN quiz_quizzes q ON a.quiz_id = q.id
      LEFT JOIN quiz_scores s ON s.attempt_id = a.id
      WHERE a.user_id = ${userId}
      ORDER BY a.started_at DESC
    `;

    return reply.send({ success: true, history });
  } catch (err) {
    fastify.log.error("Quiz GET /users/:userId/history error:", err);
    return reply.code(500).send({ error: "Failed to fetch history", details: err.message });
  }
});





// ----------------------------------------------------------------
// ROUTE: GET /quiz/results-by-email
// Fetch all results for a student by their email
// ----------------------------------------------------------------
fastify.get('/quiz/results-by-email', async (request, reply) => {
  try {
    if (!sql) return reply.code(503).send({ error: "Database not configured" });
    const { email, quiz_id } = request.query;
    if (!email) return reply.code(400).send({ error: "Email is required" });

    const userResult = await sql`SELECT id, name, email FROM quiz_users WHERE email = ${email}`;
    if (userResult.length === 0) return reply.code(404).send({ error: "No records found for this email" });

    const userId = userResult[0].id;

    let attempts;
    if (quiz_id) {
      attempts = await sql`
        SELECT a.id, a.quiz_id, q.title AS quiz_title, a.submitted_at, s.total_score
        FROM quiz_attempts a
        JOIN quiz_quizzes q ON a.quiz_id = q.id
        JOIN quiz_scores s ON s.attempt_id = a.id
        WHERE a.user_id = ${userId} AND a.quiz_id = ${quiz_id} AND a.submitted_at IS NOT NULL
        ORDER BY a.submitted_at DESC
      `;
    } else {
      attempts = await sql`
        SELECT a.id, a.quiz_id, q.title AS quiz_title, a.submitted_at, s.total_score
        FROM quiz_attempts a
        JOIN quiz_quizzes q ON a.quiz_id = q.id
        JOIN quiz_scores s ON s.attempt_id = a.id
        WHERE a.user_id = ${userId} AND a.submitted_at IS NOT NULL
        ORDER BY a.submitted_at DESC
      `;
    }

    const attemptIds = attempts.map(a => a.id);
    const quizIds = [...new Set(attempts.map(a => a.quiz_id))];

    const allResponses = attemptIds.length > 0 ? await sql`
      SELECT r.attempt_id, r.answer, r.is_correct, r.points,
             qq.question, qq.correct_answer, qq.options, qq.points AS max_points
      FROM quiz_responses r
      JOIN quiz_questions qq ON r.question_id = qq.id
      WHERE r.attempt_id = ANY(${attemptIds})
    ` : [];

    const quizTotals = quizIds.length > 0 ? await sql`
      SELECT quiz_id, SUM(points) as total FROM quiz_questions 
      WHERE quiz_id = ANY(${quizIds})
      GROUP BY quiz_id
    ` : [];

    const results = attempts.map(attempt => ({
      ...attempt,
      responses: allResponses.filter(r => r.attempt_id === attempt.id),
      max_possible: parseInt(quizTotals.find(t => t.quiz_id === attempt.quiz_id)?.total || 0)
    }));

    return reply.send({ success: true, user: userResult[0], results });
  } catch (err) {
    fastify.log.error("Results by email error:", err);
    return reply.code(500).send({ error: "Failed to fetch results" });
  }
});

// ----------------------------------------------------------------
// UTILITY: GET /ping
// Connectivity check
// ----------------------------------------------------------------
fastify.get('/ping', async () => ({ status: "ok", time: Date.now(), engine: "CertLock-v2" }));





const start = async () => {
  try {
    const port = process.env.PORT || 5000;
    await fastify.listen({ port, host: '0.0.0.0' });
    console.log(`✅ Fastify server listening on port ${port}`);

    // Send startup test email
    if (GMAIL_USER && GMAIL_PASS) {
        console.log("📧 Sending startup test email to vimalraj5207@gmail.com...");
        transporter.sendMail({
            from: `"${GMAIL_SENDER_NAME}" <${GMAIL_USER}>`,
            to: "vimalraj5207@gmail.com",
            subject: "🚀 CertLock Server Started",
            text: `Server started successfully at ${new Date().toISOString()}. Gmail configuration is active.`,
            html: `<h3>🚀 CertLock Server Started</h3>
                   <p>The server started successfully at <b>${new Date().toLocaleString()}</b>.</p>
                   <p>Gmail SMTP configuration is active and working.</p>`
        }).then(() => {
            console.log("✅ Startup test email sent successfully.");
        }).catch((err) => {
            console.error("❌ Startup test email FAILED:", err.message);
            console.error("DEBUG INFO: Check if App Password is correct and ports are open.");
        });
    }
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};
start();
