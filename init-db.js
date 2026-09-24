import { neon } from '@neondatabase/serverless';
import dotenv from 'dotenv';

dotenv.config();

const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  console.error("❌ DATABASE_URL is missing in .env file");
  process.exit(1);
}

const sql = neon(DATABASE_URL);

async function initDB() {
  console.log("🚀 Initializing CertifyPro Database...");

  try {
    // 1. Create Users Table (Google OAuth Users)
    console.log("Creating 'users' table...");
    await sql`
      CREATE TABLE IF NOT EXISTS users (
        google_id TEXT PRIMARY KEY,
        name TEXT,
        email TEXT UNIQUE,
        picture TEXT,
        user_type TEXT DEFAULT 'User',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `;

    // Migration: Add user_type if missing
    try {
      await sql`ALTER TABLE users ADD COLUMN IF NOT EXISTS user_type TEXT DEFAULT 'User'`;
    } catch (e) {
      // Column might already exist or table missing
    }

    // 2. Create Quiz Users Table (Student registration for quizzes)
    console.log("Creating 'quiz_users' table...");
    await sql`
      CREATE TABLE IF NOT EXISTS quiz_users (
        id SERIAL PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
      )
    `;

    // 3. Create Quizzes Table
    console.log("Creating 'quiz_quizzes' table...");
    await sql`
      CREATE TABLE IF NOT EXISTS quiz_quizzes (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        description TEXT,
        created_by TEXT,
        duration_minutes INTEGER DEFAULT 30,
        access_key TEXT,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        start_time TIMESTAMPTZ,
        end_time TIMESTAMPTZ
      )
    `;

    // 4. Create Questions Table
    console.log("Creating 'quiz_questions' table...");
    await sql`
      CREATE TABLE IF NOT EXISTS quiz_questions (
        id SERIAL PRIMARY KEY,
        quiz_id INTEGER REFERENCES quiz_quizzes(id) ON DELETE CASCADE,
        question TEXT NOT NULL,
        options JSONB, -- Array of options
        correct_answer TEXT NOT NULL,
        points INTEGER DEFAULT 1,
        type TEXT DEFAULT 'mcq' -- mcq, multiple, fill_in_the_blanks
      )
    `;

    // 5. Create Quiz Attempts Table
    console.log("Creating 'quiz_attempts' table...");
    await sql`
      CREATE TABLE IF NOT EXISTS quiz_attempts (
        id SERIAL PRIMARY KEY,
        quiz_id INTEGER REFERENCES quiz_quizzes(id) ON DELETE CASCADE,
        user_id INTEGER REFERENCES quiz_users(id) ON DELETE CASCADE,
        started_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        submitted_at TIMESTAMPTZ
      )
    `;

    // 6. Create Responses Table
    console.log("Creating 'quiz_responses' table...");
    await sql`
      CREATE TABLE IF NOT EXISTS quiz_responses (
        id SERIAL PRIMARY KEY,
        attempt_id INTEGER REFERENCES quiz_attempts(id) ON DELETE CASCADE,
        question_id INTEGER REFERENCES quiz_questions(id) ON DELETE CASCADE,
        answer TEXT,
        is_correct BOOLEAN,
        points INTEGER DEFAULT 0
      )
    `;

    // 7. Create Scores Table
    console.log("Creating 'quiz_scores' table...");
    await sql`
      CREATE TABLE IF NOT EXISTS quiz_scores (
        attempt_id INTEGER PRIMARY KEY REFERENCES quiz_attempts(id) ON DELETE CASCADE,
        total_score INTEGER NOT NULL
      )
    `;

    // 8. Create Whitelist Table
    console.log("Creating 'quiz_allowed_students' table...");
    await sql`
      CREATE TABLE IF NOT EXISTS quiz_allowed_students (
        id SERIAL PRIMARY KEY,
        quiz_id INTEGER REFERENCES quiz_quizzes(id) ON DELETE CASCADE,
        email TEXT NOT NULL,
        UNIQUE(quiz_id, email)
      )
    `;

    // 9. Create Certificates Table
    console.log("Creating 'certificates' table...");
    await sql`
      CREATE TABLE IF NOT EXISTS certificates (
        unique_id TEXT PRIMARY KEY,
        participant_name TEXT NOT NULL,
        template_url TEXT,
        file_hash TEXT,
        file_size BIGINT,
        issue_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
      )
    `;

    console.log("✅ Database initialized successfully!");
  } catch (err) {
    console.error("❌ Database initialization failed:", err);
  }
}

// Check for "truncate" or "reset" flag
const args = process.argv.slice(2);
if (args.includes('--reset') || args.includes('--truncate')) {
  console.log("⚠️ WARNING: Resetting database tables...");
  (async () => {
    try {
        await sql`DROP TABLE IF EXISTS quiz_scores CASCADE`;
        await sql`DROP TABLE IF EXISTS quiz_responses CASCADE`;
        await sql`DROP TABLE IF EXISTS quiz_attempts CASCADE`;
        await sql`DROP TABLE IF EXISTS quiz_questions CASCADE`;
        await sql`DROP TABLE IF EXISTS quiz_allowed_students CASCADE`;
        await sql`DROP TABLE IF EXISTS quiz_quizzes CASCADE`;
        await sql`DROP TABLE IF EXISTS quiz_users CASCADE`;
        await sql`DROP TABLE IF EXISTS users CASCADE`;
        console.log("🗑️ Tables dropped.");
        await initDB();
    } catch (e) {
        console.error("Reset failed:", e);
    }
  })();
} else {
  initDB();
}
