import dotenv from 'dotenv';
dotenv.config();
import { neon } from '@neondatabase/serverless';

const connectionString = process.env.DATABASE_URL;
if (!connectionString) {
  console.error('❌ DATABASE_URL is not defined in .env');
  process.exit(1);
}

const sql = neon(connectionString);

async function truncateDB() {
  console.log('🧹 Starting Database Truncation (Master Reset)...');
  try {
    // Truncate tables with CASCADE to handle foreign keys
    // In neon serverless, we just execute the query
    await sql`
      TRUNCATE TABLE 
        quiz_responses, 
        quiz_scores, 
        quiz_attempts, 
        quiz_allowed_students, 
        quiz_questions, 
        quiz_quizzes, 
        quiz_users, 
        users 
      RESTART IDENTITY CASCADE;
    `;
    console.log('✅ All tables truncated and identity counters reset successfully.');
  } catch (err) {
    console.error('❌ Error truncating tables:', err.message);
  } finally {
    // neon-serverless doesn't need explicit close for simple scripts in this way, 
    // but we can exit
    process.exit(0);
  }
}

truncateDB();
