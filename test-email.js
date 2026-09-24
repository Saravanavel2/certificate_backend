import nodemailer from 'nodemailer';
import dotenv from 'dotenv';

dotenv.config();

const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: process.env.GMAIL_USER,
        pass: process.env.GMAIL_PASS
    }
});

const mailOptions = {
    from: `"${process.env.GMAIL_SENDER_NAME || 'VSGRPS'}" <${process.env.GMAIL_USER}>`,
    to: 'vimalraj5207@gmail.com',
    subject: 'Test Email from Certify',
    text: 'Hello! This is a test email sent from the Certify backend test script.',
    html: `
        <div style="font-family: Arial, sans-serif; padding: 20px; color: #333;">
            <h2 style="color: #4f46e5;">Test Email Successful!</h2>
            <p>This is a test email sent from the Certify system to verify the email configuration.</p>
            <p><strong>Environment:</strong> Development</p>
            <p><strong>Timestamp:</strong> ${new Date().toLocaleString()}</p>
            <hr style="border: 0; border-top: 1px solid #eee; margin: 20px 0;">
            <p style="font-size: 0.8rem; color: #777;">Sent via VSGRPS Node.js Backend</p>
        </div>
    `
};

async function sendTestEmail() {
    console.log('🚀 Starting email test...');
    console.log(`📡 Using account: ${process.env.GMAIL_USER}`);
    
    try {
        const info = await transporter.sendMail(mailOptions);
        console.log('✅ Email sent successfully!');
        console.log('📧 Message ID:', info.messageId);
        console.log('✉️ Preview URL:', nodemailer.getTestMessageUrl(info) || 'N/A (using real Gmail)');
    } catch (error) {
        console.error('❌ Error sending email:');
        console.error(error);
    }
}

sendTestEmail();
