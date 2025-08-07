import os
import uuid
import asyncio
import mimetypes
import json
import re
import subprocess
import tempfile
from urllib.parse import quote
import logging
from datetime import datetime
from typing import Dict, List, Optional
import time

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from telegram.error import TelegramError
from flask import Flask, Response, abort, jsonify, request, render_template_string
import requests

# MongoDB imports
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuration
BOT_TOKEN = os.getenv('BOT_TOKEN')
STORAGE_CHANNEL_ID = os.getenv('STORAGE_CHANNEL_ID')
FRONTEND_URL = os.getenv('FRONTEND_URL', 'https://your-frontend.vercel.app')
MAX_FILE_SIZE = 4000 * 1024 * 1024  # 4GB limit

# MongoDB Connection String
MONGO_URI = os.getenv('MONGO_URI', 'mongodb+srv://food:food@food.1jskkt3.mongodb.net/?retryWrites=true&w=majority&appName=food')
DB_NAME = os.getenv('MONGO_DB_NAME', 'netflix_bot_db')

# Global variables (initialized once)
mongo_client = None
db = None
files_collection = None
content_collection = None
telegram_bot_app = None

# Supported video formats
SUPPORTED_VIDEO_FORMATS = {
    'mp4', 'avi', 'mkv', 'mov', 'wmv', 'flv', 'webm', 'm4v',
    'mpg', 'mpeg', 'ogv', '3gp', 'rm', 'rmvb', 'asf', 'divx'
}

# Simple HTML template for frontend
SIMPLE_FRONTEND = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Netflix Bot Streaming</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #141414; color: white; }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { color: #e50914; text-align: center; }
        .content-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 20px; margin-top: 30px; }
        .content-item { background: #222; padding: 20px; border-radius: 8px; }
        .content-item h3 { color: #fff; margin: 0 0 10px 0; }
        .content-item p { color: #999; margin: 5px 0; }
        .stream-btn { background: #e50914; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; text-decoration: none; display: inline-block; }
        .stream-btn:hover { background: #f40612; }
        .stats { text-align: center; margin-bottom: 30px; }
        .stats span { margin: 0 20px; color: #999; }
        .loading { text-align: center; padding: 50px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🎬 Netflix Bot Streaming Platform</h1>
        <div class="stats">
            <span id="movies-count">Movies: 0</span>
            <span id="series-count">Series: 0</span>
            <span id="total-count">Total: 0</span>
        </div>
        <div id="content-grid" class="content-grid">
            <div class="loading">Loading content...</div>
        </div>
    </div>

    <script>
        async function loadContent() {
            try {
                const response = await fetch('/api/content', {
                    timeout: 10000,
                    headers: {
                        'Cache-Control': 'no-cache'
                    }
                });
                
                if (!response.ok) throw new Error('Network response was not ok');
                
                const data = await response.json();

                document.getElementById('movies-count').textContent = `Movies: ${data.movies.length}`;
                document.getElementById('series-count').textContent = `Series: ${data.series.length}`;
                document.getElementById('total-count').textContent = `Total: ${data.total_content}`;

                const contentGrid = document.getElementById('content-grid');
                contentGrid.innerHTML = '';

                [...data.movies, ...data.series].forEach(item => {
                    const div = document.createElement('div');
                    div.className = 'content-item';

                    const type = item.type === 'movie' ? '🎬' : '📺';
                    const extra = item.type === 'movie' ? `(${item.year || 'N/A'})` : `S${item.season}E${item.episode}`;

                    div.innerHTML = `
                        <h3>${type} ${item.title} ${extra}</h3>
                        <p>Genre: ${Array.isArray(item.genre) ? item.genre.join(', ') : item.genre || 'N/A'}</p>
                        <p>${item.description || 'No description available'}</p>
                        <a href="${item.stream_url}" class="stream-btn" target="_blank">▶ Stream</a>
                    `;
                    contentGrid.appendChild(div);
                });

                if (data.total_content === 0) {
                    contentGrid.innerHTML = '<div class="loading">No content available yet. Upload videos via the Telegram bot!</div>';
                }
            } catch (error) {
                console.error('Error loading content:', error);
                document.getElementById('content-grid').innerHTML = '<div class="loading">Error loading content. Please try again later.</div>';
            }
        }

        // Load content on page load
        loadContent();
        
        // Refresh every 30 seconds
        setInterval(loadContent, 30000);
    </script>
</body>
</html>
"""

class VideoMetadata:
    def __init__(self, file_path: str = None):
        self.file_path = file_path
        self.metadata = {}

    def get_duration(self) -> Optional[float]:
        return None  # Skip metadata extraction for performance

    def get_resolution(self) -> Optional[tuple]:
        return None

    def get_audio_tracks(self) -> List[Dict]:
        return []

    def get_subtitle_tracks(self) -> List[Dict]:
        return []

def is_video_file(filename):
    """Check if file is a supported video format"""
    if not filename:
        return False
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in SUPPORTED_VIDEO_FORMATS

def get_video_mime_type(filename):
    """Get MIME type for video file"""
    mime_type, _ = mimetypes.guess_type(filename)
    if mime_type and mime_type.startswith('video/'):
        return mime_type

    ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else ''
    mime_map = {
        'mp4': 'video/mp4',
        'avi': 'video/x-msvideo',
        'mkv': 'video/x-matroska',
        'mov': 'video/quicktime',
        'wmv': 'video/x-ms-wmv',
        'flv': 'video/x-flv',
        'webm': 'video/webm',
        'm4v': 'video/mp4',
        'mpg': 'video/mpeg',
        'mpeg': 'video/mpeg',
        'ogv': 'video/ogg',
        '3gp': 'video/3gpp'
    }
    return mime_map.get(ext, 'video/mp4')

# Flask app for serving files
flask_app = Flask(__name__)
flask_app.config['JSON_SORT_KEYS'] = False

# Flask Routes
@flask_app.route('/')
def serve_frontend():
    """Serve the main frontend HTML page."""
    return render_template_string(SIMPLE_FRONTEND)

@flask_app.route('/stream/<file_id>')
def stream_file(file_id):
    """Stream video file with support for range requests"""
    try:
        file_info = files_collection.find_one({'_id': file_id}, {'file_url': 1, 'file_size': 1, 'filename': 1})
        if not file_info:
            abort(404)

        file_url = file_info['file_url']
        file_size = file_info['file_size']
        filename = file_info['filename']
        mime_type = get_video_mime_type(filename)

        range_header = request.environ.get('HTTP_RANGE', '').strip()
        
        if range_header:
            range_match = re.search(r'bytes=(\d+)-(\d*)', range_header)
            if range_match:
                start = int(range_match.group(1))
                end = int(range_match.group(2)) if range_match.group(2) else file_size - 1
                
                start = max(0, min(start, file_size - 1))
                end = max(start, min(end, file_size - 1))

                def generate_range():
                    try:
                        headers = {'Range': f'bytes={start}-{end}'}
                        with requests.get(file_url, headers=headers, stream=True, timeout=30) as response:
                            response.raise_for_status()
                            for chunk in response.iter_content(chunk_size=16384):
                                if chunk:
                                    yield chunk
                    except Exception as e:
        logger.error(f"Error in library_command for user {user_id}: {e}")
        await update.message.reply_text("❌ An error occurred while fetching your library. Please try again later.")

async def frontend_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends the frontend URL to the user."""
    await update.message.reply_text(
        f"🌐 **Your Netflix-style Frontend:**\n\n"
        f"Click here to access your streaming platform:\n"
        f"👉 {FRONTEND_URL}\n\n"
        f"Share this link to let others stream your content!",
        parse_mode='Markdown'
    )

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Provides bot statistics."""
    try:
        total_files = files_collection.estimated_document_count()
        total_movies = content_collection.count_documents({'type': 'movie'})
        total_series = content_collection.count_documents({'type': 'series'})

        message_text = "📊 **Bot Statistics** 📊\n\n"
        message_text += f"📂 **Total Files Stored:** {total_files}\n"
        message_text += f"🎬 **Total Movies:** {total_movies}\n"
        message_text += f"📺 **Total Series Episodes:** {total_series}\n\n"
        message_text += "This data reflects all content managed by the bot across all users."

        await update.message.reply_text(message_text, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error in stats_command: {e}")
        await update.message.reply_text("❌ An error occurred while fetching statistics. Please try again later.")

def initialize_mongodb():
    """Initialize MongoDB connection and collections - kept for compatibility"""
    return mongo_client is not None

async def initialize_telegram_bot():
    """Initialize Telegram bot - kept for compatibility"""
    return telegram_bot_app is not None

def setup_webhook_with_retry(max_retries=3):
    """Set webhook with retry mechanism"""
    if not telegram_bot_app or not BOT_TOKEN:
        logger.error("❌ Cannot set webhook - bot not initialized or token missing")
        return False
        
    domain = os.getenv('KOYEB_PUBLIC_DOMAIN')
    if not domain:
        logger.error("❌ KOYEB_PUBLIC_DOMAIN not set")
        return False
    
    webhook_url = f"https://{domain}/telegram-webhook"
    
    for attempt in range(max_retries):
        try:
            logger.info(f"🔄 Setting webhook attempt {attempt + 1}/{max_retries}")
            
            # Delete existing webhook first
            delete_url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
            delete_response = requests.post(
                delete_url, 
                json={"drop_pending_updates": True}, 
                timeout=20
            )
            logger.info(f"Delete webhook response: {delete_response.status_code}")
            
            # Wait between delete and set
            time.sleep(3)
            
            # Set new webhook
            set_url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook"
            set_response = requests.post(
                set_url, 
                json={"url": webhook_url}, 
                timeout=20
            )
            
            if set_response.status_code == 200:
                result = set_response.json()
                if result.get('ok'):
                    logger.info(f"✅ Webhook set successfully: {webhook_url}")
                    return True
                else:
                    logger.warning(f"⚠️ Webhook API returned ok=false: {result}")
            else:
                logger.warning(f"⚠️ Webhook HTTP error {set_response.status_code}: {set_response.text}")
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"⚠️ Network error on attempt {attempt + 1}: {e}")
        except Exception as e:
            logger.warning(f"⚠️ Unexpected error on attempt {attempt + 1}: {e}")
            
        if attempt < max_retries - 1:
            wait_time = (attempt + 1) * 5  # Progressive backoff
            logger.info(f"⏳ Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)
    
    logger.error(f"❌ Failed to set webhook after {max_retries} attempts")
    return False

# Initialize services at module level
logger.info("🚀 Initializing services at startup...")

# Initialize MongoDB
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    files_collection = db['files']
    content_collection = db['content']
    
    # Test connection
    mongo_client.admin.command('ping')
    
    # Create indexes for better performance
    files_collection.create_index([('user_id', 1)])
    content_collection.create_index([('added_by', 1), ('type', 1)])
    content_collection.create_index([('type', 1)])
    content_collection.create_index([('added_date', -1)])
    
    logger.info("✅ MongoDB connected successfully!")
    
except Exception as e:
    logger.error(f"❌ MongoDB initialization failed: {e}")
    mongo_client = None

# Initialize Telegram bot
if BOT_TOKEN and mongo_client:
    try:
        telegram_bot_app = Application.builder().token(BOT_TOKEN).build()
        
        # Add handlers
        telegram_bot_app.add_handler(CommandHandler("start", start))
        telegram_bot_app.add_handler(CommandHandler("library", library_command))
        telegram_bot_app.add_handler(CommandHandler("frontend", frontend_command))
        telegram_bot_app.add_handler(CommandHandler("stats", stats_command))
        telegram_bot_app.add_handler(MessageHandler(filters.VIDEO | filters.Document.ALL, handle_video_file))
        telegram_bot_app.add_handler(CallbackQueryHandler(handle_categorization))
        telegram_bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_metadata_input))
        
        logger.info("✅ Telegram bot configured successfully!")
        
    except Exception as e:
        logger.error(f"❌ Telegram bot initialization failed: {e}")
        telegram_bot_app = None
else:
    logger.error("❌ Missing BOT_TOKEN or MongoDB connection failed")
    telegram_bot_app = None

# Delayed webhook setup function
def delayed_webhook_setup():
    """Setup webhook after a delay to ensure the app is fully running"""
    logger.info("⏳ Starting delayed webhook setup...")
    time.sleep(10)  # Wait for the app to be fully operational
    
    if setup_webhook_with_retry(max_retries=5):
        logger.info("✅ Webhook setup completed successfully")
    else:
        logger.error("❌ Webhook setup failed after all retries")

# Set webhook after app starts (in a separate thread)
if telegram_bot_app:
    import threading
    webhook_thread = threading.Thread(target=delayed_webhook_setup, daemon=True)
    webhook_thread.start()

# Create the Flask app
app = flask_app

if __name__ == '__main__':
    # For local development only
    logger.info("🔧 Running in development mode...")
    port = int(os.environ.get('PORT', 8080))
    logger.info(f"🚀 Starting Flask app on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
                        logger.error(f"Error streaming range for {file_id}: {e}")

                return Response(
                    generate_range(),
                    206,
                    {
                        'Content-Type': mime_type,
                        'Accept-Ranges': 'bytes',
                        'Content-Range': f'bytes {start}-{end}/{file_size}',
                        'Content-Length': str(end - start + 1),
                        'Cache-Control': 'public, max-age=3600',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Headers': 'Range',
                    }
                )

        def generate_full():
            try:
                with requests.get(file_url, stream=True, timeout=30) as response:
                    response.raise_for_status()
                    for chunk in response.iter_content(chunk_size=16384):
                        if chunk:
                            yield chunk
            except Exception as e:
                logger.error(f"Error streaming full file for {file_id}: {e}")

        return Response(
            generate_full(),
            200,
            {
                'Content-Type': mime_type,
                'Accept-Ranges': 'bytes',
                'Content-Length': str(file_size),
                'Cache-Control': 'public, max-age=3600',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Range',
            }
        )
    except Exception as e:
        logger.error(f"Error in stream_file for {file_id}: {e}")
        abort(500)

@flask_app.route('/api/content')
def get_content_library():
    """Get content library for frontend"""
    try:
        projection = {'_id': 0, 'title': 1, 'type': 1, 'year': 1, 'season': 1, 'episode': 1, 'genre': 1, 'description': 1, 'stream_url': 1}
        
        movies = list(content_collection.find({'type': 'movie'}, projection).limit(100))
        series = list(content_collection.find({'type': 'series'}, projection).limit(100))

        all_categories = set()
        for item in movies + series:
            if 'genre' in item and isinstance(item['genre'], list):
                all_categories.update(item['genre'])

        return jsonify({
            'movies': movies,
            'series': series,
            'categories': sorted(list(all_categories)),
            'total_content': len(movies) + len(series)
        })
    except Exception as e:
        logger.error(f"Error in get_content_library: {e}")
        return jsonify({
            'movies': [],
            'series': [],
            'categories': [],
            'total_content': 0
        }), 500

@flask_app.route('/health')
def health_check():
    """Health check endpoint"""
    try:
        mongo_client.admin.command('ping')
        mongo_status = 'ok'
    except Exception as e:
        mongo_status = f'error: {str(e)[:50]}'
        logger.error(f"MongoDB health check failed: {e}")

    try:
        videos_count = files_collection.estimated_document_count()
        movies_count = content_collection.count_documents({'type': 'movie'})
        series_count = content_collection.count_documents({'type': 'series'})
    except Exception as e:
        logger.error(f"Error getting counts: {e}")
        videos_count = movies_count = series_count = 0

    return jsonify({
        'status': 'ok' if mongo_status == 'ok' else 'degraded',
        'mongodb_status': mongo_status,
        'videos_stored': videos_count,
        'movies': movies_count,
        'series': series_count,
        'storage_channel': STORAGE_CHANNEL_ID,
        'bot_ready': telegram_bot_app is not None
    })

@flask_app.route("/telegram-webhook", methods=["POST"])
def telegram_webhook():
    """Handle incoming Telegram updates from the webhook"""
    if not telegram_bot_app:
        logger.error("Telegram bot application not initialized.")
        return "Bot not ready", 500

    try:
        update_json = request.get_json(force=True)
        if not update_json:
            return "No data", 400
            
        update = Update.de_json(update_json, telegram_bot_app.bot)
        
        # Process update synchronously to avoid threading issues
        asyncio.run(telegram_bot_app.process_update(update))
        
        return "ok", 200
        
    except Exception as e:
        logger.error(f"Error in webhook: {e}")
        return "Error", 500

# Route to manually set webhook
@flask_app.route('/set-webhook', methods=['POST', 'GET'])
def manual_set_webhook():
    """Manually set webhook endpoint"""
    try:
        domain = os.getenv('KOYEB_PUBLIC_DOMAIN')
        if not domain:
            return jsonify({'error': 'KOYEB_PUBLIC_DOMAIN not set'}), 400
            
        webhook_url = f"https://{domain}/telegram-webhook"
        
        # Delete existing webhook
        delete_url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
        delete_response = requests.post(delete_url, json={"drop_pending_updates": True}, timeout=15)
        
        # Wait a moment
        time.sleep(2)
        
        # Set new webhook
        set_url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook"
        set_response = requests.post(set_url, json={"url": webhook_url}, timeout=15)
        
        if set_response.status_code == 200:
            result = set_response.json()
            if result.get('ok'):
                logger.info(f"✅ Webhook set successfully: {webhook_url}")
                return jsonify({'success': True, 'webhook_url': webhook_url, 'result': result})
            else:
                logger.error(f"❌ Webhook set failed: {result}")
                return jsonify({'success': False, 'error': result}), 500
        else:
            logger.error(f"❌ Failed to set webhook: {set_response.text}")
            return jsonify({'success': False, 'error': set_response.text}), 500
            
    except Exception as e:
        logger.error(f"❌ Error in manual webhook setup: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# Telegram Bot handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler"""
    welcome_message = """
🎬 **Netflix-Style Video Streaming Bot** 🎬

Transform your videos into a professional streaming platform!

**Features:**
✅ Netflix-like interface
✅ Android TV optimized
✅ Multi-audio track support
✅ Quality selection
✅ Movie & Series categorization
✅ Search functionality
✅ Permanent streaming URLs
✅ **Webhook-Only Deployment** ⚡

**Commands:**
/upload - Upload and categorize content
/library - View your content library
/frontend - Get frontend app link
/stats - Check bot statistics

Just send me a video file to get started! 🚀
    """
    await update.message.reply_text(welcome_message, parse_mode='Markdown')

async def handle_video_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle video file uploads"""
    video = None
    document = None

    if update.message.video:
        video = update.message.video
        filename = video.file_name or f"video_{video.file_unique_id}.mp4"
        file_size = video.file_size
    elif update.message.document:
        document = update.message.document
        filename = document.file_name
        file_size = document.file_size

        if not filename or not is_video_file(filename):
            await update.message.reply_text("❌ This bot only supports video files!")
            return
    else:
        await update.message.reply_text("❌ Please send a video file!")
        return

    if file_size > MAX_FILE_SIZE:
        await update.message.reply_text(
            f"❌ Video file too large! Maximum size is {MAX_FILE_SIZE // (1024*1024*1024)}GB"
        )
        return

    try:
        processing_msg = await update.message.reply_text("⏳ Processing your video...")

        if not STORAGE_CHANNEL_ID:
            await processing_msg.edit_text("❌ Storage channel not configured!")
            return

        forwarded_msg = await context.bot.forward_message(
            chat_id=STORAGE_CHANNEL_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id
        )

        file_obj = video if video else document
        file = await file_obj.get_file()
        file_url = file.file_path
        file_id = str(uuid.uuid4())

        domain = os.getenv('KOYEB_PUBLIC_DOMAIN', 'your-app.koyeb.app')
        stream_url = f"https://{domain}/stream/{file_id}"

        file_document = {
            '_id': file_id,
            'filename': filename,
            'file_size': file_size,
            'file_url': file_url,
            'message_id': forwarded_msg.message_id,
            'user_id': update.effective_user.id,
            'chat_id': update.effective_chat.id,
            'storage_channel_id': STORAGE_CHANNEL_ID,
            'duration': None,
            'resolution': None,
            'audio_tracks': [],
            'subtitle_tracks': [],
            'upload_date': datetime.now().isoformat(),
            'stream_url': stream_url
        }
        
        files_collection.insert_one(file_document)

        keyboard = [
            [
                InlineKeyboardButton("📽️ Add as Movie", callback_data=f"categorize_movie_{file_id}"),
                InlineKeyboardButton("📺 Add as Series", callback_data=f"categorize_series_{file_id}")
            ],
            [
                InlineKeyboardButton("🔗 Just Get URL", callback_data=f"just_url_{file_id}")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await processing_msg.edit_text(
            f"✅ **Video processed successfully!**\n\n"
            f"🎬 **File:** `{filename}`\n"
            f"📊 **Size:** {file_size/(1024*1024):.1f} MB\n"
            f"🔗 **Stream URL:** `{stream_url}`\n\n"
            f"**What would you like to do?**",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )

        logger.info(f"Video processed: {filename} -> {file_id} for user {update.effective_user.id}")

    except Exception as e:
        logger.error(f"Error processing video: {e}")
        await update.message.reply_text(
            "❌ An error occurred while processing your video. Please try again."
        )

async def handle_categorization(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle content categorization callbacks"""
    query = update.callback_query
    await query.answer()

    data = query.data
    if data.startswith('categorize_movie_'):
        file_id = data.replace('categorize_movie_', '')
        if not files_collection.find_one({'_id': file_id}):
            await query.edit_message_text("❌ File not found!")
            return
        context.user_data['categorizing'] = {'type': 'movie', 'file_id': file_id}
        await query.edit_message_text(
            "📽️ **Adding as Movie**\n\n"
            "Send details in format:\n"
            "`Title | Year | Genre | Description`\n\n"
            "Example:\n"
            "`The Matrix | 1999 | Action, Sci-Fi | A hacker discovers reality.`",
            parse_mode='Markdown'
        )

    elif data.startswith('categorize_series_'):
        file_id = data.replace('categorize_series_', '')
        if not files_collection.find_one({'_id': file_id}):
            await query.edit_message_text("❌ File not found!")
            return
        context.user_data['categorizing'] = {'type': 'series', 'file_id': file_id}
        await query.edit_message_text(
            "📺 **Adding as Series**\n\n"
            "Send details in format:\n"
            "`Title | Season | Episode | Genre | Description`\n\n"
            "Example:\n"
            "`Breaking Bad | 1 | 1 | Drama, Crime | Chemistry teacher turns cook.`",
            parse_mode='Markdown'
        )

    elif data.startswith('just_url_'):
        file_id = data.replace('just_url_', '')
        if files_collection.find_one({'_id': file_id}):
            domain = os.getenv('KOYEB_PUBLIC_DOMAIN', 'your-app.koyeb.app')
            stream_url = f"https://{domain}/stream/{file_id}"

            await query.edit_message_text(
                f"🔗 **Streaming URL Generated**\n\n"
                f"`{stream_url}`\n\n"
                f"🎮 **Frontend:** {FRONTEND_URL}",
                parse_mode='Markdown'
            )
        else:
            await query.edit_message_text("❌ File not found!")

async def handle_metadata_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle metadata input for content categorization"""
    if 'categorizing' not in context.user_data:
        return

    categorizing = context.user_data['categorizing']
    file_id = categorizing['file_id']
    content_type = categorizing['type']

    file_info = files_collection.find_one({'_id': file_id}, {'_id': 1})
    if not file_info:
        await update.message.reply_text("❌ File not found!")
        del context.user_data['categorizing']
        return

    try:
        metadata_text = update.message.text.strip()
        parts = [part.strip() for part in metadata_text.split('|')]

        domain = os.getenv('KOYEB_PUBLIC_DOMAIN', 'your-app.koyeb.app')
        stream_url = f"https://{domain}/stream/{file_id}"

        if content_type == 'movie' and len(parts) >= 4:
            title, year_str, genre_str, description = parts[:4]
            year = int(year_str) if year_str.isdigit() else None
            genre = [g.strip() for g in genre_str.split(',')]
            content_id = str(uuid.uuid4())

            content_document = {
                '_id': content_id,
                'file_id': file_id,
                'title': title,
                'year': year,
                'genre': genre,
                'description': description,
                'type': 'movie',
                'stream_url': stream_url,
                'added_date': datetime.now().isoformat(),
                'added_by': update.effective_user.id
            }

            content_collection.insert_one(content_document)

            await update.message.reply_text(
                f"✅ **Movie Added!**\n\n"
                f"🎬 **{title}** ({year_str})\n"
                f"🎭 {genre_str}\n\n"
                f"🎮 **Frontend:** {FRONTEND_URL}",
                parse_mode='Markdown'
            )

        elif content_type == 'series' and len(parts) >= 5:
            title, season_str, episode_str, genre_str, description = parts[:5]
            season = int(season_str) if season_str.isdigit() else None
            episode = int(episode_str) if episode_str.isdigit() else None
            genre = [g.strip() for g in genre_str.split(',')]
            content_id = f"{re.sub(r'[^a-z0-9]', '_', title.lower())}_s{season_str}e{episode_str}_{uuid.uuid4().hex[:8]}"

            content_document = {
                '_id': content_id,
                'file_id': file_id,
                'title': title,
                'season': season,
                'episode': episode,
                'genre': genre,
                'description': description,
                'type': 'series',
                'stream_url': stream_url,
                'added_date': datetime.now().isoformat(),
                'added_by': update.effective_user.id
            }

            content_collection.insert_one(content_document)

            await update.message.reply_text(
                f"✅ **Series Added!**\n\n"
                f"📺 **{title}** S{season_str}E{episode_str}\n"
                f"🎭 {genre_str}\n\n"
                f"🎮 **Frontend:** {FRONTEND_URL}",
                parse_mode='Markdown'
            )

        else:
            await update.message.reply_text("❌ Invalid format! Please follow the exact format.")
            return

        del context.user_data['categorizing']
        logger.info(f"Content added: {title} (Type: {content_type})")

    except Exception as e:
        logger.error(f"Error processing metadata: {e}")
        await update.message.reply_text("❌ Error processing metadata. Please try again.")

async def library_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user's content library"""
    user_id = update.effective_user.id

    try:
        user_movies_count = content_collection.count_documents({'added_by': user_id, 'type': 'movie'})
        user_series_count = content_collection.count_documents({'added_by': user_id, 'type': 'series'})

        message_text = f"📚 **Your Content Library** 📚\n\n"
        message_text += f"📽️ **Movies:** {user_movies_count}\n"
        message_text += f"📺 **Series:** {user_series_count}\n\n"

        if user_movies_count > 0:
            message_text += "--- \n**Recent Movies:**\n"
            recent_movies = content_collection.find(
                {'added_by': user_id, 'type': 'movie'},
                {'title': 1, 'year': 1, 'stream_url': 1}
            ).sort('added_date', -1).limit(5)
            for movie in recent_movies:
                message_text += f"• [{movie.get('title', 'N/A')} ({movie.get('year', 'N/A')})]({movie.get('stream_url', '#')})\n"
            message_text += "\n"

        if user_series_count > 0:
            message_text += "--- \n**Recent Series Episodes:**\n"
            recent_series = content_collection.find(
                {'added_by': user_id, 'type': 'series'},
                {'title': 1, 'season': 1, 'episode': 1, 'stream_url': 1}
            ).sort('added_date', -1).limit(5)
            for series_item in recent_series:
                message_text += (f"• [{series_item.get('title', 'N/A')} S{series_item.get('season', 'N/A')}E{series_item.get('episode', 'N/A')}]"
                                 f"({series_item.get('stream_url', '#')})\n")
            message_text += "\n"

        if user_movies_count == 0 and user_series_count == 0:
            message_text += "It looks like your library is empty! 😔\n"
            message_text += "Send me a video file to start building your collection."

        keyboard = [[InlineKeyboardButton("🚀 View Full Library on Frontend", url=FRONTEND_URL)]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(message_text, parse_mode='Markdown', reply_markup=reply_markup)

    except Exception aslogger.error(f"Error in library_command for user {user_id}: {e}")
        await update.message.reply_text("❌ An error occurred while fetching your library. Please try again later.")

async def frontend_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends the frontend URL to the user."""
    await update.message.reply_text(
        f"🌐 **Your Netflix-style Frontend:**\n\n"
        f"Click here to access your streaming platform:\n"
        f"👉 {FRONTEND_URL}\n\n"
        f"Share this link to let others stream your content!",
        parse_mode='Markdown'
    )

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Provides bot statistics."""
    try:
        total_files = files_collection.estimated_document_count()
        total_movies = content_collection.count_documents({'type': 'movie'})
        total_series = content_collection.count_documents({'type': 'series'})

        message_text = "📊 **Bot Statistics** 📊\n\n"
        message_text += f"📂 **Total Files Stored:** {total_files}\n"
        message_text += f"🎬 **Total Movies:** {total_movies}\n"
        message_text += f"📺 **Total Series Episodes:** {total_series}\n\n"
        message_text += "This data reflects all content managed by the bot across all users."

        await update.message.reply_text(message_text, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error in stats_command: {e}")
        await update.message.reply_text("❌ An error occurred while fetching statistics. Please try again later.")

def initialize_mongodb():
    """Initialize MongoDB connection and collections - kept for compatibility"""
    return mongo_client is not None

async def initialize_telegram_bot():
    """Initialize Telegram bot - kept for compatibility"""
    return telegram_bot_app is not None

def setup_webhook_with_retry(max_retries=3):
    """Set webhook with retry mechanism"""
    if not telegram_bot_app or not BOT_TOKEN:
        logger.error("❌ Cannot set webhook - bot not initialized or token missing")
        return False
        
    domain = os.getenv('KOYEB_PUBLIC_DOMAIN')
    if not domain:
        logger.error("❌ KOYEB_PUBLIC_DOMAIN not set")
        return False
    
    webhook_url = f"https://{domain}/telegram-webhook"
    
    for attempt in range(max_retries):
        try:
            logger.info(f"🔄 Setting webhook attempt {attempt + 1}/{max_retries}")
            
            # Delete existing webhook first
            delete_url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
            delete_response = requests.post(
                delete_url, 
                json={"drop_pending_updates": True}, 
                timeout=20
            )
            logger.info(f"Delete webhook response: {delete_response.status_code}")
            
            # Wait between delete and set
            time.sleep(3)
            
            # Set new webhook
            set_url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook"
            set_response = requests.post(
                set_url, 
                json={"url": webhook_url}, 
                timeout=20
            )
            
            if set_response.status_code == 200:
                result = set_response.json()
                if result.get('ok'):
                    logger.info(f"✅ Webhook set successfully: {webhook_url}")
                    return True
                else:
                    logger.warning(f"⚠️ Webhook API returned ok=false: {result}")
            else:
                logger.warning(f"⚠️ Webhook HTTP error {set_response.status_code}: {set_response.text}")
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"⚠️ Network error on attempt {attempt + 1}: {e}")
        except Exception as e:
            logger.warning(f"⚠️ Unexpected error on attempt {attempt + 1}: {e}")
            
        if attempt < max_retries - 1:
            wait_time = (attempt + 1) * 5  # Progressive backoff
            logger.info(f"⏳ Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)
    
    logger.error(f"❌ Failed to set webhook after {max_retries} attempts")
    return False

# Initialize services at module level
logger.info("🚀 Initializing services at startup...")

# Initialize MongoDB
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    files_collection = db['files']
    content_collection = db['content']
    
    # Test connection
    mongo_client.admin.command('ping')
    
    # Create indexes for better performance
    files_collection.create_index([('user_id', 1)])
    content_collection.create_index([('added_by', 1), ('type', 1)])
    content_collection.create_index([('type', 1)])
    content_collection.create_index([('added_date', -1)])
    
    logger.info("✅ MongoDB connected successfully!")
    
except Exception as e:
    logger.error(f"❌ MongoDB initialization failed: {e}")
    mongo_client = None

# Initialize Telegram bot
if BOT_TOKEN and mongo_client:
    try:
        telegram_bot_app = Application.builder().token(BOT_TOKEN).build()
        
        # Add handlers
        telegram_bot_app.add_handler(CommandHandler("start", start))
        telegram_bot_app.add_handler(CommandHandler("library", library_command))
        telegram_bot_app.add_handler(CommandHandler("frontend", frontend_command))
        telegram_bot_app.add_handler(CommandHandler("stats", stats_command))
        telegram_bot_app.add_handler(MessageHandler(filters.VIDEO | filters.Document.ALL, handle_video_file))
        telegram_bot_app.add_handler(CallbackQueryHandler(handle_categorization))
        telegram_bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_metadata_input))
        
        logger.info("✅ Telegram bot configured successfully!")
        
    except Exception as e:
        logger.error(f"❌ Telegram bot initialization failed: {e}")
        telegram_bot_app = None
else:
    logger.error("❌ Missing BOT_TOKEN or MongoDB connection failed")
    telegram_bot_app = None

# Delayed webhook setup function
def delayed_webhook_setup():
    """Setup webhook after a delay to ensure the app is fully running"""
    logger.info("⏳ Starting delayed webhook setup...")
    time.sleep(10)  # Wait for the app to be fully operational
    
    if setup_webhook_with_retry(max_retries=5):
        logger.info("✅ Webhook setup completed successfully")
    else:
        logger.error("❌ Webhook setup failed after all retries")

# Set webhook after app starts (in a separate thread)
if telegram_bot_app:
    import threading
    webhook_thread = threading.Thread(target=delayed_webhook_setup, daemon=True)
    webhook_thread.start()

# Create the Flask app
app = flask_app

if __name__ == '__main__':
    # For local development only
    logger.info("🔧 Running in development mode...")
    port = int(os.environ.get('PORT', 8080))
    logger.info(f"🚀 Starting Flask app on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
