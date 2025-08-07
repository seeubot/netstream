import os
import uuid
import mimetypes
import json
import re
import logging
from datetime import datetime
import threading
import time

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackQueryHandler, CallbackContext
from telegram.error import TelegramError
from flask import Flask, Response, abort, jsonify, request
import requests

# MongoDB imports
from pymongo import MongoClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
BOT_TOKEN = os.getenv('BOT_TOKEN')
STORAGE_CHANNEL_ID = os.getenv('STORAGE_CHANNEL_ID')
FRONTEND_URL = os.getenv('FRONTEND_URL', 'https://your-frontend.vercel.app')
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('MONGO_DB_NAME', 'netflix_bot_db')

# Global variables
mongo_client = None
db = None
files_collection = None
content_collection = None
updater = None

# Supported video formats
SUPPORTED_VIDEO_FORMATS = {
    'mp4', 'avi', 'mkv', 'mov', 'wmv', 'flv', 'webm', 'm4v'
}

def is_video_file(filename):
    if not filename:
        return False
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in SUPPORTED_VIDEO_FORMATS

def get_video_mime_type(filename):
    ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else ''
    mime_map = {
        'mp4': 'video/mp4', 'avi': 'video/x-msvideo', 'mkv': 'video/x-matroska',
        'mov': 'video/quicktime', 'wmv': 'video/x-ms-wmv', 'webm': 'video/webm'
    }
    return mime_map.get(ext, 'video/mp4')

# Flask app
app = Flask(__name__)

@app.route('/')
def home():
    return """
    <html>
    <head><title>Netflix Bot</title></head>
    <body style="background:#141414;color:white;font-family:Arial;">
        <div style="max-width:800px;margin:0 auto;padding:20px;">
            <h1 style="color:#e50914;text-align:center;">🎬 Netflix Bot Streaming</h1>
            <div id="content" style="padding:20px;text-align:center;">
                <p>Loading content...</p>
            </div>
        </div>
        <script>
            fetch('/api/content')
                .then(r => r.json())
                .then(data => {
                    document.getElementById('content').innerHTML = 
                        '<h3>📊 Statistics</h3>' +
                        '<p>Movies: ' + data.movies.length + '</p>' +
                        '<p>Series: ' + data.series.length + '</p>' +
                        '<p>Total: ' + data.total_content + '</p>';
                })
                .catch(e => {
                    document.getElementById('content').innerHTML = '<p>Error loading data</p>';
                });
        </script>
    </body>
    </html>
    """

@app.route('/stream/<file_id>')
def stream_file(file_id):
    try:
        file_info = files_collection.find_one({'_id': file_id})
        if not file_info:
            abort(404)

        file_url = file_info['file_url']
        filename = file_info['filename']
        mime_type = get_video_mime_type(filename)

        def generate():
            try:
                with requests.get(file_url, stream=True) as response:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            yield chunk
            except Exception as e:
                logger.error(f"Streaming error: {e}")

        return Response(generate(), mimetype=mime_type)
    except Exception as e:
        logger.error(f"Stream error: {e}")
        abort(500)

@app.route('/api/content')
def get_content():
    try:
        movies = list(content_collection.find({'type': 'movie'}, {'_id': 0}).limit(50))
        series = list(content_collection.find({'type': 'series'}, {'_id': 0}).limit(50))
        return jsonify({
            'movies': movies,
            'series': series,
            'total_content': len(movies) + len(series)
        })
    except Exception as e:
        logger.error(f"Content API error: {e}")
        return jsonify({'movies': [], 'series': [], 'total_content': 0})

@app.route('/health')
def health():
    try:
        mongo_client.admin.command('ping')
        return jsonify({'status': 'ok', 'bot_ready': updater is not None})
    except:
        return jsonify({'status': 'error', 'bot_ready': False})

@app.route('/telegram-webhook', methods=['POST'])
def webhook():
    try:
        if updater and updater.dispatcher:
            update = Update.de_json(request.get_json(), updater.bot)
            updater.dispatcher.process_update(update)
            return 'ok'
        return 'Bot not ready', 500
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return 'Error', 500

# Bot handlers
def start(update: Update, context: CallbackContext):
    update.message.reply_text(
        "🎬 Netflix Bot 🎬\n\n"
        "Send me a video file to get started!\n"
        "/library - View library\n"
        "/stats - Statistics"
    )

def handle_video(update: Update, context: CallbackContext):
    video = update.message.video or update.message.document
    
    if not video:
        update.message.reply_text("❌ Please send a video file!")
        return
        
    filename = getattr(video, 'file_name', f"video_{video.file_unique_id}.mp4")
    
    if update.message.document and not is_video_file(filename):
        update.message.reply_text("❌ Only video files supported!")
        return

    try:
        msg = update.message.reply_text("⏳ Processing...")

        # Forward to storage
        forwarded = context.bot.forward_message(
            chat_id=STORAGE_CHANNEL_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id
        )

        file = video.get_file()
        file_id = str(uuid.uuid4())
        domain = os.getenv('KOYEB_PUBLIC_DOMAIN', 'your-app.koyeb.app')
        stream_url = f"https://{domain}/stream/{file_id}"

        # Store in MongoDB
        files_collection.insert_one({
            '_id': file_id,
            'filename': filename,
            'file_size': video.file_size,
            'file_url': file.file_path,
            'message_id': forwarded.message_id,
            'user_id': update.effective_user.id,
            'upload_date': datetime.now().isoformat(),
            'stream_url': stream_url
        })

        keyboard = [[
            InlineKeyboardButton("📽️ Movie", callback_data=f"movie_{file_id}"),
            InlineKeyboardButton("📺 Series", callback_data=f"series_{file_id}")
        ], [
            InlineKeyboardButton("🔗 URL Only", callback_data=f"url_{file_id}")
        ]]

        msg.edit_text(
            f"✅ Video processed!\n\n🎬 {filename}\n🔗 {stream_url}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    except Exception as e:
        logger.error(f"Video processing error: {e}")
        update.message.reply_text("❌ Processing failed.")

def handle_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    
    if query.data.startswith('url_'):
        file_id = query.data.replace('url_', '')
        domain = os.getenv('KOYEB_PUBLIC_DOMAIN', 'your-app.koyeb.app')
        stream_url = f"https://{domain}/stream/{file_id}"
        query.edit_message_text(f"🔗 Streaming URL:\n{stream_url}")

def library_cmd(update: Update, context: CallbackContext):
    update.message.reply_text(f"📚 Library\n\nFrontend: {FRONTEND_URL}")

def stats_cmd(update: Update, context: CallbackContext):
    try:
        count = files_collection.estimated_document_count()
        update.message.reply_text(f"📊 Statistics\n📂 Files: {count}")
    except:
        update.message.reply_text("❌ Error getting stats")

# Initialize everything
logger.info("🚀 Initializing...")

try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    files_collection = db['files']
    content_collection = db['content']
    mongo_client.admin.command('ping')
    logger.info("✅ MongoDB connected")
except Exception as e:
    logger.error(f"❌ MongoDB failed: {e}")
    mongo_client = None

if BOT_TOKEN and mongo_client:
    try:
        updater = Updater(token=BOT_TOKEN, use_context=True)
        dp = updater.dispatcher
        
        dp.add_handler(CommandHandler('start', start))
        dp.add_handler(CommandHandler('library', library_cmd))
        dp.add_handler(CommandHandler('stats', stats_cmd))
        dp.add_handler(MessageHandler(Filters.video | Filters.document, handle_video))
        dp.add_handler(CallbackQueryHandler(handle_callback))
        
        # Set webhook
        def set_webhook():
            time.sleep(5)
            try:
                domain = os.getenv('KOYEB_PUBLIC_DOMAIN')
                if domain:
                    webhook_url = f"https://{domain}/telegram-webhook"
                    updater.bot.delete_webhook()
                    result = updater.bot.set_webhook(url=webhook_url)
                    if result:
                        logger.info(f"✅ Webhook set: {webhook_url}")
                    else:
                        logger.error("❌ Webhook failed")
            except Exception as e:
                logger.error(f"❌ Webhook error: {e}")
        
        threading.Thread(target=set_webhook, daemon=True).start()
        logger.info("✅ Bot configured")
        
    except Exception as e:
        logger.error(f"❌ Bot failed: {e}")
        updater = None

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
