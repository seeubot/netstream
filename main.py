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
import threading
from concurrent.futures import ThreadPoolExecutor

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from telegram.error import TelegramError
from flask import Flask, Response, abort, jsonify, request, render_template_string
import aiohttp
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

# Global variables for MongoDB and Telegram (initialized once)
mongo_client = None
db = None
files_collection = None
content_collection = None
telegram_bot_app = None

# Thread pool for async operations
executor = ThreadPoolExecutor(max_workers=4)

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
        if file_path and os.path.exists(file_path):
            self._extract_metadata()

    def _extract_metadata(self):
        """Extract video metadata using ffprobe with timeout"""
        try:
            cmd = [
                'ffprobe', '-v', 'quiet', '-print_format', 'json',
                '-show_format', '-show_streams', self.file_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)

            if result.returncode == 0:
                self.metadata = json.loads(result.stdout)
            else:
                logger.warning(f"ffprobe failed for {self.file_path}: {result.stderr}")
        except Exception as e:
            logger.warning(f"Metadata extraction failed for {self.file_path}: {e}")
            self.metadata = {}

    def get_duration(self) -> Optional[float]:
        try:
            return float(self.metadata['format']['duration'])
        except (KeyError, ValueError, TypeError):
            return None

    def get_resolution(self) -> Optional[tuple]:
        try:
            for stream in self.metadata['streams']:
                if stream['codec_type'] == 'video':
                    return (stream['width'], stream['height'])
        except (KeyError, TypeError):
            pass
        return None

    def get_audio_tracks(self) -> List[Dict]:
        audio_tracks = []
        try:
            for i, stream in enumerate(self.metadata['streams']):
                if stream['codec_type'] == 'audio':
                    track_info = {
                        'index': i,
                        'codec': stream.get('codec_name', 'unknown'),
                        'language': stream.get('tags', {}).get('language', 'unknown'),
                        'title': stream.get('tags', {}).get('title', f'Audio Track {len(audio_tracks) + 1}'),
                        'channels': stream.get('channels', 2),
                        'bitrate': stream.get('bit_rate', 'unknown')
                    }
                    audio_tracks.append(track_info)
        except (KeyError, TypeError):
            pass
        return audio_tracks

    def get_subtitle_tracks(self) -> List[Dict]:
        subtitle_tracks = []
        try:
            for i, stream in enumerate(self.metadata['streams']):
                if stream['codec_type'] == 'subtitle':
                    track_info = {
                        'index': i,
                        'codec': stream.get('codec_name', 'unknown'),
                        'language': stream.get('tags', {}).get('language', 'unknown'),
                        'title': stream.get('tags', {}).get('title', f'Subtitle Track {len(subtitle_tracks) + 1}')
                    }
                    subtitle_tracks.append(track_info)
        except (KeyError, TypeError):
            pass
        return subtitle_tracks

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
    """Stream video file with support for range requests - optimized"""
    try:
        # Quick MongoDB lookup
        file_info = files_collection.find_one({'_id': file_id}, {'file_url': 1, 'file_size': 1, 'filename': 1})
        if not file_info:
            abort(404)

        file_url = file_info['file_url']
        file_size = file_info['file_size']
        filename = file_info['filename']
        mime_type = get_video_mime_type(filename)

        # Handle range requests efficiently
        range_header = request.environ.get('HTTP_RANGE', '').strip()
        
        if range_header:
            range_match = re.search(r'bytes=(\d+)-(\d*)', range_header)
            if range_match:
                start = int(range_match.group(1))
                end = int(range_match.group(2)) if range_match.group(2) else file_size - 1
                
                # Ensure valid range
                start = max(0, min(start, file_size - 1))
                end = max(start, min(end, file_size - 1))

                def generate_range():
                    try:
                        headers = {'Range': f'bytes={start}-{end}'}
                        with requests.get(file_url, headers=headers, stream=True, timeout=30) as response:
                            response.raise_for_status()
                            for chunk in response.iter_content(chunk_size=16384):  # Larger chunks
                                if chunk:
                                    yield chunk
                    except Exception as e:
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

        # Full file streaming
        def generate_full():
            try:
                with requests.get(file_url, stream=True, timeout=30) as response:
                    response.raise_for_status()
                    for chunk in response.iter_content(chunk_size=16384):  # Larger chunks
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
    """Get content library for frontend - optimized with indexing"""
    try:
        # Use projection to limit data transfer
        projection = {'_id': 0, 'title': 1, 'type': 1, 'year': 1, 'season': 1, 'episode': 1, 'genre': 1, 'description': 1, 'stream_url': 1}
        
        movies = list(content_collection.find({'type': 'movie'}, projection).limit(100))  # Limit results
        series = list(content_collection.find({'type': 'series'}, projection).limit(100))

        # Get unique categories efficiently
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
    """Optimized health check endpoint"""
    try:
        # Quick ping to MongoDB
        mongo_client.admin.command('ping')
        mongo_status = 'ok'
    except Exception as e:
        mongo_status = f'error: {str(e)[:50]}'  # Truncate error message
        logger.error(f"MongoDB health check failed: {e}")

    try:
        videos_count = files_collection.estimated_document_count()  # Faster than count_documents
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

# OPTIMIZED Webhook endpoint for Telegram updates
@flask_app.route("/telegram-webhook", methods=["POST"])
def telegram_webhook():
    """Handle incoming Telegram updates from the webhook - OPTIMIZED for speed"""
    if not telegram_bot_app:
        logger.error("Telegram bot application not initialized.")
        return "Bot not ready", 500

    try:
        update_json = request.get_json(force=True)
        if not update_json:
            return "No data", 400
            
        update = Update.de_json(update_json, telegram_bot_app.bot)
        
        # Process the update in a separate thread to avoid blocking
        def process_update_async():
            try:
                # Create new event loop for this thread
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(telegram_bot_app.process_update(update))
                loop.close()
            except Exception as e:
                logger.error(f"Error processing update in thread: {e}")

        # Submit to thread pool
        executor.submit(process_update_async)
        
        return "ok", 200  # Return immediately
        
    except Exception as e:
        logger.error(f"Error in webhook: {e}")
        return "Error", 500

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
✅ **Lightning Fast Webhook Processing** ⚡

**Commands:**
/upload - Upload and categorize content
/library - View your content library
/frontend - Get frontend app link
/stats - Check bot statistics

Just send me a video file to get started! 🚀
    """
    await update.message.reply_text(welcome_message, parse_mode='Markdown')

async def handle_video_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle video file uploads - OPTIMIZED for speed"""
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

        # Forward message to storage channel
        forwarded_msg = await context.bot.forward_message(
            chat_id=STORAGE_CHANNEL_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id
        )

        file_obj = video if video else document
        file = await file_obj.get_file()
        file_url = file.file_path
        file_id = str(uuid.uuid4())

        # Skip metadata extraction for faster processing - do it asynchronously later
        video_metadata = VideoMetadata()  # Empty metadata initially

        # Store file info in MongoDB immediately
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
            'duration': None,  # Will be updated later
            'resolution': None,
            'audio_tracks': [],
            'subtitle_tracks': [],
            'upload_date': datetime.now().isoformat(),
            'stream_url': stream_url
        }
        
        files_collection.insert_one(file_document)

        # Create categorization keyboard
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

        # Extract metadata asynchronously in the background (optional)
        def extract_metadata_background():
            try:
                # This would run in background if you want metadata
                # For now, we skip it for speed
                pass
            except Exception as e:
                logger.error(f"Background metadata extraction failed: {e}")

        # Submit background task
        executor.submit(extract_metadata_background)

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
    """Handle metadata input for content categorization - OPTIMIZED"""
    if 'categorizing' not in context.user_data:
        return

    categorizing = context.user_data['categorizing']
    file_id = categorizing['file_id']
    content_type = categorizing['type']

    file_info = files_collection.find_one({'_id': file_id}, {'_id': 1})  # Just check existence
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

# Additional optimized command handlers
async def library_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user's content library - optimized"""
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

def run_flask_app():
    """Run the Flask app in a separate thread."""
    # Use 0.0.0.0 to make it accessible externally
    flask_app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=False, use_reloader=False)

def main():
    """Main function to initialize and run the bot and Flask app."""
    global mongo_client, db, files_collection, content_collection, telegram_bot_app

    # 1. Initialize MongoDB
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DB_NAME]
        files_collection = db['files']
        content_collection = db['content']
        
        # Ensure indexes for faster queries
        files_collection.create_index([('user_id', 1)])
        content_collection.create_index([('added_by', 1), ('type', 1)])
        content_collection.create_index([('type', 1)])
        content_collection.create_index([('added_date', -1)])

        logger.info("MongoDB connected and collections initialized.")
    except ConnectionFailure as e:
        logger.critical(f"MongoDB connection failed: {e}")
        exit(1)
    except OperationFailure as e:
        logger.critical(f"MongoDB operation failed (e.g., auth error): {e}")
        exit(1)

    # 2. Initialize Telegram Bot Application
    if not BOT_TOKEN:
        logger.critical("BOT_TOKEN environment variable not set.")
        exit(1)

    telegram_bot_app = Application.builder().token(BOT_TOKEN).build()

    # Add handlers
    telegram_bot_app.add_handler(CommandHandler("start", start))
    telegram_bot_app.add_handler(CommandHandler("library", library_command))
    telegram_bot_app.add_handler(CommandHandler("frontend", frontend_command))
    telegram_bot_app.add_handler(CommandHandler("stats", stats_command))
    telegram_bot_app.add_handler(MessageHandler(filters.VIDEO | filters.Document.ALL, handle_video_file))
    telegram_bot_app.add_handler(CallbackQueryHandler(handle_categorization))
    telegram_bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_metadata_input))

    # 3. Set up Telegram Webhook
    # Get the public domain from environment variables, default if not set
    domain = os.getenv('KOYEB_PUBLIC_DOMAIN', None)
    if not domain:
        logger.warning("KOYEB_PUBLIC_DOMAIN environment variable not set. Webhook might not work correctly.")
        logger.warning("Defaulting to a placeholder for webhook URL. Please set KOYEB_PUBLIC_DOMAIN.")
        webhook_url = "https://your-app.koyeb.app/telegram-webhook" # Placeholder
    else:
        webhook_url = f"https://{domain}/telegram-webhook"

    logger.info(f"Setting webhook to: {webhook_url}")
    try:
        # Use aiohttp for async webhook setup
        async def set_webhook_async():
            await telegram_bot_app.bot.set_webhook(url=webhook_url)
            logger.info("Telegram webhook set successfully.")
        
        # Run the async webhook setup in the current event loop
        asyncio.get_event_loop().run_until_complete(set_webhook_async())
    except TelegramError as e:
        logger.critical(f"Failed to set Telegram webhook: {e}")
        exit(1)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during webhook setup: {e}")
        exit(1)

    # 4. Run Flask app in a separate thread
    flask_thread = threading.Thread(target=run_flask_app)
    flask_thread.daemon = True  # Allow the main program to exit even if the thread is running
    flask_thread.start()
    logger.info("Flask app started in a separate thread.")

    # 5. Start the Telegram bot (webhook mode)
    # In webhook mode, `run_webhook` or `start_webhook` is used.
    # Since Flask handles the HTTP server, the python-telegram-bot library needs to know it's not starting its own.
    # The `telegram_webhook` Flask route directly calls `telegram_bot_app.process_update`.
    # So, we just need to keep the main thread alive.
    logger.info("Telegram bot is running in webhook mode, listening for updates via Flask.")
    # Keep the main thread alive, e.g., by joining the Flask thread (though it's a daemon)
    # or by a simple loop if other background tasks are expected.
    # For a simple webhook setup, the Flask server itself keeps the process alive.
    try:
        # This will block until the Flask thread finishes, which it won't unless the app is stopped.
        flask_thread.join() 
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped by user.")
    finally:
        if mongo_client:
            mongo_client.close()
            logger.info("MongoDB connection closed.")
        executor.shutdown(wait=True)
        logger.info("Thread pool shut down.")

if __name__ == '__main__':
    main()
