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

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from telegram.error import TelegramError
from flask import Flask, Response, abort, jsonify, request, render_template_string
import threading
import aiohttp

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

# Global variables for MongoDB (will be initialized in main)
mongo_client = None
db = None
files_collection = None
content_collection = None

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
            <p>Loading content...</p>
        </div>
    </div>

    <script>
        async function loadContent() {
            try {
                const response = await fetch('/api/content');
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
                    contentGrid.innerHTML = '<p>No content available yet. Upload videos via the Telegram bot!</p>';
                }
            } catch (error) {
                console.error('Error loading content:', error);
                document.getElementById('content-grid').innerHTML = '<p>Error loading content. Please try again later.</p>';
            }
        }

        loadContent();
    </script>
</body>
</html>
"""

# Flask app for serving files
flask_app = Flask(__name__)

class VideoMetadata:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.metadata = {}
        if file_path and os.path.exists(file_path):
            self._extract_metadata()

    def _extract_metadata(self):
        """Extract video metadata using ffprobe"""
        try:
            cmd = [
                'ffprobe', '-v', 'quiet', '-print_format', 'json',
                '-show_format', '-show_streams', self.file_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            if result.returncode == 0:
                self.metadata = json.loads(result.stdout)
            else:
                logger.warning(f"ffprobe failed for {self.file_path}: {result.stderr}")
        except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError, Exception) as e:
            logger.warning(f"Metadata extraction failed for {self.file_path}: {e}")
            self.metadata = {}

    def get_duration(self) -> Optional[float]:
        """Get video duration in seconds"""
        try:
            return float(self.metadata['format']['duration'])
        except (KeyError, ValueError):
            return None

    def get_resolution(self) -> Optional[tuple]:
        """Get video resolution (width, height)"""
        try:
            for stream in self.metadata['streams']:
                if stream['codec_type'] == 'video':
                    return (stream['width'], stream['height'])
        except KeyError:
            pass
        return None

    def get_audio_tracks(self) -> List[Dict]:
        """Get audio track information"""
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
        """Get subtitle track information"""
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

# Flask app instance (global for webhook handler)
telegram_bot_app = None

# Flask Routes
@flask_app.route('/')
def serve_frontend():
    """Serve the main frontend HTML page."""
    return render_template_string(SIMPLE_FRONTEND)

@flask_app.route('/stream/<file_id>')
def stream_file(file_id):
    """Stream video file with support for range requests"""
    try:
        # Fetch file info from MongoDB
        file_info = files_collection.find_one({'_id': file_id})
        if not file_info:
            abort(404)

        file_url = file_info['file_url']
        file_size = file_info['file_size']
        filename = file_info['filename']
        mime_type = get_video_mime_type(filename)

        # Handle range requests
        range_header = request.environ.get('HTTP_RANGE', '').strip()
        range_match = None

        if range_header:
            range_match = re.search(r'bytes=(\d+)-(\d*)', range_header)

        if range_match:
            start = int(range_match.group(1))
            end = int(range_match.group(2)) if range_match.group(2) else file_size - 1

            def generate_range():
                try:
                    import requests
                    headers = {'Range': f'bytes={start}-{end}'}
                    with requests.get(file_url, headers=headers, stream=True) as response:
                        response.raise_for_status()
                        for chunk in response.iter_content(chunk_size=8192):
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
        else:
            def generate_full():
                try:
                    import requests
                    with requests.get(file_url, stream=True) as response:
                        response.raise_for_status()
                        for chunk in response.iter_content(chunk_size=8192):
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
        movies = list(content_collection.find({'type': 'movie'}, {'_id': 0}))
        series = list(content_collection.find({'type': 'series'}, {'_id': 0}))

        all_categories = set()
        for item in movies + series:
            if 'genre' in item and isinstance(item['genre'], list):
                all_categories.update(item['genre'])

        return jsonify({
            'movies': movies,
            'series': series,
            'categories': list(all_categories),
            'total_content': len(movies) + len(series)
        })
    except Exception as e:
        logger.error(f"Error in get_content_library: {e}")
        return jsonify({
            'movies': [],
            'series': [],
            'categories': [],
            'total_content': 0
        })

@flask_app.route('/api/content/<content_type>')
def get_content_by_type(content_type):
    """Get content by type (movies/series)"""
    if content_type not in ['movies', 'series']:
        abort(404)

    query_filter = {'type': content_type}

    category = request.args.get('category')
    search = request.args.get('search', '').lower()

    if category:
        query_filter['genre'] = category # Assuming genre is an array and category is one element

    if search:
        # Case-insensitive search on title and description
        query_filter['$or'] = [
            {'title': {'$regex': search, '$options': 'i'}},
            {'description': {'$regex': search, '$options': 'i'}}
        ]

    content_list = list(content_collection.find(query_filter, {'_id': 0}))

    return jsonify({
        'content': content_list,
        'total': len(content_list)
    })

@flask_app.route('/api/content/item/<content_id>')
def get_content_item(content_id):
    """Get specific content item details"""
    content_item = content_collection.find_one({'_id': content_id}, {'_id': 0}) # Exclude _id from response
    if not content_item:
        abort(404)

    # Also fetch the associated file_info from the files collection
    file_info = files_collection.find_one({'_id': content_item['file_id']}, {'_id': 0})
    if file_info:
        content_item['file_info'] = file_info

    return jsonify(content_item)

@flask_app.route('/info/<file_id>')
def file_info_endpoint(file_id):
    """Get file information as JSON"""
    file_data = files_collection.find_one({'_id': file_id}, {'_id': 0})
    if not file_data:
        abort(404)

    domain = os.getenv('KOYEB_PUBLIC_DOMAIN', 'your-app.koyeb.app')

    # Add stream_url for direct access
    file_data['stream_url'] = f"https://{domain}/stream/{file_id}"

    # Fetch content metadata if available
    content_metadata = content_collection.find_one({'file_id': file_id}, {'_id': 0})
    if content_metadata:
        file_data['content_metadata'] = content_metadata

    return jsonify(file_data)

@flask_app.route('/health')
def health_check():
    """Health check endpoint, also checks MongoDB connection."""
    try:
        # The ping command is cheap and does not require auth.
        mongo_client.admin.command('ping')
        mongo_status = 'ok'
    except ConnectionFailure as e:
        mongo_status = f'error: {e}'
        logger.error(f"MongoDB health check failed: {e}")
    except Exception as e:
        mongo_status = f'error: {e}'
        logger.error(f"Unexpected error during MongoDB health check: {e}")

    return jsonify({
        'status': 'ok' if mongo_status == 'ok' else 'degraded',
        'mongodb_status': mongo_status,
        'videos_stored': files_collection.count_documents({}),
        'movies': content_collection.count_documents({'type': 'movie'}),
        'series': content_collection.count_documents({'type': 'series'}),
        'storage_channel': STORAGE_CHANNEL_ID
    })

# Webhook endpoint for Telegram updates
@flask_app.route("/telegram-webhook", methods=["POST"])
async def telegram_webhook():
    """Handle incoming Telegram updates from the webhook."""
    if not telegram_bot_app:
        logger.error("Telegram bot application not initialized.")
        return "Bot not ready", 500

    update_json = request.get_json(force=True)
    update = Update.de_json(update_json, telegram_bot_app.bot)

    # Process the update asynchronously
    await telegram_bot_app.process_update(update)
    return "ok"

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
✅ **Persistent Storage (MongoDB)** 🚀

**Supported formats:**
MP4, AVI, MKV, MOV, WMV, FLV, WebM, M4V, MPG, MPEG, OGV, 3GP, etc.

**Commands:**
/upload - Upload and categorize content
/library - View your content library
/frontend - Get frontend app link
/stats - Check bot statistics

Just send me a video file to get started! 🚀
    """
    await update.message.reply_text(welcome_message, parse_mode='Markdown')

async def handle_video_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle video file uploads with metadata extraction"""
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
            await update.message.reply_text(
                "❌ This bot only supports video files!"
            )
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

        temp_file_path = None
        current_loop_for_temp_download = None
        temp_session = None
        try:
            current_loop_for_temp_download = asyncio.get_event_loop()
        except RuntimeError:
            current_loop_for_temp_download = asyncio.new_event_loop()
            asyncio.set_event_loop(current_loop_for_temp_download)

        try:
            # Download a small portion for metadata extraction
            with tempfile.NamedTemporaryFile(delete=False, suffix='.tmp') as temp_file:
                temp_file_path = temp_file.name
                temp_session = aiohttp.ClientSession()
                async with temp_session.get(file_url, headers={'Range': 'bytes=0-10485760'}) as response: # First 10MB
                    response.raise_for_status() # Raise for HTTP errors
                    async for chunk in response.content.iter_chunked(8192):
                        temp_file.write(chunk)
                        break # Just need a small sample

            video_metadata = VideoMetadata(temp_file_path)

        except Exception as e:
            logger.error(f"Error during metadata extraction preparation: {e}")
            video_metadata = VideoMetadata("") # Empty metadata
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                except Exception as e:
                    logger.error(f"Error cleaning up temp file {temp_file_path}: {e}")
            if temp_session:
                current_loop_for_temp_download.run_until_complete(temp_session.close())

        # Store file info in MongoDB
        file_document = {
            '_id': file_id, # Use file_id as MongoDB document _id
            'filename': filename,
            'file_size': file_size,
            'file_url': file_url,
            'message_id': forwarded_msg.message_id,
            'user_id': update.effective_user.id,
            'chat_id': update.effective_chat.id,
            'storage_channel_id': STORAGE_CHANNEL_ID,
            'duration': video_metadata.get_duration(),
            'resolution': video_metadata.get_resolution(),
            'audio_tracks': video_metadata.get_audio_tracks(),
            'subtitle_tracks': video_metadata.get_subtitle_tracks(),
            'upload_date': datetime.now().isoformat()
        }
        files_collection.insert_one(file_document)

        domain = os.getenv('KOYEB_PUBLIC_DOMAIN', 'your-app.koyeb.app')
        stream_url = f"https://{domain}/stream/{file_id}"

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
            f"🎵 **Audio Tracks:** {len(video_metadata.get_audio_tracks())}\n"
            f"💬 **Subtitles:** {len(video_metadata.get_subtitle_tracks())}\n\n"
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
        # Check if file_id exists in files collection
        if not files_collection.find_one({'_id': file_id}):
            await query.edit_message_text("❌ File not found or already deleted!")
            return
        context.user_data['categorizing'] = {'type': 'movie', 'file_id': file_id}
        await query.edit_message_text(
            "📽️ **Adding as Movie**\n\n"
            "Please send me the movie details in this format:\n\n"
            "`Title | Year | Genre | Description`\n\n"
            "Example:\n"
            "`The Matrix | 1999 | Action, Sci-Fi | A computer hacker learns the truth about reality.`"
        )

    elif data.startswith('categorize_series_'):
        file_id = data.replace('categorize_series_', '')
        # Check if file_id exists in files collection
        if not files_collection.find_one({'_id': file_id}):
            await query.edit_message_text("❌ File not found or already deleted!")
            return
        context.user_data['categorizing'] = {'type': 'series', 'file_id': file_id}
        await query.edit_message_text(
            "📺 **Adding as Series**\n\n"
            "Please send me the series details in this format:\n\n"
            "`Title | Season | Episode | Genre | Description`\n\n"
            "Example:\n"
            "`Breaking Bad | 1 | 1 | Drama, Crime | A high school chemistry teacher turned meth manufacturer.`"
        )

    elif data.startswith('just_url_'):
        file_id = data.replace('just_url_', '')
        # Check if file_id exists in files collection
        if files_collection.find_one({'_id': file_id}):
            domain = os.getenv('KOYEB_PUBLIC_DOMAIN', 'your-app.koyeb.app')
            stream_url = f"https://{domain}/stream/{file_id}"

            await query.edit_message_text(
                f"🔗 **Streaming URL Generated**\n\n"
                f"`{stream_url}`\n\n"
                f"🎮 **Frontend App:** {FRONTEND_URL}\n\n"
                f"Use this URL in any video player or our Netflix-style frontend!",
                parse_mode='Markdown'
            )
        else:
            await query.edit_message_text("❌ File not found or already deleted!")


async def handle_metadata_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle metadata input for content categorization"""
    if 'categorizing' not in context.user_data:
        return # Not in categorization state

    categorizing = context.user_data['categorizing']
    file_id = categorizing['file_id']
    content_type = categorizing['type']

    # Ensure the file still exists in the files collection
    file_info = files_collection.find_one({'_id': file_id})
    if not file_info:
        await update.message.reply_text("❌ Associated file not found or already deleted!")
        del context.user_data['categorizing']
        return

    try:
        metadata_text = update.message.text.strip()
        parts = [part.strip() for part in metadata_text.split('|')]

        domain = os.getenv('KOYEB_PUBLIC_DOMAIN', 'your-app.koyeb.app')
        stream_url = f"https://{domain}/stream/{file_id}"

        content_document = None

        if content_type == 'movie' and len(parts) >= 4:
            title, year_str, genre_str, description = parts[:4]
            year = int(year_str) if year_str.isdigit() else None
            genre = [g.strip() for g in genre_str.split(',')]
            content_id = str(uuid.uuid4()) # Unique ID for movie content

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

            await update.message.reply_text(
                f"✅ **Movie Added Successfully!**\n\n"
                f"🎬 **Title:** {title}\n"
                f"📅 **Year:** {year_str}\n"
                f"🎭 **Genre:** {genre_str}\n\n"
                f"🎮 **Watch on Frontend:** {FRONTEND_URL}\n"
                f"🔗 **Direct Stream:** `{stream_url}`",
                parse_mode='Markdown'
            )

        elif content_type == 'series' and len(parts) >= 5:
            title, season_str, episode_str, genre_str, description = parts[:5]
            season = int(season_str) if season_str.isdigit() else None
            episode = int(episode_str) if episode_str.isdigit() else None
            genre = [g.strip() for g in genre_str.split(',')]
            content_id = f"{re.sub(r'[^a-z0-9]', '_', title.lower())}_s{season_str}e{episode_str}_{uuid.uuid4().hex[:8]}" # More robust ID

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

            await update.message.reply_text(
                f"✅ **Series Episode Added Successfully!**\n\n"
                f"📺 **Title:** {title}\n"
                f"🗓️ **Season {season_str}, Episode {episode_str}**\n"
                f"🎭 **Genre:** {genre_str}\n\n"
                f"🎮 **Watch on Frontend:** {FRONTEND_URL}\n"
                f"🔗 **Direct Stream:** `{stream_url}`",
                parse_mode='Markdown'
            )

        else:
            await update.message.reply_text(
                "❌ Invalid format! Please follow the exact format shown above."
            )
            return

        if content_document:
            content_collection.insert_one(content_document)
            logger.info(f"Content added to MongoDB: {content_document['title']} (Type: {content_document['type']})")

        # Clear categorization state
        del context.user_data['categorizing']

    except Exception as e:
        logger.error(f"Error processing metadata and saving to MongoDB: {e}")
        await update.message.reply_text(
            "❌ Error processing metadata. Please check the format and try again."
        )

async def library_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user's content library"""
    user_id = update.effective_user.id

    user_movies = list(content_collection.find({'added_by': user_id, 'type': 'movie'}, {'_id': 0}))
    user_series = list(content_collection.find({'added_by': user_id, 'type': 'series'}, {'_id': 0}))

    if not user_movies and not user_series:
        await update.message.reply_text(
            "📚 **Your library is empty!**\n\n"
            "Upload some videos and categorize them as movies or series episodes to build your collection."
        )
        return

    library_text = f"📚 **Your Content Library**\n\n"

    if user_movies:
        library_text += f"🎬 **Movies ({len(user_movies)}):**\n"
        for movie in user_movies[:10]:  # Show first 10
            library_text += f"• {movie['title']} ({movie.get('year', 'N/A')})\n"
        if len(user_movies) > 10:
            library_text += f"• ... and {len(user_movies) - 10} more\n"
        library_text += "\n"

    if user_series:
        library_text += f"📺 **Series Episodes ({len(user_series)}):**\n"
        # Group series by title
        series_groups = {}
        for series_item in user_series:
            title = series_item['title']
            if title not in series_groups:
                series_groups[title] = []
            series_groups[title].append(series_item)

        for title, episodes in list(series_groups.items())[:5]:  # Show first 5 series
            library_text += f"• **{title}:** {len(episodes)} episodes\n"

        if len(series_groups) > 5:
            library_text += f"• ... and {len(series_groups) - 5} more series\n"
        library_text += "\n"

    library_text += f"🎮 **View in Frontend:** {FRONTEND_URL}"

    await update.message.reply_text(library_text, parse_mode='Markdown')

async def frontend_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show frontend app information"""
    await update.message.reply_text(
        f"🎮 **Netflix-Style Frontend App**\n\n"
        f"🔗 **App URL:** {FRONTEND_URL}\n\n"
        f"**Features:**\n"
        f"✅ Netflix-like interface\n"
        f"✅ Search & filter content\n"
        f"✅ Movie & series categories\n"
        f"✅ Multi-audio track support\n"
        f"✅ Android TV optimized\n"
        f"✅ Responsive design\n"
        f"✅ **Persistent Content Library (MongoDB)**\n\n"
        f"Open the link above to access your streaming platform!",
        parse_mode='Markdown'
    )

async def upload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Upload command handler"""
    await update.message.reply_text(
        "📤 **Upload Content**\n\n"
        "Simply send me any video file (up to 4GB) and I'll:\n\n"
        "1️⃣ Extract video metadata\n"
        "2️⃣ Generate streaming URL\n"
        "3️⃣ Add to your persistent library (MongoDB)\n"
        "4️⃣ Make it available on frontend\n\n"
        "**Supported formats:**\n"
        "MP4, AVI, MKV, MOV, WMV, FLV, WebM, M4V, MPG, MPEG, OGV, 3GP, etc.\n\n"
        "Just drop your video file here! 🎬"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help command handler"""
    help_text = """
🔧 **Bot Commands & Features**

**Commands:**
• `/start` - Welcome message and features
• `/upload` - Instructions for uploading videos
• `/library` - View your content collection
• `/frontend` - Get frontend app link
• `/help` - This help message
• `/stats` - Check bot statistics
• `/delete <file_id>` - Delete content and its associated file

**How to Use:**
1️⃣ Send me any video file (up to 4GB)
2️⃣ Choose to categorize as Movie or Series
3️⃣ Provide metadata (title, year, genre, etc.)
4️⃣ Get permanent streaming URL
5️⃣ Watch on Netflix-style frontend

**Features:**
✅ Range request support for streaming
✅ Multi-audio track detection
✅ Subtitle track extraction
✅ Video metadata analysis
✅ Content categorization
✅ Search functionality
✅ Mobile & TV friendly interface
✅ **Persistent Data Storage (MongoDB)**

**Supported Formats:**
MP4, AVI, MKV, MOV, WMV, FLV, WebM, M4V, MPG, MPEG, OGV, 3GP, RM, RMVB, ASF, DIVX

**File Size Limit:** 4GB per file

Need more help? Just ask! 😊
    """
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show bot statistics"""
    user_id = update.effective_user.id

    # User-specific stats from MongoDB
    user_movies_count = content_collection.count_documents({'added_by': user_id, 'type': 'movie'})
    user_series_count = content_collection.count_documents({'added_by': user_id, 'type': 'series'})
    user_files_cursor = files_collection.find({'user_id': user_id}, {'file_size': 1})
    user_total_size = sum(f['file_size'] for f in user_files_cursor)

    # Global stats from MongoDB
    total_movies = content_collection.count_documents({'type': 'movie'})
    total_series = content_collection.count_documents({'type': 'series'})
    total_files = files_collection.count_documents({})

    # Dynamically get categories
    all_genres_cursor = content_collection.distinct('genre')
    total_categories = len(all_genres_cursor)

    stats_text = f"""
📊 **Your Statistics**

**Your Content:**
🎬 Movies: {user_movies_count}
📺 Series Episodes: {user_series_count}
📁 Total Files Uploaded by You: {files_collection.count_documents({'user_id': user_id})}
💾 Storage Used by You: {user_total_size/(1024*1024*1024):.2f} GB

**Platform Statistics:**
🎬 Total Movies: {total_movies}
📺 Total Episodes: {total_series}
📂 Total Files Stored: {total_files}
🏷️ Unique Categories: {total_categories}

**Popular Genres (first 10):**
{', '.join(all_genres_cursor[:10]) if all_genres_cursor else 'None yet'}

🎮 **Frontend:** {FRONTEND_URL}
    """
    await update.message.reply_text(stats_text, parse_mode='Markdown')

async def delete_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle content deletion"""
    if not context.args:
        await update.message.reply_text(
            "❌ **Usage:** `/delete <file_id>`\n\n"
            "To get file IDs, use `/library` command."
        )
        return

    file_id = context.args[0]
    user_id = update.effective_user.id

    file_info = files_collection.find_one({'_id': file_id})
    if not file_info:
        await update.message.reply_text("❌ File not found!")
        return

    if file_info['user_id'] != user_id:
        await update.message.reply_text("❌ You can only delete your own files!")
        return

    # Delete from content collection first
    delete_result_content = content_collection.delete_many({'file_id': file_id})

    # Delete from files collection
    delete_result_file = files_collection.delete_one({'_id': file_id})

    if delete_result_file.deleted_count > 0:
        await update.message.reply_text(
            f"✅ **Content Deleted**\n\n"
            f"🗑️ **File:** `{file_info['filename']}`\n"
            f"📊 **Removed:** {delete_result_content.deleted_count} content entries\n\n"
            f"The streaming URL is no longer accessible.",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text("❌ Failed to delete file. It might have been deleted already.")


def run_flask_and_webhook(bot_app_instance: Application):
    """Run Flask app and webhook in the main thread."""
    global telegram_bot_app
    telegram_bot_app = bot_app_instance # Assign the bot app instance to the global variable

    port = int(os.getenv('PORT', 5000))
    public_domain = os.getenv('KOYEB_PUBLIC_DOMAIN')

    if not public_domain:
        logger.error("KOYEB_PUBLIC_DOMAIN environment variable is not set. Webhook will not be set.")
        # Fallback to polling if domain is not set, though not ideal for Koyeb
        bot_app_instance.run_polling(drop_pending_updates=True)
        return

    webhook_path = "/telegram-webhook"
    webhook_url = f"https://{public_domain}{webhook_path}"

    logger.info(f"Setting webhook to: {webhook_url}")

    # Set up the webhook
    bot_app_instance.run_webhook(
        listen="0.0.0.0",
        port=port,
        url_path=webhook_path,
        webhook_url=webhook_url
    )

if __name__ == "__main__":
    # Environment setup instructions
    print("""
🎬 Netflix-Style Video Streaming Bot Setup

Required Environment Variables:
- BOT_TOKEN: Your Telegram bot token from @BotFather
- STORAGE_CHANNEL_ID: Channel ID for storing video files (use @username2idbot)
- KOYEB_PUBLIC_DOMAIN: Your deployed domain (e.g., your-app.koyeb.app)
- FRONTEND_URL: Your frontend application URL
- MONGO_URI: Your MongoDB connection string (e.g., mongodb+srv://user:pass@cluster.mongodb.net/?)
- PORT: Port for Flask server (default: 5000)

Optional:
- MAX_FILE_SIZE: Maximum file size in bytes (default: 4GB)
- MONGO_DB_NAME: Name of the MongoDB database to use (default: netflix_bot_db)

Make sure to:
1. Create a Telegram bot with @BotFather
2. Create a private channel and add your bot as admin
3. Get the channel ID using @username2idbot
4. **Create a MongoDB Atlas cluster and get your connection string.**
5. Deploy this script to a cloud platform (Koyeb, Railway, Heroku, etc.)
6. Set up a frontend application for Netflix-style interface

Starting bot...
    """)

    # Check for essential environment variables
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN environment variable is required!")
        exit(1)

    if not STORAGE_CHANNEL_ID:
        logger.error("STORAGE_CHANNEL_ID environment variable is required!")
        exit(1)

    if not MONGO_URI:
        logger.error("MONGO_URI environment variable is required for persistence!")
        exit(1)

    # Initialize MongoDB connection
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DB_NAME]
        files_collection = db['files']
        content_collection = db['content']
        # Test connection
        mongo_client.admin.command('ping')
        logger.info("Successfully connected to MongoDB!")
    except ConnectionFailure as e:
        logger.error(f"Could not connect to MongoDB: {e}")
        exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred during MongoDB initialization: {e}")
        exit(1)

    # Initialize bot
    bot_app = Application.builder().token(BOT_TOKEN).build()

    # Add handlers
    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CommandHandler("help", help_command))
    bot_app.add_handler(CommandHandler("upload", upload_command))
    bot_app.add_handler(CommandHandler("library", library_command))
    bot_app.add_handler(CommandHandler("frontend", frontend_command))
    bot_app.add_handler(CommandHandler("stats", stats_command))
    bot_app.add_handler(CommandHandler("delete", delete_command))

    # Handle video files
    bot_app.add_handler(MessageHandler(
        filters.VIDEO | (filters.Document.ALL & filters.Document.VIDEO),
        handle_video_file
    ))

    # Handle categorization callbacks
    bot_app.add_handler(CallbackQueryHandler(handle_categorization))

    # Handle metadata input (text messages when categorizing)
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_metadata_input))

    logger.info("Starting Netflix Bot...")

    try:
        # Run Flask and webhook in the main thread
        run_flask_and_webhook(bot_app)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot error: {e}")

