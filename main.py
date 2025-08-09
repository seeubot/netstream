import os
import uuid
import asyncio
import mimetypes
import json
import re
import logging
from datetime import datetime
from typing import Dict, List, Optional
import time
import requests
import sys
from urllib.parse import quote
from hypercorn.asyncio import serve
from hypercorn.config import Config as HypercornConfig

from quart import Quart, request, jsonify, Response, render_template_string, abort, redirect
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler
from telegram.error import TelegramError
from pymongo import MongoClient
import pymongo.errors
import httpx
from bson import ObjectId

# Configure logging for production
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Suppress noisy logs
logging.getLogger('pymongo').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('quart.serving').setLevel(logging.WARNING)

# Configuration with defaults and validation
BOT_TOKEN = os.getenv('BOT_TOKEN')
STORAGE_CHANNEL_ID = os.getenv('STORAGE_CHANNEL_ID')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb+srv://food:food@food.1jskkt3.mongodb.net/?retryWrites=true&w=majority&appName=food')
DB_NAME = os.getenv('MONGO_DB_NAME', 'netflix_bot_db')
PORT = int(os.getenv('PORT', 8080))
MAX_FILE_SIZE = 4 * 1024 * 1024 * 1024  # 4GB

# Telegram API limits
TELEGRAM_FILE_SIZE_LIMIT = 20 * 1024 * 1024  # 20MB - Telegram API limit for get_file

# Webhook configuration
WEBHOOK_PATH = f'/{uuid.uuid4()}'

# Global state
app_state = {
    'mongo_client': None,
    'db': None,
    'files_collection': None,
    'content_collection': None,
    'bot_app': None,
    'webhook_set': False,
    'webhook_url': None
}

# Supported formats
SUPPORTED_VIDEO_FORMATS = {
    'mp4', 'avi', 'mkv', 'mov', 'wmv', 'flv', 'webm', 'm4v',
    'mpg', 'mpeg', 'ogv', '3gp', 'rm', 'rmvb', 'asf', 'divx',
    'ts', 'vob', 'ogg', 'hevc', 'av1', 'vp9', 'h264', 'h265'
}

SUPPORTED_AUDIO_FORMATS = {
    'mp3', 'wav', 'aac', 'flac', 'ogg', 'm4a'
}

def get_deployment_domain():
    """Get the deployment domain from environment variables."""
    domain = (
        os.getenv('KOYEB_PUBLIC_DOMAIN') or
        os.getenv('KOYEB_DOMAIN') or
        os.getenv('PUBLIC_DOMAIN') or
        os.getenv('RAILWAY_STATIC_URL') or
        os.getenv('VERCEL_URL') or
        os.getenv('RENDER_EXTERNAL_URL')
    )
    if not domain:
        logger.warning("No deployment domain found in environment variables. Webhook will likely fail.")
        return None
    if not domain.startswith('http'):
        domain = f"https://{domain}"
    
    return domain.rstrip('/')

def get_file_type(filename):
    """Check if file is a supported media format and return its type."""
    if not filename or '.' not in filename:
        return 'unknown'
    ext = filename.rsplit('.', 1)[1].lower()
    if ext in SUPPORTED_VIDEO_FORMATS:
        return 'video'
    if ext in SUPPORTED_AUDIO_FORMATS:
        return 'audio'
    return 'unknown'

def get_media_mime_type(filename, default='application/octet-stream'):
    """Get MIME type for video or audio file"""
    mime_type, _ = mimetypes.guess_type(filename)
    if mime_type:
        return mime_type
    ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else ''
    mime_map = {
        # Video formats
        'mp4': 'video/mp4', 'avi': 'video/x-msvideo', 'mkv': 'video/x-matroska',
        'mov': 'video/quicktime', 'wmv': 'video/x-ms-wmv', 'flv': 'video/x-flv',
        'webm': 'video/webm', 'm4v': 'video/mp4', 'mpg': 'video/mpeg',
        'mpeg': 'video/mpeg', 'ogv': 'video/ogg', '3gp': 'video/3gpp',
        'ts': 'video/mp2t', 'vob': 'video/dvd', 'ogg': 'video/ogg', 'hevc': 'video/hevc',
        'av1': 'video/av1', 'vp9': 'video/vp9', 'h264': 'video/h264', 'h265': 'video/h265',
        # Audio formats
        'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'aac': 'audio/aac',
        'flac': 'audio/flac', 'm4a': 'audio/mp4'
    }
    return mime_map.get(ext, default)

def initialize_mongodb():
    """Initialize MongoDB connection with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Connecting to MongoDB (attempt {attempt + 1}/{max_retries})")
            client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=10000,
                socketTimeoutMS=10000,
                maxPoolSize=10,
                retryWrites=True
            )
            client.admin.command('ping')
            db = client[DB_NAME]
            files_collection = db['files']
            content_collection = db['content']
            try:
                # Ensure indexes exist
                files_collection.create_index([('user_id', 1)], background=True)
                content_collection.create_index([('added_by', 1), ('type', 1)], background=True)
                content_collection.create_index([('type', 1)], background=True)
            except Exception as e:
                logger.warning(f"Index creation warning: {e}")
            app_state.update({
                'mongo_client': client,
                'db': db,
                'files_collection': files_collection,
                'content_collection': content_collection
            })
            logger.info("✅ MongoDB connected successfully!")
            return True
        except Exception as e:
            logger.error(f"MongoDB connection attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                logger.error("❌ All MongoDB connection attempts failed")
                return False
            time.sleep(2 ** attempt)
    return False

# Quart application
app = Quart(__name__)

# Simple Video Player Frontend
PLAYER_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ title }} - StreamPlayer</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
            background: #000;
            color: white;
            min-height: 100vh;
        }
        .header {
            background: rgba(0,0,0,0.9);
            padding: 1rem 2rem;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }
        .header h1 {
            color: #e50914;
            font-size: 1.5rem;
            font-weight: 700;
        }
        .container {
            max-width: 1280px;
            margin: 0 auto;
            padding: 2rem;
        }
        .video-player {
            width: 100%;
            max-width: 100%;
            background: #000;
            border-radius: 12px;
            overflow: hidden;
            margin-bottom: 2rem;
        }
        video {
            width: 100%;
            height: auto;
            max-height: 70vh;
            display: block;
        }
        .video-info {
            padding: 1.5rem;
            background: rgba(255,255,255,0.05);
            border-radius: 12px;
            margin-bottom: 2rem;
        }
        .video-title {
            font-size: 1.8rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
            color: white;
        }
        .video-meta {
            display: flex;
            gap: 1rem;
            margin-bottom: 1rem;
            flex-wrap: wrap;
        }
        .meta-item {
            background: rgba(255,255,255,0.1);
            padding: 0.3rem 0.8rem;
            border-radius: 20px;
            font-size: 0.9rem;
            color: #ccc;
        }
        .video-description {
            color: #aaa;
            line-height: 1.6;
            margin-bottom: 1.5rem;
        }
        .controls {
            display: flex;
            gap: 1rem;
            flex-wrap: wrap;
        }
        .btn {
            padding: 0.8rem 1.5rem;
            background: linear-gradient(45deg, #e50914, #ff3030);
            color: white;
            text-decoration: none;
            border-radius: 8px;
            font-weight: 600;
            transition: all 0.3s ease;
            border: none;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
        }
        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 25px rgba(229, 9, 20, 0.4);
        }
        .btn-secondary {
            background: rgba(255,255,255,0.1);
            color: white;
        }
        .btn-secondary:hover {
            background: rgba(255,255,255,0.2);
        }
        .library-section {
            margin-top: 3rem;
        }
        .library-title {
            font-size: 1.5rem;
            font-weight: 700;
            color: white;
            margin-bottom: 1rem;
        }
        .content-list {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 1rem;
        }
        .content-item {
            background: rgba(255,255,255,0.05);
            padding: 1rem;
            border-radius: 8px;
            transition: all 0.3s ease;
            cursor: pointer;
            border: 1px solid rgba(255,255,255,0.1);
        }
        .content-item:hover {
            background: rgba(255,255,255,0.1);
            transform: translateY(-2px);
        }
        .content-item h3 {
            color: white;
            margin-bottom: 0.5rem;
            font-size: 1rem;
        }
        .content-item p {
            color: #aaa;
            font-size: 0.9rem;
            margin-bottom: 0.5rem;
        }
        .loading {
            text-align: center;
            padding: 2rem;
            color: #666;
        }
        .error-state {
            text-align: center;
            padding: 2rem;
            color: #666;
        }
        .error-state h2 {
            color: #e50914;
            margin-bottom: 1rem;
        }
        @media (max-width: 768px) {
            .header { padding: 1rem; }
            .container { padding: 1rem; }
            .controls { flex-direction: column; }
            .content-list { grid-template-columns: 1fr; }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🎬 StreamPlayer</h1>
    </div>

    <div class="container">
        {% if video_url %}
        <div class="video-player">
            <video controls preload="metadata" crossorigin="anonymous">
                <source src="{{ video_url }}" type="{{ mime_type }}">
                Your browser does not support the video tag.
            </video>
        </div>
        
        <div class="video-info">
            <h2 class="video-title">{{ title }}</h2>
            <div class="video-meta">
                <span class="meta-item">{{ content_type }}</span>
                {% if year %}<span class="meta-item">{{ year }}</span>{% endif %}
                {% if season and episode %}<span class="meta-item">S{{ season }}E{{ episode }}</span>{% endif %}
                {% if genre %}<span class="meta-item">{{ genre }}</span>{% endif %}
            </div>
            {% if description %}
            <p class="video-description">{{ description }}</p>
            {% endif %}
            <div class="controls">
                <a href="{{ video_url }}" download class="btn">
                    📥 Download
                </a>
                <a href="intent:{{ video_url }}#Intent;package=com.mxtech.videoplayer.ad;end;" class="btn-secondary btn">
                    📱 MX Player
                </a>
                <a href="vlc://{{ video_url }}" class="btn-secondary btn">
                    🎥 VLC Player
                </a>
                <button onclick="copyToClipboard('{{ video_url }}')" class="btn-secondary btn">
                    📋 Copy URL
                </button>
            </div>
        </div>
        {% endif %}

        <div class="library-section">
            <h2 class="library-title">Your Library</h2>
            <div id="content-list" class="content-list">
                <div class="loading">Loading your content...</div>
            </div>
        </div>
    </div>

    <script>
        function copyToClipboard(text) {
            navigator.clipboard.writeText(text).then(() => {
                alert('URL copied to clipboard!');
            }).catch(() => {
                const textArea = document.createElement('textarea');
                textArea.value = text;
                document.body.appendChild(textArea);
                textArea.select();
                document.execCommand('copy');
                document.body.removeChild(textArea);
                alert('URL copied to clipboard!');
            });
        }

        function playVideo(videoUrl, title, contentType, year, season, episode, genre, description) {
            const params = new URLSearchParams({
                url: videoUrl,
                title: title || 'Untitled',
                type: contentType || 'video',
                year: year || '',
                season: season || '',
                episode: episode || '',
                genre: genre || '',
                description: description || ''
            });
            window.location.href = `/play?${params.toString()}`;
        }

        async function loadLibrary() {
            try {
                const response = await fetch('/api/content');
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
                
                const data = await response.json();
                const allContent = [...data.movies, ...data.series];
                const contentList = document.getElementById('content-list');
                
                if (allContent.length === 0) {
                    contentList.innerHTML = `
                        <div class="error-state">
                            <h2>No Content Available</h2>
                            <p>Upload videos via the Telegram bot to start building your library!</p>
                        </div>
                    `;
                    return;
                }

                contentList.innerHTML = allContent.map(item => {
                    const meta = item.type === 'movie' 
                        ? `${item.year || 'Unknown Year'}` 
                        : `Season ${item.season || 'N/A'} • Episode ${item.episode || 'N/A'}`;
                    const genres = Array.isArray(item.genre) ? item.genre.join(', ') : (item.genre || 'Unknown');
                    
                    return `
                        <div class="content-item" onclick="playVideo('${item.stream_url}', '${(item.title || 'Untitled').replace(/'/g, '\\\')}', '${item.type === 'movie' ? 'Movie' : 'Series'}', '${item.year || ''}', '${item.season || ''}', '${item.episode || ''}', '${genres.replace(/'/g, '\\\')}', '${(item.description || '').replace(/'/g, '\\\'')}')">
                            <h3>${item.title || 'Untitled'}</h3>
                            <p>${meta} • ${genres}</p>
                            <p>${item.description ? item.description.substring(0, 100) + '...' : 'No description available'}</p>
                        </div>
                    `;
                }).join('');
            } catch (error) {
                console.error('Failed to load library:', error);
                document.getElementById('content-list').innerHTML = `
                    <div class="error-state">
                        <h2>Connection Error</h2>
                        <p>Unable to load content. Please try again later.</p>
                    </div>
                `;
            }
        }

        // Load library on page load
        loadLibrary();
    </script>
</body>
</html>
"""

# Quart Routes
@app.route('/')
async def serve_library():
    """Serve the library page"""
    return await render_template_string(PLAYER_HTML)

@app.route('/play')
async def play_video():
    """Serve the video player with specific video"""
    video_url = request.args.get('url')
    title = request.args.get('title', 'Untitled')
    content_type = request.args.get('type', 'Video')
    year = request.args.get('year')
    season = request.args.get('season')
    episode = request.args.get('episode')
    genre = request.args.get('genre')
    description = request.args.get('description')
    
    if not video_url:
        return await render_template_string(PLAYER_HTML)
    
    # Get MIME type from URL (extract filename)
    filename = video_url.split('/')[-1] if '/' in video_url else 'video.mp4'
    mime_type = get_media_mime_type(filename, 'video/mp4')
    
    return await render_template_string(PLAYER_HTML, 
                                      video_url=video_url,
                                      title=title,
                                      content_type=content_type,
                                      year=year,
                                      season=season,
                                      episode=episode,
                                      genre=genre,
                                      description=description,
                                      mime_type=mime_type)

@app.route('/health')
async def health_check():
    """Comprehensive health check"""
    health_status = {
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'services': {}
    }
    try:
        if app_state['mongo_client']:
            app_state['mongo_client'].admin.command('ping')
            health_status['services']['mongodb'] = 'ok'
        else:
            health_status['services']['mongodb'] = 'not_connected'
            health_status['status'] = 'degraded'
    except Exception as e:
        health_status['services']['mongodb'] = f'error: {str(e)[:50]}'
        health_status['status'] = 'degraded'
    
    health_status['services']['telegram_bot'] = 'ok' if app_state['bot_app'] else 'not_initialized'
    health_status['services']['webhook'] = 'set' if app_state['webhook_set'] else 'not_set'
    
    return jsonify(health_status), 200 if health_status['status'] == 'ok' else 503

@app.route('/check-webhook')
async def check_webhook_url():
    """Returns the webhook URL being used by the server."""
    if not app_state['webhook_url']:
        return jsonify({
            'status': 'error',
            'message': 'Webhook URL not set yet. Please check server logs.'
        }), 500
    return jsonify({
        'status': 'ok',
        'webhook_url': app_state['webhook_url']
    })

@app.route(WEBHOOK_PATH, methods=['POST'])
async def webhook_handler():
    """Handles incoming Telegram updates from the webhook."""
    if not app_state['bot_app']:
        return jsonify({'error': 'Bot application not initialized'}), 503
    try:
        update = Update.de_json(await request.get_json(), app_state['bot_app'].bot)
        await app_state['bot_app'].process_update(update)
        return jsonify({'status': 'ok'})
    except Exception as e:
        logger.error(f"Error processing webhook update: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/content')
async def get_content_library():
    """Get content library with error handling."""
    try:
        if app_state['content_collection'] is None:
            return jsonify({
                'movies': [],
                'series': [],
                'total_content': 0,
                'error': 'Database not available'
            }), 503
        
        projection = {
            '_id': 0, 'title': 1, 'type': 1, 'year': 1, 'season': 1,
            'episode': 1, 'genre': 1, 'description': 1, 'stream_url': 1
        }
        
        # Only retrieve content that has been fully categorized
        movies = list(app_state['content_collection'].find(
            {'type': 'movie', 'status': 'completed'}, projection
        ).sort('added_date', -1).limit(200))
        
        series = list(app_state['content_collection'].find(
            {'type': 'series', 'status': 'completed'}, projection
        ).sort('added_date', -1).limit(200))
        
        return jsonify({
            'movies': movies,
            'series': series,
            'total_content': len(movies) + len(series),
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in get_content_library: {e}")
        return jsonify({
            'movies': [],
            'series': [],
            'total_content': 0,
            'error': 'Internal server error'
        }), 500

@app.route('/stream/<file_id>')
async def stream_file(file_id):
    """Stream video files with chunked transfer encoding to avoid Content-Length issues."""
    try:
        if app_state['files_collection'] is None:
            abort(503)
        
        # Get file info from database
        file_info = app_state['files_collection'].find_one(
            {'_id': file_id},
            {'filename': 1, 'file_size': 1}
        )
        
        if not file_info:
            abort(404)
        
        filename = file_info['filename']
        file_size = file_info.get('file_size', 0)
        mime_type = get_media_mime_type(filename)
        
        # Try to get Telegram file URL for small files
        telegram_file_url = None
        if file_size <= TELEGRAM_FILE_SIZE_LIMIT:
            try:
                file_obj = await app_state['bot_app'].bot.get_file(file_id)
                telegram_file_url = file_obj.file_path
                logger.info(f"Got Telegram URL for {file_id}: {telegram_file_url}")
            except Exception as e:
                logger.error(f"Error getting Telegram file URL for {file_id}: {e}")
        
        if not telegram_file_url:
            # For large files or if Telegram API fails, return error
            logger.error(f"Cannot stream file {file_id}: No accessible URL")
            abort(404)
        
        # Handle range requests
        range_header = request.headers.get('Range', '').strip()
        
        async def stream_content():
            try:
                headers = {}
                if range_header:
                    headers['Range'] = range_header
                
                async with httpx.AsyncClient(timeout=60.0) as client:
                    async with client.stream("GET", telegram_file_url, headers=headers) as response:
                        response.raise_for_status()
                        async for chunk in response.aiter_bytes(8192):
                            yield chunk
                            
            except Exception as e:
                logger.error(f"Error streaming from Telegram: {e}")
                # Yield empty chunk to close the stream gracefully
                yield b''
        
        # Determine response headers - DON'T set Content-Length to avoid mismatch
        response_headers = {
            'Content-Type': mime_type,
            'Accept-Ranges': 'bytes',
            'Cache-Control': 'public, max-age=3600',
            'Access-Control-Allow-Origin': '*',
            'Transfer-Encoding': 'chunked'  # Use chunked encoding
        }
        
        status_code = 200
        if range_header:
            # For range requests, let the upstream server handle Content-Range
            status_code = 206
        
        return Response(
            stream_content(),
            status=status_code,
            headers=response_headers
        )
        
    except Exception as e:
        logger.error(f"Stream error for {file_id}: {e}")
        abort(500)

# Telegram Bot Handlers
async def start_command(update, context):
    """Start command handler"""
    try:
        domain = get_deployment_domain()
        frontend_url = domain if domain else "https://your-app.koyeb.app"
        welcome_text = f"""
🎬 **StreamPlayer - Simple Video Streaming Bot** 🎬

Welcome to your streaming platform! Upload any video and get instant streaming URLs.

**✨ Features:**
• Direct video streaming in browser
• HTML5 video player with controls
• Mobile & desktop compatible
• MX Player & VLC integration
• Movie & Series categorization
• Download support

**🎯 Commands:**
/start - Welcome message
/library - Browse your content
/player - Access web player
/stats - View library statistics

**📝 File Support:**
• Videos up to 20MB: Direct streaming
• Larger files: Download only
• All major video formats supported

**🚀 Get Started:**
1. Send me a video file
2. I'll categorize it (Movie/Series)  
3. Watch instantly at: {frontend_url}

Ready to start streaming! 🚀
"""
        await update.message.reply_text(welcome_text, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Start command error: {e}")
        await update.message.reply_text("An error occurred while starting the bot. Please try again.")

async def library_command(update, context):
    """Display the content library from the database."""
    try:
        if app_state['content_collection'] is None:
            await update.message.reply_text("Database is not available. Please try again later.")
            return
        
        await update.message.reply_text("Fetching your library... Please wait.")
        
        movies = list(app_state['content_collection'].find({'type': 'movie', 'status': 'completed'}).sort('added_date', -1).limit(10))
        series = list(app_state['content_collection'].find({'type': 'series', 'status': 'completed'}).sort('added_date', -1).limit(10))
        
        if not movies and not series:
            await update.message.reply_text("Your library is empty. Send me a video file to get started!")
            return
        
        message = "🎬 **Your StreamPlayer Library** 🎬\n\n"
        
        if movies:
            message += "**Movies:**\n"
            for m in movies:
                message += f"• **{m.get('title', 'Untitled')}** ({m.get('year', 'N/A')})\n"
        
        if series:
            message += "\n**Series:**\n"
            for s in series:
                message += f"• **{s.get('title', 'Untitled')}** (S{s.get('season', 'N/A')}E{s.get('episode', 'N/A')})\n"
        
        message += "\nTo watch videos, visit the web player."
        await update.message.reply_text(message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Library command error: {e}")
        await update.message.reply_text("An error occurred while fetching your library.")

async def player_command(update, context):
    """Send the user a link to the web player."""
    try:
        domain = get_deployment_domain()
        if not domain:
            await update.message.reply_text("The web player URL is not configured. Please contact the administrator.")
            return
        await update.message.reply_text(f"🔗 **Access your StreamPlayer here:**\n{domain}", parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Player command error: {e}")
        await update.message.reply_text("An error occurred while generating the player link.")

async def stats_command(update, context):
    """Display statistics about the content library."""
    try:
        if app_state['content_collection'] is None or app_state['files_collection'] is None:
            await update.message.reply_text("Database is not available. Please try again later.")
            return
        
        movies_count = app_state['content_collection'].count_documents({'type': 'movie', 'status': 'completed'})
        series_count = app_state['content_collection'].count_documents({'type': 'series', 'status': 'completed'})
        total_files = app_state['files_collection'].count_documents({})
        total_content = movies_count + series_count
        
        # Get storage information
        pipeline = [
            {'$group': {
                '_id': None,
                'total_size': {'$sum': '$file_size'},
                'count': {'$sum': 1}
            }}
        ]
        storage_stats = list(app_state['files_collection'].aggregate(pipeline))
        total_size = storage_stats[0]['total_size'] if storage_stats else 0
        size_gb = total_size / (1024**3)
        
        stats_text = f"""
📊 **StreamPlayer Statistics** 📊

**Content Library:**
🎬 Movies: {movies_count}
📺 Series: {series_count}
📁 Total Content: {total_content}

**Storage:**
📂 Total Files: {total_files}
💾 Storage Used: {size_gb:.2f} GB

**Bot Status:**
✅ Database: Connected
✅ Streaming: Active
✅ Web Player: Available

Use /library to browse your content or /player to access the web interface.
"""
        await update.message.reply_text(stats_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Stats command error: {e}")
        await update.message.reply_text("An error occurred while fetching statistics.")

async def handle_document(update, context):
    """Handle file uploads from users."""
    try:
        user_id = update.effective_user.id
        document = update.message.document
        
        if not document:
            await update.message.reply_text("No file received. Please try again.")
            return
        
        filename = document.file_name or "unknown_file"
        file_size = document.file_size or 0
        file_type = get_file_type(filename)
        
        if file_type == 'unknown':
            await update.message.reply_text(
                "❌ Unsupported file format. Please send video or audio files only.\n\n"
                f"Supported formats: {', '.join(SUPPORTED_VIDEO_FORMATS | SUPPORTED_AUDIO_FORMATS)}"
            )
            return
        
        if file_size > MAX_FILE_SIZE:
            await update.message.reply_text(f"❌ File too large. Maximum size is {MAX_FILE_SIZE / (1024**3):.1f} GB")
            return
        
        # Send processing message
        processing_msg = await update.message.reply_text("🔄 Processing your file...")
        
        # Store file info in database
        file_id = document.file_id
        file_record = {
            '_id': file_id,
            'user_id': user_id,
            'filename': filename,
            'file_size': file_size,
            'file_type': file_type,
            'uploaded_date': datetime.now(),
            'mime_type': get_media_mime_type(filename)
        }
        
        try:
            app_state['files_collection'].insert_one(file_record)
        except pymongo.errors.DuplicateKeyError:
            # File already exists, update the record
            app_state['files_collection'].update_one(
                {'_id': file_id},
                {'$set': file_record}
            )
        
        # Create stream URL
        domain = get_deployment_domain()
        if domain:
            stream_url = f"{domain}/stream/{file_id}"
        else:
            stream_url = f"https://your-app.koyeb.app/stream/{file_id}"
        
        # Create content categorization buttons with shorter callback data
        # Store mapping in context for callback handling
        callback_map = {
            f"mv_{file_id[:8]}": f"categorize_movie_{file_id}",
            f"sr_{file_id[:8]}": f"categorize_series_{file_id}", 
            f"st_{file_id[:8]}": f"store_only_{file_id}"
        }
        
        # Store in bot context (temporary solution)
        context.bot_data.setdefault('callbacks', {}).update(callback_map)
        
        keyboard = [
            [
                InlineKeyboardButton("🎬 Movie", callback_data=f"mv_{file_id[:8]}"),
                InlineKeyboardButton("📺 Series", callback_data=f"sr_{file_id[:8]}")
            ],
            [InlineKeyboardButton("📂 Just Store File", callback_data=f"st_{file_id[:8]}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Send success message with categorization options
        success_text = f"""
✅ **File Uploaded Successfully!**

**📄 File Info:**
• Name: `{filename}`
• Size: {file_size / (1024**2):.1f} MB
• Type: {file_type.title()}

**🔗 Stream URL:**
`{stream_url}`

**Next Step:** How would you like to categorize this content?
"""
        
        await processing_msg.edit_text(
            success_text,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Document handler error: {e}")
        await update.message.reply_text("An error occurred while processing your file. Please try again.")

async def handle_categorization(update, context):
    """Handle content categorization callbacks."""
    try:
        query = update.callback_query
        await query.answer()
        
        data = query.data
        user_id = update.effective_user.id
        
        # Get full callback data from stored mapping
        callback_map = context.bot_data.get('callbacks', {})
        full_data = callback_map.get(data, data)
        
        if full_data.startswith("categorize_movie_"):
            file_id = full_data.replace("categorize_movie_", "")
            await start_movie_categorization(query, file_id)
        elif full_data.startswith("categorize_series_"):
            file_id = full_data.replace("categorize_series_", "")
            await start_series_categorization(query, file_id)
        elif full_data.startswith("store_only_"):
            file_id = full_data.replace("store_only_", "")
            await store_file_only(query, file_id)
        else:
            # Fallback for direct handling of short patterns
            if data.startswith("mv_"):
                file_id = await get_file_id_from_short_callback(data, "movie")
                if file_id:
                    await start_movie_categorization(query, file_id)
                else:
                    await query.edit_message_text("Session expired. Please upload the file again.")
            elif data.startswith("sr_"):
                file_id = await get_file_id_from_short_callback(data, "series") 
                if file_id:
                    await start_series_categorization(query, file_id)
                else:
                    await query.edit_message_text("Session expired. Please upload the file again.")
            elif data.startswith("st_"):
                file_id = await get_file_id_from_short_callback(data, "store")
                if file_id:
                    await store_file_only(query, file_id)
                else:
                    await query.edit_message_text("Session expired. Please upload the file again.")
            else:
                await query.edit_message_text("Invalid option selected.")
            
    except Exception as e:
        logger.error(f"Categorization handler error: {e}")
        await query.edit_message_text("An error occurred during categorization.")

async def get_file_id_from_short_callback(short_data, action_type):
    """Get full file_id from short callback data by searching recent uploads."""
    try:
        short_id = short_data.split("_")[1]  # Extract the 8-char file_id prefix
        
        # Search for recent files with matching prefix
        recent_files = app_state['files_collection'].find(
            {'_id': {'$regex': f'^{short_id}'}},
            {'_id': 1}
        ).sort('uploaded_date', -1).limit(5)
        
        for file_doc in recent_files:
            return file_doc['_id']
            
        return None
    except Exception as e:
        logger.error(f"Error getting file_id from short callback: {e}")
        return None

async def start_movie_categorization(query, file_id):
    """Start movie categorization process."""
    try:
        # Get file info
        file_info = app_state['files_collection'].find_one({'_id': file_id})
        if not file_info:
            await query.edit_message_text("File not found.")
            return
        
        filename = file_info['filename']
        
        # Try to extract title from filename
        title = extract_title_from_filename(filename)
        
        # Store initial content record
        domain = get_deployment_domain() or "https://your-app.koyeb.app"
        content_record = {
            '_id': str(ObjectId()),
            'file_id': file_id,
            'type': 'movie',
            'title': title,
            'filename': filename,
            'added_by': query.from_user.id,
            'added_date': datetime.now(),
            'stream_url': f"{domain}/stream/{file_id}",
            'status': 'completed'
        }
        
        app_state['content_collection'].insert_one(content_record)
        
        success_text = f"""
🎬 **Movie Added Successfully!**

**Title:** {title}
**File:** {filename}

The movie has been added to your library and is available for streaming!

Use /library to see all your content or /player to access the web interface.
"""
        
        await query.edit_message_text(success_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Movie categorization error: {e}")
        await query.edit_message_text("An error occurred while adding the movie.")

async def start_series_categorization(query, file_id):
    """Start series categorization process."""
    try:
        # Get file info
        file_info = app_state['files_collection'].find_one({'_id': file_id})
        if not file_info:
            await query.edit_message_text("File not found.")
            return
        
        filename = file_info['filename']
        
        # Try to extract series info from filename
        series_info = extract_series_info_from_filename(filename)
        
        # Store initial content record
        domain = get_deployment_domain() or "https://your-app.koyeb.app"
        content_record = {
            '_id': str(ObjectId()),
            'file_id': file_id,
            'type': 'series',
            'title': series_info['title'],
            'season': series_info['season'],
            'episode': series_info['episode'],
            'filename': filename,
            'added_by': query.from_user.id,
            'added_date': datetime.now(),
            'stream_url': f"{domain}/stream/{file_id}",
            'status': 'completed'
        }
        
        app_state['content_collection'].insert_one(content_record)
        
        success_text = f"""
📺 **Series Episode Added Successfully!**

**Title:** {series_info['title']}
**Season:** {series_info['season']}
**Episode:** {series_info['episode']}
**File:** {filename}

The episode has been added to your library and is available for streaming!

Use /library to see all your content or /player to access the web interface.
"""
        
        await query.edit_message_text(success_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Series categorization error: {e}")
        await query.edit_message_text("An error occurred while adding the series episode.")

async def store_file_only(query, file_id):
    """Store file without categorization."""
    try:
        file_info = app_state['files_collection'].find_one({'_id': file_id})
        if not file_info:
            await query.edit_message_text("File not found.")
            return
        
        filename = file_info['filename']
        domain = get_deployment_domain() or "https://your-app.koyeb.app"
        stream_url = f"{domain}/stream/{file_id}"
        
        success_text = f"""
📂 **File Stored Successfully!**

**File:** {filename}
**Stream URL:** `{stream_url}`

Your file is stored and accessible via the stream URL. You can categorize it later using the web interface.
"""
        
        await query.edit_message_text(success_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Store only error: {e}")
        await query.edit_message_text("An error occurred while storing the file.")

def extract_title_from_filename(filename):
    """Extract movie title from filename."""
    # Remove file extension
    title = filename.rsplit('.', 1)[0] if '.' in filename else filename
    
    # Remove common patterns
    patterns = [
        r'\b\d{4}\b',  # Year
        r'\b(720p|1080p|480p|4K|HD|BluRay|DVDRip|CAMRip|HDTV)\b',  # Quality
        r'\b(x264|x265|H264|H265|HEVC)\b',  # Codecs
        r'\[.*?\]',  # Brackets
        r'\(.*?\)',  # Parentheses
    ]
    
    for pattern in patterns:
        title = re.sub(pattern, '', title, flags=re.IGNORECASE)
    
    # Clean up
    title = re.sub(r'[._-]+', ' ', title)  # Replace dots, underscores, dashes with spaces
    title = re.sub(r'\s+', ' ', title)     # Normalize whitespace
    title = title.strip()
    
    return title or "Untitled Movie"

def extract_series_info_from_filename(filename):
    """Extract series information from filename."""
    # Remove file extension
    name = filename.rsplit('.', 1)[0] if '.' in filename else filename
    
    # Common patterns for series episodes
    patterns = [
        r'(.+?)[.\s_-]+S(\d+)E(\d+)',  # Title.S01E01
        r'(.+?)[.\s_-]+(\d+)x(\d+)',   # Title.1x01
        r'(.+?)[.\s_-]+Season[.\s_-]*(\d+)[.\s_-]+Episode[.\s_-]*(\d+)',  # Title Season 1 Episode 01
    ]
    
    for pattern in patterns:
        match = re.search(pattern, name, re.IGNORECASE)
        if match:
            title = match.group(1)
            season = int(match.group(2))
            episode = int(match.group(3))
            
            # Clean title
            title = re.sub(r'[._-]+', ' ', title)
            title = re.sub(r'\s+', ' ', title)
            title = title.strip()
            
            return {
                'title': title or "Untitled Series",
                'season': season,
                'episode': episode
            }
    
    # If no pattern matches, return defaults
    return {
        'title': name or "Untitled Series",
        'season': 1,
        'episode': 1
    }

async def unknown_command(update, context):
    """Handle unknown commands."""
    await update.message.reply_text(
        "I don't understand that command. Use /start to see available commands."
    )

async def setup_telegram_bot():
    """Initialize and configure the Telegram bot."""
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN environment variable is required")
        return None
    
    try:
        # Create bot application
        app = Application.builder().token(BOT_TOKEN).build()
        
        # Add handlers
        app.add_handler(CommandHandler("start", start_command))
        app.add_handler(CommandHandler("library", library_command))
        app.add_handler(CommandHandler("player", player_command))
        app.add_handler(CommandHandler("stats", stats_command))
        app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
        app.add_handler(CallbackQueryHandler(handle_categorization))
        app.add_handler(MessageHandler(filters.COMMAND, unknown_command))
        
        app_state['bot_app'] = app
        logger.info("✅ Telegram bot initialized successfully!")
        return app
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize Telegram bot: {e}")
        return None

async def setup_webhook():
    """Set up webhook for the Telegram bot."""
    try:
        domain = get_deployment_domain()
        if not domain:
            logger.error("No deployment domain available. Cannot set webhook.")
            return False
        
        webhook_url = f"{domain}{WEBHOOK_PATH}"
        app_state['webhook_url'] = webhook_url
        
        bot = app_state['bot_app'].bot
        
        # Delete existing webhook first
        await bot.delete_webhook()
        await asyncio.sleep(1)
        
        # Set new webhook
        await bot.set_webhook(
            url=webhook_url,
            allowed_updates=["message", "callback_query"]
        )
        
        # Verify webhook was set
        webhook_info = await bot.get_webhook_info()
        if webhook_info.url == webhook_url:
            app_state['webhook_set'] = True
            logger.info(f"✅ Webhook set successfully: {webhook_url}")
            return True
        else:
            logger.error(f"❌ Webhook verification failed. Expected: {webhook_url}, Got: {webhook_info.url}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Failed to set webhook: {e}")
        return False

async def main():
    """Main application entry point."""
    logger.info("🚀 Starting StreamPlayer Bot...")
    
    # Initialize MongoDB
    if not initialize_mongodb():
        logger.error("❌ Failed to initialize MongoDB. Exiting.")
        sys.exit(1)
    
    # Setup Telegram bot
    bot_app = await setup_telegram_bot()
    if not bot_app:
        logger.error("❌ Failed to setup Telegram bot. Exiting.")
        sys.exit(1)
    
    # Initialize bot application
    await bot_app.initialize()
    
    # Setup webhook
    await setup_webhook()
    
    # Configure Hypercorn
    config = HypercornConfig()
    config.bind = [f"0.0.0.0:{PORT}"]
    config.access_log_format = "%(h)s %(r)s %(s)s %(b)s %(D)s"
    config.access_logger = logging.getLogger("hypercorn.access")
    config.error_logger = logging.getLogger("hypercorn.error")
    
    logger.info(f"🌐 Starting web server on port {PORT}")
    logger.info(f"🔗 Webhook path: {WEBHOOK_PATH}")
    logger.info("✅ StreamPlayer Bot is ready!")
    
    # Start the server
    await serve(app, config)

if __name__ == "__main__":
    asyncio.run(main())logging.getLogger('pymongo').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('quart.serving').setLevel(logging.WARNING)

# Configuration with defaults and validation
BOT_TOKEN = os.getenv('BOT_TOKEN')
STORAGE_CHANNEL_ID = os.getenv('STORAGE_CHANNEL_ID')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb+srv://food:food@food.1jskkt3.mongodb.net/?retryWrites=true&w=majority&appName=food')
DB_NAME = os.getenv('MONGO_DB_NAME', 'netflix_bot_db')
PORT = int(os.getenv('PORT', 8080))
MAX_FILE_SIZE = 4 * 1024 * 1024 * 1024  # 4GB

# Telegram API limits
TELEGRAM_FILE_SIZE_LIMIT = 20 * 1024 * 1024  # 20MB - Telegram API limit for get_file

# Webhook configuration
WEBHOOK_PATH = f'/{uuid.uuid4()}'

# Global state
app_state = {
    'mongo_client': None,
    'db': None,
    'files_collection': None,
    'content_collection': None,
    'bot_app': None,
    'webhook_set': False,
    'webhook_url': None
}

# Supported formats
SUPPORTED_VIDEO_FORMATS = {
    'mp4', 'avi', 'mkv', 'mov', 'wmv', 'flv', 'webm', 'm4v',
    'mpg', 'mpeg', 'ogv', '3gp', 'rm', 'rmvb', 'asf', 'divx',
    'ts', 'vob', 'ogg', 'hevc', 'av1', 'vp9', 'h264', 'h265'
}

SUPPORTED_AUDIO_FORMATS = {
    'mp3', 'wav', 'aac', 'flac', 'ogg', 'm4a'
}

def get_deployment_domain():
    """Get the deployment domain from environment variables."""
    domain = (
        os.getenv('KOYEB_PUBLIC_DOMAIN') or
        os.getenv('KOYEB_DOMAIN') or
        os.getenv('PUBLIC_DOMAIN') or
        os.getenv('RAILWAY_STATIC_URL') or
        os.getenv('VERCEL_URL') or
        os.getenv('RENDER_EXTERNAL_URL')
    )
    if not domain:
        logger.warning("No deployment domain found in environment variables. Webhook will likely fail.")
        return None
    if not domain.startswith('http'):
        domain = f"https://{domain}"
    
    return domain.rstrip('/')

def get_file_type(filename):
    """Check if file is a supported media format and return its type."""
    if not filename or '.' not in filename:
        return 'unknown'
    ext = filename.rsplit('.', 1)[1].lower()
    if ext in SUPPORTED_VIDEO_FORMATS:
        return 'video'
    if ext in SUPPORTED_AUDIO_FORMATS:
        return 'audio'
    return 'unknown'

def get_media_mime_type(filename, default='application/octet-stream'):
    """Get MIME type for video or audio file"""
    mime_type, _ = mimetypes.guess_type(filename)
    if mime_type:
        return mime_type
    ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else ''
    mime_map = {
        # Video formats
        'mp4': 'video/mp4', 'avi': 'video/x-msvideo', 'mkv': 'video/x-matroska',
        'mov': 'video/quicktime', 'wmv': 'video/x-ms-wmv', 'flv': 'video/x-flv',
        'webm': 'video/webm', 'm4v': 'video/mp4', 'mpg': 'video/mpeg',
        'mpeg': 'video/mpeg', 'ogv': 'video/ogg', '3gp': 'video/3gpp',
        'ts': 'video/mp2t', 'vob': 'video/dvd', 'ogg': 'video/ogg', 'hevc': 'video/hevc',
        'av1': 'video/av1', 'vp9': 'video/vp9', 'h264': 'video/h264', 'h265': 'video/h265',
        # Audio formats
        'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'aac': 'audio/aac',
        'flac': 'audio/flac', 'm4a': 'audio/mp4'
    }
    return mime_map.get(ext, default)

def initialize_mongodb():
    """Initialize MongoDB connection with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Connecting to MongoDB (attempt {attempt + 1}/{max_retries})")
            client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=10000,
                socketTimeoutMS=10000,
                maxPoolSize=10,
                retryWrites=True
            )
            client.admin.command('ping')
            db = client[DB_NAME]
            files_collection = db['files']
            content_collection = db['content']
            try:
                # Ensure indexes exist
                files_collection.create_index([('user_id', 1)], background=True)
                content_collection.create_index([('added_by', 1), ('type', 1)], background=True)
                content_collection.create_index([('type', 1)], background=True)
            except Exception as e:
                logger.warning(f"Index creation warning: {e}")
            app_state.update({
                'mongo_client': client,
                'db': db,
                'files_collection': files_collection,
                'content_collection': content_collection
            })
            logger.info("✅ MongoDB connected successfully!")
            return True
        except Exception as e:
            logger.error(f"MongoDB connection attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                logger.error("❌ All MongoDB connection attempts failed")
                return False
            time.sleep(2 ** attempt)
    return False

# Quart application
app = Quart(__name__)

# Simple Video Player Frontend
PLAYER_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ title }} - StreamPlayer</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
            background: #000;
            color: white;
            min-height: 100vh;
        }
        .header {
            background: rgba(0,0,0,0.9);
            padding: 1rem 2rem;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }
        .header h1 {
            color: #e50914;
            font-size: 1.5rem;
            font-weight: 700;
        }
        .container {
            max-width: 1280px;
            margin: 0 auto;
            padding: 2rem;
        }
        .video-player {
            width: 100%;
            max-width: 100%;
            background: #000;
            border-radius: 12px;
            overflow: hidden;
            margin-bottom: 2rem;
        }
        video {
            width: 100%;
            height: auto;
            max-height: 70vh;
            display: block;
        }
        .video-info {
            padding: 1.5rem;
            background: rgba(255,255,255,0.05);
            border-radius: 12px;
            margin-bottom: 2rem;
        }
        .video-title {
            font-size: 1.8rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
            color: white;
        }
        .video-meta {
            display: flex;
            gap: 1rem;
            margin-bottom: 1rem;
            flex-wrap: wrap;
        }
        .meta-item {
            background: rgba(255,255,255,0.1);
            padding: 0.3rem 0.8rem;
            border-radius: 20px;
            font-size: 0.9rem;
            color: #ccc;
        }
        .video-description {
            color: #aaa;
            line-height: 1.6;
            margin-bottom: 1.5rem;
        }
        .controls {
            display: flex;
            gap: 1rem;
            flex-wrap: wrap;
        }
        .btn {
            padding: 0.8rem 1.5rem;
            background: linear-gradient(45deg, #e50914, #ff3030);
            color: white;
            text-decoration: none;
            border-radius: 8px;
            font-weight: 600;
            transition: all 0.3s ease;
            border: none;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
        }
        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 25px rgba(229, 9, 20, 0.4);
        }
        .btn-secondary {
            background: rgba(255,255,255,0.1);
            color: white;
        }
        .btn-secondary:hover {
            background: rgba(255,255,255,0.2);
        }
        .library-section {
            margin-top: 3rem;
        }
        .library-title {
            font-size: 1.5rem;
            font-weight: 700;
            color: white;
            margin-bottom: 1rem;
        }
        .content-list {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 1rem;
        }
        .content-item {
            background: rgba(255,255,255,0.05);
            padding: 1rem;
            border-radius: 8px;
            transition: all 0.3s ease;
            cursor: pointer;
            border: 1px solid rgba(255,255,255,0.1);
        }
        .content-item:hover {
            background: rgba(255,255,255,0.1);
            transform: translateY(-2px);
        }
        .content-item h3 {
            color: white;
            margin-bottom: 0.5rem;
            font-size: 1rem;
        }
        .content-item p {
            color: #aaa;
            font-size: 0.9rem;
            margin-bottom: 0.5rem;
        }
        .loading {
            text-align: center;
            padding: 2rem;
            color: #666;
        }
        .error-state {
            text-align: center;
            padding: 2rem;
            color: #666;
        }
        .error-state h2 {
            color: #e50914;
            margin-bottom: 1rem;
        }
        @media (max-width: 768px) {
            .header { padding: 1rem; }
            .container { padding: 1rem; }
            .controls { flex-direction: column; }
            .content-list { grid-template-columns: 1fr; }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🎬 StreamPlayer</h1>
    </div>

    <div class="container">
        {% if video_url %}
        <div class="video-player">
            <video controls preload="metadata" crossorigin="anonymous">
                <source src="{{ video_url }}" type="{{ mime_type }}">
                Your browser does not support the video tag.
            </video>
        </div>
        
        <div class="video-info">
            <h2 class="video-title">{{ title }}</h2>
            <div class="video-meta">
                <span class="meta-item">{{ content_type }}</span>
                {% if year %}<span class="meta-item">{{ year }}</span>{% endif %}
                {% if season and episode %}<span class="meta-item">S{{ season }}E{{ episode }}</span>{% endif %}
                {% if genre %}<span class="meta-item">{{ genre }}</span>{% endif %}
            </div>
            {% if description %}
            <p class="video-description">{{ description }}</p>
            {% endif %}
            <div class="controls">
                <a href="{{ video_url }}" download class="btn">
                    📥 Download
                </a>
                <a href="intent:{{ video_url }}#Intent;package=com.mxtech.videoplayer.ad;end;" class="btn-secondary btn">
                    📱 MX Player
                </a>
                <a href="vlc://{{ video_url }}" class="btn-secondary btn">
                    🎥 VLC Player
                </a>
                <button onclick="copyToClipboard('{{ video_url }}')" class="btn-secondary btn">
                    📋 Copy URL
                </button>
            </div>
        </div>
        {% endif %}

        <div class="library-section">
            <h2 class="library-title">Your Library</h2>
            <div id="content-list" class="content-list">
                <div class="loading">Loading your content...</div>
            </div>
        </div>
    </div>

    <script>
        function copyToClipboard(text) {
            navigator.clipboard.writeText(text).then(() => {
                alert('URL copied to clipboard!');
            }).catch(() => {
                const textArea = document.createElement('textarea');
                textArea.value = text;
                document.body.appendChild(textArea);
                textArea.select();
                document.execCommand('copy');
                document.body.removeChild(textArea);
                alert('URL copied to clipboard!');
            });
        }

        function playVideo(videoUrl, title, contentType, year, season, episode, genre, description) {
            const params = new URLSearchParams({
                url: videoUrl,
                title: title || 'Untitled',
                type: contentType || 'video',
                year: year || '',
                season: season || '',
                episode: episode || '',
                genre: genre || '',
                description: description || ''
            });
            window.location.href = `/play?${params.toString()}`;
        }

        async function loadLibrary() {
            try {
                const response = await fetch('/api/content');
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
                
                const data = await response.json();
                const allContent = [...data.movies, ...data.series];
                const contentList = document.getElementById('content-list');
                
                if (allContent.length === 0) {
                    contentList.innerHTML = `
                        <div class="error-state">
                            <h2>No Content Available</h2>
                            <p>Upload videos via the Telegram bot to start building your library!</p>
                        </div>
                    `;
                    return;
                }

                contentList.innerHTML = allContent.map(item => {
                    const meta = item.type === 'movie' 
                        ? `${item.year || 'Unknown Year'}` 
                        : `Season ${item.season || 'N/A'} • Episode ${item.episode || 'N/A'}`;
                    const genres = Array.isArray(item.genre) ? item.genre.join(', ') : (item.genre || 'Unknown');
                    
                    return `
                        <div class="content-item" onclick="playVideo('${item.stream_url}', '${(item.title || 'Untitled').replace(/'/g, '\\\')}', '${item.type === 'movie' ? 'Movie' : 'Series'}', '${item.year || ''}', '${item.season || ''}', '${item.episode || ''}', '${genres.replace(/'/g, '\\\')}', '${(item.description || '').replace(/'/g, '\\\'')}')">
                            <h3>${item.title || 'Untitled'}</h3>
                            <p>${meta} • ${genres}</p>
                            <p>${item.description ? item.description.substring(0, 100) + '...' : 'No description available'}</p>
                        </div>
                    `;
                }).join('');
            } catch (error) {
                console.error('Failed to load library:', error);
                document.getElementById('content-list').innerHTML = `
                    <div class="error-state">
                        <h2>Connection Error</h2>
                        <p>Unable to load content. Please try again later.</p>
                    </div>
                `;
            }
        }

        // Load library on page load
        loadLibrary();
    </script>
</body>
</html>
"""

# Quart Routes
@app.route('/')
async def serve_library():
    """Serve the library page"""
    return await render_template_string(PLAYER_HTML)

@app.route('/play')
async def play_video():
    """Serve the video player with specific video"""
    video_url = request.args.get('url')
    title = request.args.get('title', 'Untitled')
    content_type = request.args.get('type', 'Video')
    year = request.args.get('year')
    season = request.args.get('season')
    episode = request.args.get('episode')
    genre = request.args.get('genre')
    description = request.args.get('description')
    
    if not video_url:
        return await render_template_string(PLAYER_HTML)
    
    # Get MIME type from URL (extract filename)
    filename = video_url.split('/')[-1] if '/' in video_url else 'video.mp4'
    mime_type = get_media_mime_type(filename, 'video/mp4')
    
    return await render_template_string(PLAYER_HTML, 
                                      video_url=video_url,
                                      title=title,
                                      content_type=content_type,
                                      year=year,
                                      season=season,
                                      episode=episode,
                                      genre=genre,
                                      description=description,
                                      mime_type=mime_type)

@app.route('/health')
async def health_check():
    """Comprehensive health check"""
    health_status = {
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'services': {}
    }
    try:
        if app_state['mongo_client']:
            app_state['mongo_client'].admin.command('ping')
            health_status['services']['mongodb'] = 'ok'
        else:
            health_status['services']['mongodb'] = 'not_connected'
            health_status['status'] = 'degraded'
    except Exception as e:
        health_status['services']['mongodb'] = f'error: {str(e)[:50]}'
        health_status['status'] = 'degraded'
    
    health_status['services']['telegram_bot'] = 'ok' if app_state['bot_app'] else 'not_initialized'
    health_status['services']['webhook'] = 'set' if app_state['webhook_set'] else 'not_set'
    
    return jsonify(health_status), 200 if health_status['status'] == 'ok' else 503

@app.route('/check-webhook')
async def check_webhook_url():
    """Returns the webhook URL being used by the server."""
    if not app_state['webhook_url']:
        return jsonify({
            'status': 'error',
            'message': 'Webhook URL not set yet. Please check server logs.'
        }), 500
    return jsonify({
        'status': 'ok',
        'webhook_url': app_state['webhook_url']
    })

@app.route(WEBHOOK_PATH, methods=['POST'])
async def webhook_handler():
    """Handles incoming Telegram updates from the webhook."""
    if not app_state['bot_app']:
        return jsonify({'error': 'Bot application not initialized'}), 503
    try:
        update = Update.de_json(await request.get_json(), app_state['bot_app'].bot)
        await app_state['bot_app'].process_update(update)
        return jsonify({'status': 'ok'})
    except Exception as e:
        logger.error(f"Error processing webhook update: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/content')
async def get_content_library():
    """Get content library with error handling."""
    try:
        if app_state['content_collection'] is None:
            return jsonify({
                'movies': [],
                'series': [],
                'total_content': 0,
                'error': 'Database not available'
            }), 503
        
        projection = {
            '_id': 0, 'title': 1, 'type': 1, 'year': 1, 'season': 1,
            'episode': 1, 'genre': 1, 'description': 1, 'stream_url': 1
        }
        
        # Only retrieve content that has been fully categorized
        movies = list(app_state['content_collection'].find(
            {'type': 'movie', 'status': 'completed'}, projection
        ).sort('added_date', -1).limit(200))
        
        series = list(app_state['content_collection'].find(
            {'type': 'series', 'status': 'completed'}, projection
        ).sort('added_date', -1).limit(200))
        
        return jsonify({
            'movies': movies,
            'series': series,
            'total_content': len(movies) + len(series),
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in get_content_library: {e}")
        return jsonify({
            'movies': [],
            'series': [],
            'total_content': 0,
            'error': 'Internal server error'
        }), 500

@app.route('/stream/<file_id>')
async def stream_file(file_id):
    """Stream video files with chunked transfer encoding to avoid Content-Length issues."""
    try:
        if app_state['files_collection'] is None:
            abort(503)
        
        # Get file info from database
        file_info = app_state['files_collection'].find_one(
            {'_id': file_id},
            {'filename': 1, 'file_size': 1}
        )
        
        if not file_info:
            abort(404)
        
        filename = file_info['filename']
        file_size = file_info.get('file_size', 0)
        mime_type = get_media_mime_type(filename)
        
        # Try to get Telegram file URL for small files
        telegram_file_url = None
        if file_size <= TELEGRAM_FILE_SIZE_LIMIT:
            try:
                file_obj = await app_state['bot_app'].bot.get_file(file_id)
                telegram_file_url = file_obj.file_path
                logger.info(f"Got Telegram URL for {file_id}: {telegram_file_url}")
            except Exception as e:
                logger.error(f"Error getting Telegram file URL for {file_id}: {e}")
        
        if not telegram_file_url:
            # For large files or if Telegram API fails, return error
            logger.error(f"Cannot stream file {file_id}: No accessible URL")
            abort(404)
        
        # Handle range requests
        range_header = request.headers.get('Range', '').strip()
        
        async def stream_content():
            try:
                headers = {}
                if range_header:
                    headers['Range'] = range_header
                
                async with httpx.AsyncClient(timeout=60.0) as client:
                    async with client.stream("GET", telegram_file_url, headers=headers) as response:
                        response.raise_for_status()
                        async for chunk in response.aiter_bytes(8192):
                            yield chunk
                            
            except Exception as e:
                logger.error(f"Error streaming from Telegram: {e}")
                # Yield empty chunk to close the stream gracefully
                yield b''
        
        # Determine response headers - DON'T set Content-Length to avoid mismatch
        response_headers = {
            'Content-Type': mime_type,
            'Accept-Ranges': 'bytes',
            'Cache-Control': 'public, max-age=3600',
            'Access-Control-Allow-Origin': '*',
            'Transfer-Encoding': 'chunked'  # Use chunked encoding
        }
        
        status_code = 200
        if range_header:
            # For range requests, let the upstream server handle Content-Range
            status_code = 206
        
        return Response(
            stream_content(),
            status=status_code,
            headers=response_headers
        )
        
    except Exception as e:
        logger.error(f"Stream error for {file_id}: {e}")
        abort(500)

# Telegram Bot Handlers
async def start_command(update, context):
    """Start command handler"""
    try:
        domain = get_deployment_domain()
        frontend_url = domain if domain else "https://your-app.koyeb.app"
        welcome_text = f"""
🎬 **StreamPlayer - Simple Video Streaming Bot** 🎬

Welcome to your streaming platform! Upload any video and get instant streaming URLs.

**✨ Features:**
• Direct video streaming in browser
• HTML5 video player with controls
• Mobile & desktop compatible
• MX Player & VLC integration
• Movie & Series categorization
• Download support

**🎯 Commands:**
/start - Welcome message
/library - Browse your content
/player - Access web player
/stats - View library statistics

**📝 File Support:**
• Videos up to 20MB: Direct streaming
• Larger files: Download only
• All major video formats supported

**🚀 Get Started:**
1. Send me a video file
2. I'll categorize it (Movie/Series)  
3. Watch instantly at: {frontend_url}

Ready to start streaming! 🚀
"""
        await update.message.reply_text(welcome_text, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Start command error: {e}")
        await update.message.reply_text("An error occurred while starting the bot. Please try again.")

async def library_command(update, context):
    """Display the content library from the database."""
    try:
        if app_state['content_collection'] is None:
            await update.message.reply_text("Database is not available. Please try again later.")
            return
        
        await update.message.reply_text("Fetching your library... Please wait.")
        
        movies = list(app_state['content_collection'].find({'type': 'movie', 'status': 'completed'}).sort('added_date', -1).limit(10))
        series = list(app_state['content_collection'].find({'type': 'series', 'status': 'completed'}).sort('added_date', -1).limit(10))
        
        if not movies and not series:
            await update.message.reply_text("Your library is empty. Send me a video file to get started!")
            return
        
        message = "🎬 **Your StreamPlayer Library** 🎬\n\n"
        
        if movies:
            message += "**Movies:**\n"
            for m in movies:
                message += f"• **{m.get('title', 'Untitled')}** ({m.get('year', 'N/A')})\n"
        
        if series:
            message += "\n**Series:**\n"
            for s in series:
                message += f"• **{s.get('title', 'Untitled')}** (S{s.get('season', 'N/A')}E{s.get('episode', 'N/A')})\n"
        
        message += "\nTo watch videos, visit the web player."
        await update.message.reply_text(message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Library command error: {e}")
        await update.message.reply_text("An error occurred while fetching your library.")

async def player_command(update, context):
    """Send the user a link to the web player."""
    try:
        domain = get_deployment_domain()
        if not domain:
            await update.message.reply_text("The web player URL is not configured. Please contact the administrator.")
            return
        await update.message.reply_text(f"🔗 **Access your StreamPlayer here:**\n{domain}", parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Player command error: {e}")
        await update.message.reply_text("An error occurred while generating the player link.")

async def stats_command(update, context):
    """Display statistics about the content library."""
    try:
        if app_state['content_collection'] is None or app_state['files_collection'] is None:
            await update.message.reply_text("Database is not available. Please try again later.")
            return
        
        movies_count = app_state['content_collection'].count_documents({'type': 'movie', 'status': 'completed'})
        series_count = app_state['content_collection'].count_documents({'type': 'series', 'status': 'completed'})
        total_files = app_state['files_collection'].count_documents({})
        total_content = movies_count + series_count
        
        # Get storage information
        pipeline = [
            {'$group': {
                '_id': None,
                'total_size': {'$sum': '$file_size'},
                'count': {'$sum': 1}
            }}
        ]
        storage_stats = list(app_state['files_collection'].aggregate(pipeline))
        total_size = storage_stats[0]['total_size'] if storage_stats else 0
        size_gb = total_size / (1024**3)
        
        stats_text = f"""
📊 **StreamPlayer Statistics** 📊

**Content Library:**
🎬 Movies: {movies_count}
📺 Series: {series_count}
📁 Total Content: {total_content}

**Storage:**
📂 Total Files: {total_files}
💾 Storage Used: {size_gb:.2f} GB

**Bot Status:**
✅ Database: Connected
✅ Streaming: Active
✅ Web Player: Available

Use /library to browse your content or /player to access the web interface.
"""
        await update.message.reply_text(stats_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Stats command error: {e}")
        await update.message.reply_text("An error occurred while fetching statistics.")

async def handle_document(update, context):
    """Handle file uploads from users."""
    try:
        user_id = update.effective_user.id
        document = update.message.document
        
        if not document:
            await update.message.reply_text("No file received. Please try again.")
            return
        
        filename = document.file_name or "unknown_file"
        file_size = document.file_size or 0
        file_type = get_file_type(filename)
        
        if file_type == 'unknown':
            await update.message.reply_text(
                "❌ Unsupported file format. Please send video or audio files only.\n\n"
                f"Supported formats: {', '.join(SUPPORTED_VIDEO_FORMATS | SUPPORTED_AUDIO_FORMATS)}"
            )
            return
        
        if file_size > MAX_FILE_SIZE:
            await update.message.reply_text(f"❌ File too large. Maximum size is {MAX_FILE_SIZE / (1024**3):.1f} GB")
            return
        
        # Send processing message
        processing_msg = await update.message.reply_text("🔄 Processing your file...")
        
        # Store file info in database
        file_id = document.file_id
        file_record = {
            '_id': file_id,
            'user_id': user_id,
            'filename': filename,
            'file_size': file_size,
            'file_type': file_type,
            'uploaded_date': datetime.now(),
            'mime_type': get_media_mime_type(filename)
        }
        
        try:
            app_state['files_collection'].insert_one(file_record)
        except pymongo.errors.DuplicateKeyError:
            # File already exists, update the record
            app_state['files_collection'].update_one(
                {'_id': file_id},
                {'$set': file_record}
            )
        
        # Create stream URL
        domain = get_deployment_domain()
        if domain:
            stream_url = f"{domain}/stream/{file_id}"
        else:
            stream_url = f"https://your-app.koyeb.app/stream/{file_id}"
        
        # Create content categorization buttons
        keyboard = [
            [
                InlineKeyboardButton("🎬 Movie", callback_data=f"categorize_movie_{file_id}"),
                InlineKeyboardButton("📺 Series", callback_data=f"categorize_series_{file_id}")
            ],
            [InlineKeyboardButton("📂 Just Store File", callback_data=f"store_only_{file_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Send success message with categorization options
        success_text = f"""
✅ **File Uploaded Successfully!**

**📄 File Info:**
• Name: `{filename}`
• Size: {file_size / (1024**2):.1f} MB
• Type: {file_type.title()}

**🔗 Stream URL:**
`{stream_url}`

**Next Step:** How would you like to categorize this content?
"""
        
        await processing_msg.edit_text(
            success_text,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Document handler error: {e}")
        await update.message.reply_text("An error occurred while processing your file. Please try again.")

async def handle_categorization(update, context):
    """Handle content categorization callbacks."""
    try:
        query = update.callback_query
        await query.answer()
        
        data = query.data
        user_id = update.effective_user.id
        
        if data.startswith("categorize_movie_"):
            file_id = data.replace("categorize_movie_", "")
            await start_movie_categorization(query, file_id)
        elif data.startswith("categorize_series_"):
            file_id = data.replace("categorize_series_", "")
            await start_series_categorization(query, file_id)
        elif data.startswith("store_only_"):
            file_id = data.replace("store_only_", "")
            await store_file_only(query, file_id)
        else:
            await query.edit_message_text("Invalid option selected.")
            
    except Exception as e:
        logger.error(f"Categorization handler error: {e}")
        await query.edit_message_text("An error occurred during categorization.")

async def start_movie_categorization(query, file_id):
    """Start movie categorization process."""
    try:
        # Get file info
        file_info = app_state['files_collection'].find_one({'_id': file_id})
        if not file_info:
            await query.edit_message_text("File not found.")
            return
        
        filename = file_info['filename']
        
        # Try to extract title from filename
        title = extract_title_from_filename(filename)
        
        # Store initial content record
        domain = get_deployment_domain() or "https://your-app.koyeb.app"
        content_record = {
            '_id': str(ObjectId()),
            'file_id': file_id,
            'type': 'movie',
            'title': title,
            'filename': filename,
            'added_by': query.from_user.id,
            'added_date': datetime.now(),
            'stream_url': f"{domain}/stream/{file_id}",
            'status': 'completed'
        }
        
        app_state['content_collection'].insert_one(content_record)
        
        success_text = f"""
🎬 **Movie Added Successfully!**

**Title:** {title}
**File:** {filename}

The movie has been added to your library and is available for streaming!

Use /library to see all your content or /player to access the web interface.
"""
        
        await query.edit_message_text(success_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Movie categorization error: {e}")
        await query.edit_message_text("An error occurred while adding the movie.")

async def start_series_categorization(query, file_id):
    """Start series categorization process."""
    try:
        # Get file info
        file_info = app_state['files_collection'].find_one({'_id': file_id})
        if not file_info:
            await query.edit_message_text("File not found.")
            return
        
        filename = file_info['filename']
        
        # Try to extract series info from filename
        series_info = extract_series_info_from_filename(filename)
        
        # Store initial content record
        domain = get_deployment_domain() or "https://your-app.koyeb.app"
        content_record = {
            '_id': str(ObjectId()),
            'file_id': file_id,
            'type': 'series',
            'title': series_info['title'],
            'season': series_info['season'],
            'episode': series_info['episode'],
            'filename': filename,
            'added_by': query.from_user.id,
            'added_date': datetime.now(),
            'stream_url': f"{domain}/stream/{file_id}",
            'status': 'completed'
        }
        
        app_state['content_collection'].insert_one(content_record)
        
        success_text = f"""
📺 **Series Episode Added Successfully!**

**Title:** {series_info['title']}
**Season:** {series_info['season']}
**Episode:** {series_info['episode']}
**File:** {filename}

The episode has been added to your library and is available for streaming!

Use /library to see all your content or /player to access the web interface.
"""
        
        await query.edit_message_text(success_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Series categorization error: {e}")
        await query.edit_message_text("An error occurred while adding the series episode.")

async def store_file_only(query, file_id):
    """Store file without categorization."""
    try:
        file_info = app_state['files_collection'].find_one({'_id': file_id})
        if not file_info:
            await query.edit_message_text("File not found.")
            return
        
        filename = file_info['filename']
        domain = get_deployment_domain() or "https://your-app.koyeb.app"
        stream_url = f"{domain}/stream/{file_id}"
        
        success_text = f"""
📂 **File Stored Successfully!**

**File:** {filename}
**Stream URL:** `{stream_url}`

Your file is stored and accessible via the stream URL. You can categorize it later using the web interface.
"""
        
        await query.edit_message_text(success_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Store only error: {e}")
        await query.edit_message_text("An error occurred while storing the file.")

def extract_title_from_filename(filename):
    """Extract movie title from filename."""
    # Remove file extension
    title = filename.rsplit('.', 1)[0] if '.' in filename else filename
    
    # Remove common patterns
    patterns = [
        r'\b\d{4}\b',  # Year
        r'\b(720p|1080p|480p|4K|HD|BluRay|DVDRip|CAMRip|HDTV)\b',  # Quality
        r'\b(x264|x265|H264|H265|HEVC)\b',  # Codecs
        r'\[.*?\]',  # Brackets
        r'\(.*?\)',  # Parentheses
    ]
    
    for pattern in patterns:
        title = re.sub(pattern, '', title, flags=re.IGNORECASE)
    
    # Clean up
    title = re.sub(r'[._-]+', ' ', title)  # Replace dots, underscores, dashes with spaces
    title = re.sub(r'\s+', ' ', title)     # Normalize whitespace
    title = title.strip()
    
    return title or "Untitled Movie"

def extract_series_info_from_filename(filename):
    """Extract series information from filename."""
    # Remove file extension
    name = filename.rsplit('.', 1)[0] if '.' in filename else filename
    
    # Common patterns for series episodes
    patterns = [
        r'(.+?)[.\s_-]+S(\d+)E(\d+)',  # Title.S01E01
        r'(.+?)[.\s_-]+(\d+)x(\d+)',   # Title.1x01
        r'(.+?)[.\s_-]+Season[.\s_-]*(\d+)[.\s_-]+Episode[.\s_-]*(\d+)',  # Title Season 1 Episode 01
    ]
    
    for pattern in patterns:
        match = re.search(pattern, name, re.IGNORECASE)
        if match:
            title = match.group(1)
            season = int(match.group(2))
            episode = int(match.group(3))
            
            # Clean title
            title = re.sub(r'[._-]+', ' ', title)
            title = re.sub(r'\s+', ' ', title)
            title = title.strip()
            
            return {
                'title': title or "Untitled Series",
                'season': season,
                'episode': episode
            }
    
    # If no pattern matches, return defaults
    return {
        'title': name or "Untitled Series",
        'season': 1,
        'episode': 1
    }

async def unknown_command(update, context):
    """Handle unknown commands."""
    await update.message.reply_text(
        "I don't understand that command. Use /start to see available commands."
    )

async def setup_telegram_bot():
    """Initialize and configure the Telegram bot."""
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN environment variable is required")
        return None
    
    try:
        # Create bot application
        app = Application.builder().token(BOT_TOKEN).build()
        
        # Add handlers
        app.add_handler(CommandHandler("start", start_command))
        app.add_handler(CommandHandler("library", library_command))
        app.add_handler(CommandHandler("player", player_command))
        app.add_handler(CommandHandler("stats", stats_command))
        app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
        app.add_handler(CallbackQueryHandler(handle_categorization))
        app.add_handler(MessageHandler(filters.COMMAND, unknown_command))
        
        app_state['bot_app'] = app
        logger.info("✅ Telegram bot initialized successfully!")
        return app
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize Telegram bot: {e}")
        return None

async def setup_webhook():
    """Set up webhook for the Telegram bot."""
    try:
        domain = get_deployment_domain()
        if not domain:
            logger.error("No deployment domain available. Cannot set webhook.")
            return False
        
        webhook_url = f"{domain}{WEBHOOK_PATH}"
        app_state['webhook_url'] = webhook_url
        
        bot = app_state['bot_app'].bot
        
        # Delete existing webhook first
        await bot.delete_webhook()
        await asyncio.sleep(1)
        
        # Set new webhook
        await bot.set_webhook(
            url=webhook_url,
            allowed_updates=["message", "callback_query"]
        )
        
        # Verify webhook was set
        webhook_info = await bot.get_webhook_info()
        if webhook_info.url == webhook_url:
            app_state['webhook_set'] = True
            logger.info(f"✅ Webhook set successfully: {webhook_url}")
            return True
        else:
            logger.error(f"❌ Webhook verification failed. Expected: {webhook_url}, Got: {webhook_info.url}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Failed to set webhook: {e}")
        return False

async def main():
    """Main application entry point."""
    logger.info("🚀 Starting StreamPlayer Bot...")
    
    # Initialize MongoDB
    if not initialize_mongodb():
        logger.error("❌ Failed to initialize MongoDB. Exiting.")
        sys.exit(1)
    
    # Setup Telegram bot
    bot_app = await setup_telegram_bot()
    if not bot_app:
        logger.error("❌ Failed to setup Telegram bot. Exiting.")
        sys.exit(1)
    
    # Initialize bot application
    await bot_app.initialize()
    
    # Setup webhook
    await setup_webhook()
    
    # Configure Hypercorn
    config = HypercornConfig()
    config.bind = [f"0.0.0.0:{PORT}"]
    config.access_log_format = "%(h)s %(r)s %(s)s %(b)s %(D)s"
    config.access_logger = logging.getLogger("hypercorn.access")
    config.error_logger = logging.getLogger("hypercorn.error")
    
    logger.info(f"🌐 Starting web server on port {PORT}")
    logger.info(f"🔗 Webhook path: {WEBHOOK_PATH}")
    logger.info("✅ StreamPlayer Bot is ready!")
    
    # Start the server
    await serve(app, config)

if __name__ == "__main__":
    asyncio.run(main())
