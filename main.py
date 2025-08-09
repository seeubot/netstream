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

# Video Player Frontend
PLAYER_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ title }} - Video Player</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
            background: #000;
            color: white;
            min-height: 100vh;
            display: flex;
            flex-direction: column;
        }
        .header {
            background: rgba(0,0,0,0.9);
            backdrop-filter: blur(10px);
            padding: 1rem 2rem;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }
        .header h1 {
            color: #e50914;
            font-size: 1.5rem;
            font-weight: 700;
        }
        .video-container {
            flex: 1;
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 2rem;
            background: radial-gradient(circle at center, #1a1a1a 0%, #000 100%);
        }
        .video-player {
            width: 100%;
            max-width: 1280px;
            background: #000;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 20px 60px rgba(0,0,0,0.5);
        }
        video {
            width: 100%;
            height: auto;
            max-height: 80vh;
            display: block;
        }
        .video-info {
            padding: 1.5rem;
            background: linear-gradient(135deg, rgba(229, 9, 20, 0.1) 0%, rgba(0,0,0,0.3) 100%);
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
            background: linear-gradient(45deg, #ff3030, #e50914);
        }
        .btn-secondary {
            background: rgba(255,255,255,0.1);
            color: white;
        }
        .btn-secondary:hover {
            background: rgba(255,255,255,0.2);
            box-shadow: 0 8px 25px rgba(255,255,255,0.1);
        }
        .library-grid {
            max-width: 1280px;
            margin: 2rem auto;
            padding: 0 2rem;
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
            display: flex;
            justify-content: center;
            align-items: center;
            height: 200px;
            color: #666;
        }
        .error-state {
            text-align: center;
            padding: 4rem 2rem;
            color: #666;
        }
        .error-state h2 {
            color: #e50914;
            margin-bottom: 1rem;
        }
        @media (max-width: 768px) {
            .header { padding: 1rem; }
            .video-container { padding: 1rem; }
            .video-info { padding: 1rem; }
            .video-title { font-size: 1.3rem; }
            .controls { flex-direction: column; }
            .content-list { grid-template-columns: 1fr; }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🎬 StreamPlayer</h1>
    </div>

    {% if video_url %}
    <div class="video-container">
        <div class="video-player">
            <video controls autoplay preload="metadata">
                <source src="{{ video_url }}" type="{{ mime_type }}">
                Your browser does not support the video tag.
            </video>
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
        </div>
    </div>
    {% endif %}

    <div class="library-grid">
        <h2 class="library-title">Your Library</h2>
        <div id="content-list" class="content-list">
            <div class="loading">Loading your content...</div>
        </div>
    </div>

    <script>
        function copyToClipboard(text) {
            navigator.clipboard.writeText(text).then(() => {
                alert('URL copied to clipboard!');
            }).catch(() => {
                // Fallback for older browsers
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
                        <div class="content-item" onclick="playVideo('${item.stream_url}', '${item.title || 'Untitled'}', '${item.type === 'movie' ? 'Movie' : 'Series'}', '${item.year || ''}', '${item.season || ''}', '${item.episode || ''}', '${genres}', '${item.description || ''}')">
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
    """Stream video files with proper Telegram API integration."""
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
        
        # Get file URL from Telegram
        try:
            file_obj = await app_state['bot_app'].bot.get_file(file_id)
            telegram_file_url = file_obj.file_path
        except Exception as e:
            logger.error(f"Error getting Telegram file URL for {file_id}: {e}")
            abort(404)
        
        # Handle range requests for video streaming
        range_header = request.headers.get('Range', '').strip()
        
        if range_header:
            range_match = re.search(r'bytes=(\d+)-(\d*)', range_header)
            if range_match:
                start = int(range_match.group(1))
                end = int(range_match.group(2)) if range_match.group(2) else file_size - 1
                
                if file_size > 0:
                    start = max(0, min(start, file_size - 1))
                    end = max(start, min(end, file_size - 1))
                
                async def generate_range():
                    try:
                        headers = {'Range': f'bytes={start}-{end}'} if file_size > 0 else {}
                        async with httpx.AsyncClient() as client:
                            async with client.stream("GET", telegram_file_url, headers=headers) as response:
                                response.raise_for_status()
                                async for chunk in response.aiter_bytes(8192):
                                    yield chunk
                    except Exception as e:
                        logger.error(f"Range streaming error for {file_id}: {e}")
                
                response_headers = {
                    'Content-Type': mime_type,
                    'Accept-Ranges': 'bytes',
                    'Cache-Control': 'public, max-age=3600',
                    'Access-Control-Allow-Origin': '*'
                }
                
                if file_size > 0:
                    response_headers.update({
                        'Content-Range': f'bytes {start}-{end}/{file_size}',
                        'Content-Length': str(end - start + 1)
                    })
                
                return Response(
                    generate_range(),
                    status=206,
                    headers=response_headers
                )
        
        # Full file streaming
        async def generate_full():
            try:
                async with httpx.AsyncClient() as client:
                    async with client.stream("GET", telegram_file_url) as response:
                        response.raise_for_status()
                        async for chunk in response.aiter_bytes(8192):
                            yield chunk
            except Exception as e:
                logger.error(f"Full streaming error for {file_id}: {e}")
        
        response_headers = {
            'Content-Type': mime_type,
            'Accept-Ranges': 'bytes',
            'Cache-Control': 'public, max-age=3600',
            'Access-Control-Allow-Origin': '*'
        }
        
        if file_size > 0:
            response_headers['Content-Length'] = str(file_size)
        
        return Response(
            generate_full(),
            status=200,
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
🎬 **StreamPlayer - Your Personal Video Streaming Bot** 🎬

Welcome to your streaming platform! Upload any video and get instant streaming URLs.

**✨ Features:**
• Direct video streaming in browser
• Mobile & Android TV compatible
• MX Player & VLC integration
• Movie & Series categorization
• Instant playback URLs

**🎯 Commands:**
/start - Welcome message
/library - Browse your content
/player - Access web player
/stats - View library statistics

**🚀 Get Started:**
1. Send me any video file
2. I'll process and categorize it
3. Access your player at: {frontend_url}

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
        total_files_count = app_state['files_collection'].count_documents({})
        
        message = f"""
📊 **StreamPlayer Library Statistics** 📊

• **Movies:** {movies_count}
• **Series:** {series_count}  
• **Total Files:** {total_files_count}

Keep uploading content to grow your library!
"""
        await update.message.reply_text(message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Stats command error: {e}")
        await update.message.reply_text("An error occurred while fetching statistics.")

async def handle_video_file(update, context):
    """Handle incoming video or document files, save to DB, and prompt for categorization."""
    try:
        file_to_process = None
        if update.message.video:
            file_to_process = update.message.video
        elif update.message.document:
            file_to_process = update.message.document
        else:
            await update.message.reply_text("Please send a video file or a document containing a video.")
            return

        filename = file_to_process.file_name
        file_type = get_file_type(filename)
        
        if file_type == 'unknown':
            await update.message.reply_text(
                "This file does not appear to be a supported video or audio format. Please upload a valid media file."
            )
            return

        if file_to_process.file_size > MAX_FILE_SIZE:
            await update.message.reply_text(
                f"File size exceeds the {MAX_FILE_SIZE / (1024**3):.1f} GB limit."
            )
            return
        
        if not STORAGE_CHANNEL_ID:
            await update.message.reply_text("Storage channel ID is not configured. Please contact the administrator.")
            return

        await update.message.reply_text("Uploading your file and processing it... this might take a moment.")
        
        # Upload to the storage channel
        if update.message.video:
            storage_message = await context.bot.send_video(
                chat_id=STORAGE_CHANNEL_ID,
                video=file_to_process.file_id,
                caption=f"Stored by {update.message.from_user.username or update.message.from_user.full_name}"
            )
        else:
            storage_message = await context.bot.send_document(
                chat_id=STORAGE_CHANNEL_ID,
                document=file_to_process.file_id,
                caption=f"Stored by {update.message.from_user.username or update.message.from_user.full_name}"
            )

        if not storage_message.effective_attachment:
            await update.message.reply_text("An error occurred while uploading the file to the storage channel.")
            return

        file_id_in_channel = storage_message.effective_attachment.file_id
        domain = get_deployment_domain()
        
        if not domain:
            await update.message.reply_text("Frontend URL is not configured. Cannot generate a stream link.")
            return

        file_url = f"{domain.rstrip('/')}/stream/{file_id_in_channel}"

        file_doc = {
            '_id': file_id_in_channel,
            'original_file_id': file_to_process.file_id,
            'file_url': file_url,
            'filename': filename,
            'file_size': file_to_process.file_size,
            'user_id': update.message.from_user.id,
            'uploaded_date': datetime.now()
        }

        # Fix: Use upsert to prevent duplicate key errors
        app_state['files_collection'].update_one(
            {'_id': file_id_in_channel},
            {'$set': file_doc},
            upsert=True
        )

        content_doc_query = {'file_id': file_id_in_channel, 'added_by': update.message.from_user.id}
        existing_content = app_state['content_collection'].find_one(content_doc_query)
        
        if existing_content:
            content_id = str(existing_content['_id'])
            await update.message.reply_text("This file has already been processed. It is available in your library.")
            return

        content_doc = {
            'file_id': file_id_in_channel,
            'stream_url': file_url,
            'added_by': update.message.from_user.id,
            'added_date': datetime.now(),
            'status': 'categorizing'
        }
        
        result = app_state['content_collection'].insert_one(content_doc)
        content_id = str(result.inserted_id)

        keyboard = [
            [
                InlineKeyboardButton("🎬 Movie", callback_data=f"categorize_movie_{content_id}"),
                InlineKeyboardButton("📺 Series", callback_data=f"categorize_series_{content_id}")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            "✅ File uploaded! Now, please categorize this content:",
            reply_markup=reply_markup
        )
        context.user_data['current_metadata_id'] = content_id
    except Exception as e:
        logger.error(f"Handle video file error: {e}")
        await update.message.reply_text("An unexpected error occurred while processing your file.")

async def handle_categorization(update, context):
    """Handle the inline button callback for content categorization."""
    try:
        query = update.callback_query
        await query.answer()
        data = query.data.split('_')
        category_type = data[1]
        content_id = data[2]

        app_state['content_collection'].update_one(
            {'_id': ObjectId(content_id)},
            {'$set': {'type': category_type, 'status': 'metadata_pending'}}
        )

        message = "✅ Content categorized! Now, please provide the metadata.\n"
        if category_type == 'movie':
            message += "Send me the **title**, **year**, **genre**, and **description** in this format:\n\n"
            message += "`Title: My Awesome Movie\nYear: 2023\nGenre: Action, Sci-Fi\nDescription: A great description of the movie.`"
        else:
            message += "Send me the **title**, **season**, **episode**, **genre**, and **description** in this format:\n\n"
            message += "`Title: My Awesome Series\nSeason: 1\nEpisode: 5\nGenre: Drama\nDescription: A description for this episode.`"
        
        await query.edit_message_text(message, parse_mode='Markdown')
        context.user_data['current_metadata_id'] = content_id
    except Exception as e:
        logger.error(f"Handle categorization error: {e}")
        await query.edit_message_text("An error occurred during categorization. Please try again.")

async def handle_metadata_input(update, context):
    """Handle text input for metadata and update the content document."""
    try:
        content_id = context.user_data.pop('current_metadata_id', None)
        if not content_id:
            # If there's no pending metadata, just ignore the message
            return
        
        text = update.message.text
        metadata = {}
        title_match = re.search(r'Title: (.+)', text, re.IGNORECASE)
        year_match = re.search(r'Year: (\d{4})', text, re.IGNORECASE)
        season_match = re.search(r'Season: (\d+)', text, re.IGNORECASE)
        episode_match = re.search(r'Episode: (\d+)', text, re.IGNORECASE)
        genre_match = re.search(r'Genre: (.+)', text, re.IGNORECASE)
        desc_match = re.search(r'Description: (.+)', text, re.IGNORECASE)

        if title_match:
            metadata['title'] = title_match.group(1).strip()
        if year_match:
            metadata['year'] = int(year_match.group(1))
        if season_match:
            metadata['season'] = int(season_match.group(1))
        if episode_match:
            metadata['episode'] = int(episode_match.group(1))
        if genre_match:
            genres = [g.strip() for g in genre_match.group(1).split(',')]
            metadata['genre'] = genres
        if desc_match:
            metadata['description'] = desc_match.group(1).strip()
        
        app_state['content_collection'].update_one(
            {'_id': ObjectId(content_id)},
            {'$set': {**metadata, 'status': 'completed'}}
        )

        # Get the updated content for the streaming link
        content = app_state['content_collection'].find_one({'_id': ObjectId(content_id)})
        if content:
            domain = get_deployment_domain()
            play_url = f"{domain}/play?url={content['stream_url']}&title={metadata.get('title', 'Untitled')}&type={content['type']}"
            
            message = f"✅ Metadata saved successfully! Your content is now available.\n\n"
            message += f"🎬 **{metadata.get('title', 'Untitled')}**\n"
            message += f"▶️ Watch now: {play_url}\n\n"
            message += f"📱 Direct streaming link: {content['stream_url']}"
            
            await update.message.reply_text(message, parse_mode='Markdown')
        else:
            await update.message.reply_text("✅ Metadata saved successfully! Your content is now available in your library.")
            
    except Exception as e:
        logger.error(f"Handle metadata input error: {e}")
        await update.message.reply_text("An error occurred while saving the metadata. Please try again.")

def initialize_telegram_bot_app():
    """Initialize Telegram bot application with handlers"""
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not provided")
        return None
    try:
        bot_app = Application.builder().token(BOT_TOKEN).build()
        bot_app.add_handler(CommandHandler("start", start_command))
        bot_app.add_handler(CommandHandler("library", library_command))
        bot_app.add_handler(CommandHandler("player", player_command))
        bot_app.add_handler(CommandHandler("stats", stats_command))
        bot_app.add_handler(MessageHandler(filters.VIDEO | filters.Document.ALL, handle_video_file))
        bot_app.add_handler(CallbackQueryHandler(handle_categorization))
        bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_metadata_input))
        logger.info("✅ Telegram bot initialized successfully!")
        return bot_app
    except Exception as e:
        logger.error(f"❌ Telegram bot initialization failed: {e}")
        return None

def set_webhook_sync():
    """Set webhook synchronously using requests with retry logic."""
    domain = get_deployment_domain()
    if not domain:
        logger.error("❌ Cannot set webhook: Deployment domain not found.")
        return

    webhook_url = f"{domain}{WEBHOOK_PATH}"
    app_state['webhook_url'] = webhook_url
    logger.info(f"Setting webhook to: {webhook_url}")

    max_retries = 5
    initial_delay = 2  # seconds

    for attempt in range(max_retries):
        try:
            telegram_api_url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook"
            response = requests.post(
                telegram_api_url,
                json={'url': webhook_url},
                timeout=30
            )
            if response.status_code == 200:
                result = response.json()
                if result.get('ok'):
                    app_state['webhook_set'] = True
                    logger.info("✅ Webhook set successfully!")
                    return  # Exit on success
                else:
                    logger.error(f"Failed to set webhook on attempt {attempt + 1}: {result.get('description')}")
            else:
                logger.error(f"Failed to set webhook on attempt {attempt + 1}: HTTP {response.status_code}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Connection error on webhook setup (attempt {attempt + 1}): {e}")
        if attempt < max_retries - 1:
            delay = initial_delay * (2 ** attempt)
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
    logger.error("❌ Failed to set webhook after multiple retries. Bot will not receive updates.")

async def main():
    """Main asynchronous function to initialize and run the application."""
    if not initialize_mongodb():
        logger.error("Failed to connect to MongoDB, exiting.")
        sys.exit(1)

    app_state['bot_app'] = initialize_telegram_bot_app()
    if not app_state['bot_app']:
        logger.error("Failed to initialize Telegram bot, exiting.")
        sys.exit(1)
    
    # Initialize the Application instance
    await app_state['bot_app'].initialize()

    # Set the webhook before starting the server
    set_webhook_sync()

    # Create a Hypercorn configuration object
    config = HypercornConfig()
    config.bind = [f"0.0.0.0:{PORT}"]

    logger.info("Starting Quart application with Hypercorn...")
    await serve(app, config)

if __name__ == '__main__':
    # Use asyncio.run to start the async main function
    asyncio.run(main())
