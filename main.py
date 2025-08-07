import os
import uuid
import asyncio
import mimetypes
import json
import re
import logging
from datetime import datetime
from typing import Dict, List, Optional
import threading
import signal
import sys
from urllib.parse import quote
import time
import atexit

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from telegram.error import TelegramError
from flask import Flask, Response, abort, jsonify, request, render_template_string
import requests
from pymongo import MongoClient
import pymongo.errors

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
logging.getLogger('werkzeug').setLevel(logging.WARNING)

# Configuration with defaults and validation
BOT_TOKEN = os.getenv('BOT_TOKEN')
STORAGE_CHANNEL_ID = os.getenv('STORAGE_CHANNEL_ID')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb+srv://food:food@food.1jskkt3.mongodb.net/?retryWrites=true&w=majority&appName=food')
DB_NAME = os.getenv('MONGO_DB_NAME', 'netflix_bot_db')
PORT = int(os.getenv('PORT', 8080))
MAX_FILE_SIZE = 4000 * 1024 * 1024  # 4GB

# Webhook configuration
WEBHOOK_URL = os.getenv('WEBHOOK_URL')  # Set this to your deployment URL
WEBHOOK_PATH = '/webhook'

# Global state
app_state = {
    'mongo_client': None,
    'db': None,
    'files_collection': None,
    'content_collection': None,
    'bot_app': None,
    'shutdown': False,
    'flask_app': None
}

# Supported formats
SUPPORTED_VIDEO_FORMATS = {
    'mp4', 'avi', 'mkv', 'mov', 'wmv', 'flv', 'webm', 'm4v',
    'mpg', 'mpeg', 'ogv', '3gp', 'rm', 'rmvb', 'asf', 'divx'
}

def get_deployment_domain():
    """Get the deployment domain from environment variables"""
    # Try various common deployment environment variables
    domain = (
        os.getenv('WEBHOOK_URL') or
        os.getenv('KOYEB_PUBLIC_DOMAIN') or
        os.getenv('KOYEB_DOMAIN') or
        os.getenv('PUBLIC_DOMAIN') or
        os.getenv('RAILWAY_STATIC_URL') or
        os.getenv('VERCEL_URL') or
        os.getenv('RENDER_EXTERNAL_URL')
    )

    if not domain:
        logger.warning("No deployment domain found in environment variables")
        return None

    # Ensure domain has https prefix
    if not domain.startswith('http'):
        domain = f"https://{domain}"

    return domain

def is_video_file(filename):
    """Check if file is a supported video format"""
    if not filename or '.' not in filename:
        return False
    return filename.rsplit('.', 1)[1].lower() in SUPPORTED_VIDEO_FORMATS

def get_video_mime_type(filename):
    """Get MIME type for video file"""
    mime_type, _ = mimetypes.guess_type(filename)
    if mime_type and mime_type.startswith('video/'):
        return mime_type

    ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else ''
    mime_map = {
        'mp4': 'video/mp4', 'avi': 'video/x-msvideo', 'mkv': 'video/x-matroska',
        'mov': 'video/quicktime', 'wmv': 'video/x-ms-wmv', 'flv': 'video/x-flv',
        'webm': 'video/webm', 'm4v': 'video/mp4', 'mpg': 'video/mpeg',
        'mpeg': 'video/mpeg', 'ogv': 'video/ogg', '3gp': 'video/3gpp'
    }
    return mime_map.get(ext, 'video/mp4')

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

            # Test connection
            client.admin.command('ping')

            db = client[DB_NAME]
            files_collection = db['files']
            content_collection = db['content']

            # Create indexes
            try:
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
            time.sleep(2 ** attempt)  # Exponential backoff

    return False

async def initialize_telegram_bot():
    """Initialize Telegram bot application with webhook"""
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not provided")
        return False

    try:
        # Create bot application
        bot_app = Application.builder().token(BOT_TOKEN).build()

        # Add handlers
        bot_app.add_handler(CommandHandler("start", start_command))
        bot_app.add_handler(CommandHandler("library", library_command))
        bot_app.add_handler(CommandHandler("frontend", frontend_command))
        bot_app.add_handler(CommandHandler("stats", stats_command))
        bot_app.add_handler(MessageHandler(filters.VIDEO | filters.Document.ALL, handle_video_file))
        bot_app.add_handler(CallbackQueryHandler(handle_categorization))
        bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_metadata_input))

        # Initialize the bot
        await bot_app.initialize()

        # Set webhook if URL is provided
        webhook_url = get_deployment_domain()
        if webhook_url:
            webhook_full_url = f"{webhook_url.rstrip('/')}{WEBHOOK_PATH}"
            logger.info(f"Setting webhook to: {webhook_full_url}")

            try:
                await bot_app.bot.set_webhook(
                    url=webhook_full_url,
                    drop_pending_updates=True,
                    max_connections=10
                )
                logger.info("✅ Webhook set successfully!")
            except Exception as e:
                logger.error(f"Failed to set webhook: {e}")
                return False
        else:
            logger.warning("⚠️ No webhook URL provided, webhook not set")

        app_state['bot_app'] = bot_app
        logger.info("✅ Telegram bot initialized successfully!")
        return True

    except Exception as e:
        logger.error(f"❌ Telegram bot initialization failed: {e}")
        return False

# Flask application
app = Flask(__name__)
app.config.update({
    'JSON_SORT_KEYS': False,
    'JSONIFY_PRETTYPRINT_REGULAR': False
})

# Modern Netflix-style frontend (same as before)
FRONTEND_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>StreamFlix - Your Personal Netflix</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
            background: linear-gradient(135deg, #0f0f0f 0%, #1a1a1a 100%);
            color: white;
            min-height: 100vh;
        }
        .navbar {
            background: rgba(0,0,0,0.9);
            backdrop-filter: blur(10px);
            padding: 1rem 2rem;
            position: fixed;
            top: 0;
            width: 100%;
            z-index: 1000;
            border-bottom: 1px solid rgba(229, 9, 20, 0.3);
        }
        .navbar h1 {
            color: #e50914;
            font-size: 2rem;
            font-weight: 700;
            text-shadow: 0 2px 10px rgba(229, 9, 20, 0.5);
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 100px 2rem 2rem;
        }
        .stats-bar {
            display: flex;
            justify-content: center;
            gap: 3rem;
            margin: 2rem 0;
            padding: 1.5rem;
            background: rgba(255,255,255,0.05);
            border-radius: 15px;
            backdrop-filter: blur(10px);
        }
        .stat-item {
            text-align: center;
            padding: 0.5rem;
        }
        .stat-number {
            font-size: 2rem;
            font-weight: 700;
            color: #e50914;
            display: block;
        }
        .stat-label {
            color: #ccc;
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .search-container {
            margin: 2rem 0;
            position: relative;
        }
        .search-input {
            width: 100%;
            padding: 1rem 1.5rem;
            font-size: 1.1rem;
            background: rgba(255,255,255,0.1);
            border: 2px solid transparent;
            border-radius: 50px;
            color: white;
            backdrop-filter: blur(10px);
            transition: all 0.3s ease;
        }
        .search-input:focus {
            outline: none;
            border-color: #e50914;
            background: rgba(255,255,255,0.15);
            transform: translateY(-2px);
            box-shadow: 0 10px 30px rgba(229, 9, 20, 0.3);
        }
        .search-input::placeholder {
            color: #999;
        }
        .content-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
            gap: 1.5rem;
            margin-top: 2rem;
        }
        .content-card {
            background: linear-gradient(145deg, rgba(255,255,255,0.1) 0%, rgba(255,255,255,0.05) 100%);
            border-radius: 15px;
            padding: 1.5rem;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255,255,255,0.1);
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }
        .content-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: linear-gradient(90deg, #e50914, #ff6b6b);
        }
        .content-card:hover {
            transform: translateY(-5px) scale(1.02);
            box-shadow: 0 20px 40px rgba(0,0,0,0.3);
            border-color: rgba(229, 9, 20, 0.5);
        }
        .content-type {
            display: inline-block;
            padding: 0.3rem 0.8rem;
            background: #e50914;
            color: white;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 600;
            margin-bottom: 1rem;
            text-transform: uppercase;
        }
        .content-title {
            font-size: 1.3rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
            color: white;
        }
        .content-meta {
            color: #ccc;
            margin-bottom: 0.8rem;
            font-size: 0.9rem;
        }
        .content-description {
            color: #aaa;
            line-height: 1.5;
            margin-bottom: 1.5rem;
            display: -webkit-box;
            -webkit-line-clamp: 3;
            -webkit-box-orient: vertical;
            overflow: hidden;
        }
        .player-controls {
            display: flex;
            gap: 0.8rem;
            align-items: center;
        }
        .player-select {
            flex: 1;
            padding: 0.8rem;
            background: rgba(255,255,255,0.1);
            color: white;
            border: 1px solid rgba(255,255,255,0.2);
            border-radius: 8px;
            font-size: 0.9rem;
        }
        .stream-btn {
            padding: 0.8rem 1.5rem;
            background: linear-gradient(45deg, #e50914, #ff3030);
            color: white;
            text-decoration: none;
            border-radius: 8px;
            font-weight: 600;
            transition: all 0.3s ease;
            border: none;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        .stream-btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 25px rgba(229, 9, 20, 0.4);
            background: linear-gradient(45deg, #ff3030, #e50914);
        }
        .loading {
            text-align: center;
            padding: 4rem 2rem;
            color: #666;
            font-size: 1.2rem;
        }
        .loading::before {
            content: '';
            display: inline-block;
            width: 40px;
            height: 40px;
            border: 4px solid #333;
            border-top: 4px solid #e50914;
            border-radius: 50%;
            animation: spin 1s linear infinite;
            margin-bottom: 1rem;
        }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        .empty-state {
            text-align: center;
            padding: 4rem 2rem;
            color: #666;
        }
        .empty-state h2 {
            color: #e50914;
            margin-bottom: 1rem;
            font-size: 2rem;
        }
        @media (max-width: 768px) {
            .navbar { padding: 1rem; }
            .navbar h1 { font-size: 1.5rem; }
            .container { padding: 80px 1rem 1rem; }
            .stats-bar {
                flex-direction: column;
                gap: 1rem;
                text-align: center;
            }
            .content-grid {
                grid-template-columns: 1fr;
                gap: 1rem;
            }
            .player-controls {
                flex-direction: column;
                align-items: stretch;
            }
        }
    </style>
</head>
<body>
    <nav class="navbar">
        <h1>🎬 StreamFlix</h1>
    </nav>

    <div class="container">
        <div class="stats-bar">
            <div class="stat-item">
                <span class="stat-number" id="movies-count">0</span>
                <span class="stat-label">Movies</span>
            </div>
            <div class="stat-item">
                <span class="stat-number" id="series-count">0</span>
                <span class="stat-label">Series</span>
            </div>
            <div class="stat-item">
                <span class="stat-number" id="total-count">0</span>
                <span class="stat-label">Total</span>
            </div>
        </div>

        <div class="search-container">
            <input type="text" class="search-input" id="searchInput" placeholder="🔍 Search your library...">
        </div>

        <div id="content-grid" class="content-grid">
            <div class="loading">Loading your content...</div>
        </div>
    </div>

    <script>
        let allContent = [];

        function updatePlayerLink(selectElement, encodedUrl) {
            const selectedPlayer = selectElement.value;
            const parentDiv = selectElement.closest('.player-controls');
            const streamButton = parentDiv.querySelector('.stream-btn');
            let url = decodeURIComponent(encodedUrl);

            if (selectedPlayer === 'mxplayer') {
                url = `intent:${url}#Intent;package=com.mxtech.videoplayer.ad;end;`;
            } else if (selectedPlayer === 'vlc') {
                url = `vlc://${url}`;
            }

            streamButton.href = url;
        }

        function renderContent(content) {
            const contentGrid = document.getElementById('content-grid');
            contentGrid.innerHTML = '';

            if (content.length === 0) {
                contentGrid.innerHTML = `
                    <div class="empty-state">
                        <h2>No Content Found</h2>
                        <p>Start building your library by uploading videos via the Telegram bot!</p>
                    </div>
                `;
                return;
            }

            content.forEach(item => {
                const card = document.createElement('div');
                card.className = 'content-card';

                const type = item.type === 'movie' ? 'Movie' : 'Series';
                const typeIcon = item.type === 'movie' ? '🎬' : '📺';
                const meta = item.type === 'movie'
                    ? `${item.year || 'Unknown Year'}`
                    : `Season ${item.season || 'N/A'} • Episode ${item.episode || 'N/A'}`;
                const genres = Array.isArray(item.genre) ? item.genre.join(', ') : (item.genre || 'Unknown');
                const encodedUrl = encodeURIComponent(item.stream_url);

                card.innerHTML = `
                    <div class="content-type">${typeIcon} ${type}</div>
                    <h3 class="content-title">${item.title || 'Untitled'}</h3>
                    <p class="content-meta">${meta} • ${genres}</p>
                    <p class="content-description">${item.description || 'No description available.'}</p>
                    <div class="player-controls">
                        <select class="player-select" onchange="updatePlayerLink(this, '${encodedUrl}')">
                            <option value="default">Browser Player</option>
                            <option value="mxplayer">MX Player</option>
                            <option value="vlc">VLC Player</option>
                        </select>
                        <a href="${item.stream_url}" class="stream-btn" target="_blank">
                            ▶️ Play
                        </a>
                    </div>
                `;
                contentGrid.appendChild(card);
            });
        }

        function handleSearch() {
            const searchTerm = document.getElementById('searchInput').value.toLowerCase();
            const filtered = allContent.filter(item => {
                const title = (item.title || '').toLowerCase();
                const description = (item.description || '').toLowerCase();
                const genres = Array.isArray(item.genre)
                    ? item.genre.join(' ').toLowerCase()
                    : (item.genre || '').toLowerCase();

                return title.includes(searchTerm) ||
                       description.includes(searchTerm) ||
                       genres.includes(searchTerm);
            });
            renderContent(filtered);
        }

        async function loadContent() {
            try {
                const response = await fetch('/api/content', {
                    cache: 'no-cache',
                    headers: { 'Cache-Control': 'no-cache' }
                });

                if (!response.ok) throw new Error(`HTTP ${response.status}`);

                const data = await response.json();

                document.getElementById('movies-count').textContent = data.movies.length;
                document.getElementById('series-count').textContent = data.series.length;
                document.getElementById('total-count').textContent = data.total_content;

                allContent = [...data.movies, ...data.series];
                renderContent(allContent);

            } catch (error) {
                console.error('Failed to load content:', error);
                document.getElementById('content-grid').innerHTML = `
                    <div class="empty-state">
                        <h2>Connection Error</h2>
                        <p>Unable to load content. Please check your connection and try again.</p>
                    </div>
                `;
            }
        }

        // Event listeners
        document.getElementById('searchInput').addEventListener('input', handleSearch);

        // Initial load and periodic refresh
        loadContent();
        setInterval(loadContent, 30000);
    </script>
</body>
</html>
"""

# Flask Routes
@app.route('/')
def serve_frontend():
    """Serve the main frontend"""
    return render_template_string(FRONTEND_HTML)

@app.route('/health')
def health_check():
    """Comprehensive health check"""
    health_status = {
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'services': {}
    }

    # Check MongoDB
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

    # Check Bot
    health_status['services']['telegram_bot'] = 'ok' if app_state['bot_app'] else 'not_initialized'

    return jsonify(health_status), 200 if health_status['status'] == 'ok' else 503

@app.route(WEBHOOK_PATH, methods=['POST'])
async def webhook_handler():
    """Handle incoming webhook from Telegram"""
    try:
        if not app_state['bot_app']:
            logger.error("Bot application not initialized")
            return '', 500

        # Get update from request
        update_data = request.get_json(force=True)

        if not update_data:
            logger.warning("Empty webhook update received")
            return '', 400

        # Create Update object
        update = Update.de_json(update_data, app_state['bot_app'].bot)

        if update:
            # Process update asynchronously
            await app_state['bot_app'].process_update(update)
            return '', 200
        else:
            logger.warning("Failed to parse webhook update")
            return '', 400

    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return '', 500

@app.route('/api/content')
def get_content_library():
    """Get content library with error handling"""
    try:
        if not app_state['content_collection']:
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

        movies = list(app_state['content_collection'].find(
            {'type': 'movie'}, projection
        ).sort('added_date', -1).limit(200))

        series = list(app_state['content_collection'].find(
            {'type': 'series'}, projection
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
def stream_file(file_id):
    """Stream video files with range request support"""
    try:
        if not app_state['files_collection']:
            abort(503)

        file_info = app_state['files_collection'].find_one(
            {'_id': file_id},
            {'file_url': 1, 'file_size': 1, 'filename': 1}
        )

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
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    yield chunk
                    except Exception as e:
                        logger.error(f"Range streaming error for {file_id}: {e}")

                return Response(
                    generate_range(),
                    206,
                    {
                        'Content-Type': mime_type,
                        'Accept-Ranges': 'bytes',
                        'Content-Range': f'bytes {start}-{end}/{file_size}',
                        'Content-Length': str(end - start + 1),
                        'Cache-Control': 'public, max-age=3600',
                        'Access-Control-Allow-Origin': '*'
                    }
                )

        def generate_full():
            try:
                with requests.get(file_url, stream=True, timeout=30) as response:
                    response.raise_for_status()
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            yield chunk
            except Exception as e:
                logger.error(f"Full streaming error for {file_id}: {e}")

        return Response(
            generate_full(),
            200,
            {
                'Content-Type': mime_type,
                'Accept-Ranges': 'bytes',
                'Content-Length': str(file_size),
                'Cache-Control': 'public, max-age=3600',
                'Access-Control-Allow-Origin': '*'
            }
        )

    except Exception as e:
        logger.error(f"Stream error for {file_id}: {e}")
        abort(500)

# Telegram Bot Handlers
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler"""
    try:
        domain = get_deployment_domain()
        frontend_url = domain if domain else "https://your-app.herokuapp.com"

        welcome_text = f"""
🎬 **StreamFlix - Your Personal Netflix** 🎬

Welcome to your own streaming platform! Transform any video into a Netflix-style streaming experience.

**✨ Features:**
• Netflix-style interface with modern design
• Mobile & Android TV optimized
• MX Player & VLC integration
• Movie & Series categorization
• Search functionality
• Permanent streaming URLs

**🎯 Commands:**
/start - Welcome message
/library - Browse your content
/frontend - Access web interface
/stats - View library statistics

**🚀 Get Started:**
1. Send me any video file
2. I'll categorize it (Movie/Series)
3. Access your library at: {frontend_url}

Ready to build your streaming empire! 🚀
"""
        await update.message.reply_text(welcome_text, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Start command error: {e}")
        await update.message.reply_text("An error occurred while starting the bot. Please try again.")


async def library_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display the content library from the database."""
    try:
        if not app_state['content_collection']:
            await update.message.reply_text("Database is not available. Please try again later.")
            return

        await update.message.reply_text("Fetching your library... Please wait.")

        # Fetch a limited number of items to avoid overwhelming the chat
        movies = await asyncio.to_thread(
            list,
            app_state['content_collection'].find({'type': 'movie'}).limit(10).sort('added_date', -1)
        )
        series = await asyncio.to_thread(
            list,
            app_state['content_collection'].find({'type': 'series'}).limit(10).sort('added_date', -1)
        )

        if not movies and not series:
            await update.message.reply_text("Your library is empty. Send me a video file to get started!")
            return

        message = "🎬 **Your StreamFlix Library** 🎬\n\n"
        if movies:
            message += "__**Movies:**__\n"
            for m in movies:
                message += f"• **{m.get('title', 'Untitled')}** ({m.get('year', 'N/A')})\n"
        if series:
            message += "\n__**Series:**__\n"
            for s in series:
                message += f"• **{s.get('title', 'Untitled')}** (S{s.get('season', 'N/A')}E{s.get('episode', 'N/A')})\n"

        message += "\nTo see more, visit the web frontend."
        await update.message.reply_text(message, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Library command error: {e}")
        await update.message.reply_text("An error occurred while fetching your library.")


async def frontend_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send the user a link to the web frontend."""
    try:
        domain = get_deployment_domain()
        if not domain:
            await update.message.reply_text("The web frontend URL is not configured. Please contact the administrator.")
            return
        await update.message.reply_text(f"🔗 **Access your StreamFlix library here:**\n{domain}", parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Frontend command error: {e}")
        await update.message.reply_text("An error occurred while generating the frontend link.")


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display statistics about the content library."""
    try:
        if not app_state['content_collection'] or not app_state['files_collection']:
            await update.message.reply_text("Database is not available. Please try again later.")
            return

        movies_count = await asyncio.to_thread(app_state['content_collection'].count_documents, {'type': 'movie'})
        series_count = await asyncio.to_thread(app_state['content_collection'].count_documents, {'type': 'series'})
        total_files_count = await asyncio.to_thread(app_state['files_collection'].count_documents, {})

        message = f"""
📊 **StreamFlix Library Statistics** 📊

• **Movies:** {movies_count}
• **Series:** {series_count}
• **Total Files:** {total_files_count}

Keep uploading content to grow your library!
"""
        await update.message.reply_text(message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Stats command error: {e}")
        await update.message.reply_text("An error occurred while fetching statistics.")


async def handle_video_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

        if file_to_process.file_size > MAX_FILE_SIZE:
            await update.message.reply_text(
                f"File size exceeds the {MAX_FILE_SIZE / (1024**3):.1f} GB limit."
            )
            return

        filename = file_to_process.file_name
        if not is_video_file(filename):
            await update.message.reply_text(
                "This file does not appear to be a supported video format. Please upload a valid video file."
            )
            return

        if not STORAGE_CHANNEL_ID:
            await update.message.reply_text("Storage channel ID is not configured. Please contact the administrator.")
            return

        await update.message.reply_text("Uploading your file and processing it... this might take a moment.")

        # Forward the file to the storage channel
        # Using send_video to ensure it's handled as a video, or send_document if video is not available
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

        # Check if the file was successfully forwarded
        if not storage_message.effective_attachment:
            await update.message.reply_text("An error occurred while uploading the file to the storage channel.")
            return

        # Get the new file_id from the storage channel message
        file_id_in_channel = storage_message.effective_attachment.file_id
        file_url = f"{get_deployment_domain().rstrip('/')}/stream/{file_id_in_channel}"

        # Save file metadata to MongoDB using asyncio.to_thread
        file_doc = {
            '_id': file_id_in_channel,
            'original_file_id': file_to_process.file_id,
            'file_url': file_url,
            'filename': filename,
            'file_size': file_to_process.file_size,
            'user_id': update.message.from_user.id,
            'uploaded_date': datetime.now()
        }
        await asyncio.to_thread(app_state['files_collection'].insert_one, file_doc)

        # Create a new content entry linked to the file
        content_doc = {
            'file_id': file_id_in_channel,
            'stream_url': file_url,
            'added_by': update.message.from_user.id,
            'added_date': datetime.now(),
            'status': 'categorizing'
        }

        result = await asyncio.to_thread(app_state['content_collection'].insert_one, content_doc)
        content_id = str(result.inserted_id)

        # Prompt for categorization with an inline keyboard
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

        # Store the content_id for the next step (metadata input)
        context.user_data['current_content_id'] = content_id

    except Exception as e:
        logger.error(f"Handle video file error: {e}")
        await update.message.reply_text("An unexpected error occurred while processing your file.")


async def handle_categorization(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the inline button callback for content categorization."""
    try:
        query = update.callback_query
        await query.answer()

        data = query.data.split('_')
        category_type = data[1]
        content_id = data[2]

        # Update the content document with the chosen category
        await asyncio.to_thread(
            app_state['content_collection'].update_one,
            {'_id': content_id},
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

        # Store the content_id in a different way to associate the next message
        context.user_data['current_metadata_id'] = content_id

    except Exception as e:
        logger.error(f"Handle categorization error: {e}")
        await query.edit_message_text("An error occurred during categorization. Please try again.")


async def handle_metadata_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text input for metadata and update the content document."""
    try:
        content_id = context.user_data.pop('current_metadata_id', None)
        if not content_id:
            return # Ignore messages not part of the metadata flow

        text = update.message.text
        metadata = {}

        # Use regex to parse the metadata
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

        # Update the content document
        await asyncio.to_thread(
            app_state['content_collection'].update_one,
            {'_id': content_id},
            {'$set': {**metadata, 'status': 'completed'}}
        )

        message = "✅ Metadata saved successfully! Your content is now available in your library."
        await update.message.reply_text(message)

    except Exception as e:
        logger.error(f"Handle metadata input error: {e}")
        await update.message.reply_text("An error occurred while saving the metadata. Please try again.")


def handle_signal(signum, frame):
    """Signal handler for graceful shutdown."""
    logger.info(f"Received signal {signum}. Shutting down gracefully...")
    app_state['shutdown'] = True
    # Stop the Flask development server (if it's running)
    if 'werkzeug' in sys.modules:
        threading.Thread(target=request.environ.get('werkzeug.server.shutdown')).start()


def close_mongodb_connection():
    """Close MongoDB connection on exit."""
    if app_state['mongo_client']:
        logger.info("Closing MongoDB connection.")
        app_state['mongo_client'].close()


def run_flask_app():
    """Run Flask app in a separate thread for local development."""
    app_state['flask_app'] = app
    app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False)


async def main():
    """Main function to initialize and run the application."""
    if not initialize_mongodb():
        logger.error("Failed to connect to MongoDB, exiting.")
        sys.exit(1)

    if not await initialize_telegram_bot():
        logger.error("Failed to initialize Telegram bot, exiting.")
        sys.exit(1)

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Register cleanup function
    atexit.register(close_mongodb_connection)

    # Run the Flask app in a separate thread
    flask_thread = threading.Thread(target=run_flask_app, daemon=True)
    flask_thread.start()

    logger.info("Starting Telegram bot polling loop...")
    await app_state['bot_app'].run_webhook(listen='0.0.0.0', port=PORT, url_path=WEBHOOK_PATH)


if __name__ == '__main__':
    # Wrap the main function in an asyncio event loop
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped by user.")
    finally:
        if app_state['mongo_client']:
            close_mongodb_connection()
        logger.info("Shutdown complete.")

