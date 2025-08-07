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

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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

# Configuration with defaults
BOT_TOKEN = os.getenv('BOT_TOKEN')
STORAGE_CHANNEL_ID = os.getenv('STORAGE_CHANNEL_ID')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb+srv://food:food@food.1jskkt3.mongodb.net/?retryWrites=true&w=majority&appName=food')
DB_NAME = os.getenv('MONGO_DB_NAME', 'netflix_bot_db')
PORT = int(os.getenv('PORT', 8080))
MAX_FILE_SIZE = 4000 * 1024 * 1024  # 4GB

# Global state
app_state = {
    'mongo_client': None,
    'db': None,
    'files_collection': None,
    'content_collection': None,
    'bot_app': None,
    'shutdown': False
}

# Supported formats
SUPPORTED_VIDEO_FORMATS = {
    'mp4', 'avi', 'mkv', 'mov', 'wmv', 'flv', 'webm', 'm4v',
    'mpg', 'mpeg', 'ogv', '3gp', 'rm', 'rmvb', 'asf', 'divx'
}

def get_koyeb_domain():
    """Get the Koyeb domain from environment"""
    domain = os.getenv('KOYEB_PUBLIC_DOMAIN')
    if not domain:
        domain = os.getenv('KOYEB_DOMAIN') or os.getenv('PUBLIC_DOMAIN')
    
    if not domain:
        logger.warning("No Koyeb domain found in environment variables")
        return None
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

async def initialize_mongodb():
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
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
    
    return False

async def initialize_telegram_bot():
    """Initialize Telegram bot application"""
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

# Modern Netflix-style frontend
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
    domain = get_koyeb_domain()
    frontend_url = f"https://{domain}" if domain else "https://your-app.koyeb.app"
    
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

async def library_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Library command handler"""
    try:
        if not app_state['content_collection']:
            await update.message.reply_text("❌ Database unavailable")
            return
        
        movie_count = app_state['content_collection'].count_documents({'type': 'movie'})
        series_count = app_state['content_collection'].count_documents({'type': 'series'})
        total_count = movie_count + series_count
        
        domain = get_koyeb_domain()
        frontend_url = f"https://{domain}" if domain else "https://your-app.koyeb.app"
        
        library_text = f"""
📚 **Your Library Statistics**

🎬 Movies: {movie_count}
📺 Series: {series_count}
📊 Total Content: {total_count}

🌐 **Access Your Library:**
{frontend_url}

Upload more videos to expand your collection!
"""
        
        await update.message.reply_text(library_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Library command error: {e}")
        await update.message.reply_text("❌ Error retrieving library stats")

async def frontend_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Frontend command handler"""
    domain = get_koyeb_domain()
    frontend_url = f"https://{domain}" if domain else "https://your-app.koyeb.app"
    
    frontend_text = f"""
🌐 **StreamFlix Web Interface**

Access your Netflix-style streaming platform:
{frontend_url}

**Features:**
• Modern Netflix-like design
• Search & filter content
• Mobile optimized
• External player support (MX, VLC)
• Permanent streaming URLs

Enjoy your personal streaming service! 🍿
"""
    
    await update.message.reply_text(frontend_text, parse_mode='Markdown')

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stats command handler"""
    try:
        if not app_state['content_collection']:
            await update.message.reply_text("❌ Database unavailable")
            return
        
        # Aggregate statistics
        pipeline = [
            {"$group": {
                "_id": "$type",
                "count": {"$sum": 1}
            }}
        ]
        
        stats = list(app_state['content_collection'].aggregate(pipeline))
        movie_count = next((s['count'] for s in stats if s['_id'] == 'movie'), 0)
        series_count = next((s['count'] for s in stats if s['_id'] == 'series'), 0)
        
        # Get recent uploads
        recent = list(app_state['content_collection'].find(
            {}, {'title': 1, 'type': 1, 'added_date': 1}
        ).sort('added_date', -1).limit(5))
        
        recent_text = "\n".join([
            f"• {item['title']} ({item['type']})" 
            for item in recent
        ]) if recent else "No recent uploads"
        
        stats_text = f"""
📊 **Detailed Statistics**

**Content Breakdown:**
🎬 Movies: {movie_count}
📺 Series: {series_count}
📈 Total: {movie_count + series_count}

**Storage Info:**
✅ MongoDB Connected
🔗 Streaming URLs Active
"""
        
        await update.message.reply_text(stats_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Stats command error: {e}")
        await update.message.reply_text("❌ Error retrieving statistics")

async def handle_video_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle video file uploads"""
    try:
        user_id = update.effective_user.id
        
        if update.message.video:
            file_obj = update.message.video
            file_size = file_obj.file_size
            filename = file_obj.file_name or f"video_{file_obj.file_unique_id}.mp4"
        elif update.message.document:
            file_obj = update.message.document
            file_size = file_obj.file_size
            filename = file_obj.file_name or f"document_{file_obj.file_unique_id}"
            
            if not is_video_file(filename):
                await update.message.reply_text(
                    "❌ Please send a video file. Supported formats: MP4, AVI, MKV, MOV, etc."
                )
                return
        else:
            await update.message.reply_text("❌ No valid file detected")
            return
        
        if file_size and file_size > MAX_FILE_SIZE:
            await update.message.reply_text(
                f"❌ File too large. Maximum size: {MAX_FILE_SIZE // (1024*1024)}MB"
            )
            return
        
        processing_msg = await update.message.reply_text("🎬 Processing your video...")
        
        file = await context.bot.get_file(file_obj.file_id)
        file_url = file.file_path
        
        file_id = str(uuid.uuid4())
        
        file_doc = {
            '_id': file_id,
            'user_id': user_id,
            'filename': filename,
            'file_size': file_size,
            'file_url': file_url,
            'telegram_file_id': file_obj.file_id,
            'upload_date': datetime.now(),
            'mime_type': get_video_mime_type(filename)
        }
        
        app_state['files_collection'].insert_one(file_doc)
        
        domain = get_koyeb_domain()
        stream_url = f"https://{domain}/stream/{file_id}" if domain else f"https://your-app.koyeb.app/stream/{file_id}"
        
        context.user_data['pending_file'] = {
            'file_id': file_id,
            'filename': filename,
            'stream_url': stream_url,
            'user_id': user_id
        }
        
        keyboard = [
            [InlineKeyboardButton("🎬 Movie", callback_data="type_movie")],
            [InlineKeyboardButton("📺 Series", callback_data="type_series")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await processing_msg.edit_text(
            f"✅ **Video uploaded successfully!**\n\n"
            f"📁 File: {filename}\n"
            f"📏 Size: {file_size/(1024*1024):.1f}MB\n\n"
            f"Please categorize your content:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"Video upload error: {e}")
        await update.message.reply_text("❌ Error processing video. Please try again.")

async def handle_categorization(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle content categorization callbacks"""
    try:
        query = update.callback_query
        await query.answer()
        
        if not context.user_data.get('pending_file'):
            await query.edit_message_text("❌ No pending file found")
            return
        
        file_info = context.user_data['pending_file']
        
        if query.data == "type_movie":
            context.user_data['content_type'] = 'movie'
            await query.edit_message_text(
                "🎬 **Movie Selected**\n\n"
                "Please provide movie details in this format:\n\n"
                "**Title:** Movie Name\n"
                "**Year:** 2024\n"
                "**Genre:** Action, Drama\n"
                "**Description:** Brief description...\n\n"
                "Send the details as a single message:",
                parse_mode='Markdown'
            )
            
        elif query.data == "type_series":
            context.user_data['content_type'] = 'series'
            await query.edit_message_text(
                "📺 **Series Selected**\n\n"
                "Please provide series details in this format:\n\n"
                "**Title:** Series Name\n"
                "**Season:** 1\n"
                "**Episode:** 1\n"
                "**Genre:** Drama, Thriller\n"
                "**Description:** Brief description...\n\n"
                "Send the details as a single message:",
                parse_mode='Markdown'
            )
    
    except Exception as e:
        logger.error(f"Categorization error: {e}")
        await query.edit_message_text("❌ Error processing selection")

async def handle_metadata_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle metadata input from users"""
    try:
        if not context.user_data.get('pending_file') or not context.user_data.get('content_type'):
            return  # Not in metadata input mode
        
        file_info = context.user_data['pending_file']
        content_type = context.user_data['content_type']
        metadata_text = update.message.text
        
        metadata = {}
        lines = metadata_text.strip().split('\n')
        
        for line in lines:
            if ':' in line:
                key, value = line.split(':', 1)
                key = key.strip('* ').lower()
                value = value.strip()
                
                if key in ['title', 'year', 'season', 'episode', 'genre', 'description']:
                    metadata[key] = value
        
        if not metadata.get('title'):
            await update.message.reply_text("❌ Title is required. Please try again.")
            return
        
        content_doc = {
            '_id': str(uuid.uuid4()),
            'file_id': file_info['file_id'],
            'type': content_type,
            'title': metadata.get('title', 'Untitled'),
            'stream_url': file_info['stream_url'],
            'filename': file_info['filename'],
            'added_by': file_info['user_id'],
            'added_date': datetime.now(),
            'description': metadata.get('description', ''),
            'genre': [g.strip() for g in metadata.get('genre', '').split(',')] if metadata.get('genre') else []
        }
        
        if content_type == 'movie':
            content_doc['year'] = metadata.get('year', '')
        elif content_type == 'series':
            content_doc['season'] = metadata.get('season', '')
            content_doc['episode'] = metadata.get('episode', '')
        
        app_state['content_collection'].insert_one(content_doc)
        
        context.user_data.clear()
        
        domain = get_koyeb_domain()
        frontend_url = f"https://{domain}" if domain else "https://your-app.koyeb.app"
        
        success_text = f"""
✅ **Content Added Successfully!**

🎬 **{content_doc['title']}** 📂 Type: {content_type.title()}
🔗 Stream URL: {file_info['stream_url']}

🌐 **Access your library:**
{frontend_url}

Ready for your next upload! 🚀
"""
        
        await update.message.reply_text(success_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Metadata input error: {e}")
        await update.message.reply_text("❌ Error saving content. Please try again.")

def run_flask_app():
    """Start the Flask application"""
    logger.info(f"🌐 Starting Flask server on port {PORT}")
    app.run(
        host='0.0.0.0',
        port=PORT,
        debug=False,
        use_reloader=False
    )

def run_bot_polling():
    """Start the bot with long polling"""
    logger.info("🤖 Starting Telegram bot with long polling...")
    app_state['bot_app'].run_polling(poll_interval=2.0)

async def initialize_application():
    """Initialize the complete application"""
    logger.info("🚀 Starting StreamFlix application...")
    
    try:
        if not await initialize_mongodb():
            logger.error("❌ Failed to initialize MongoDB")
            return False
        
        if not await initialize_telegram_bot():
            logger.error("❌ Failed to initialize Telegram Bot")
            return False
        
        logger.info("✅ Application initialized successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Application initialization failed: {e}")
        return False

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    app_state['shutdown'] = True
    
    if app_state['mongo_client']:
        app_state['mongo_client'].close()
        logger.info("MongoDB connection closed")
    
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN environment variable is required")
        sys.exit(1)
    
    if not STORAGE_CHANNEL_ID:
        logger.warning("⚠️ STORAGE_CHANNEL_ID not set")
    
    asyncio.run(initialize_application())
    
    # Run Flask and the bot in separate threads
    flask_thread = threading.Thread(target=run_flask_app, daemon=True)
    bot_thread = threading.Thread(target=run_bot_polling, daemon=True)

    flask_thread.start()
    bot_thread.start()

    # Keep the main thread alive to handle signals and manage threads
    try:
        while not app_state['shutdown']:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    finally:
        logger.info("Main thread exiting.")

