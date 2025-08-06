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
from flask import Flask, Response, abort, jsonify, request, render_template
import threading
import aiohttp

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
MAX_FILE_SIZE = 2000 * 1024 * 1024  # 2GB limit

# Supported video formats
SUPPORTED_VIDEO_FORMATS = {
    'mp4', 'avi', 'mkv', 'mov', 'wmv', 'flv', 'webm', 'm4v', 
    'mpg', 'mpeg', 'ogv', '3gp', 'rm', 'rmvb', 'asf', 'divx'
}

# Flask app for serving files
flask_app = Flask(__name__, template_folder='templates')

# Store file metadata with enhanced structure
file_registry = {}
content_library = {
    'movies': {},
    'series': {},
    'categories': set()
}

# Global bot instance
bot_app = None

class VideoMetadata:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.metadata = {}
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
        except Exception as e:
            logger.error(f"Error extracting metadata: {e}")
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
            return None
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
        except KeyError:
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
        except KeyError:
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

async def download_telegram_file(file_url, start_byte=0, end_byte=None):
    """Download file from Telegram with range support"""
    headers = {}
    if end_byte:
        headers['Range'] = f'bytes={start_byte}-{end_byte}'
    elif start_byte > 0:
        headers['Range'] = f'bytes={start_byte}-'
    
    async with aiohttp.ClientSession() as session:
        async with session.get(file_url, headers=headers) as response:
            if response.status in [200, 206]:
                return response.content, response.status, dict(response.headers)
            return None, response.status, {}

# Flask Routes
@flask_app.route('/') # New route to serve the frontend
def serve_frontend():
    """Serve the main frontend HTML page."""
    return render_template('index.html')

@flask_app.route('/stream/<file_id>')
def stream_file(file_id):
    """Stream video file with support for range requests and audio track selection"""
    if file_id not in file_registry:
        abort(404)
    
    file_info = file_registry[file_id]
    file_url = file_info['file_url']
    file_size = file_info['file_size']
    filename = file_info['filename']
    mime_type = get_video_mime_type(filename)
    
    # Get audio track parameter
    audio_track = request.args.get('audio', '0')
    quality = request.args.get('quality', 'original')
    
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
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                content, status, headers = loop.run_until_complete(
                    download_telegram_file(file_url, start, end)
                )
                if content:
                    async def get_all_chunks():
                        chunks = []
                        async for chunk in content.iter_chunked(8192):
                            chunks.append(chunk)
                        return b''.join(chunks)
                    
                    data = loop.run_until_complete(get_all_chunks())
                    for i in range(0, len(data), 8192):
                        yield data[i:i+8192]
                        
            except Exception as e:
                logger.error(f"Error streaming range: {e}")
                return
        
        response = Response(
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
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                content, status, headers = loop.run_until_complete(
                    download_telegram_file(file_url)
                )
                if content:
                    async def get_all_chunks():
                        chunks = []
                        async for chunk in content.iter_chunked(8192):
                            chunks.append(chunk)
                        return b''.join(chunks)
                    
                    data = loop.run_until_complete(get_all_chunks())
                    for i in range(0, len(data), 8192):
                        yield data[i:i+8192]
                        
            except Exception as e:
                logger.error(f"Error streaming full file: {e}")
                return
        
        response = Response(
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
    
    return response

@flask_app.route('/api/content')
def get_content_library():
    """Get content library for frontend"""
    return jsonify({
        'movies': list(content_library['movies'].values()),
        'series': list(content_library['series'].values()),
        'categories': list(content_library['categories']),
        'total_content': len(content_library['movies']) + len(content_library['series'])
    })

@flask_app.route('/api/content/<content_type>')
def get_content_by_type(content_type):
    """Get content by type (movies/series)"""
    if content_type not in ['movies', 'series']:
        abort(404)
    
    category = request.args.get('category')
    search = request.args.get('search', '').lower()
    
    content_list = list(content_library[content_type].values())
    
    if category:
        content_list = [c for c in content_list if category in c.get('categories', [])]
    
    if search:
        content_list = [c for c in content_list if search in c.get('title', '').lower() or 
                       search in c.get('description', '').lower()]
    
    return jsonify({
        'content': content_list,
        'total': len(content_list)
    })

@flask_app.route('/api/content/item/<content_id>')
def get_content_item(content_id):
    """Get specific content item details"""
    # Search in both movies and series
    for content_type in ['movies', 'series']:
        if content_id in content_library[content_type]:
            return jsonify(content_library[content_type][content_id])
    
    abort(404)

@flask_app.route('/info/<file_id>')
def file_info_endpoint(file_id):
    """Get file information as JSON"""
    if file_id not in file_registry:
        abort(404)
    
    file_data = file_registry[file_id]
    domain = os.getenv('KOYEB_PUBLIC_DOMAIN', 'your-app.koyeb.app')
    
    return jsonify({
        'file_id': file_id,
        'filename': file_data['filename'],
        'file_size': file_data['file_size'],
        'mime_type': get_video_mime_type(file_data['filename']),
        'stream_url': f"https://{domain}/stream/{file_id}",
        'audio_tracks': file_data.get('audio_tracks', []),
        'subtitle_tracks': file_data.get('subtitle_tracks', []),
        'resolution': file_data.get('resolution'),
        'duration': file_data.get('duration'),
        'telegram_message_id': file_data['message_id'],
        'uploaded_by': file_data['user_id'],
        'content_metadata': file_data.get('content_metadata', {})
    })

@flask_app.route('/health')
def health_check():
    return jsonify({
        'status': 'ok', 
        'videos_stored': len(file_registry),
        'movies': len(content_library['movies']),
        'series': len(content_library['series']),
        'storage_channel': STORAGE_CHANNEL_ID
    })

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

**Supported formats:**
MP4, AVI, MKV, MOV, WMV, FLV, WebM, M4V, MPG, MPEG, OGV, 3GP, etc.

**Commands:**
/upload - Upload and categorize content
/library - View your content library
/frontend - Get frontend app link

Just send me a video file to get started! 🚀
    """
    await update.message.reply_text(welcome_message, parse_mode='Markdown')

async def handle_video_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle video file uploads with metadata extraction"""
    # Handle both video and document types
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
        
        # Forward the video to storage channel
        if not STORAGE_CHANNEL_ID:
            await processing_msg.edit_text("❌ Storage channel not configured!")
            return
        
        forwarded_msg = await context.bot.forward_message(
            chat_id=STORAGE_CHANNEL_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id
        )
        
        # Get file URL
        file_obj = video if video else document
        file = await file_obj.get_file()
        file_url = file.file_path
        
        # Generate unique file ID
        file_id = str(uuid.uuid4())
        
        # Create temporary file for metadata extraction
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.tmp') as temp_file:
                # Download a small portion for metadata extraction
                async with aiohttp.ClientSession() as session:
                    async with session.get(file_url, headers={'Range': 'bytes=0-10485760'}) as response:  # First 10MB
                        if response.status in [200, 206]:
                            async for chunk in response.content.iter_chunked(8192):
                                temp_file.write(chunk)
                                break  # Just need a small sample
                
                # Extract metadata
                video_metadata = VideoMetadata(temp_file.name)
                
        except Exception as e:
            logger.error(f"Error extracting metadata: {e}")
            video_metadata = VideoMetadata("")  # Empty metadata
        finally:
            try:
                os.unlink(temp_file.name)
            except:
                pass
        
        # Store file info with metadata
        file_registry[file_id] = {
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
        
        # Generate URLs
        domain = os.getenv('KOYEB_PUBLIC_DOMAIN', 'your-app.koyeb.app')
        stream_url = f"https://{domain}/stream/{file_id}"
        
        # Create inline keyboard for content categorization
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
        if file_id in file_registry:
            domain = os.getenv('KOYEB_PUBLIC_DOMAIN', 'your-app.koyeb.app')
            stream_url = f"https://{domain}/stream/{file_id}"
            
            await query.edit_message_text(
                f"🔗 **Streaming URL Generated**\n\n"
                f"`{stream_url}`\n\n"
                f"🎮 **Frontend App:** {FRONTEND_URL}\n\n"
                f"Use this URL in any video player or our Netflix-style frontend!",
                parse_mode='Markdown'
            )

async def handle_metadata_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle metadata input for content categorization"""
    if 'categorizing' not in context.user_data:
        return
    
    categorizing = context.user_data['categorizing']
    file_id = categorizing['file_id']
    content_type = categorizing['type']
    
    if file_id not in file_registry:
        await update.message.reply_text("❌ File not found!")
        return
    
    try:
        metadata_text = update.message.text.strip()
        parts = [part.strip() for part in metadata_text.split('|')]
        
        if content_type == 'movie' and len(parts) >= 4:
            title, year, genre, description = parts[:4]
            content_id = str(uuid.uuid4())
            
            movie_data = {
                'content_id': content_id,
                'file_id': file_id,
                'title': title,
                'year': int(year) if year.isdigit() else None,
                'genre': [g.strip() for g in genre.split(',')],
                'description': description,
                'type': 'movie',
                'stream_url': f"https://{os.getenv('KOYEB_PUBLIC_DOMAIN', 'your-app.koyeb.app')}/stream/{file_id}",
                'file_info': file_registry[file_id],
                'added_date': datetime.now().isoformat(),
                'added_by': update.effective_user.id
            }
            
            content_library['movies'][content_id] = movie_data
            content_library['categories'].update(movie_data['genre'])
            
            # Update file registry with content metadata
            file_registry[file_id]['content_metadata'] = movie_data
            
            await update.message.reply_text(
                f"✅ **Movie Added Successfully!**\n\n"
                f"🎬 **Title:** {title}\n"
                f"📅 **Year:** {year}\n"
                f"🎭 **Genre:** {genre}\n\n"
                f"🎮 **Watch on Frontend:** {FRONTEND_URL}\n"
                f"🔗 **Direct Stream:** `{movie_data['stream_url']}`",
                parse_mode='Markdown'
            )
        
        elif content_type == 'series' and len(parts) >= 5:
            title, season, episode, genre, description = parts[:5]
            content_id = f"{title.lower().replace(' ', '_')}_s{season}e{episode}"
            
            series_data = {
                'content_id': content_id,
                'file_id': file_id,
                'title': title,
                'season': int(season) if season.isdigit() else None,
                'episode': int(episode) if episode.isdigit() else None,
                'genre': [g.strip() for g in genre.split(',')],
                'description': description,
                'type': 'series',
                'stream_url': f"https://{os.getenv('KOYEB_PUBLIC_DOMAIN', 'your-app.koyeb.app')}/stream/{file_id}",
                'file_info': file_registry[file_id],
                'added_date': datetime.now().isoformat(),
                'added_by': update.effective_user.id
            }
            
            content_library['series'][content_id] = series_data
            content_library['categories'].update(series_data['genre'])
            
            # Update file registry with content metadata
            file_registry[file_id]['content_metadata'] = series_data
            
            await update.message.reply_text(
                f"✅ **Series Episode Added Successfully!**\n\n"
                f"📺 **Title:** {title}\n"
                f"🗓️ **Season {season}, Episode {episode}**\n"
                f"🎭 **Genre:** {genre}\n\n"
                f"🎮 **Watch on Frontend:** {FRONTEND_URL}\n"
                f"🔗 **Direct Stream:** `{series_data['stream_url']}`",
                parse_mode='Markdown'
            )
        
        else:
            await update.message.reply_text(
                "❌ Invalid format! Please follow the exact format shown above."
            )
            return
        
        # Clear categorization state
        del context.user_data['categorizing']
        
    except Exception as e:
        logger.error(f"Error processing metadata: {e}")
        await update.message.reply_text(
            "❌ Error processing metadata. Please check the format and try again."
        )

async def library_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user's content library"""
    user_id = update.effective_user.id
    
    user_movies = [m for m in content_library['movies'].values() if m['added_by'] == user_id]
    user_series = [s for s in content_library['series'].values() if s['added_by'] == user_id]
    
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
            library_text += f"• {movie['title']} ({movie['year']})\n"
        if len(user_movies) > 10:
            library_text += f"• ... and {len(user_movies) - 10} more\n"
        library_text += "\n"
    
    if user_series:
        library_text += f"📺 **Series Episodes ({len(user_series)}):**\n"
        # Group series by title
        series_groups = {}
        for series in user_series:
            title = series['title']
            if title not in series_groups:
                series_groups[title] = []
            series_groups[title].append(series)
        
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
        f"✅ Responsive design\n\n"
        f"Open the link above to access your streaming platform!",
        parse_mode='Markdown'
    )

async def upload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Upload command handler"""
    await update.message.reply_text(
        "📤 **Upload Content**\n\n"
        "Simply send me any video file (up to 2GB) and I'll:\n\n"
        "1️⃣ Extract video metadata\n"
        "2️⃣ Generate streaming URL\n"
        "3️⃣ Add to your library\n"
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

**How to Use:**
1️⃣ Send me any video file
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

**Supported Formats:**
MP4, AVI, MKV, MOV, WMV, FLV, WebM, M4V, MPG, MPEG, OGV, 3GP, RM, RMVB, ASF, DIVX

**File Size Limit:** 2GB per file

Need more help? Just ask! 😊
    """
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show bot statistics"""
    user_id = update.effective_user.id
    
    # User-specific stats
    user_movies = [m for m in content_library['movies'].values() if m['added_by'] == user_id]
    user_series = [s for s in content_library['series'].values() if s['added_by'] == user_id]
    user_files = [f for f in file_registry.values() if f['user_id'] == user_id]
    
    # Global stats
    total_movies = len(content_library['movies'])
    total_series = len(content_library['series'])
    total_files = len(file_registry)
    total_categories = len(content_library['categories'])
    
    # Calculate total size for user
    user_total_size = sum(f['file_size'] for f in user_files)
    
    stats_text = f"""
📊 **Your Statistics**

**Your Content:**
🎬 Movies: {len(user_movies)}
📺 Series Episodes: {len(user_series)}
📁 Total Files: {len(user_files)}
💾 Storage Used: {user_total_size/(1024*1024*1024):.2f} GB

**Platform Statistics:**
🎬 Total Movies: {total_movies}
📺 Total Episodes: {total_series}
📂 Total Files: {total_files}
🏷️ Categories: {total_categories}

**Popular Genres:**
{', '.join(list(content_library['categories'])[:10]) if content_library['categories'] else 'None yet'}

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
    
    if file_id not in file_registry:
        await update.message.reply_text("❌ File not found!")
        return
    
    file_info = file_registry[file_id]
    if file_info['user_id'] != user_id:
        await update.message.reply_text("❌ You can only delete your own files!")
        return
    
    # Remove from content library
    content_to_remove = []
    for content_type in ['movies', 'series']:
        for content_id, content_data in content_library[content_type].items():
            if content_data['file_id'] == file_id:
                content_to_remove.append((content_type, content_id))
    
    for content_type, content_id in content_to_remove:
        del content_library[content_type][content_id]
    
    # Remove from file registry
    filename = file_info['filename']
    del file_registry[file_id]
    
    await update.message.reply_text(
        f"✅ **Content Deleted**\n\n"
        f"🗑️ **File:** `{filename}`\n"
        f"📊 **Removed:** {len(content_to_remove)} content entries\n\n"
        f"The streaming URL is no longer accessible.",
        parse_mode='Markdown'
    )

def run_flask():
    """Run Flask app in a separate thread"""
    port = int(os.getenv('PORT', 5000))
    # It's good practice to set use_reloader=False when running in a separate thread
    flask_app.run(host='0.0.0.0', port=port, debug=False, threaded=True, use_reloader=False)

# The main function logic is moved directly into the if __name__ == "__main__": block
# and the async def main() wrapper is removed.

if __name__ == "__main__":
    # Environment setup instructions
    print("""
🎬 Netflix-Style Video Streaming Bot Setup

Required Environment Variables:
- BOT_TOKEN: Your Telegram bot token from @BotFather
- STORAGE_CHANNEL_ID: Channel ID for storing video files (use @username2idbot)
- KOYEB_PUBLIC_DOMAIN: Your deployed domain (e.g., your-app.koyeb.app)
- FRONTEND_URL: Your frontend application URL
- PORT: Port for Flask server (default: 5000)

Optional:
- MAX_FILE_SIZE: Maximum file size in bytes (default: 2GB)

Example .env file:
BOT_TOKEN=1234567890:ABCdefGHIjklMNOpqrsTUVwxyz
STORAGE_CHANNEL_ID=-1001234567890
KOYEB_PUBLIC_DOMAIN=my-streaming-bot.koyeb.app
FRONTEND_URL=https://my-streaming-app.vercel.app
PORT=8000

Make sure to:
1. Create a Telegram bot with @BotFather
2. Create a private channel and add your bot as admin
3. Get the channel ID using @username2idbot
4. Deploy this script to a cloud platform (Koyeb, Railway, Heroku, etc.)
5. Set up a frontend application for Netflix-style interface

Starting bot...
    """)
    
    # Check for essential environment variables
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN environment variable is required!")
        exit(1) # Exit if critical variable is missing
    
    if not STORAGE_CHANNEL_ID:
        logger.error("STORAGE_CHANNEL_ID environment variable is required!")
        exit(1) # Exit if critical variable is missing

    # Start Flask app in background thread
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True # Allow the main program to exit even if thread is running
    flask_thread.start()
    logger.info("Flask server started")
    
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
    
    # Start the bot directly using run_polling
    try:
        bot_app.run_polling(drop_pending_updates=True)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot error: {e}")
        # If an error occurs, ensure the Flask thread is also signaled to stop if possible
        # For simple threading, setting daemon=True allows the process to exit

