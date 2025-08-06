import os
import uuid
import asyncio
import mimetypes
import json
from urllib.parse import quote
import logging

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.error import TelegramError
from flask import Flask, Response, abort, jsonify
import threading
import aiohttp

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuration
BOT_TOKEN = os.getenv('BOT_TOKEN')  # Set this in Koyeb environment variables
STORAGE_CHANNEL_ID = os.getenv('STORAGE_CHANNEL_ID')  # Channel/Group ID for storing files
MAX_FILE_SIZE = 2000 * 1024 * 1024  # 2GB limit (Telegram's limit)

# Supported video formats only
SUPPORTED_VIDEO_FORMATS = {
    'mp4', 'avi', 'mkv', 'mov', 'wmv', 'flv', 'webm', 'm4v', 
    'mpg', 'mpeg', 'ogv', '3gp', 'rm', 'rmvb', 'asf', 'divx'
}

# Flask app for serving files
flask_app = Flask(__name__)

# Store file metadata (file_id -> telegram_message_info)
file_registry = {}

# Global bot instance for Flask routes
bot_app = None

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
    
    # Fallback based on extension
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

@flask_app.route('/stream/<file_id>')
def stream_file(file_id):
    """Stream video file with support for range requests"""
    if file_id not in file_registry:
        abort(404)
    
    file_info = file_registry[file_id]
    file_url = file_info['file_url']
    file_size = file_info['file_size']
    filename = file_info['filename']
    mime_type = get_video_mime_type(filename)
    
    # Handle range requests
    range_header = flask_app.request.environ.get('HTTP_RANGE', '').strip()
    range_match = None
    
    if range_header:
        import re
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
                    async def read_chunks():
                        async for chunk in content.iter_chunked(8192):
                            yield chunk
                    
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
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
            206,  # Partial Content
            {
                'Content-Type': mime_type,
                'Accept-Ranges': 'bytes',
                'Content-Range': f'bytes {start}-{end}/{file_size}',
                'Content-Length': str(end - start + 1),
                'Cache-Control': 'public, max-age=3600',
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
            }
        )
    
    return response

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
        'telegram_message_id': file_data['message_id'],
        'uploaded_by': file_data['user_id']
    })

@flask_app.route('/health')
def health_check():
    return jsonify({
        'status': 'ok', 
        'videos_stored': len(file_registry),
        'storage_channel': STORAGE_CHANNEL_ID
    })

# Telegram Bot handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler"""
    welcome_message = """
🎬 **Video Streaming Bot** 🎬

Send me any video file and I'll convert it to a permanent streaming URL that works with any video player!

**Supported video formats:**
• MP4, AVI, MKV, MOV, WMV, FLV, WebM
• M4V, MPG, MPEG, OGV, 3GP, RM, RMVB
• ASF, DivX and more!

**Features:**
✅ Permanent streaming URLs (no expiry)
✅ Works with any video player (VLC, web players, etc.)
✅ Range request support for smooth seeking
✅ Up to 2GB file size support
✅ Files stored securely in Telegram

**How to use:**
1. Send me a video file
2. Get your permanent streaming URL instantly
3. Use the URL in any video player or share it!

Just send me a video file to get started! 🚀
    """
    await update.message.reply_text(welcome_message, parse_mode='Markdown')

async def handle_video_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle video file uploads"""
    # Handle both video and document types (for video files sent as documents)
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
        
        # Check if document is actually a video
        if not filename or not is_video_file(filename):
            await update.message.reply_text(
                "❌ This bot only supports video files! Please send a video file with one of the supported formats."
            )
            return
    else:
        await update.message.reply_text(
            "❌ Please send a video file!"
        )
        return
    
    # Check file size (Telegram's limit is 2GB)
    if file_size > MAX_FILE_SIZE:
        await update.message.reply_text(
            f"❌ Video file too large! Maximum size is {MAX_FILE_SIZE // (1024*1024*1024)}GB"
        )
        return
    
    try:
        # Send processing message
        processing_msg = await update.message.reply_text("⏳ Processing your video...")
        
        # Forward the video to storage channel
        if not STORAGE_CHANNEL_ID:
            await processing_msg.edit_text("❌ Storage channel not configured!")
            return
        
        # Forward message to storage channel
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
        
        # Store file info in registry
        file_registry[file_id] = {
            'filename': filename,
            'file_size': file_size,
            'file_url': file_url,
            'message_id': forwarded_msg.message_id,
            'user_id': update.effective_user.id,
            'chat_id': update.effective_chat.id,
            'storage_channel_id': STORAGE_CHANNEL_ID
        }
        
        # Generate URLs
        domain = os.getenv('KOYEB_PUBLIC_DOMAIN', 'your-app.koyeb.app')
        stream_url = f"https://{domain}/stream/{file_id}"
        info_url = f"https://{domain}/info/{file_id}"
        
        # Format file size
        if file_size < 1024*1024:
            size_str = f"{file_size/1024:.1f} KB"
        elif file_size < 1024*1024*1024:
            size_str = f"{file_size/(1024*1024):.1f} MB"
        else:
            size_str = f"{file_size/(1024*1024*1024):.2f} GB"
        
        # Create response message
        response_message = f"""
✅ **Video processed successfully!**

🎬 **Video:** `{filename}`
📊 **Size:** {size_str}
🆔 **File ID:** `{file_id}`

🔗 **Streaming URL:**
`{stream_url}`

ℹ️ **File Info API:**
`{info_url}`

**How to use:**
• **VLC Player:** Media → Open Network Stream → Paste URL
• **Web Players:** Use URL directly in HTML5 video players
• **Mobile Apps:** Paste URL in any video player app
• **Sharing:** Send the streaming URL to anyone!

**Features:**
✅ Supports video seeking/scrubbing
✅ Works with any video player
✅ Permanent URL (won't expire)
✅ Fast streaming with range support

Enjoy your permanent video streaming URL! 🚀
        """
        
        # Delete processing message and send result
        await processing_msg.delete()
        await update.message.reply_text(response_message, parse_mode='Markdown')
        
        logger.info(f"Video processed: {filename} -> {file_id} for user {update.effective_user.id}")
        
    except TelegramError as e:
        logger.error(f"Telegram error: {e}")
        await update.message.reply_text(
            "❌ Error storing video in channel. Please make sure the bot is admin in the storage channel."
        )
    except Exception as e:
        logger.error(f"Error processing video: {e}")
        await update.message.reply_text(
            "❌ An error occurred while processing your video. Please try again."
        )

async def handle_non_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle non-video files"""
    await update.message.reply_text(
        "🎬 This bot only accepts video files!\n\n"
        "Please send a video file in one of these formats:\n"
        "MP4, AVI, MKV, MOV, WMV, FLV, WebM, M4V, MPG, MPEG, OGV, 3GP, etc.\n\n"
        "Use /start to see all supported formats."
    )

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show bot statistics"""
    user_videos = sum(1 for f in file_registry.values() if f['user_id'] == update.effective_user.id)
    total_videos = len(file_registry)
    total_size = sum(f['file_size'] for f in file_registry.values())
    
    stats_message = f"""
📊 **Bot Statistics**

👤 **Your videos:** {user_videos}
🌐 **Total videos:** {total_videos}
💾 **Total storage:** {total_size / (1024*1024*1024):.2f} GB

🔗 **Service URL:** https://{os.getenv('KOYEB_PUBLIC_DOMAIN', 'your-app.koyeb.app')}
📺 **Storage Channel:** `{STORAGE_CHANNEL_ID}`

Send /start to see how to use the bot!
    """
    
    await update.message.reply_text(stats_message, parse_mode='Markdown')

async def setup_commands(application):
    """Setup bot commands for better UX"""
    commands = [
        ("start", "Get started with the video streaming bot"),
        ("stats", "View bot statistics and your uploaded videos"),
    ]
    await application.bot.set_my_commands(commands)

def run_flask():
    """Run Flask app in a separate thread"""
    port = int(os.getenv('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port, debug=False, threaded=True)

def main():
    """Main function to run the bot"""
    global bot_app
    
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN environment variable not set!")
        return
    
    if not STORAGE_CHANNEL_ID:
        logger.error("STORAGE_CHANNEL_ID environment variable not set!")
        logger.error("Please create a channel/group and add the bot as admin, then set the channel ID")
        return
    
    # Start Flask server in a separate thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Create Telegram bot application
    app = Application.builder().token(BOT_TOKEN).build()
    bot_app = app
    
    # Add handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    
    # Handle video files (both as video and document)
    app.add_handler(MessageHandler(filters.VIDEO, handle_video_file))
    app.add_handler(MessageHandler(
        filters.Document.ALL & ~filters.PHOTO, 
        handle_video_file
    ))
    
    # Handle non-video files
    app.add_handler(MessageHandler(filters.PHOTO, handle_non_video))
    app.add_handler(MessageHandler(filters.AUDIO, handle_non_video))
    
    # Setup commands
    app.post_init = setup_commands
    
    # Start the bot
    logger.info("Starting Telegram Video Streaming Bot...")
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
