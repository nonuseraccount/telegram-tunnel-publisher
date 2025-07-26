# -*- coding: utf-8 -*-
"""
Telegram Folder Publisher
=========================

Author: Unavailable User (Conceptual Request)
Developed by: Gemini
Date: 2025-07-25
Version: 19.0.0

Project Overview:
-----------------
This script clones a specified folder from a Git repository, compresses it
into a proper multi-volume (split) .7z archive using the highest compression
settings (LZMA2 at Ultra level), and sends the parts to a Telegram channel.

It intelligently handles file naming, removing the '.001' extension if the
archive is small enough to fit into a single part. The caption is dynamically
generated to include Jalali and Gregorian timestamps and a channel handle,
and is safely escaped for Telegram's MarkdownV2 format.

System Dependencies:
--------------------
- git
- 7-Zip (must be installed and the '7z' command available in the system's PATH)

Required Python Libraries:
--------------------------
- requests
- jdatetime
"""

import json
import logging
import os
import re
import sys
import time
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime, timezone, timedelta

try:
    import requests
    import jdatetime
except ImportError:
    print("Error: A required library is missing.")
    print("Please install all required libraries by running: pip install -r requirements.txt")
    sys.exit(1)

# ================================================================
# 1. LOGGER SETUP
# ================================================================

class ColorFormatter(logging.Formatter):
    """A custom logging formatter that adds color to log levels for readability."""
    GREY = "\x1b[38;20m"
    YELLOW = "\x1b[33;20m"
    RED = "\x1b[31;20m"
    BOLD_RED = "\x1b[31;1m"
    RESET = "\x1b[0m"
    FORMAT = "%(asctime)s - [%(levelname)s] - %(message)s"
    FORMATS = {
        logging.DEBUG: GREY + FORMAT + RESET,
        logging.INFO: GREY + FORMAT + RESET,
        logging.WARNING: YELLOW + FORMAT + RESET,
        logging.ERROR: RED + FORMAT + RESET,
        logging.CRITICAL: BOLD_RED + FORMAT + RESET,
    }

    def format(self, record: logging.LogRecord) -> str:
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)

def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Sets up and configures a new logger instance."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    if logger.hasHandlers():
        logger.handlers.clear()
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ColorFormatter())
    logger.addHandler(console_handler)
    return logger

# ================================================================
# 2. CONFIGURATION & RUNTIME MANAGERS
# ================================================================

class ConfigManager:
    """Handles loading all necessary configuration files."""

    def __init__(self, log: logging.Logger, config_path: Path):
        self.log = log
        self.config_path = config_path
        self.config: Dict[str, Any] = {}

    def load(self) -> Dict[str, Any]:
        """Loads the main preferences.json, injects secrets, and validates critical values."""
        self.log.info("--- Stage: Configuration Loading ---")
        if not self.config_path.exists():
            self.log.critical(f"Configuration file not found at '{self.config_path}'. Aborting.")
            sys.exit(1)

        try:
            with self.config_path.open('r', encoding='utf-8') as f:
                self.config = json.load(f)
        except json.JSONDecodeError as e:
            self.log.critical(f"Error parsing '{self.config_path}': {e}. Aborting.")
            sys.exit(1)

        self._inject_environment_variables()
        self._validate_critical_configs()
        self.log.info("Configuration loaded successfully.")
        return self.config

    def _inject_environment_variables(self):
        """Overrides config with environment variables for security (e.g., GitHub Secrets)."""
        bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        channel_id = os.environ.get('TELEGRAM_CHANNEL_ID')
        channel_handle = os.environ.get('TELEGRAM_CHANNEL_HANDLE')

        if bot_token:
            self.config['telegram']['bot_token'] = bot_token
            self.log.info("Loaded Telegram Bot Token from environment variable.")
        if channel_id:
            self.config['telegram']['channel_id'] = channel_id
            self.log.info("Loaded Telegram Channel ID from environment variable.")
        if channel_handle:
            self.config['telegram']['channel_handle_id'] = channel_handle
            self.log.info("Loaded Telegram Channel Handle from environment variable.")

    def _validate_critical_configs(self):
        """Ensures that essential configuration values are present."""
        if not self.config.get('telegram', {}).get('bot_token'):
            self.log.critical("Telegram Bot Token is not configured. Please set the TELEGRAM_BOT_TOKEN environment variable/secret.")
            sys.exit(1)
        if not self.config.get('telegram', {}).get('channel_id'):
            self.log.critical("Telegram Channel ID is not configured. Please set the TELEGRAM_CHANNEL_ID environment variable/secret.")
            sys.exit(1)

class RuntimeManager:
    """Tracks script execution time to prevent timeouts."""
    def __init__(self, start_time: float, config: Dict[str, Any], log: logging.Logger):
        self.start_time = start_time
        self.log = log
        self.max_seconds = config.get('runtime', {}).get('max_execution_seconds', 3300)
        self.time_exceeded = False

    def is_time_exceeded(self) -> bool:
        """Checks if the maximum execution time has been reached."""
        if self.time_exceeded:
            return True
        elapsed_time = time.time() - self.start_time
        if elapsed_time > self.max_seconds:
            self.log.warning(f"Execution time limit of {self.max_seconds} seconds reached. Stopping operations.")
            self.time_exceeded = True
            return True
        return False

# ================================================================
# 3. UTILITY FUNCTIONS
# ================================================================

def escape_markdown_v2(text: str) -> str:
    """
    Escapes text for Telegram's MarkdownV2 parse mode, preserving code blocks.
    
    It splits the string by the backtick (`) character. Text outside the
    backticks (at even indices) is escaped, while text inside (at odd indices)
    is left untouched. This allows for monospace formatting while preventing
    parsing errors from other special characters.
    """
    escape_chars = r'_*[]()~>#+-=|{}.!'
    
    parts = text.split('`')
    escaped_parts = []
    
    for i, part in enumerate(parts):
        if i % 2 == 0:
            escaped_parts.append(re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', part))
        else:
            escaped_parts.append(part)
            
    return '`'.join(escaped_parts)

def create_multivolume_archive(log: logging.Logger, directory_to_zip: Path, archive_output_path: Path, chunk_size_mb: int) -> List[Path]:
    """
    Creates a multi-volume (split) .7z archive using the 7-Zip command-line tool
    with the highest compression settings (LZMA2).
    """
    if not directory_to_zip.is_dir():
        log.error(f"Cannot zip '{directory_to_zip}' as it is not a valid directory.")
        return []

    source_path = str(directory_to_zip / '.')
    
    command = [
        '7z', 'a', '-t7z', '-m0=lzma2', '-mx=9', f'-v{chunk_size_mb}m',
        str(archive_output_path),
        source_path
    ]

    log.info(f"Executing 7-Zip command for maximum compression .7z archive...")
    log.debug(f"Command: {' '.join(command)}")
    try:
        process = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8'
        )
        log.info("7-Zip process completed successfully.")
        log.debug(f"7-Zip output:\n{process.stdout}")
    except FileNotFoundError:
        log.critical("7-Zip command ('7z') not found. Please ensure 7-Zip is installed and in your system's PATH.")
        return []
    except subprocess.CalledProcessError as e:
        log.error(f"7-Zip failed with exit code {e.returncode}.\nStderr: {e.stderr}")
        return []
    except Exception as e:
        log.error(f"An unexpected error occurred during 7-Zip execution: {e}")
        return []

    archive_parts = sorted(archive_output_path.parent.glob(f"{archive_output_path.name}.*"))
    
    if not archive_parts:
        if archive_output_path.exists():
             log.info("Archive was created as a single file (smaller than chunk size).")
             return [archive_output_path]
        log.error("Could not find any archive parts created by 7-Zip.")
        return []

    log.info(f"Found {len(archive_parts)} archive parts created by 7-Zip.")
    return archive_parts

# ================================================================
# 4. CORE LOGIC CLASSES
# ================================================================

class FolderFetcher:
    """Handles cloning a Git repository to a local temporary directory."""
    def __init__(self, config: Dict[str, Any], log: logging.Logger):
        self.log = log
        self.repo_config = config.get('source_repo', {})
        self.repo_url = self.repo_config.get('url')
        self.branch = self.repo_config.get('branch', 'main')

    def clone_repo(self, temp_dir: Path) -> bool:
        """Clones the configured repository into the specified temporary directory."""
        self.log.info("--- Stage: Data Loading (Git Repo) ---")
        if not self.repo_url:
            self.log.error("`source_repo.url` is not defined in config.")
            return False
        
        self.log.info(f"Cloning repo '{self.repo_url}' (branch: {self.branch}) into '{temp_dir}'")
        
        git_command = [
            'git', 'clone', '--depth', '1', '--branch', self.branch, self.repo_url, str(temp_dir)
        ]
        
        try:
            subprocess.run(git_command, check=True, capture_output=True, text=True, encoding='utf-8')
            self.log.info("Git clone successful.")
            return True
        except FileNotFoundError:
            self.log.critical("Git command ('git') not found. Please ensure Git is installed and in your system's PATH.")
            return False
        except subprocess.CalledProcessError as e:
            self.log.error(f"Git clone failed with exit code {e.returncode}.\nStderr: {e.stderr}")
            return False
        except Exception as e:
            self.log.error(f"An unexpected error occurred during git clone: {e}")
            return False

class TelegramPoster:
    """Sends files to the Telegram channel."""
    def __init__(self, config: Dict[str, Any], log: logging.Logger):
        self.config = config
        self.log = log
        self.bot_token = self.config['telegram']['bot_token']
        self.channel_id = self.config['telegram']['channel_id']
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}"

    def send_document(self, file_path: Path, caption: str = "") -> bool:
        """Sends a single document (file) to the configured Telegram channel."""
        if not file_path.exists():
            self.log.error(f"Document to send not found at: {file_path}")
            return False

        escaped_caption = escape_markdown_v2(caption)

        send_doc_url = f'{self.api_url}/sendDocument'
        payload = {
            'chat_id': self.channel_id,
            'caption': escaped_caption,
            'parse_mode': 'MarkdownV2'
        }
        
        self.log.info(f"Uploading '{file_path.name}' to channel {self.channel_id}...")
        try:
            with open(file_path, 'rb') as doc_file:
                files = {'document': (file_path.name, doc_file, 'application/x-7z-compressed')}
                timeout = self.config.get('runtime', {}).get('request_timeout', 120)
                response = requests.post(send_doc_url, data=payload, files=files, timeout=timeout)
                response.raise_for_status()
            self.log.info(f"Successfully sent document: {file_path.name}")
            return True
        except requests.exceptions.RequestException as e:
            self.log.error(f"Error sending document: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.log.error(f"Telegram API response: {e.response.text}")
            return False

# ================================================================
# 5. MAIN EXECUTION
# ================================================================

def main():
    """Main function to orchestrate the entire pipeline."""
    start_time = time.time()
    log = setup_logger("FolderPublisher")
    log.info("====== Starting Telegram Folder Publisher ======")

    try:
        config_manager = ConfigManager(log, Path("data/preferences.json"))
        config = config_manager.load()
        runtime_manager = RuntimeManager(start_time, config, log)

        with tempfile.TemporaryDirectory(prefix="git_clone_") as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            log.info(f"Created temporary directory: {temp_dir}")

            # 1. Fetch the repository
            fetcher = FolderFetcher(config, log)
            if not fetcher.clone_repo(temp_dir):
                log.error("Aborting workflow due to clone failure.")
                return

            # 2. Identify the target folder and prepare archive path
            repo_config = config.get('source_repo', {})
            output_config = config.get('output', {})
            
            folder_to_zip_name = repo_config.get('folder_to_zip')
            if not folder_to_zip_name:
                log.error("`source_repo.folder_to_zip` not specified in config. Aborting.")
                return
            
            source_dir = temp_dir / folder_to_zip_name
            
            if not source_dir.is_dir():
                log.error(f"The specified folder to zip '{folder_to_zip_name}' does not exist in the cloned repository. Aborting.")
                return

            output_archive_basename = output_config.get('archive_name', 'archive')
            archive_output_path = (temp_dir / output_archive_basename).with_suffix('.7z')
            
            # 3. Create the multi-volume .7z archive
            chunk_size_mb = output_config.get('chunk_size_mb', 15)
            archive_chunks = create_multivolume_archive(log, source_dir, archive_output_path, chunk_size_mb)
            
            if not archive_chunks:
                log.error("Aborting workflow due to archive creation failure.")
                return

            # 4. Intelligent renaming for single-part archives
            if len(archive_chunks) == 1 and archive_chunks[0].name.endswith('.001'):
                single_part = archive_chunks[0]
                target_path = archive_output_path
                log.info(f"Only one archive part was created. Renaming '{single_part.name}' to '{target_path.name}'.")
                try:
                    single_part.rename(target_path)
                    archive_chunks = [target_path]
                except OSError as e:
                    log.error(f"Failed to rename single archive part: {e}. Proceeding with '.001' name.")

            # 5. Dynamically generate the full caption with timestamps
            log.info("Generating dynamic caption with timestamps...")
            
            # Get timestamps
            utc_now = datetime.now(timezone.utc)
            iran_tz = timezone(timedelta(hours=3, minutes=30))
            iran_now = utc_now.astimezone(iran_tz)
            jalali_now = jdatetime.datetime.fromgregorian(datetime=iran_now)

            utc_str = utc_now.strftime("%Y-%m-%d %H:%M:%S")
            jalali_str = jalali_now.strftime("%Y/%m/%d %H:%M:%S")

            # Get components from config
            base_text = output_config.get('telegram_caption', '')
            channel_handle = config.get('telegram', {}).get('channel_handle_id', '')

            # Construct the final caption
            full_caption_parts = [
                base_text,
                f"\nLatest Update: `{jalali_str} | {utc_str} UTC`",
                f"\n{channel_handle}"
            ]
            full_caption = "\n".join(part for part in full_caption_parts if part)

            # 6. Send each archive part to Telegram
            poster = TelegramPoster(config, log)
            total_parts = len(archive_chunks)

            for i, chunk_path in enumerate(archive_chunks):
                if runtime_manager.is_time_exceeded():
                    log.warning("Stopping file upload due to execution time limit.")
                    break
                
                part_num = i + 1
                # Use the full, dynamic caption for the first part (or only part)
                # and only a simple part indicator for subsequent parts.
                if i == 0:
                    current_caption = f"{full_caption} (Part {part_num}/{total_parts})" if total_parts > 1 else full_caption
                else:
                    current_caption = f"{output_archive_basename} (Part {part_num}/{total_parts})"

                if not poster.send_document(chunk_path, current_caption):
                    log.error(f"Failed to send chunk: {chunk_path.name}. Aborting remaining uploads.")
                    break
                
                if part_num < total_parts:
                    log.info("Waiting 3 seconds before sending next part...")
                    time.sleep(3)

    except Exception as e:
        log.critical(f"An unhandled exception occurred in the main execution block: {e}", exc_info=True)
    finally:
        elapsed_time = time.time() - start_time
        log.info(f"====== Pipeline Finished in {elapsed_time:.2f} seconds ======")

if __name__ == "__main__":
    main()
