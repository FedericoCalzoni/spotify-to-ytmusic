#!/usr/bin/env python3
"""
Spotify to YouTube Music Playlist Sync
Syncs Spotify playlists (CSV format downloaded from  https://exportify.app/) to YouTube Music
"""

import os
import sys
import csv
import time
import json
import argparse
import logging
import pickle
import requests
import threading
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from difflib import SequenceMatcher
from unicodedata import normalize
from tqdm import tqdm
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed
from ytmusicapi import YTMusic, OAuthCredentials
from ytmusicapi.setup import setup_oauth

MIN_SCORE = 0.5

# Configure logging to both file and console
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout) 
    ]
)

def retry_on_network_error(max_retries=3, base_delay=1, max_delay=8):
    """Retry decorator for network errors, but NOT 404s (permanent failures)"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.HTTPError as e:
                    # Don't retry 404s - they're permanent
                    if e.response and e.response.status_code == 404:
                        return None
                    # Handle rate limiting (429) with longer delay
                    if e.response and e.response.status_code == 429:
                        delay = min(delay * 3, 60)  # Exponential backoff for rate limits
                    if attempt == max_retries:
                        return None
                    time.sleep(min(delay, max_delay))
                    delay *= 2
                except (requests.exceptions.RequestException, Exception) as e:
                    if attempt == max_retries:
                        return None
                    time.sleep(min(delay, max_delay))
                    delay *= 2
            return None
        return wrapper
    return decorator

class RateLimiter:
    """Adaptive rate limiter based on response times and errors"""
    def __init__(self, initial_delay=0.3, min_delay=0.1, max_delay=2.0):
        self.delay = initial_delay
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.error_count = 0
        self.success_count = 0
        self.lock = threading.Lock()
        self.last_request_time = 0
    
    def wait(self):
        """Wait appropriate time before next request"""
        with self.lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.delay:
                time.sleep(self.delay - time_since_last)
            self.last_request_time = time.time()
    
    def record_success(self, response_time=None):
        """Record successful request and potentially reduce delay"""
        with self.lock:
            self.success_count += 1
            self.error_count = max(0, self.error_count - 1)
            
            # Reduce delay if we're getting fast responses
            if response_time and response_time < 1.0 and self.success_count % 10 == 0:
                self.delay = max(self.min_delay, self.delay * 0.9)
    
    def record_error(self, is_rate_limit=False):
        """Record error and increase delay"""
        with self.lock:
            self.error_count += 1
            
            if is_rate_limit:
                self.delay = min(self.max_delay, self.delay * 2)
            elif self.error_count > 3:
                self.delay = min(self.max_delay, self.delay * 1.5)
    
    def get_current_delay(self):
        return self.delay

@dataclass
class Track:
    """Represents a track from Spotify CSV"""
    uri: str
    name: str
    artists: List[str]
    album: str
    duration_ms: int
    added_at: str
    
    @classmethod
    def from_csv_row(cls, row: Dict[str, str]) -> 'Track':
        """Create Track from CSV row"""
        artists = row['Artist Name(s)'].split(', ') if row['Artist Name(s)'] else []
        return cls(
            uri=row['Track URI'],
            name=row['Track Name'],
            artists=artists,
            album=row['Album Name'],
            duration_ms=int(row['Track Duration (ms)']) if row['Track Duration (ms)'] else 0,
            added_at=row['Added At']
        )

class PlaylistSyncer:
    def __init__(self, auth_file: str = "oauth.json", creds_file: str = "oauth_creds.json"):
        self.auth_file = auth_file
        self.creds_file = creds_file
        self.ytmusic = None
        self.processed_tracks = set()
        self.failed_tracks: List[Tuple[str, Track, str]] = []
        self.cache_file = "search_cache.pkl"
        self.search_cache = self._load_search_cache()
        self.rate_limiter = RateLimiter()
        self.max_workers = 4

    def _load_search_cache(self) -> Dict[str, Dict]:
        """Load search results cache"""
        cache_file = Path(self.cache_file)
        if cache_file.exists():
            try:
                with open(cache_file, 'rb') as f:
                    return pickle.load(f)
            except Exception:
                return {}
        return {}
    
    def _save_search_cache(self):
        """Save search results cache"""
        try:
            with open(self.cache_file, 'wb') as f:
                pickle.dump(self.search_cache, f)
        except Exception as e:
            tqdm.write(f"Warning: Could not save search cache: {e}")

    def _log_failed_track(self, track: Track, playlist_name: str, reason: str):
        """Record a failed track in memory and log it immediately"""
        self.failed_tracks.append((playlist_name, track, reason))
        artist = track.artists[0] if track.artists else 'Unknown Artist'
        logging.warning(
            f"FAILED: Playlist='{playlist_name}' | Track='{track.name}' | Artist='{artist}' | Reason={reason}"
        )
        
    def _load_oauth_credentials(self) -> Optional[Dict[str, str]]:
        """Load OAuth credentials from file"""
        if os.path.exists(self.creds_file):
            try:
                with open(self.creds_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                tqdm.write(f"Error loading OAuth credentials: {str(e)}")
        return None
    
    def _save_oauth_credentials(self, client_id: str, client_secret: str):
        """Save OAuth credentials to file"""
        try:
            with open(self.creds_file, 'w') as f:
                json.dump({
                    'client_id': client_id,
                    'client_secret': client_secret
                }, f, indent=2)
            tqdm.write(f"OAuth credentials saved to {self.creds_file}")
        except Exception as e:
            tqdm.write(f"Error saving OAuth credentials: {str(e)}")
    
    def _setup_oauth(self):
        """Interactive OAuth setup"""
        tqdm.write("\n" + "="*50)
        tqdm.write("YouTube Music OAuth Setup")
        tqdm.write("="*50)
        tqdm.write("\nYou need to set up OAuth authentication.")
        tqdm.write("Follow the guide at: https://ytmusicapi.readthedocs.io/en/stable/setup/oauth.html")
        
        # Check if credentials already exist
        creds = self._load_oauth_credentials()
        if creds:
            return creds['client_id'], creds['client_secret']
        
        # Get credentials from user
        client_id = input("Enter your OAuth client_id: ").strip()
        client_secret = input("Enter your OAuth client_secret: ").strip()
        
        if not client_id or not client_secret:
            tqdm.write("Client ID and secret are required!")
            return None, None
        
        self._save_oauth_credentials(client_id, client_secret)
        
        return client_id, client_secret
    
    def authenticate(self):
        """Handle YouTube Music authentication"""
        # First, check if we have OAuth credentials
        creds = self._load_oauth_credentials()
        
        if os.path.exists(self.auth_file) and creds:
            # Try to authenticate with existing files
            tqdm.write("Found existing authentication files.")
            try:
                self.ytmusic = YTMusic(
                    self.auth_file,
                    oauth_credentials=OAuthCredentials(
                        client_id=creds['client_id'],
                        client_secret=creds['client_secret']
                    )
                )
                tqdm.write("Successfully authenticated with YouTube Music!")
                return
            except Exception as e:
                tqdm.write(f"Failed to authenticate with existing files: {str(e)}")
                tqdm.write("Will proceed with new authentication...")
        
        # Need to set up OAuth
        if not creds:
            client_id, client_secret = self._setup_oauth()
            if not client_id or not client_secret:
                tqdm.write("OAuth setup cancelled.")
                sys.exit(1)
        else:
            client_id = creds['client_id']
            client_secret = creds['client_secret']
        
        # Run the OAuth flow
        tqdm.write("Running ytmusicapi oauth with provided credentials...")
        
        # Run ytmusicapi oauth with credentials
        setup_oauth(
            client_id=client_id,
            client_secret=client_secret,
            filepath=self.auth_file
        )
        
        # Check if oauth.json was created
        if os.path.exists(self.auth_file):
            self.ytmusic = YTMusic(self.auth_file, oauth_credentials=OAuthCredentials(
                    client_id=client_id,
                    client_secret=client_secret
                )
            )
            tqdm.write("Authentication successful!")
        else:
            raise Exception("OAuth file was not created")
    
    def read_csv_playlist(self, csv_path: str) -> List[Track]:
        """Read tracks from Spotify CSV file"""
        tracks = []
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        track = Track.from_csv_row(row)
                        tracks.append(track)
                    except Exception as e:
                        logging.error(f"Error parsing track in {csv_path}: {str(e)}")
        except Exception as e:
            logging.error(f"Error reading CSV file {csv_path}: {str(e)}")
        
        return tracks
    
    @retry_on_network_error()
    def search_track(self, track: Track, playlist_name: str = None) -> Tuple[Optional[Dict], float]:
        """Search for a track on YouTube Music"""
        # Apply rate limiting
        self.rate_limiter.wait()
        
        # Primary search: "Artist - Track Name"
        primary_artist = track.artists[0] if track.artists else "Unknown"
        search_query = f"{primary_artist} - {track.name}"
        
        start_time = time.time()
        try:
            results = self.ytmusic.search(search_query, filter="songs", limit=5)
            response_time = time.time() - start_time
            self.rate_limiter.record_success(response_time)
            
            if not results:
                return None, 0.0
            
            # Try to find best match with confidence scoring
            best_match = None
            best_score = 0.0
            
            for result in results:
                score = self._calculate_match_score(track, result)
                if score > best_score:
                    best_score = score
                    best_match = result
            
            return best_match, best_score
                
        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 429:
                self.rate_limiter.record_error(is_rate_limit=True)
            else:
                self.rate_limiter.record_error()
            logging.error(f"Search error for '{track.name}': {str(e)}")
            return None, 0.0
        except Exception as e:
            self.rate_limiter.record_error()
            logging.error(f"Search error for '{track.name}': {str(e)}")
            return None, 0.0

    def search_track_cached(self, track: Track, playlist_name: str = None) -> Tuple[Optional[Dict], float]:
        """Search with caching"""
        # Create cache key
        cache_key = f"{track.artists[0] if track.artists else 'Unknown'} - {track.name}".lower()
        
        # Check cache first
        if cache_key in self.search_cache:
            cached_result = self.search_cache[cache_key]
            if cached_result:  # Don't cache failures
                return cached_result, 1.0  # Assume cached results are good matches
        
        # Search and cache result
        result, score = self.search_track(track, playlist_name)
        if result and score >= MIN_SCORE:  # Only cache good matches
            self.search_cache[cache_key] = result
        
        return result, score

    def search_tracks_parallel(self, tracks: List[Track], playlist_name: str = None) -> Dict[int, Tuple[Optional[Dict], float]]:
        """Search multiple tracks in parallel with conservative threading"""
        results = {}
        
        def search_single_track(track_with_index):
            index, track = track_with_index
            try:
                result, score = self.search_track_cached(track, playlist_name)
                return index, (result, score)
            except Exception as e:
                logging.error(f"Parallel search error for track {index}: {e}")
                return index, (None, 0.0)
        
        # Create list of (index, track) tuples for tracks not in cache
        tracks_to_search = []
        for i, track in enumerate(tracks):
            cache_key = f"{track.artists[0] if track.artists else 'Unknown'} - {track.name}".lower()
            if cache_key in self.search_cache:
                results[i] = (self.search_cache[cache_key], 1.0)
            else:
                tracks_to_search.append((i, track))
        
        # Search uncached tracks in parallel
        if tracks_to_search:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_index = {
                    executor.submit(search_single_track, track_data): track_data[0] 
                    for track_data in tracks_to_search
                }
                
                for future in as_completed(future_to_index):
                    try:
                        index, result_tuple = future.result()
                        results[index] = result_tuple
                    except Exception as e:
                        index = future_to_index[future]
                        logging.error(f"Future error for track {index}: {e}")
                        results[index] = (None, 0.0)
        
        return results
    
    def _normalize_string(self, s: str) -> str:
        """Normalize string: lowercase, remove accents, unicode normalize, strip."""
        s = s or ""
        s = normalize('NFKD', s)
        s = ''.join(c for c in s if not ord(c) > 127 or not c.isascii() or c.isalnum() or c.isspace())
        s = s.encode('ascii', 'ignore').decode('ascii')
        return s.lower().strip()

    def _remove_parentheses(self, s: str) -> Tuple[str, List[str]]:
        """Remove content in parentheses and return base string and list of parenthetical contents."""
        matches = re.findall(r'\(([^)]*)\)', s)
        s_no_paren = re.sub(r'\([^)]*\)', '', s)
        return s_no_paren.strip(), matches

    def _calculate_match_score(self, track: Track, yt_result: Dict) -> float:
        """Calculate confidence score for a YouTube Music search result with normalization and parentheses handling."""
        score = 0.0

        noise_words = {
            "official", "video", "music", "audio", "lyrics", "lyric", "hd", "remastered", "explicit",
            "ft", "feat", "featuring", "feat.", "ft.", "karaoke", "live", "radio edit", "instrumental", "uncut", "unplugged"
        }

        # Normalize and clean track name
        track_name_norm = self._normalize_string(track.name)
        track_name_words = re.findall(r'\w+', track_name_norm)
        track_name_clean = ' '.join(
            word for word in track_name_words if word not in noise_words
        ).strip()

        # Remove parentheses from track name
        track_name_base, track_parens = self._remove_parentheses(track_name_clean)

        # Normalize and clean YouTube title
        yt_title = yt_result.get('title', '')
        yt_title_norm = self._normalize_string(yt_title)
        yt_title_words = re.findall(r'\w+', yt_title_norm)
        yt_title_clean = ' '.join(
            word for word in yt_title_words if word not in noise_words
        ).strip()
        yt_title_base, yt_parens = self._remove_parentheses(yt_title_clean)

        # Parentheses matching: if both have same parenthetical content, boost score
        paren_match = False
        if track_parens and yt_parens:
            for t_paren in track_parens:
                for y_paren in yt_parens:
                    if self._normalize_string(t_paren) == self._normalize_string(y_paren):
                        score += 0.2
                        paren_match = True

        # Exact or substring match (base, ignoring parentheses)
        if track_name_base == yt_title_base or track_name_base in yt_title_base or yt_title_base in track_name_base:
            score += 0.5
            if paren_match:
                score += 0.1  # Extra boost if parentheses also match
        else:
            similarity = self._calculate_string_similarity(track_name_base, yt_title_base)
            if similarity >= 0.8:
                score += 0.5
            elif similarity >= 0.6:
                score += 0.3
            elif similarity >= 0.4:
                score += 0.1

        # Check artist match (normalized, penalize "null")
        yt_artists = []
        if 'artists' in yt_result:
            yt_artists = [self._normalize_string(a.get('name', '')) for a in yt_result['artists']]

        track_artists = [self._normalize_string(a) for a in track.artists]

        artist_score = 0.0
        for track_artist in track_artists:
            for yt_artist in yt_artists:
                if yt_artist == "null" or not yt_artist:
                    artist_score -= 0.4  # Heavy penalty for null/empty artist
                    continue
                if track_artist == yt_artist or track_artist in yt_artist or yt_artist in track_artist:
                    artist_score = 0.5
                    break
                elif self._calculate_string_similarity(track_artist, yt_artist) >= 0.8:
                    artist_score = max(artist_score, 0.3)
            if artist_score >= 0.5:
                break

        score += artist_score

        # Check duration match
        if 'duration_seconds' in yt_result and track.duration_ms > 0:
            duration_diff = abs(yt_result['duration_seconds'] - (track.duration_ms / 1000))
            if duration_diff <= 1:
                score += 0.4
            elif duration_diff <= 5:
                score += 0.2

        return score
    
    def _calculate_string_similarity(self, str1: str, str2: str) -> float:
        """Calculate similarity between two strings using Levenshtein distance"""
        return SequenceMatcher(None, str1, str2).ratio()

    @retry_on_network_error()
    def get_or_create_playlist(self, playlist_name: str) -> Optional[str]:
        """Get existing playlist or create new one"""
        try:
            playlists = self.ytmusic.get_library_playlists(limit=None)
            
            for playlist in playlists:
                if playlist['title'] == playlist_name:
                    tqdm.write(f"Found existing playlist: {playlist_name}")
                    return playlist['playlistId']
            
            playlist_id = self.ytmusic.create_playlist(
                title=playlist_name,
                description=f"Synced from Spotify on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
            tqdm.write(f"Created new playlist: {playlist_name}")
            return playlist_id
            
        except Exception as e:
            logging.error(f"Error managing playlist '{playlist_name}': {str(e)}")
            return None
    
    @retry_on_network_error()
    def get_playlist_tracks(self, playlist_id: str) -> Dict[str, Dict]:
        """Get existing tracks in a YouTube Music playlist with metadata"""
        try:
            playlist = self.ytmusic.get_playlist(playlist_id, limit=None)
            existing_tracks = {}
            
            if 'tracks' in playlist:
                for track in playlist['tracks']:
                    if track and 'videoId' in track:
                        # Store comprehensive metadata for better matching
                        existing_tracks[track['videoId']] = {
                            'title': track.get('title', '').lower(),
                            'artists': [a.get('name', '').lower() for a in track.get('artists', [])],
                            'duration': track.get('duration_seconds', 0),
                            'album': track.get('album', {}).get('name', '').lower() if track.get('album') else ''
                        }
            
            return existing_tracks
        except Exception as e:
            logging.error(f"Error getting playlist tracks: {str(e)}")
            return {}

    def is_track_in_playlist(self, track: Track, existing_tracks: Dict[str, Dict]) -> bool:
        """Check if a Spotify track already exists in the YouTube playlist"""
        track_name = track.name.lower()
        track_artists = [a.lower() for a in track.artists]
        track_duration = track.duration_ms / 1000 if track.duration_ms > 0 else 0
        
        for video_id, yt_track in existing_tracks.items():
            # Check title similarity
            title_match = (track_name in yt_track['title'] or 
                          yt_track['title'] in track_name or
                          self._calculate_string_similarity(track_name, yt_track['title']) > 0.8)
            
            # Check artist match
            artist_match = any(
                any(track_artist in yt_artist or yt_artist in track_artist
                    for yt_artist in yt_track['artists'])
                for track_artist in track_artists
            )
            
            # Check duration match (within 10 seconds)
            duration_match = (track_duration == 0 or yt_track['duration'] == 0 or 
                             abs(track_duration - yt_track['duration']) <= 10)
            
            if title_match and artist_match and duration_match:
                return True
        
        return False
    
    def sync_playlist(self, csv_path: str) -> Tuple[int, int, int]:
        """Sync playlist with parallel search processing"""
        playlist_name = Path(csv_path).stem
        tqdm.write(f"\n{'='*50}")
        tqdm.write(f"Syncing playlist: {playlist_name}")
        tqdm.write(f"{'='*50}")
        
        # Read tracks from CSV
        tracks = self.read_csv_playlist(csv_path)
        if not tracks:
            tqdm.write(f"No tracks found in {csv_path}")
            return 0, 0, 0
        
        tqdm.write(f"Found {len(tracks)} tracks in CSV")
        
        # Get or create playlist
        playlist_id = self.get_or_create_playlist(playlist_name)
        if not playlist_id:
            tqdm.write(f"Failed to get/create playlist: {playlist_name}")
            return 0, 0, len(tracks)
        
        # Get existing tracks with full metadata
        existing_tracks = self.get_playlist_tracks(playlist_id)
        tqdm.write(f"Playlist currently has {len(existing_tracks)} tracks")
        
        # Filter out tracks that already exist and duplicates
        tracks_to_process = []
        processed_uris = set()
        
        for track in tracks:
            # Skip duplicates within the CSV
            if track.uri in processed_uris:
                self._log_failed_track(track, playlist_name, "Duplicate track in CSV")
                continue
            processed_uris.add(track.uri)
            
            # Skip tracks already in playlist
            if self.is_track_in_playlist(track, existing_tracks):
                tqdm.write(f"Already in playlist: {track.name}")
                continue
                
            tracks_to_process.append(track)
                
        if not tracks_to_process:
            return 0, len(tracks) - len(tracks_to_process), 0
        
        # Process tracks in chunks with parallel search
        added = 0
        failed = 0
        video_ids_to_add = []
        chunk_size = 20  # Process 20 tracks at a time for parallel search
        
        with tqdm(
            total=len(tracks_to_process),
            desc=f"Syncing '{playlist_name}'",
            unit="track",
            leave=False,
            position=1,
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] Rate: {postfix}",
            colour="green"
        ) as pbar:
            
            for chunk_start in range(0, len(tracks_to_process), chunk_size):
                chunk_end = min(chunk_start + chunk_size, len(tracks_to_process))
                chunk_tracks = tracks_to_process[chunk_start:chunk_end]
                
                # Update progress bar with current processing info
                pbar.set_postfix({"delay": f"{self.rate_limiter.get_current_delay():.2f}s"})
                
                # Search tracks in parallel for this chunk
                search_results = self.search_tracks_parallel(chunk_tracks, playlist_name)
                
                # Process results from parallel search
                for i, track in enumerate(chunk_tracks):
                    global_index = chunk_start + i
                    yt_track, score = search_results.get(i, (None, 0.0))
                    
                    # Apply threshold check here (single place)
                    if yt_track and score >= MIN_SCORE:
                        video_ids_to_add.append(yt_track['videoId'])
                    else:
                        # Single failure log with appropriate reason
                        if yt_track:
                            reason = f"Low confidence match (score: {score:.2f})"
                        else:
                            reason = "Track not found on YouTube Music"
                        self._log_failed_track(track, playlist_name, reason)
                        failed += 1
                    
                    # Add tracks in batches
                    if len(video_ids_to_add) >= 10 or global_index == len(tracks_to_process) - 1:
                        if video_ids_to_add:
                            try:
                                self.ytmusic.add_playlist_items(playlist_id, video_ids_to_add)
                                added += len(video_ids_to_add)
                            except Exception:
                                # Fallback: try adding tracks individually
                                for j, video_id in enumerate(video_ids_to_add):
                                    try:
                                        self.ytmusic.add_playlist_items(playlist_id, [video_id])
                                        added += 1
                                    except Exception as individual_e:
                                        # Find corresponding track for error logging
                                        track_idx = global_index - len(video_ids_to_add) + j + 1
                                        if track_idx < len(tracks_to_process):
                                            self._log_failed_track(
                                                tracks_to_process[track_idx], 
                                                playlist_name, 
                                                f"Failed to add to playlist: {str(individual_e)}"
                                            )
                                        failed += 1
                            
                            video_ids_to_add.clear()
                    
                    pbar.update(1)
                
                # Save cache after each chunk
                self._save_search_cache()
        
        skipped = len(tracks) - len(tracks_to_process)
        
        tqdm.write(f"\nPlaylist '{playlist_name}' sync complete:")
        tqdm.write(f"  - Added: {added}")
        tqdm.write(f"  - Skipped: {skipped}")
        tqdm.write(f"  - Failed: {failed}")
        
        return added, skipped, failed
    
    def sync_folder(self, folder_path: str):
        """Sync all CSV files in a folder with overall progress"""
        csv_files = list(Path(folder_path).glob("*.csv"))
        
        if not csv_files:
            tqdm.write(f"No CSV files found in {folder_path}")
            return
        
        tqdm.write(f"Found {len(csv_files)} playlists to sync")
        
        total_added = 0
        total_skipped = 0
        total_failed = 0
        
        with tqdm(
            total=len(csv_files),
            desc="Overall Progress",
            unit="playlist",
            leave=True,
            position=0,
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}",
            colour="blue"
        ) as outer_pbar:
            
            for csv_file in csv_files:
                playlist_name = csv_file.stem
                outer_pbar.set_postfix({"Current Playlist": playlist_name[:30]})
                
                added, skipped, failed = self.sync_playlist(str(csv_file))
                total_added += added
                total_skipped += skipped
                total_failed += failed
                
                outer_pbar.update(1)
        
        tqdm.write(f"\n{'='*50}")
        tqdm.write("SYNC COMPLETE - SUMMARY:")
        tqdm.write(f"  - Total playlists: {len(csv_files)}")
        tqdm.write(f"  - Total tracks added: {total_added}")
        tqdm.write(f"  - Total tracks skipped: {total_skipped}")
        tqdm.write(f"  - Total tracks failed: {total_failed}")
        tqdm.write(f"  - Log file: {LOG_FILE}")
        tqdm.write(f"{'='*50}")
    
    def __del__(self):
        """Save cache on destruction"""
        if hasattr(self, 'search_cache'):
            self._save_search_cache()


def main():
    parser = argparse.ArgumentParser(
        description="Sync Spotify playlists to YouTube Music",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sync a single playlist
  python sync_playlists.py playlist.csv
  
  # Sync all playlists in a folder
  python sync_playlists.py spotify_playlists/
  
  # Use custom OAuth file
  python sync_playlists.py -a my_oauth.json spotify_playlists/
        """
    )
    
    parser.add_argument(
        'path',
        help='Path to CSV file or folder containing CSV files'
    )
    
    parser.add_argument(
        '-a', '--auth',
        default='oauth.json',
        help='Path to OAuth authentication file (default: oauth.json)'
    )
    
    parser.add_argument(
        '-c', '--creds',
        default='oauth_creds.json',
        help='Path to OAuth credentials file (default: oauth_creds.json)'
    )
    
    args = parser.parse_args()
    
    # Create syncer instance
    syncer = PlaylistSyncer(auth_file=args.auth, creds_file=args.creds)
    
    # Authenticate
    syncer.authenticate()
    
    # Check if path is file or directory
    path = Path(args.path)
    
    if path.is_file() and path.suffix == '.csv':
        syncer.sync_playlist(str(path))
    elif path.is_dir():
        syncer.sync_folder(str(path))
    else:
        tqdm.write(f"Error: {args.path} is not a valid CSV file or directory")
        sys.exit(1)

if __name__ == "__main__":
    main()