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
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher
from ytmusicapi import YTMusic, OAuthCredentials

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)

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

    def _load_search_cache(self) -> Dict[str, Dict]:
        """Load search results cache"""
        cache_file = Path(self.cache_file)
        if cache_file.exists():
            try:
                with open(cache_file, 'rb') as f:
                    return pickle.load(f)
            except Exception:
                pass
        return {}
    
    def _save_search_cache(self):
        """Save search results cache"""
        try:
            with open(self.cache_file, 'wb') as f:
                pickle.dump(self.search_cache, f)
        except Exception as e:
            print(f"Warning: Could not save search cache: {e}")

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
                print(f"Error loading OAuth credentials: {str(e)}")
        return None
    
    def _save_oauth_credentials(self, client_id: str, client_secret: str):
        """Save OAuth credentials to file"""
        try:
            with open(self.creds_file, 'w') as f:
                json.dump({
                    'client_id': client_id,
                    'client_secret': client_secret
                }, f, indent=2)
            print(f"OAuth credentials saved to {self.creds_file}")
        except Exception as e:
            print(f"Error saving OAuth credentials: {str(e)}")
    
    def _setup_oauth(self):
        """Interactive OAuth setup"""
        print("\n" + "="*50)
        print("YouTube Music OAuth Setup")
        print("="*50)
        print("\nYou need to set up OAuth authentication.")
        print("Follow the guide at: https://ytmusicapi.readthedocs.io/en/stable/setup/oauth.html")
        
        # Check if credentials already exist
        creds = self._load_oauth_credentials()
        if creds:
            return creds['client_id'], creds['client_secret']
        
        # Get credentials from user
        client_id = input("Enter your OAuth client_id: ").strip()
        client_secret = input("Enter your OAuth client_secret: ").strip()
        
        if not client_id or not client_secret:
            print("Client ID and secret are required!")
            return None, None
        
        self._save_oauth_credentials(client_id, client_secret)
        
        return client_id, client_secret
    
    def authenticate(self):
        """Handle YouTube Music authentication"""
        # First, check if we have OAuth credentials
        creds = self._load_oauth_credentials()
        
        if os.path.exists(self.auth_file) and creds:
            # Try to authenticate with existing files
            print("Found existing authentication files.")
            try:
                self.ytmusic = YTMusic(
                    self.auth_file,
                    oauth_credentials=OAuthCredentials(
                        client_id=creds['client_id'],
                        client_secret=creds['client_secret']
                    )
                )
                print("Successfully authenticated with YouTube Music!")
                return
            except Exception as e:
                print(f"Failed to authenticate with existing files: {str(e)}")
                print("Will proceed with new authentication...")
        
        # Need to set up OAuth
        if not creds:
            client_id, client_secret = self._setup_oauth()
            if not client_id or not client_secret:
                print("OAuth setup cancelled.")
                sys.exit(1)
        else:
            client_id = creds['client_id']
            client_secret = creds['client_secret']
        
        # Run the OAuth flow
        print("Running ytmusicapi oauth with provided credentials...")
        
        # Run ytmusicapi oauth with credentials
        YTMusic.setup_oauth(filepath=self.auth_file, oauth_credentials=OAuthCredentials(
            client_id=client_id,
            client_secret=client_secret
        ))
        
        # Check if oauth.json was created
        if os.path.exists(self.auth_file):
            self.ytmusic = YTMusic(self.auth_file, oauth_credentials=OAuthCredentials(
                    client_id=client_id,
                    client_secret=client_secret
                )
            )
            print("Authentication successful!")
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
                        print(f"Error parsing track in {csv_path}: {str(e)}")
        except Exception as e:
            print(f"Error reading CSV file {csv_path}: {str(e)}")
        
        return tracks
    
    def search_track(self, track: Track, playlist_name: str = None) -> Optional[Dict]:
        """Search for a track on YouTube Music"""
        # Primary search: "Artist - Track Name"
        primary_artist = track.artists[0] if track.artists else "Unknown"
        search_query = f"{primary_artist} - {track.name}"
        
        try:
            results = self.ytmusic.search(search_query, filter="songs", limit=5)
            
            if not results:
                return None
            
            # Try to find best match with confidence scoring
            best_match = None
            best_score = 0
            
            for result in results:
                score = self._calculate_match_score(track, result)
                if score > best_score:
                    best_score = score
                    best_match = result
            
            # Require at least 70% confidence
            if best_score >= 0.7:
                return best_match
            else:
                print(f"Low confidence match for '{track.name}' by {primary_artist} (score: {best_score:.2f})")
                if playlist_name:
                    self._log_failed_track(track, playlist_name, f"Low confidence match (score: {best_score:.2f})")
                return None
                
        except Exception as e:
            print(f"Search error for '{track.name}': {str(e)}")
            return None

    def search_track_cached(self, track: Track, playlist_name: str = None) -> Optional[Dict]:
        """Search with caching"""
        # Create cache key
        cache_key = f"{track.artists[0] if track.artists else 'Unknown'} - {track.name}".lower()
        
        # Check cache first
        if cache_key in self.search_cache:
            cached_result = self.search_cache[cache_key]
            if cached_result:  # Don't cache failures
                return cached_result
        
        # Search and cache result
        result = self.search_track(track, playlist_name)
        if result:
            self.search_cache[cache_key] = result
        
        return result
    
    def _calculate_match_score(self, track: Track, yt_result: Dict) -> float:
        """Calculate confidence score for a YouTube Music search result"""
        score = 0.0
        
        # Check track name similarity
        yt_title = yt_result.get('title', '').lower()
        track_name = track.name.lower()
        
        if track_name in yt_title or yt_title in track_name:
            score += 0.4
        elif any(word in yt_title for word in track_name.split() if len(word) > 3):
            score += 0.2
        
        # Check artist match
        yt_artists = []
        if 'artists' in yt_result:
            yt_artists = [a.get('name', '').lower() for a in yt_result['artists']]
        
        track_artists = [a.lower() for a in track.artists]
        
        for track_artist in track_artists:
            if any(track_artist in yt_artist or yt_artist in track_artist 
                   for yt_artist in yt_artists):
                score += 0.4
                break
        
        # Check duration match (within 10 seconds)
        if 'duration_seconds' in yt_result and track.duration_ms > 0:
            duration_diff = abs(yt_result['duration_seconds'] - (track.duration_ms / 1000))
            if duration_diff <= 10:
                score += 0.2
            elif duration_diff <= 30:
                score += 0.1
        
        return score
    
    def _calculate_string_similarity(self, str1: str, str2: str) -> float:
        """Calculate similarity between two strings using Levenshtein distance"""
        return SequenceMatcher(None, str1, str2).ratio()

    def get_or_create_playlist(self, playlist_name: str) -> Optional[str]:
        """Get existing playlist or create new one"""
        try:
            # Get user's playlists
            playlists = self.ytmusic.get_library_playlists(limit=None)
            
            # Check if playlist already exists
            for playlist in playlists:
                if playlist['title'] == playlist_name:
                    print(f"Found existing playlist: {playlist_name}")
                    return playlist['playlistId']
            
            # Create new playlist
            playlist_id = self.ytmusic.create_playlist(
                title=playlist_name,
                description=f"Synced from Spotify on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
            print(f"Created new playlist: {playlist_name}")
            return playlist_id
            
        except Exception as e:
            print(f"Error managing playlist '{playlist_name}': {str(e)}")
            return None
    
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
            print(f"Error getting playlist tracks: {str(e)}")
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
        """Optimized sync with batch processing and better duplicate detection"""
        playlist_name = Path(csv_path).stem
        print(f"\n{'='*50}")
        print(f"Syncing playlist: {playlist_name}")
        print(f"{'='*50}")
        
        # Read tracks from CSV
        tracks = self.read_csv_playlist(csv_path)
        if not tracks:
            print(f"No tracks found in {csv_path}")
            return 0, 0, 0
        
        print(f"Found {len(tracks)} tracks in CSV")
        
        # Get or create playlist
        playlist_id = self.get_or_create_playlist(playlist_name)
        if not playlist_id:
            print(f"Failed to get/create playlist: {playlist_name}")
            return 0, 0, len(tracks)
        
        # Get existing tracks with full metadata
        existing_tracks = self.get_playlist_tracks(playlist_id)
        print(f"Playlist currently has {len(existing_tracks)} tracks")
        
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
                print(f"Already in playlist: {track.name}")
                continue
                
            tracks_to_process.append(track)
        
        print(f"Need to search and add {len(tracks_to_process)} tracks")
        
        if not tracks_to_process:
            return 0, len(tracks) - len(tracks_to_process), 0
        
        # Process tracks in batches
        added = 0
        failed = 0
        video_ids_to_add = []
        
        for i, track in enumerate(tracks_to_process, 1):
            print(f"Searching {i}/{len(tracks_to_process)}: {track.name}")
            
            yt_track = self.search_track_cached(track, playlist_name)
            if yt_track:
                video_ids_to_add.append(yt_track['videoId'])
            else:
                self._log_failed_track(track, playlist_name, "Track not found on YouTube Music")
                failed += 1
            
            # Add tracks in batches of 50 or at the end
            if len(video_ids_to_add) >= 50 or i == len(tracks_to_process):
                if video_ids_to_add:
                    batch_tracks = tracks_to_process[i-len(video_ids_to_add):i] if i <= len(tracks_to_process) else []
                    
                    try:
                        self.ytmusic.add_playlist_items(playlist_id, video_ids_to_add)
                        added += len(video_ids_to_add)
                        print(f"✓ Added batch of {len(video_ids_to_add)} tracks")
                    except Exception:
                        # Fallback: try adding tracks individually
                        for j, video_id in enumerate(video_ids_to_add):
                            try:
                                self.ytmusic.add_playlist_items(playlist_id, [video_id])
                                added += 1
                            except Exception as individual_e:
                                track_idx = i - len(video_ids_to_add) + j
                                if track_idx < len(tracks_to_process):
                                    self._log_failed_track(
                                        tracks_to_process[track_idx], 
                                        playlist_name, 
                                        f"Failed to add to playlist: {str(individual_e)}"
                                    )
                                failed += 1
                    
                    video_ids_to_add.clear()
            
            # Rate limiting for searches
            time.sleep(0.25)
        
        skipped = len(tracks) - len(tracks_to_process)
        
        print(f"\nPlaylist '{playlist_name}' sync complete:")
        print(f"  - Added: {added}")
        print(f"  - Skipped: {skipped}")
        print(f"  - Failed: {failed}")
        
        return added, skipped, failed
    
    def sync_folder(self, folder_path: str):
        """Sync all CSV files in a folder"""
        csv_files = list(Path(folder_path).glob("*.csv"))
        
        if not csv_files:
            print(f"No CSV files found in {folder_path}")
            return
        
        print(f"Found {len(csv_files)} playlists to sync")
        
        total_added = 0
        total_skipped = 0
        total_failed = 0
        
        for csv_file in csv_files:
            added, skipped, failed = self.sync_playlist(str(csv_file))
            total_added += added
            total_skipped += skipped
            total_failed += failed
        
        print(f"\n{'='*50}")
        print("SYNC COMPLETE - SUMMARY:")
        print(f"  - Total playlists: {len(csv_files)}")
        print(f"  - Total tracks added: {total_added}")
        print(f"  - Total tracks skipped: {total_skipped}")
        print(f"  - Total tracks failed: {total_failed}")
        print(f"{'='*50}")
    
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
        print(f"Error: {args.path} is not a valid CSV file or directory")
        sys.exit(1)

if __name__ == "__main__":
    main()