# Spotify to YouTube Music Playlist Sync

This tool syncs your Spotify playlists (exported as CSV from [Exportify](https://exportify.app/)) to YouTube Music using the [ytmusicapi](https://ytmusicapi.readthedocs.io/).

## Features

- Sync a single playlist or all playlists in a folder.
- Avoids duplicates.
- Handles authentication with YouTube Music using OAuth.

## Setup

1. **Clone this repository**

2. **Create and activate a virtual environment:**

   ```sh
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies:**

   ```sh
   pip install -r requirements.txt
   ```

4. **Obtain YouTube Music OAuth credentials:**
   - Follow the gude in [ytmusicapi.readthedocs.io](https://ytmusicapi.readthedocs.io/en/stable/setup/oauth.html). Otherwise you can follow the steps of this [video](https://www.youtube.com/watch?v=LLAZUTbc97I), but at the end instead of creating and API, we want to creat an OAuth Client ID. **Make sure to use as application type TVs and Limited Input devices**.

## Usage

1. **Export your Spotify playlists as CSV:**
Use [Exportify](https://exportify.app/) to download your playlists.

2. Sync a single playlist or Sync all playlists in a folder

```sh
python spotify-to-ytmusic.py path/to/playlist.csv

python spotify-to-ytmusic.py path/to/folder/
```

- The first run will prompt you to complete the YouTube Music OAuth flow in your browser.
- Your credentials are saved in `oauth_creds.json` and tokens in `oauth.json`.
- Failed sync of tracks are reported in the logs.

## Troubleshooting

- For API errors, ensure your credentials are correct and you have enabled the YouTube Data API v3 in your Google Cloud project.
- oauth access "code" for the device: https://github.com/sigma67/ytmusicapi/discussions/775