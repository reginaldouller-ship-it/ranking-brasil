#!/usr/bin/env python3
"""
Scraper semanal: Spotify + YouTube Brasil (kworb.net)
Gera ranking combinado por soma de streams
"""

import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import re
import time
import os
import concurrent.futures

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RankingBrasilBot/1.0)",
    "Accept-Charset": "utf-8",
}

SPOTIFY_URL = "https://kworb.net/spotify/country/br_weekly.html"
YOUTUBE_URL = "https://kworb.net/youtube/insights/br.html"

SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET", "")


def fetch(url):
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.encoding = "utf-8"
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def parse_streams(text):
    cleaned = re.sub(r"[^\d]", "", text.strip())
    return int(cleaned) if cleaned else 0


def get_spotify_token():
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        return None
    try:
        r = requests.post(
            "https://accounts.spotify.com/api/token",
            data={"grant_type": "client_credentials"},
            auth=(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET),
            timeout=10,
        )
        r.raise_for_status()
        return r.json()["access_token"]
    except Exception:
        return None


def get_thumbnail(spotify_id):
    if not spotify_id:
        return ""
    try:
        r = requests.get(
            f"https://open.spotify.com/oembed?url=https://open.spotify.com/track/{spotify_id}",
            headers=HEADERS,
            timeout=10,
        )
        r.encoding = "utf-8"
        r.raise_for_status()
        return r.json().get("thumbnail_url", "")
    except Exception:
        return ""


def search_spotify_track(artist, title, token):
    if not token:
        return "", "", ""
    try:
        q = f"{artist} {title}"
        r = requests.get(
            "https://api.spotify.com/v1/search",
            params={"q": q, "type": "track", "market": "BR", "limit": 1},
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        r.encoding = "utf-8"
        r.raise_for_status()
        items = r.json().get("tracks", {}).get("items", [])
        if not items:
            return "", "", ""
        track = items[0]
        spotify_id = track["id"]
        spotify_url = f"https://open.spotify.com/track/{spotify_id}"
        thumbnail_url = get_thumbnail(spotify_id)
        return spotify_id, spotify_url, thumbnail_url
    except Exception:
        return "", "", ""


def get_genre(spotify_id, token):
    if not spotify_id or not token:
        return "outros"
    try:
        r = requests.get(
            f"https://api.spotify.com/v1/tracks/{spotify_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        r.encoding = "utf-8"
        r.raise_for_status()
        artists = r.json().get("artists", [])
        if not artists:
            return "outros"
        artist_id = artists[0]["id"]

        r2 = requests.get(
            f"https://api.spotify.com/v1/artists/{artist_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        r2.encoding = "utf-8"
        r2.raise_for_status()
        genres = r2.json().get("genres", [])
        return genres[0] if genres else "outros"
    except Exception:
        return "outros"


def scrape_spotify():
    print("📡 Buscando Spotify BR Weekly...")
    soup = fetch(SPOTIFY_URL)
    tracks = []
    rows = soup.select("table tr")[1:]
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 7:
            continue
        pos = cols[0].get_text(strip=True)
        if not pos.isdigit():
            continue
        cell = cols[2]
        links = cell.find_all("a", href=True)
        track_name = ""
        artist_name = ""
        spotify_id = ""
        for link in links:
            href = link["href"]
            if "/track/" in href:
                track_name = link.get_text(strip=True)
                match = re.search(r"/track/([a-zA-Z0-9]+)\.html", href)
                if match:
                    spotify_id = match.group(1)
            elif "/artist/" in href and not artist_name:
                artist_name = link.get_text(strip=True)
        if not track_name:
            continue
        streams = parse_streams(cols[6].get_text(strip=True))
        spotify_url = f"https://open.spotify.com/track/{spotify_id}" if spotify_id else ""
        tracks.append({
            "pos_spotify": int(pos),
            "artist": artist_name,
            "title": track_name,
            "streams_spotify": streams,
            "spotify_id": spotify_id,
            "spotify_url": spotify_url,
        })
        if int(pos) >= 200:
            break
    print(f"  ✅ {len(tracks)} músicas do Spotify")
    return tracks


def scrape_youtube():
    print("📡 Buscando YouTube BR Weekly...")
    soup = fetch(YOUTUBE_URL)
    tracks = []
    rows = soup.select("table tr")[1:]
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 7:
            continue
        pos = cols[0].get_text(strip=True)
        if not pos.isdigit():
            continue
        full_text = cols[2].get_text(strip=True)
        if " - " in full_text:
            parts = full_text.split(" - ", 1)
            artist_name = parts[0].strip()
            track_name = parts[1].strip()
        else:
            artist_name = ""
            track_name = full_text
        streams = parse_streams(cols[6].get_text(strip=True))
        tracks.append({
            "pos_youtube": int(pos),
            "artist": artist_name,
            "title": track_name,
            "streams_youtube": streams,
        })
        if int(pos) >= 100:
            break
    print(f"  ✅ {len(tracks)} músicas do YouTube")
    return tracks


def normalize(text):
    text = text.lower().strip()
    text = re.sub(r"\(.*?\)", "", text)   # remove parênteses
    text = re.sub(r"\s+", " ", text).strip()
    return text


def match_tracks(spotify_tracks, youtube_tracks):
    print("🔗 Combinando rankings...")
    combined = []
    matched_yt = set()

    for sp in spotify_tracks:
        sp_title = normalize(sp["title"])
        sp_artist = normalize(sp["artist"])
        best_match = None
        best_score = 0

        for i, yt in enumerate(youtube_tracks):
            if i in matched_yt:
                continue
            yt_title = normalize(yt["title"])
            yt_artist = normalize(yt["artist"])
            title_match = sp_title == yt_title or sp_title in yt_title or yt_title in sp_title
            artist_match = sp_artist in yt_artist or yt_artist in sp_artist
            score = (2 if title_match else 0) + (1 if artist_match else 0)
            if score >= 2 and score > best_score:
                best_score = score
                best_match = (i, yt)

        entry = {
            "artist": sp["artist"],
            "title": sp["title"],
            "spotify_url": sp["spotify_url"],
            "spotify_id": sp["spotify_id"],
            "pos_spotify": sp["pos_spotify"],
            "streams_spotify": sp["streams_spotify"],
            "pos_youtube": None,
            "streams_youtube": 0,
            "in_both": False,
        }

        if best_match:
            idx, yt = best_match
            matched_yt.add(idx)
            entry["pos_youtube"] = yt["pos_youtube"]
            entry["streams_youtube"] = yt["streams_youtube"]
            entry["in_both"] = True

        entry["total_streams"] = entry["streams_spotify"] + entry["streams_youtube"]
        combined.append(entry)

    for i, yt in enumerate(youtube_tracks):
        if i not in matched_yt:
            combined.append({
                "artist": yt["artist"],
                "title": yt["title"],
                "spotify_url": "",
                "spotify_id": "",
                "pos_spotify": None,
                "streams_spotify": 0,
                "pos_youtube": yt["pos_youtube"],
                "streams_youtube": yt["streams_youtube"],
                "in_both": False,
                "total_streams": yt["streams_youtube"],
            })

    combined.sort(key=lambda x: x["total_streams"], reverse=True)
    for i, e in enumerate(combined):
        e["rank"] = i + 1

    in_both = sum(1 for e in combined if e["in_both"])
    print(f"  ✅ {in_both} músicas em ambas as plataformas")
    return combined


def run():
    print(f"\n🎵 Ranking Brasil Semanal — {datetime.now().strftime('%d/%m/%Y %H:%M')}\n")
    os.makedirs("data", exist_ok=True)
    spotify_tracks = scrape_spotify()
    time.sleep(2)
    youtube_tracks = scrape_youtube()
    ranking = match_tracks(spotify_tracks, youtube_tracks)

    token = get_spotify_token()
    if token:
        print("🔑 Token Spotify obtido com sucesso")
    else:
        print("⚠️  Sem credenciais Spotify — thumbnails e gêneros serão limitados")

    # Passo 5a: thumbnails para tracks do Spotify chart
    spotify_chart_tracks = [e for e in ranking if e["spotify_id"]]
    print(f"🖼  Buscando thumbnails para {len(spotify_chart_tracks)} músicas do Spotify...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        thumb_futures = {pool.submit(get_thumbnail, e["spotify_id"]): e for e in spotify_chart_tracks}
        for future in concurrent.futures.as_completed(thumb_futures):
            entry = thumb_futures[future]
            entry["thumbnail_url"] = future.result()

    # Passo 5b: buscar Spotify para tracks só no YouTube
    yt_only_tracks = [e for e in ranking if not e["in_both"] and e["pos_spotify"] is None]
    if token and yt_only_tracks:
        print(f"🔍 Buscando links Spotify para {len(yt_only_tracks)} músicas só no YouTube...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            search_futures = {
                pool.submit(search_spotify_track, e["artist"], e["title"], token): e
                for e in yt_only_tracks
            }
            for future in concurrent.futures.as_completed(search_futures):
                entry = search_futures[future]
                spotify_id, spotify_url, thumbnail_url = future.result()
                entry["spotify_id"] = spotify_id
                entry["spotify_url"] = spotify_url
                entry["thumbnail_url"] = thumbnail_url
    else:
        for e in yt_only_tracks:
            if "thumbnail_url" not in e:
                e["thumbnail_url"] = ""

    # Garantir thumbnail_url em todos os entries sem ele
    for e in ranking:
        e.setdefault("thumbnail_url", "")

    # Passo 6: buscar gêneros para todos os tracks com spotify_id
    tracks_with_id = [e for e in ranking if e.get("spotify_id")]
    if token and tracks_with_id:
        print(f"🎼 Buscando gêneros para {len(tracks_with_id)} músicas...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            genre_futures = {
                pool.submit(get_genre, e["spotify_id"], token): e
                for e in tracks_with_id
            }
            for future in concurrent.futures.as_completed(genre_futures):
                entry = genre_futures[future]
                entry["genre"] = future.result()

    # Tracks sem spotify_id ficam com "outros"
    for e in ranking:
        e.setdefault("genre", "outros")

    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "week_label": f"Semana de {datetime.now().strftime('%d/%m/%Y')}",
        "tracks": ranking,
    }
    with open("data/ranking.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n✅ data/ranking.json gerado — {len(ranking)} músicas")
    if ranking:
        print(f"🥇 #1: {ranking[0]['artist']} — {ranking[0]['title']} ({ranking[0]['total_streams']:,} streams)")


if __name__ == "__main__":
    run()
