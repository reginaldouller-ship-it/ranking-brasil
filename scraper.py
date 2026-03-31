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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RankingBrasilBot/1.0)"
}

SPOTIFY_URL = "https://kworb.net/spotify/country/br_weekly.html"
YOUTUBE_URL = "https://kworb.net/youtube/insights/br.html"

def fetch(url):
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def parse_streams(text):
    cleaned = re.sub(r"[^\d]", "", text.strip())
    return int(cleaned) if cleaned else 0

def scrape_spotify():
    print("Buscando Spotify BR Weekly...")
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
        if int(pos) >= 50:
            break
    print(f"  {len(tracks)} musicas do Spotify")
    return tracks

def scrape_youtube():
    print("Buscando YouTube BR Weekly...")
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
        if int(pos) >= 50:
            break
    print(f"  {len(tracks)} musicas do YouTube")
    return tracks

def normalize(text):
    text = text.lower().strip()
    text = re.sub(r"\(.*?\)", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def match_tracks(spotify_tracks, youtube_tracks):
    print("Combinando rankings...")
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
    print(f"  {in_both} musicas em ambas as plataformas")
    return combined

def run():
    print(f"Ranking Brasil Semanal - {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    os.makedirs("data", exist_ok=True)
    spotify_tracks = scrape_spotify()
    time.sleep(2)
    youtube_tracks = scrape_youtube()
    ranking = match_tracks(spotify_tracks, youtube_tracks)
    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "week_label": f"Semana de {datetime.now().strftime('%d/%m/%Y')}",
        "tracks": ranking[:100]
    }
    with open("data/ranking.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"data/ranking.json gerado - {len(ranking)} musicas")
    if ranking:
        print(f"#1: {ranking[0]['artist']} - {ranking[0]['title']} ({ranking[0]['total_streams']:,} streams)")

if __name__ == "__main__":
    run()
