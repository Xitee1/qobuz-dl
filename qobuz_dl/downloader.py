import logging
import os
from io import BytesIO
from typing import Tuple

import requests
from pathvalidate import sanitize_filename, sanitize_filepath
from tqdm import tqdm

import qobuz_dl.metadata as metadata
from qobuz_dl.color import OFF, GREEN, RED, YELLOW, CYAN
from qobuz_dl.exceptions import NonStreamable

QL_DOWNGRADE = "FormatRestrictedByFormatAvailability"

DEFAULT_TRACK_FORMAT = "{artist} - {album} - {trackname} ({year}) [{bit_depth}B-{sampling_rate}kHz]"
DEFAULT_ALBUM_FORMAT = "{artist}/{album} ({year})/{tracknumber}. {tracktitle} [{bit_depth}B-{sampling_rate}kHz]"
DEFAULT_PLAYLIST_FORMAT = "Playlists/{playlist}/{artist} - {album} - {trackname}"

logger = logging.getLogger(__name__)


class Download:
    def __init__(
        self,
        client,
        item_id: str,
        path: str,
        quality: int,
        embed_art: bool = False,
        albums_only: bool = False,
        downgrade_quality: bool = False,
        cover_og_quality: bool = False,
        no_cover: bool = False,
        track_format=DEFAULT_TRACK_FORMAT,
        album_format=DEFAULT_ALBUM_FORMAT,
        playlist_format=DEFAULT_PLAYLIST_FORMAT,
        playlist_name=None,
    ):
        self.client = client
        self.item_id = item_id
        self.path = path
        self.quality = quality
        self.albums_only = albums_only
        self.embed_art = embed_art
        self.downgrade_quality = downgrade_quality
        self.cover_og_quality = cover_og_quality
        self.no_cover = no_cover
        self.track_format = track_format
        self.album_format = album_format
        self.playlist_format = playlist_format
        self.playlist_name = playlist_name

    def download_id_by_type(self, track=True):
        if not track:
            self.download_release()
        else:
            self.download_track()

    def download_release(self):
        count = 0
        meta = self.client.get_album_meta(self.item_id)

        if not meta.get("streamable"):
            raise NonStreamable("This release is not streamable")

        if self.albums_only and (
            meta.get("release_type") != "album"
            or meta.get("artist").get("name") == "Various Artists"
        ):
            logger.info(f'{OFF}Ignoring Single/EP/VA: {meta.get("title", "n/a")}')
            return

        album_title = _get_title(meta)

        format_info = self._get_format(meta)
        file_format, quality_met, bit_depth, sampling_rate = format_info

        if not self.downgrade_quality and not quality_met:
            logger.info(
                f"{OFF}Skipping {album_title} as it doesn't meet quality requirement"
            )
            return

        logger.info(
            f"\n{YELLOW}Downloading: {album_title}\nQuality: {file_format}"
            f" ({bit_depth}/{sampling_rate})\n"
        )
        album_attr = self._get_album_attr(
            meta, album_title, file_format, bit_depth, sampling_rate
        )
        
        # Add playlist name tp available attributes if downloading as part of a playlist
        if self.playlist_name:
            album_attr["playlist"] = sanitize_filename(self.playlist_name)

        # Download cover: to memory if only embedding, to disk if keeping
        cover_image_data = None
        
        if self.no_cover and not self.embed_art:
            logger.info(f"{OFF}Skipping cover")
        elif self.no_cover and self.embed_art:
            # Download to memory only for embedding
            cover_image_data = _download_to_memory(meta["image"]["large"], og_quality=self.cover_og_quality)
        else:
            # Download to disk - extract first directory from album format for cover/goodies
            temp_format = self.album_format.split('/')[0] if '/' in self.album_format else self.album_format
            temp_sanitized = sanitize_filepath(temp_format.format(**album_attr))
            cover_dir = os.path.join(self.path, temp_sanitized)
            os.makedirs(cover_dir, exist_ok=True)
            _get_extra(meta["image"]["large"], cover_dir, og_quality=self.cover_og_quality)
            
            if "goodies" in meta:
                try:
                    _get_extra(meta["goodies"][0]["url"], cover_dir, "booklet.pdf")
                except:  # noqa
                    pass

        media_numbers = [track["media_number"] for track in meta["tracks"]["items"]]
        is_multiple = True if len([*{*media_numbers}]) > 1 else False
        for i in meta["tracks"]["items"]:
            parse = self.client.get_track_url(i["id"], fmt_id=self.quality)
            if "sample" not in parse and parse["sampling_rate"]:
                is_mp3 = True if int(self.quality) == 5 else False
                self._download_and_tag(
                    self.path,
                    count,
                    parse,
                    i,
                    meta,
                    False,
                    is_mp3,
                    i["media_number"] if is_multiple else None,
                    cover_image_data,
                    album_attr,
                    "album",
                )
            else:
                logger.info(f"{OFF}Demo. Skipping")
            count = count + 1
        logger.info(f"{GREEN}Completed")

    def download_track(self):
        parse = self.client.get_track_url(self.item_id, self.quality)

        if "sample" not in parse and parse["sampling_rate"]:
            meta = self.client.get_track_meta(self.item_id)
            track_title = _get_title(meta)
            artist = _safe_get(meta, "performer", "name")
            logger.info(f"\n{YELLOW}Downloading: {artist} - {track_title}")
            format_info = self._get_format(meta, is_track_id=True, track_url_dict=parse)
            file_format, quality_met, bit_depth, sampling_rate = format_info

            if not self.downgrade_quality and not quality_met:
                logger.info(
                    f"{OFF}Skipping {track_title} as it doesn't "
                    "meet quality requirement"
                )
                return
            track_attr = self._get_track_attr(
                meta, track_title, bit_depth, sampling_rate
            )
            
            # Add playlist name if available
            if self.playlist_name:
                track_attr["playlist"] = sanitize_filename(self.playlist_name)
            
            # Determine content type and format
            content_type = "playlist" if self.playlist_name else "track"
            
            # Download cover: to memory if only embedding, to disk if keeping
            cover_image_data = None
            if self.no_cover and not self.embed_art:
                logger.info(f"{OFF}Skipping cover")
            elif self.no_cover and self.embed_art:
                # Download to memory only for embedding
                cover_image_data = _download_to_memory(meta["album"]["image"]["large"], og_quality=self.cover_og_quality)
            else:
                # Download to disk - will be placed in first directory of format
                selected_format = self.playlist_format if self.playlist_name else self.track_format
                temp_format = selected_format.split('/')[0] if '/' in selected_format else ""
                if temp_format:
                    temp_sanitized = sanitize_filepath(temp_format.format(**track_attr))
                    cover_dir = os.path.join(self.path, temp_sanitized)
                else:
                    cover_dir = self.path
                os.makedirs(cover_dir, exist_ok=True)
                _get_extra(
                    meta["album"]["image"]["large"],
                    cover_dir,
                    og_quality=self.cover_og_quality,
                )
            is_mp3 = True if int(self.quality) == 5 else False
            self._download_and_tag(
                self.path,
                1,
                parse,
                meta,
                meta,
                True,
                is_mp3,
                False,
                cover_image_data,
                track_attr,
                content_type,
            )
        else:
            logger.info(f"{OFF}Demo. Skipping")
        logger.info(f"{GREEN}Completed")

    def _download_and_tag(
        self,
        root_dir,
        tmp_count,
        track_url_dict,
        track_metadata,
        album_or_track_metadata,
        is_track,
        is_mp3,
        multiple=None,
        cover_image_data=None,
        format_attr=None,
        content_type="album",
    ):
        extension = ".mp3" if is_mp3 else ".flac"

        try:
            url = track_url_dict["url"]
        except KeyError:
            logger.info(f"{OFF}Track not available for download")
            return

        if multiple:
            root_dir = os.path.join(root_dir, f"Disc {multiple}")
            os.makedirs(root_dir, exist_ok=True)

        filename = os.path.join(root_dir, f".{tmp_count:02}.tmp")

        # Determine the filename using the appropriate format
        track_title = _get_title(track_metadata)
        artist = _safe_get(track_metadata, "performer", "name")
        
        # Merge format_attr with track-specific attributes
        if format_attr is None:
            format_attr = {}
        
        filename_attr = self._get_filename_attr(artist, track_metadata, track_title, album_or_track_metadata)
        filename_attr.update(format_attr)
        
        # Select the appropriate format based on content type
        if content_type == "track":
            format_string = self.track_format
        elif content_type == "playlist":
            format_string = self.playlist_format
        else:  # album
            format_string = self.album_format
        
        # Build the full path from format string
        formatted_path = self._build_path_from_format(format_string, filename_attr, root_dir)
        final_file = formatted_path[:250] + extension

        if os.path.isfile(final_file):
            logger.info(f"{OFF}{track_title} was already downloaded")
            return

        # Ensure parent directory exists
        os.makedirs(os.path.dirname(final_file), exist_ok=True)

        tqdm_download(url, filename, track_title)
        tag_function = metadata.tag_mp3 if is_mp3 else metadata.tag_flac
        try:
            tag_function(
                filename,
                os.path.dirname(final_file),
                final_file,
                track_metadata,
                album_or_track_metadata,
                is_track,
                self.embed_art,
                cover_image_data,
            )
        except Exception as e:
            logger.error(f"{RED}Error tagging the file: {e}", exc_info=True)

    def _build_path_from_format(self, format_string, attributes, base_dir):
        """Build a file path from a format string that may contain / separators."""
        # Split by / and process each part
        parts = format_string.split('/')
        sanitized_parts = []
        
        for part in parts:
            formatted = part.format(**attributes)
            sanitized = sanitize_filename(formatted)
            sanitized_parts.append(sanitized)
        
        # Join all parts to create the full path
        if len(sanitized_parts) > 1:
            # Last part is the filename (without extension)
            dir_parts = sanitized_parts[:-1]
            filename = sanitized_parts[-1]
            full_dir = os.path.join(base_dir, *dir_parts)
            os.makedirs(full_dir, exist_ok=True)
            return os.path.join(full_dir, filename)
        else:
            # No subdirectories, just filename
            return os.path.join(base_dir, sanitized_parts[0])

    @staticmethod
    def _get_filename_attr(artist, track_metadata, track_title, album_or_track_metadata=None):
        """Build comprehensive attributes for formatting."""
        attrs = {
            "artist": sanitize_filename(artist),
            "albumartist": sanitize_filename(_safe_get(
                track_metadata, "album", "artist", "name", default=artist
            )),
            "bit_depth": track_metadata.get("maximum_bit_depth"),
            "sampling_rate": track_metadata.get("maximum_sampling_rate"),
            "tracktitle": sanitize_filename(track_title),
            "version": track_metadata.get("version", ""),
            "tracknumber": f"{track_metadata.get('track_number', 1):02}",
        }
        
        # Add album information if available
        if album_or_track_metadata:
            if "album" in track_metadata:
                attrs["album"] = sanitize_filename(track_metadata["album"].get("title", ""))
                attrs["year"] = track_metadata["album"].get("release_date_original", "").split("-")[0] if "release_date_original" in track_metadata["album"] else ""
            elif "title" in album_or_track_metadata:
                # This is album metadata itself
                attrs["album"] = sanitize_filename(album_or_track_metadata.get("title", ""))
                attrs["year"] = album_or_track_metadata.get("release_date_original", "").split("-")[0] if "release_date_original" in album_or_track_metadata else ""
        
        return attrs

    @staticmethod
    def _get_track_attr(meta, track_title, bit_depth, sampling_rate):
        """Get attributes for single track formatting."""
        artist = _safe_get(meta, "performer", "name", default="Unknown Artist")
        album_artist = _safe_get(meta, "album", "artist", "name", default=artist)
        
        return {
            "album": sanitize_filename(meta["album"]["title"]),
            "artist": sanitize_filename(artist),
            "albumartist": sanitize_filename(album_artist),
            "tracktitle": sanitize_filename(track_title),
            "year": meta["album"]["release_date_original"].split("-")[0] if "release_date_original" in meta["album"] else "",
            "bit_depth": bit_depth,
            "sampling_rate": sampling_rate,
            "tracknumber": f"{meta.get('track_number', 1):02}",
            "version": meta.get("version", ""),
        }

    @staticmethod
    def _get_album_attr(meta, album_title, file_format, bit_depth, sampling_rate):
        """Get attributes for album formatting."""
        return {
            "artist": sanitize_filename(meta["artist"]["name"]),
            "albumartist": sanitize_filename(meta["artist"]["name"]),
            "album": sanitize_filename(album_title),
            "year": meta["release_date_original"].split("-")[0] if "release_date_original" in meta else "",
            "format": file_format,
            "bit_depth": bit_depth,
            "sampling_rate": sampling_rate,
            "version": meta.get("version", ""),
        }

    def _get_format(self, item_dict, is_track_id=False, track_url_dict=None):
        quality_met = True
        if int(self.quality) == 5:
            return ("MP3", quality_met, None, None)
        track_dict = item_dict
        if not is_track_id:
            track_dict = item_dict["tracks"]["items"][0]

        try:
            new_track_dict = (
                self.client.get_track_url(track_dict["id"], fmt_id=self.quality)
                if not track_url_dict
                else track_url_dict
            )
            restrictions = new_track_dict.get("restrictions")
            if isinstance(restrictions, list):
                if any(
                    restriction.get("code") == QL_DOWNGRADE
                    for restriction in restrictions
                ):
                    quality_met = False

            return (
                "FLAC",
                quality_met,
                new_track_dict["bit_depth"],
                new_track_dict["sampling_rate"],
            )
        except (KeyError, requests.exceptions.HTTPError):
            return ("Unknown", quality_met, None, None)


def tqdm_download(url, fname, desc):
    r = requests.get(url, allow_redirects=True, stream=True)
    total = int(r.headers.get("content-length", 0))
    download_size = 0
    with open(fname, "wb") as file, tqdm(
        total=total,
        unit="iB",
        unit_scale=True,
        unit_divisor=1024,
        desc=desc,
        bar_format=CYAN + "{n_fmt}/{total_fmt} /// {desc}",
    ) as bar:
        for data in r.iter_content(chunk_size=1024):
            size = file.write(data)
            bar.update(size)
            download_size += size

    if total != download_size:
        # https://stackoverflow.com/questions/69919912/requests-iter-content-thinks-file-is-complete-but-its-not
        raise ConnectionError("File download was interrupted for " + fname)


def _get_description(item: dict, track_title, multiple=None):
    downloading_title = f"{track_title} "
    f'[{item["bit_depth"]}/{item["sampling_rate"]}]'
    if multiple:
        downloading_title = f"[Disc {multiple}] {downloading_title}"
    return downloading_title


def _get_title(item_dict):
    album_title = item_dict["title"]
    version = item_dict.get("version")
    if version:
        album_title = (
            f"{album_title} ({version})"
            if version.lower() not in album_title.lower()
            else album_title
        )
    return album_title


def _get_extra(item, dirn, extra="cover.jpg", og_quality=False):
    extra_file = os.path.join(dirn, extra)
    if os.path.isfile(extra_file):
        logger.info(f"{OFF}{extra} was already downloaded")
        return
    tqdm_download(
        item.replace("_600.", "_org.") if og_quality else item,
        extra_file,
        extra,
    )


def _download_to_memory(url, og_quality=False):
    """Download image to memory (BytesIO) instead of disk."""
    url = url.replace("_600.", "_org.") if og_quality else url
    r = requests.get(url, allow_redirects=True)
    r.raise_for_status()
    return BytesIO(r.content)


def _safe_get(d: dict, *keys, default=None):
    """A replacement for chained `get()` statements on dicts:
    >>> d = {'foo': {'bar': 'baz'}}
    >>> _safe_get(d, 'baz')
    None
    >>> _safe_get(d, 'foo', 'bar')
    'baz'
    """
    curr = d
    res = default
    for key in keys:
        res = curr.get(key, default)
        if res == default or not hasattr(res, "__getitem__"):
            return res
        else:
            curr = res
    return res
