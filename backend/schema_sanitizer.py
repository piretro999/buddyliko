"""
Buddyliko Schema Sanitizer — Gatekeeper
────────────────────────────────────────
Strips everything from uploaded schema ZIPs that is NOT a schema definition.
A 50 GB UBL ZIP becomes ~2 MB after sanitization.

Whitelist approach: ONLY explicitly allowed file types survive.
Everything else is deleted. No mercy.

Usage:
    from schema_sanitizer import sanitize_zip, sanitize_directory

    # From ZIP file → clean directory
    clean_dir, stats = sanitize_zip("/tmp/upload.zip", "/tmp/clean/")

    # Or clean an already-extracted directory
    stats = sanitize_directory("/tmp/extracted/")
"""
import os
import zipfile
import shutil
import time
from dataclasses import dataclass, field
from typing import Tuple, Optional, Set

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# WHITELIST — ONLY these extensions survive
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SCHEMA_EXTENSIONS: Set[str] = {
    # XML Schema
    '.xsd',
    # Schematron
    '.sch', '.schematron',
    # XML samples, catalogs, instances
    '.xml',
    # XSLT transformations
    '.xsl', '.xslt',
    # JSON Schema
    '.json',
    # RELAX NG
    '.rng', '.rnc',
    # DTD
    '.dtd',
    # ASN.1 (for HL7/telecom)
    '.asn', '.asn1',
    # EDI definitions
    '.edi', '.seg', '.ele',
    # CSV code lists (small)
    '.csv',
    # WSDL
    '.wsdl',
    # Catalog files
    '.cat',
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BLACKLIST DIRECTORIES — deleted entirely, not even opened
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLACKLIST_DIRS: Set[str] = {
    '__macosx', '.git', '.svn', '.hg', '.bzr',
    'node_modules', '.gradle', '.mvn', 'target', 'build',
    '__pycache__', '.idea', '.vscode', '.eclipse',
    'bin', 'lib', 'dist', 'out',
    'javadoc', 'apidoc', 'docs', 'documentation',
    'doc', 'site', 'gh-pages',
    'images', 'img', 'icons', 'figures', 'artwork',
    'test', 'tests', 'spec', 'specs', 'examples',
    'src', 'java', 'main', 'resources',  # Java project structure bloat
    '.settings', '.project',
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BLACKLIST FILES — specific filenames always removed
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLACKLIST_FILES: Set[str] = {
    '.ds_store', 'thumbs.db', 'desktop.ini',
    '.gitignore', '.gitattributes', '.editorconfig',
    'makefile', 'rakefile', 'gemfile', 'gemfile.lock',
    'pom.xml', 'build.xml', 'build.gradle', 'settings.gradle',
    'package.json', 'package-lock.json', 'yarn.lock',
    'readme', 'readme.md', 'readme.txt', 'readme.html',
    'license', 'license.md', 'license.txt', 'licence',
    'changelog', 'changelog.md', 'changes.txt',
    'contributing.md', 'authors', 'notice', 'notice.txt',
}

# Max file size: 10 MB per file (schema files are small)
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB

# Max total size after cleaning: 100 MB
MAX_TOTAL_SIZE = 100 * 1024 * 1024  # 100 MB


@dataclass
class SanitizeStats:
    """Statistics from sanitization."""
    files_kept: int = 0
    files_removed: int = 0
    dirs_removed: int = 0
    bytes_before: int = 0
    bytes_after: int = 0
    files_too_large: int = 0
    time_seconds: float = 0.0
    kept_extensions: dict = field(default_factory=dict)
    removed_extensions: dict = field(default_factory=dict)

    @property
    def reduction_pct(self) -> float:
        if self.bytes_before == 0:
            return 0.0
        return (1 - self.bytes_after / self.bytes_before) * 100

    def summary(self) -> str:
        return (
            f"🛡️ Sanitizer: {self.files_kept} kept, {self.files_removed} removed, "
            f"{self.dirs_removed} dirs purged | "
            f"{_human_size(self.bytes_before)} → {_human_size(self.bytes_after)} "
            f"(-{self.reduction_pct:.1f}%) | {self.time_seconds:.1f}s"
        )


def _human_size(nbytes: int) -> str:
    for unit in ('B', 'KB', 'MB', 'GB'):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"


def _is_blacklisted_dir(dirname: str) -> bool:
    """Check if directory name is blacklisted."""
    return dirname.lower() in BLACKLIST_DIRS or dirname.startswith('.')


def _should_keep(filepath: str, filename: str) -> Tuple[bool, str]:
    """
    Decide if a file should be kept. Whitelist approach.

    Returns:
        (keep: bool, reason: str)
    """
    name_lower = filename.lower()
    ext = os.path.splitext(name_lower)[1]

    # Blacklisted filenames
    name_no_ext = os.path.splitext(name_lower)[0]
    if name_lower in BLACKLIST_FILES or name_no_ext in BLACKLIST_FILES:
        return False, "blacklisted_filename"

    # Hidden files
    if filename.startswith('.'):
        return False, "hidden_file"

    # Must have an extension
    if not ext:
        return False, "no_extension"

    # Whitelist check
    if ext not in SCHEMA_EXTENSIONS:
        return False, f"extension_{ext}"

    # Size check
    try:
        size = os.path.getsize(filepath)
        if size > MAX_FILE_SIZE:
            return False, f"too_large_{_human_size(size)}"
        if size == 0:
            return False, "empty_file"
    except OSError:
        return False, "unreadable"

    return True, "ok"


def sanitize_directory(directory: str) -> SanitizeStats:
    """
    Clean a directory in-place. Remove everything that's not a schema file.

    Args:
        directory: Path to the extracted schema directory

    Returns:
        SanitizeStats with details of what was removed
    """
    t0 = time.time()
    stats = SanitizeStats()

    # Calculate size before
    for root, dirs, files in os.walk(directory):
        for f in files:
            try:
                stats.bytes_before += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass

    # Phase 1: Remove blacklisted directories entirely
    for root, dirs, files in os.walk(directory, topdown=True):
        to_remove = []
        for d in dirs:
            if _is_blacklisted_dir(d):
                dir_path = os.path.join(root, d)
                try:
                    shutil.rmtree(dir_path)
                    stats.dirs_removed += 1
                except OSError:
                    pass
                to_remove.append(d)
        # Modify dirs in-place to prevent os.walk from descending
        for d in to_remove:
            dirs.remove(d)

    # Phase 2: Remove files that don't pass whitelist
    for root, dirs, files in os.walk(directory, topdown=False):
        for filename in files:
            filepath = os.path.join(root, filename)
            keep, reason = _should_keep(filepath, filename)

            if keep:
                stats.files_kept += 1
                ext = os.path.splitext(filename.lower())[1]
                stats.kept_extensions[ext] = stats.kept_extensions.get(ext, 0) + 1
                try:
                    stats.bytes_after += os.path.getsize(filepath)
                except OSError:
                    pass
            else:
                ext = os.path.splitext(filename.lower())[1] or "(none)"
                stats.removed_extensions[ext] = stats.removed_extensions.get(ext, 0) + 1
                if reason.startswith("too_large"):
                    stats.files_too_large += 1
                try:
                    os.remove(filepath)
                except OSError:
                    pass
                stats.files_removed += 1

    # Phase 3: Remove empty directories (bottom-up)
    for root, dirs, files in os.walk(directory, topdown=False):
        for d in dirs:
            dir_path = os.path.join(root, d)
            try:
                if os.path.exists(dir_path) and not os.listdir(dir_path):
                    os.rmdir(dir_path)
                    stats.dirs_removed += 1
            except OSError:
                pass

    # Check total size limit
    if stats.bytes_after > MAX_TOTAL_SIZE:
        print(f"⚠️ Schema still {_human_size(stats.bytes_after)} after cleaning (limit {_human_size(MAX_TOTAL_SIZE)})")

    stats.time_seconds = time.time() - t0
    return stats


def sanitize_zip(
    zip_path: str,
    extract_to: str,
    delete_zip: bool = True,
) -> Tuple[str, SanitizeStats]:
    """
    Extract ZIP, sanitize, optionally delete the ZIP.

    Args:
        zip_path: Path to the uploaded .zip file
        extract_to: Directory where cleaned content will be placed
        delete_zip: If True, delete the ZIP after extraction

    Returns:
        (clean_directory_path, stats)
    """
    os.makedirs(extract_to, exist_ok=True)

    # Extract
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(extract_to)

    # Sanitize
    stats = sanitize_directory(extract_to)

    # Delete original ZIP
    if delete_zip:
        try:
            os.remove(zip_path)
        except OSError:
            pass

    print(stats.summary())
    return extract_to, stats


def sanitize_zip_streaming(
    zip_path: str,
    extract_to: str,
    delete_zip: bool = True,
) -> Tuple[str, SanitizeStats]:
    """
    Streaming sanitization: extract ONLY whitelisted files from ZIP.
    Never writes bloat to disk. Ideal for huge ZIPs (50 GB+).

    This is the preferred method for large files because it:
    - Never extracts unwanted files (saves I/O and disk space)
    - Checks file size from ZIP metadata before extracting
    - Skips blacklisted directories entirely
    """
    t0 = time.time()
    stats = SanitizeStats()
    os.makedirs(extract_to, exist_ok=True)

    with zipfile.ZipFile(zip_path, 'r') as zf:
        for info in zf.infolist():
            # Skip directories
            if info.is_dir():
                continue

            stats.bytes_before += info.file_size
            filepath = info.filename
            filename = os.path.basename(filepath)

            # Check if any path component is blacklisted
            parts = filepath.replace('\\', '/').split('/')
            if any(_is_blacklisted_dir(p) for p in parts[:-1]):
                stats.files_removed += 1
                stats.dirs_removed += 1  # Approximate
                ext = os.path.splitext(filename.lower())[1] or "(none)"
                stats.removed_extensions[ext] = stats.removed_extensions.get(ext, 0) + 1
                continue

            # Check filename
            name_lower = filename.lower()
            ext = os.path.splitext(name_lower)[1]

            # Quick reject: no extension, hidden, blacklisted name
            if not ext or filename.startswith('.') or name_lower in BLACKLIST_FILES:
                stats.files_removed += 1
                stats.removed_extensions[ext or "(none)"] = stats.removed_extensions.get(ext or "(none)", 0) + 1
                continue

            # Whitelist check
            if ext not in SCHEMA_EXTENSIONS:
                stats.files_removed += 1
                stats.removed_extensions[ext] = stats.removed_extensions.get(ext, 0) + 1
                continue

            # Size check (from ZIP metadata, before extracting)
            if info.file_size > MAX_FILE_SIZE:
                stats.files_removed += 1
                stats.files_too_large += 1
                continue

            if info.file_size == 0:
                stats.files_removed += 1
                continue

            # ✅ File passes all checks — extract it
            # Preserve directory structure
            dest_path = os.path.join(extract_to, filepath)

            # Zip-slip protection: resolved path MUST stay inside extract_to
            real_dest = os.path.realpath(dest_path)
            real_base = os.path.realpath(extract_to)
            if not real_dest.startswith(real_base + os.sep) and real_dest != real_base:
                print(f"⛔ Zip-slip blocked: {filepath}")
                stats.files_removed += 1
                continue

            os.makedirs(os.path.dirname(dest_path), exist_ok=True)

            try:
                with zf.open(info) as src, open(dest_path, 'wb') as dst:
                    shutil.copyfileobj(src, dst)
                stats.files_kept += 1
                stats.bytes_after += info.file_size
                stats.kept_extensions[ext] = stats.kept_extensions.get(ext, 0) + 1
            except Exception as e:
                print(f"⚠️ Failed to extract {filepath}: {e}")
                stats.files_removed += 1

    # Delete original ZIP
    if delete_zip:
        try:
            os.remove(zip_path)
        except OSError:
            pass

    stats.time_seconds = time.time() - t0
    return extract_to, stats


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI test
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python schema_sanitizer.py <path.zip> [output_dir]")
        sys.exit(1)

    src = sys.argv[1]
    dst = sys.argv[2] if len(sys.argv) > 2 else "/tmp/sanitized_schema"

    if src.endswith('.zip'):
        clean_dir, stats = sanitize_zip_streaming(src, dst, delete_zip=False)
    else:
        stats = sanitize_directory(src)
        clean_dir = src

    print(f"\n{stats.summary()}")
    print(f"\nKept by extension: {stats.kept_extensions}")
    print(f"Removed by extension: {stats.removed_extensions}")
    print(f"\nClean output: {clean_dir}")
