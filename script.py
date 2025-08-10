import os
import sys
import argparse
import requests
import aiohttp
import asyncio
from tqdm import tqdm
import re

TOKEN = os.getenv("DISCORD_BOT_TOKEN")
CHANNEL_ID = "1404048550655950908"
CHUNK_SIZE = 10 * 1024 * 1024
MAX_CONCURRENT_UPLOADS = 3
MAX_RETRIES = 5
MAX_MESSAGES_TO_FETCH = 100

def clean_filename_part(filename):
    return re.sub(r"\.part\d+$", "", filename)

def upload_sync(path):
    if not os.path.isfile(path):
        print(f"File '{path}' does not exist.")
        return
    url = f"https://discord.com/api/v10/channels/{CHANNEL_ID}/messages"
    headers = {"Authorization": f"Bot {TOKEN}"}
    part = 1
    file_size = os.path.getsize(path)
    total_parts = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
    with open(path, "rb") as f, tqdm(total=total_parts, desc="Uploading", unit="part") as pbar:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            filename = f"{os.path.basename(path)}.part{part}"
            files = {"file": (filename, chunk)}
            r = requests.post(url, headers=headers, files=files)
            if r.status_code == 200:
                pbar.update(1)
            else:
                print(f"\nError on part {part}: {r.status_code} - {r.text}")
                break
            part += 1

async def upload_chunk(session, path, chunk, part, pbar, semaphore):
    url = f"https://discord.com/api/v10/channels/{CHANNEL_ID}/messages"
    headers = {"Authorization": f"Bot {TOKEN}"}
    filename = f"{os.path.basename(path)}.part{part}"
    data = aiohttp.FormData()
    data.add_field("file", chunk, filename=filename)

    retry_count = 0
    while retry_count < MAX_RETRIES:
        async with semaphore:
            async with session.post(url, headers=headers, data=data) as resp:
                if resp.status == 200:
                    pbar.update(1)
                    return
                elif resp.status == 429:
                    retry_after = 0.5
                    try:
                        j = await resp.json()
                        retry_after = j.get("retry_after", 0.5)
                    except Exception:
                        pass
                    print(f"\nRate limited on part {part}, retrying after {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                    retry_count += 1
                else:
                    text = await resp.text()
                    print(f"\nError on part {part}: {resp.status} - {text}")
                    return
    print(f"\nFailed to upload part {part} after {MAX_RETRIES} retries.")

async def upload_async(path):
    if not os.path.isfile(path):
        print(f"File '{path}' does not exist.")
        return
    file_size = os.path.getsize(path)
    total_parts = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)
    async with aiohttp.ClientSession() as session:
        part = 1
        tasks = []
        pbar = tqdm(total=total_parts, desc="Uploading", unit="part")
        with open(path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                tasks.append(upload_chunk(session, path, chunk, part, pbar, semaphore))
                part += 1
        await asyncio.gather(*tasks)
        pbar.close()

async def fetch_messages(session, limit=100):
    url = f"https://discord.com/api/v10/channels/{CHANNEL_ID}/messages"
    headers = {"Authorization": f"Bot {TOKEN}"}
    messages = []
    last_id = None

    while len(messages) < limit:
        batch_limit = min(100, limit - len(messages))
        params = {"limit": str(batch_limit)}
        if last_id:
            params["before"] = last_id
        async with session.get(url, headers=headers, params=params) as resp:
            if resp.status == 200:
                batch = await resp.json()
                if not batch:
                    break
                messages.extend(batch)
                last_id = batch[-1]["id"]
            else:
                text = await resp.text()
                print(f"Error fetching messages: {resp.status} - {text}")
                break
    return messages[:limit]

def sizeof_fmt(num, suffix="B"):
    for unit in ["", "K", "M", "G", "T"]:
        if abs(num) < 1024.0:
            return f"{num:.2f} {unit}{suffix}"
        num /= 1024.0
    return f"{num:.2f} P{suffix}"

async def list_files():
    async with aiohttp.ClientSession() as session:
        messages = await fetch_messages(session, limit=MAX_MESSAGES_TO_FETCH)
        files_map = {}
        files_size = {}
        for msg in messages:
            for att in msg.get("attachments", []):
                base_name = clean_filename_part(att["filename"])
                files_map.setdefault(base_name, 0)
                files_map[base_name] += 1
                files_size[base_name] = files_size.get(base_name, 0) + att["size"]

        if not files_map:
            print("No files found.")
            return

        print("Files found:")
        for fname, count in files_map.items():
            size_str = sizeof_fmt(files_size.get(fname, 0))
            print(f"- {fname} ({count} parts, {size_str})")

async def download_file(file_name):
    async with aiohttp.ClientSession() as session:
        messages = await fetch_messages(session, limit=MAX_MESSAGES_TO_FETCH)
        parts = []
        for msg in messages:
            for att in msg.get("attachments", []):
                if clean_filename_part(att["filename"]) == file_name:
                    parts.append((att["filename"], att["url"]))
        if not parts:
            print(f"No parts found for file '{file_name}'")
            return
        parts.sort(key=lambda x: int(re.search(r"\.part(\d+)$", x[0]).group(1)) if re.search(r"\.part(\d+)$", x[0]) else 0)
        out_file = file_name
        with open(out_file, "wb") as f, tqdm(total=len(parts), desc="Downloading", unit="part") as pbar:
            for fname, url in parts:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        chunk = await resp.read()
                        f.write(chunk)
                        pbar.update(1)
                    else:
                        text = await resp.text()
                        print(f"\nError downloading part {fname}: {resp.status} - {text}")
                        return
        print(f"\nFile '{out_file}' downloaded successfully.")

async def delete_file(file_name):
    async with aiohttp.ClientSession() as session:
        messages = await fetch_messages(session, limit=MAX_MESSAGES_TO_FETCH)
        to_delete = []
        for msg in messages:
            for att in msg.get("attachments", []):
                if clean_filename_part(att["filename"]) == file_name:
                    to_delete.append(msg["id"])

        if not to_delete:
            print(f"No parts found for file '{file_name}' to delete.")
            return

        headers = {"Authorization": f"Bot {TOKEN}"}
        deleted_count = 0

        with tqdm(total=len(to_delete), desc="Deleting", unit="part") as pbar:
            for message_id in to_delete:
                retry_count = 0
                while retry_count < MAX_RETRIES:
                    async with session.delete(f"https://discord.com/api/v10/channels/{CHANNEL_ID}/messages/{message_id}", headers=headers) as resp:
                        if resp.status == 204:
                            deleted_count += 1
                            pbar.update(1)
                            break
                        elif resp.status == 429:
                            j = await resp.json()
                            retry_after = j.get("retry_after", 1)
                            await asyncio.sleep(retry_after)
                            retry_count += 1
                        else:
                            text = await resp.text()
                            print(f"\nError deleting message {message_id}: {resp.status} - {text}")
                            pbar.update(1)
                            break
                else:
                    print(f"\nFailed to delete message {message_id} after {MAX_RETRIES} retries.")
                    pbar.update(1)

        print(f"Deleted {deleted_count} parts of file '{file_name}'.")

def main():
    parser = argparse.ArgumentParser(description="Discord File Bot")
    subparsers = parser.add_subparsers(dest="command")

    upload_parser = subparsers.add_parser("upload", help="Upload a file")
    upload_parser.add_argument("file", help="File path to upload")
    upload_parser.add_argument("--async-upload", "--async", action="store_true", help="Use async upload")

    subparsers.add_parser("list", help="List files uploaded")

    download_parser = subparsers.add_parser("download", help="Download file by name")
    download_parser.add_argument("file_name", help="File name to download")

    delete_parser = subparsers.add_parser("delete", help="Delete file by name")
    delete_parser.add_argument("file_name", help="File name to delete")

    args = parser.parse_args()

    if not TOKEN:
        print("Error: DISCORD_BOT_TOKEN environment variable not set.")
        sys.exit(1)

    if args.command == "upload":
        if args.async_upload:
            asyncio.run(upload_async(args.file))
        else:
            upload_sync(args.file)
    elif args.command == "list":
        asyncio.run(list_files())
    elif args.command == "download":
        asyncio.run(download_file(args.file_name))
    elif args.command == "delete":
        asyncio.run(delete_file(args.file_name))
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
