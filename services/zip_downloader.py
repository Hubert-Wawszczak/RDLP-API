"""
Module for downloading and extracting ZIP files from BDL sharing service.
"""
import aiohttp
import aiofiles
import asyncio
import zipfile
import io
from pathlib import Path
from typing import List, Optional
from utils.logger.logger import AsyncLogger

logger = AsyncLogger()


class ZIPDownloader:
    """
    Downloads ZIP files from BDL sharing service and extracts them.
    """
    
    def __init__(self, save_dir: Path = Path(__file__).parent.parent / 'api_data'):
        self.save_dir = save_dir
        self.save_dir.mkdir(exist_ok=True)
        self.extracted_dir = save_dir / 'extracted'
        self.extracted_dir.mkdir(exist_ok=True)
    
    async def download_zip(self, url: str, session: aiohttp.ClientSession) -> Optional[Path]:
        """
        Downloads a ZIP file from the given URL.
        
        Args:
            url (str): URL to download the ZIP file from
            session (aiohttp.ClientSession): HTTP session
            
        Returns:
            Optional[Path]: Path to downloaded ZIP file, or None if failed
        """
        try:
            # Extract filename from URL
            filename = url.split('file=')[-1] if 'file=' in url else f"download_{int(asyncio.get_event_loop().time())}.zip"
            zip_path = self.save_dir / filename
            
            logger.log("INFO", f"Downloading ZIP file: {filename}")
            
            async with session.get(url) as response:
                if response.status == 200:
                    # Download in chunks
                    async with aiofiles.open(zip_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            await f.write(chunk)
                    
                    logger.log("INFO", f"Successfully downloaded: {filename}")
                    return zip_path
                else:
                    logger.log("ERROR", f"Failed to download {url}: HTTP {response.status}")
                    return None
        except Exception as e:
            logger.log("ERROR", f"Error downloading {url}: {str(e)}")
            return None
    
    async def extract_zip(self, zip_path: Path) -> Optional[Path]:
        """
        Extracts a ZIP file to the extracted directory.
        
        Args:
            zip_path (Path): Path to the ZIP file
            
        Returns:
            Optional[Path]: Path to extraction directory, or None if failed
        """
        try:
            # Create extraction directory for this ZIP
            extract_dir = self.extracted_dir / zip_path.stem
            extract_dir.mkdir(exist_ok=True)
            
            logger.log("INFO", f"Extracting {zip_path.name} to {extract_dir}")
            
            # Read ZIP file
            async with aiofiles.open(zip_path, 'rb') as f:
                zip_content = await f.read()
            
            # Extract ZIP (synchronous operation, but file reading was async)
            with zipfile.ZipFile(io.BytesIO(zip_content), 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            logger.log("INFO", f"Successfully extracted {zip_path.name}")
            return extract_dir
        except Exception as e:
            logger.log("ERROR", f"Error extracting {zip_path}: {str(e)}")
            return None
    
    async def download_and_extract(self, url: str, session: aiohttp.ClientSession) -> Optional[Path]:
        """
        Downloads and extracts a ZIP file in one operation.
        
        Args:
            url (str): URL to download the ZIP file from
            session (aiohttp.ClientSession): HTTP session
            
        Returns:
            Optional[Path]: Path to extraction directory, or None if failed
        """
        zip_path = await self.download_zip(url, session)
        if zip_path:
            return await self.extract_zip(zip_path)
        return None
    
    async def download_multiple(self, urls: List[str], max_concurrent: int = 5) -> List[Path]:
        """
        Downloads and extracts multiple ZIP files concurrently.
        
        Args:
            urls (List[str]): List of URLs to download
            max_concurrent (int): Maximum number of concurrent downloads
            
        Returns:
            List[Path]: List of extraction directories
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        results = []
        
        async def download_with_semaphore(url):
            async with semaphore:
                async with aiohttp.ClientSession() as session:
                    return await self.download_and_extract(url, session)
        
        tasks = [download_with_semaphore(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out None and exceptions
        extracted_dirs = [r for r in results if isinstance(r, Path)]
        return extracted_dirs

