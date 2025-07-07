#!/usr/bin/env python3
"""
STAC Catalog Uploader
Uploads STAC catalog files to Google Cloud Storage with cache-busting headers.
Supports both gsutil and native GCS client approaches.
"""

import shlex
import shutil
import subprocess
import argparse
from pathlib import Path
from typing import Dict, Optional

from google.cloud import storage


class GCSUploader:
    """
    Handles uploading STAC catalog files to Google Cloud Storage.
    Supports both gsutil and native GCS client approaches.
    Sets Cache-Control headers to prevent browser caching of JSON files.
    """
    
    def __init__(self, bucket_name: str, project_id: Optional[str] = None):
        """
        Initialize the GCS uploader.
        
        Args:
            bucket_name: Name of the GCS bucket
            project_id: GCP project ID (optional)
        """
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.use_gsutil = shutil.which("gsutil") is not None
        
        # Try to initialize GCS client as fallback
        self.client = None
        self.bucket = None
        if not self.use_gsutil:
            try:
                if project_id:
                    self.client = storage.Client(project=project_id)
                else:
                    self.client = storage.Client()
                self.bucket = self.client.bucket(bucket_name)
                print("Using native GCS client for uploads")
            except Exception as e:
                print(f"WARNING: Could not initialize GCS client: {e}")
                print("Upload functionality will be limited")
        else:
            print("Using gsutil for uploads")
    
    def upload_directory(self, root_dir: str, prefix: str = "", dry_run: bool = False) -> Dict:
        """
        Upload all STAC JSON files from a directory structure to GCS.
        
        Args:
            root_dir: Local directory containing STAC files
            prefix: GCS path prefix (e.g., "public/layers/")
            dry_run: If True, show what would be uploaded without doing it
            
        Returns:
            Dictionary with upload results
        """
        root_path = Path(root_dir).resolve()
        
        if not root_path.is_dir():
            raise ValueError(f"Directory does not exist: {root_path}")
        
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        
        print(f"Scanning directory: {root_path}")
        print(f"Target GCS location: gs://{self.bucket_name}/{prefix}")
        print(f"Dry run: {dry_run}")
        print("-" * 50)
        
        results = {
            "uploaded": [],
            "skipped": [],
            "failed": [],
            "total_files": 0
        }
        
        # Find all JSON files to upload
        json_files = list(root_path.rglob("*.json"))
        results["total_files"] = len(json_files)
        
        if not json_files:
            print("No JSON files found to upload")
            return results
        
        print(f"Found {len(json_files)} JSON files to upload")
        print("-" * 50)
        
        for json_file in json_files:
            try:
                self._upload_single_file(json_file, root_path, prefix, dry_run, results)
            except Exception as e:
                print(f"ERROR: Failed to upload {json_file}: {e}")
                results["failed"].append({
                    "file": str(json_file),
                    "error": str(e)
                })
        
        # Print summary
        self._print_summary(results, prefix)
        return results
    
    def _upload_single_file(self, file_path: Path, root_path: Path, prefix: str, 
                          dry_run: bool, results: Dict):
        """Upload a single file to GCS."""
        # Get relative path from the catalog root, not the full local path
        relative_path = file_path.relative_to(root_path)
        
        # Remove any leading "catalog/" from the relative path if it exists
        # This ensures we upload to the correct GCS structure
        path_parts = relative_path.parts
        if path_parts[0] == "catalog":
            # Strip the "catalog" prefix - we want vector/collection.json not catalog/vector/collection.json
            relative_path = Path(*path_parts[1:])
        
        gcs_path = f"{prefix}{relative_path.as_posix()}"
        gcs_url = f"gs://{self.bucket_name}/{gcs_path}"
        
        print(f"📄 {file_path.name}")
        print(f"   Local:  {relative_path}")
        print(f"   GCS:    {gcs_url}")
        
        if dry_run:
            print("   → DRY RUN: Would upload with Cache-Control headers")
            results["skipped"].append(str(file_path))
            return
        
        # Try gsutil first, fall back to native client
        success = False
        
        if self.use_gsutil:
            success = self._upload_with_gsutil(file_path, gcs_url)
        
        if not success and self.client:
            success = self._upload_with_client(file_path, gcs_path)
        
        if success:
            print("   ✅ Upload successful")
            results["uploaded"].append(str(file_path))
        else:
            print("   ❌ Upload failed")
            results["failed"].append({
                "file": str(file_path),
                "error": "All upload methods failed"
            })
        
        print()
    
    def _upload_with_gsutil(self, file_path: Path, gcs_url: str) -> bool:
        """Upload using gsutil command with Cache-Control headers."""
        try:
            # Set Cache-Control header to prevent browser caching
            # This forces browsers to re-fetch the STAC catalog files on each request
            cmd = [
                "gsutil", 
                "-h", "Cache-Control:no-cache, no-store, must-revalidate",
                "-h", "Content-Type:application/json",
                "cp", 
                str(file_path), 
                gcs_url
            ]
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                check=True,
                timeout=30
            )
            return True
        except subprocess.CalledProcessError as e:
            print(f"   gsutil error: {e.stderr.strip()}")
            return False
        except subprocess.TimeoutExpired:
            print("   gsutil timeout")
            return False
        except Exception as e:
            print(f"   gsutil exception: {e}")
            return False
    
    def _upload_with_client(self, file_path: Path, gcs_path: str) -> bool:
        """Upload using native GCS client with Cache-Control headers."""
        try:
            blob = self.bucket.blob(gcs_path)
            
            # Set Cache-Control header to prevent browser caching
            blob.cache_control = "no-cache, no-store, must-revalidate"
            blob.content_type = "application/json"
            
            blob.upload_from_filename(str(file_path))
            return True
        except Exception as e:
            print(f"   GCS client error: {e}")
            return False
    
    def _print_summary(self, results: Dict, prefix: str = ""):
        """Print upload summary."""
        print("=" * 50)
        print("📊 UPLOAD SUMMARY")
        print("=" * 50)
        print(f"Total files found: {results['total_files']}")
        print(f"Successfully uploaded: {len(results['uploaded'])}")
        print(f"Skipped (dry run): {len(results['skipped'])}")
        print(f"Failed: {len(results['failed'])}")
        
        if results['failed']:
            print(f"\n❌ Failed uploads:")
            for failure in results['failed']:
                if isinstance(failure, dict):
                    print(f"   • {failure['file']}: {failure['error']}")
                else:
                    print(f"   • {failure}")
        
        if results['uploaded']:
            print(f"\n✅ Upload complete! Files available at:")
            print(f"   gs://{self.bucket_name}/")
            print(f"\n🔄 Cache-Control headers set to 'no-cache, no-store, must-revalidate'")
            print(f"   This ensures STAC Browser always fetches the latest catalog data")
            
            # Add Radiant Earth STAC Browser links
            self._print_radiant_earth_links(prefix, results)
    
    def _print_radiant_earth_links(self, prefix: str = "", results: Dict = None):
        """Print Radiant Earth STAC Browser links for the uploaded catalog."""
        print(f" View on Radiant Earth STAC Browser:")
        
        # Main catalog link
        if prefix:
            catalog_url = f"https://radiantearth.github.io/stac-browser/#/external/storage.googleapis.com/{self.bucket_name}/{prefix}catalog.json"
        else:
            catalog_url = f"https://radiantearth.github.io/stac-browser/#/external/storage.googleapis.com/{self.bucket_name}/catalog.json"
        
        print(f"   📋 Main Catalog: {catalog_url}")
        
        # Try to find and list collection links
        if results:
            try:
                # Look for collection.json files in the uploaded results to generate collection links
                collections = []
                for file_path in results.get('uploaded', []):
                    if file_path.endswith('collection.json'):
                        # Extract collection path from file path
                        # Example: "catalog/vector/collection.json" -> "vector"
                        path_parts = Path(file_path).parts
                        if 'catalog' in path_parts:
                            catalog_index = path_parts.index('catalog')
                            if catalog_index + 1 < len(path_parts):
                                collection_path = '/'.join(path_parts[catalog_index + 1:-1])  # Exclude 'collection.json'
                                if collection_path:
                                    collections.append(collection_path)
                
                if collections:
                    print(f"   📁 Collections:")
                    for collection in collections:
                        if prefix:
                            collection_url = f"https://radiantearth.github.io/stac-browser/#/external/storage.googleapis.com/{self.bucket_name}/{prefix}{collection}/collection.json"
                        else:
                            collection_url = f"https://radiantearth.github.io/stac-browser/#/external/storage.googleapis.com/{self.bucket_name}/{collection}/collection.json"
                        print(f"      • {collection}: {collection_url}")
            except Exception as e:
                # If we can't generate collection links, just show the main catalog
                print(f"   (Collection links not available: {e})")
        
        print(f"\n💡 Tip: You can bookmark these URLs to easily access your STAC catalog!")


def upload_stac_catalog(root_dir: str, bucket_name: str, prefix: str = "", 
                       dry_run: bool = False, project_id: Optional[str] = None) -> Dict:
    """
    Convenience function to upload STAC catalog files to GCS with cache-busting headers.
    
    Args:
        root_dir: Local directory containing STAC files (should point to the catalog folder)
        bucket_name: GCS bucket name
        prefix: GCS path prefix (e.g., "public/layers/")
        dry_run: If True, show what would be uploaded without doing it
        project_id: GCP project ID (optional)
        
    Returns:
        Dictionary with upload results
    """
    uploader = GCSUploader(bucket_name, project_id)
    return uploader.upload_directory(root_dir, prefix, dry_run)


def main():
    """Main function to upload STAC catalog to GCS."""
    parser = argparse.ArgumentParser(description="Upload STAC catalog to GCS.")
    parser.add_argument("--bucket", required=True, help="GCS bucket name (e.g., 'swhm_data')")
    parser.add_argument("--prefix", default="", help="Path prefix within the bucket")
    parser.add_argument("--catalog", default="", help="Local path of catalog folder")
    #!SECTIONparser.add_argument("--project", default=None, help="GCP project ID (optional)")
    args = parser.parse_args()

    GCS_BUCKET = args.bucket
    GCS_PREFIX = args.prefix
    
    # Configuration
    
    # IMPORTANT: Point directly to the catalog directory, not a parent directory
    # This ensures relative paths are calculated correctly
    CATALOG_DIR = "catalog"  # Use relative path from script location, or absolute path to catalog folder
    
    # Alternative: Use absolute path to catalog directory
    # CATALOG_DIR = "/Users/christiannilsen/Documents/repos/swmh-stac-catalog/catalog"
    
    print("=== UPLOADING STAC CATALOG TO GCS ===\n")
    
    # Upload with dry run first to see what would be uploaded
    print("1. Dry run to preview uploads:")
    results = upload_stac_catalog(
        root_dir=CATALOG_DIR,
        bucket_name=GCS_BUCKET,
        prefix=GCS_PREFIX,
        dry_run=True
    )
    
    # Ask user if they want to proceed
    print(f"\n" + "=" * 50)
    proceed = input("Do you want to proceed with the actual upload? (y/N): ").strip().lower()
    
    if proceed in ['y', 'yes']:
        print(f"\n2. Actual upload:")
        results = upload_stac_catalog(
            root_dir=CATALOG_DIR,
            bucket_name=GCS_BUCKET,
            prefix=GCS_PREFIX,
            dry_run=False
        )
        
        if results['uploaded']:
            print(f" Upload completed successfully!")
            
            # Create uploader instance to use the Radiant Earth links method
            uploader = GCSUploader(GCS_BUCKET)
            uploader._print_radiant_earth_links(GCS_PREFIX, results)
    else:
        print("Upload cancelled.")
    


if __name__ == "__main__":
    main()