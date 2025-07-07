#!/usr/bin/env python3
"""
GCP Bucket Crawler and STAC Catalog Generator
Crawls a GCP storage bucket to discover vector and raster data,
then generates collections, individual STAC items, and a comprehensive catalog.
Saves all files locally for later upload.
"""

import json
import os
import argparse
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

from google.cloud import storage
import rasterio
from shapely.geometry import Polygon
from shapely.geometry import mapping
try:
    import pystac
    from pystac.validation import validate_dict
    PYSTAC_AVAILABLE = True
except ImportError:
    print("Warning: pystac not available. STAC validation will be skipped.")
    PYSTAC_AVAILABLE = False


def get_bbox_and_footprint(raster_url: str) -> Tuple[List[float], Dict]:
    """
    Get bounding box and footprint geometry from a raster file.
    
    Args:
        raster_url: URL or path to the raster file
        
    Returns:
        Tuple of (bbox, footprint_geometry)
    """
    try:
        with rasterio.open(raster_url) as r:
            bounds = r.bounds
            bbox = [bounds.left, bounds.bottom, bounds.right, bounds.top]
            footprint = Polygon([
                [bounds.left, bounds.bottom],
                [bounds.left, bounds.top],
                [bounds.right, bounds.top],
                [bounds.right, bounds.bottom]
            ])
            
            return (bbox, mapping(footprint))
    except Exception as e:
        print(f"Warning: Could not extract bbox/footprint from {raster_url}: {e}")
        # Return default Puget Sound region bbox and None geometry
        return ([-124.81791282934638, 46.593055784134464, -120.65459447215042, 49.00245266588554], None)


def calculate_collection_bbox(items: List[Dict]) -> List[float]:
    """
    Calculate the bounding box for a collection based on its items.
    
    Args:
        items: List of items with bbox information
        
    Returns:
        Combined bbox [west, south, east, north]
    """
    if not items:
        return [-124.81791282934638, 46.593055784134464, -120.65459447215042, 49.00245266588554]
    
    # Initialize with first item's bbox or default
    min_x = min_y = float('inf')
    max_x = max_y = float('-inf')
    
    for item in items:
        if item['type'] == 'raster':
            try:
                bbox, _ = get_bbox_and_footprint(item['url'])
                min_x = min(min_x, bbox[0])
                min_y = min(min_y, bbox[1])
                max_x = max(max_x, bbox[2])
                max_y = max(max_y, bbox[3])
            except Exception as e:
                print(f"Warning: Could not get bbox for {item['name']}: {e}")
                continue
    
    # If we couldn't get any valid bboxes, return default
    if min_x == float('inf'):
        return [-124.81791282934638, 46.593055784134464, -120.65459447215042, 49.00245266588554]
    
    return [min_x, min_y, max_x, max_y]


def load_layer_metadata(metadata_path: str = "catalog/layer_metadata/layer_metadata.json") -> Dict:
    """
    Load layer metadata from JSON file.
    
    Args:
        metadata_path: Path to the layer metadata JSON file
        
    Returns:
        Dictionary containing layer metadata
    """
    try:
        with open(metadata_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Could not load layer metadata from {metadata_path}: {e}")
        return {"rasters": {}, "cocs": {}}


def find_layer_metadata(layer_name: str, metadata: Dict) -> Optional[Dict]:
    """
    Find metadata for a layer by matching names.
    
    Args:
        layer_name: Name of the layer to find metadata for
        metadata: Complete metadata dictionary
        
    Returns:
        Layer metadata if found, None otherwise
    """
    # Normalize layer name for matching
    normalized_name = layer_name.lower().replace('_', ' ').replace('-', ' ')
    
    # Search in rasters and cocs
    for category in ['rasters', 'cocs']:
        for key, layer_meta in metadata.get(category, {}).items():
            # Try exact match first
            if key.lower() == normalized_name:
                print(f"Found metadata match for '{layer_name}' -> '{key}'")
                return layer_meta
            
            # Try safe_name match
            if layer_meta.get('safe_name', '').lower() == layer_name.lower():
                print(f"Found metadata match for '{layer_name}' via safe_name -> '{key}'")
                return layer_meta
            
            # Try normalized key match
            if key.lower().replace(' ', '_') == layer_name.lower():
                print(f"Found metadata match for '{layer_name}' via normalized key -> '{key}'")
                return layer_meta
            
            # Try with underscores in normalized name
            if key.lower().replace(' ', '_') == normalized_name.replace(' ', '_'):
                print(f"Found metadata match for '{layer_name}' via underscore normalization -> '{key}'")
                return layer_meta
    return None


class GCPBucketCrawler:
    def __init__(self, bucket_name: str, prefix: str = "", project_id: Optional[str] = None):
        """
        Initialize the crawler with GCP bucket details.
        
        Args:
            bucket_name: Name of the GCP storage bucket (e.g., 'swhm_data')
            prefix: Prefix to filter objects (e.g., 'public/layers/')
            project_id: GCP project ID (optional, will use default if not provided)
        """
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.project_id = project_id
        self.directory_tree = defaultdict(dict)
        self.items = []
        self.processed_items = set()  # Track processed items to prevent duplicates
        
        # Initialize the GCS client
        try:
            if project_id:
                self.client = storage.Client(project=project_id)
            else:
                self.client = storage.Client()
            self.bucket = self.client.bucket(bucket_name)
            print(f"Successfully connected to bucket: {bucket_name}")
        except Exception as e:
            print(f"Error initializing GCS client: {e}")
            print("Make sure you have proper authentication set up:")
            print("1. Set GOOGLE_APPLICATION_CREDENTIALS environment variable")
            print("2. Or run 'gcloud auth application-default login'")
            self.client = None
            self.bucket = None
        
    def crawl_bucket(self) -> Dict:
        """
        Crawl the GCP bucket to discover all items and build directory structure.
        Returns a dictionary with discovered items and directory tree.
        """
        if not self.client or not self.bucket:
            print("No valid GCS client available, creating sample data...")
            return self._create_sample_data()
            
        print(f"Crawling bucket '{self.bucket_name}' with prefix '{self.prefix}'...")
        
        try:
            # List all blobs in the bucket with the specified prefix
            blobs = self.bucket.list_blobs(prefix=self.prefix)
            
            blob_count = 0
            for blob in blobs:
                blob_count += 1
                self._process_blob(blob)
                
            print(f"Processed {blob_count} objects from bucket")
            print(f"Found {len(self.items)} unique items")
            
            # Build directory structure
            self._build_directory_tree()
            
        except Exception as e:
            print(f"Error crawling bucket: {e}")
            return self._create_sample_data()
            
        return {
            'items': self.items,
            'directory_tree': dict(self.directory_tree),
            'total_items': len(self.items)
        }
    
    def _process_blob(self, blob):
        """Process a single blob to determine if it's a data item."""
        blob_name = blob.name
        blob_path = Path(blob_name)
        
        # Skip directories (blobs ending with '/')
        if blob_name.endswith('/'):
            return
            
        # Check for data files - vectors (.geojson) and rasters (.tiff, .tif, .gtiff)
        if blob_path.suffix.lower() == '.geojson':
            self._add_item(blob, 'vector')
        elif blob_path.suffix.lower() in ['.tiff', '.tif', '.gtiff']:
            self._add_item(blob, 'raster')
        # Skip other files (like .json metadata files)
    
    def _add_item(self, blob, item_type: str):
        """Add an item to the collection."""
        blob_path = Path(blob.name)
        item_name = blob_path.stem
        
        # Create unique identifier to prevent duplicates
        item_key = f"{item_type}:{item_name}:{blob.name}"
        if item_key in self.processed_items:
            print(f"Skipping duplicate {item_type}: {item_name}")
            return
        
        # Create public URL
        public_url = f"https://storage.googleapis.com/{self.bucket_name}/{blob.name}"
        
        # Get the directory path relative to prefix
        parent_dir = blob_path.parent
        stac_dir = str(parent_dir).replace(self.prefix, '') if self.prefix else str(parent_dir)
        
        # Remove leading slash if present
        stac_dir = stac_dir.lstrip('/')
        
        item_data = {
            'name': item_name,
            'filename': blob.name,
            'url': public_url,
            'type': item_type,
            'format': 'GeoJSON' if item_type == 'vector' else 'GeoTIFF',
            'size_bytes': blob.size,
            'content_type': blob.content_type,
            'created': blob.time_created.isoformat() if blob.time_created else None,
            'updated': blob.updated.isoformat() if blob.updated else None,
            'discovered_at': datetime.now().isoformat(),
            'etag': blob.etag,
            'md5_hash': blob.md5_hash,
            'stac_dir': stac_dir,  # Directory where STAC item should be saved
            'path_parts': stac_dir.split('/') if stac_dir else []  # For building hierarchy
        }
        
        self.items.append(item_data)
        self.processed_items.add(item_key)
        print(f"Found {item_type}: {item_name} in {stac_dir}")
    
    def get_blob_info(self, blob_name: str) -> Optional[Dict]:
        """Get detailed information about a specific blob."""
        if not self.bucket:
            return None
            
        try:
            blob = self.bucket.blob(blob_name)
            if blob.exists():
                return {
                    'name': blob.name,
                    'size': blob.size,
                    'content_type': blob.content_type,
                    'created': blob.time_created.isoformat() if blob.time_created else None,
                    'updated': blob.updated.isoformat() if blob.updated else None,
                    'etag': blob.etag,
                    'md5_hash': blob.md5_hash,
                    'public_url': f"https://storage.googleapis.com/{self.bucket_name}/{blob.name}"
                }
        except Exception as e:
            print(f"Error getting blob info for {blob_name}: {e}")
            
        return None
    
    def _build_directory_tree(self):
        """Build a hierarchical directory tree from discovered items."""
        for item in self.items:
            path_parts = item['path_parts']
            current_level = self.directory_tree
            
            # Only create collections if path is 2 or more levels deep, but not at root
            if len(path_parts) >= 2:
                # Build the nested structure - but only for parent directories (not the leaf)
                # Collections should be created at parent level, not where items are
                for i, part in enumerate(path_parts[:-1]):  # Exclude the last part (leaf directory)
                    if part not in current_level:
                        current_level[part] = {
                            'type': 'collection',
                            'name': part,
                            'path': '/'.join(path_parts[:i+1]),
                            'children': {},
                            'items': []
                        }
                    current_level = current_level[part]['children']
                
                # Add item to the parent directory (not the leaf)
                # The parent directory is the second-to-last part of the path
                if len(path_parts) >= 2:
                    # Navigate to the parent collection
                    parent_collection = self.directory_tree
                    for part in path_parts[:-2]:  # Navigate to grandparent level
                        parent_collection = parent_collection[part]['children']
                    
                    # Add item to the parent directory (second-to-last path part)
                    parent_dir = path_parts[-2]
                    if parent_dir in parent_collection:
                        parent_collection[parent_dir]['items'].append(item)
                    else:
                        print(f"Warning: Parent directory {parent_dir} not found for item {item['name']}")
                else:
                    # This shouldn't happen if we have 2+ path parts
                    print(f"Warning: Unexpected path structure for item {item['name']}")
            else:
                # For items with less than 2 path parts, skip collection creation
                print(f"Skipping collection creation for item {item['name']} - path depth {len(path_parts)} is less than 2")
    
    def _create_sample_data(self):
        """Create sample data structure when bucket can't be crawled directly."""
        print("Creating sample data structure...")
        
        base_url = f"https://storage.googleapis.com/{self.bucket_name}"
        
        # Sample items based on a generic structure
        sample_items = [
            {
                'name': 'sample_vector',
                'filename': f'{self.prefix}category1/subcategory1/sample_vector.geojson',
                'url': f"{base_url}/{self.prefix}category1/subcategory1/sample_vector.geojson",
                'type': 'vector',
                'format': 'GeoJSON',
                'size_bytes': None,
                'content_type': 'application/geo+json',
                'created': None,
                'updated': None,
                'discovered_at': datetime.now().isoformat(),
                'etag': None,
                'md5_hash': None,
                'stac_dir': 'category1/subcategory1',
                'path_parts': ['category1', 'subcategory1']
            },
            {
                'name': 'sample_raster',
                'filename': f'{self.prefix}category2/subcategory2/sample_raster.tiff',
                'url': f"{base_url}/{self.prefix}category2/subcategory2/sample_raster.tiff",
                'type': 'raster',
                'format': 'GeoTIFF',
                'size_bytes': None,
                'content_type': 'image/tiff',
                'created': None,
                'updated': None,
                'discovered_at': datetime.now().isoformat(),
                'etag': None,
                'md5_hash': None,
                'stac_dir': 'category2/subcategory2',
                'path_parts': ['category2', 'subcategory2']
            }
        ]
        
        self.items = sample_items
        self._build_directory_tree()
        
        return {
            'items': self.items,
            'directory_tree': dict(self.directory_tree),
            'total_items': len(self.items)
        }


class CatalogGenerator:
    def __init__(self, crawler_data: Dict, metadata_path: str = "catalog/layer_metadata/layer_metadata.json"):
        """Initialize with data from the crawler."""
        self.data = crawler_data
        self.directory_tree = crawler_data.get('directory_tree', {})
        self.items = crawler_data.get('items', [])
        self.base_url = f"https://storage.googleapis.com/{crawler_data.get('bucket_name', 'bucket')}"
        self.prefix = crawler_data.get('prefix', '')
        
        # Load layer metadata
        print("Loading layer metadata...")
        self.layer_metadata = load_layer_metadata(metadata_path)
        
    def generate_stac_item(self, item_data: Dict) -> Dict:
        """Generate a STAC item for data asset."""
        item_id = item_data['name']
        item_type = item_data['type']
        
        # Determine parent collection path (parent directory, not the leaf directory)
        parent_path = '/'.join(item_data['path_parts'][:-1]) if len(item_data['path_parts']) > 1 else ''
        
        # Check if item has a collection (2+ levels deep)
        has_collection = len(item_data['path_parts']) >= 2
        
        # Get actual bbox and geometry for raster files
        bbox = [-124.81791282934638, 46.593055784134464, -120.65459447215042, 49.00245266588554]  # Default Puget Sound region bbox
        geometry = None
        
        if item_type == 'raster':
            try:
                print(f"Extracting bbox and footprint for {item_data['name']}...")
                bbox, geometry = get_bbox_and_footprint(item_data['url'])
            except Exception as e:
                print(f"Warning: Could not extract raster metadata for {item_data['name']}: {e}")
        
        # Find layer metadata
        layer_meta = find_layer_metadata(item_data['name'], self.layer_metadata)
        
        # Enhanced properties with layer metadata
        properties = {
            "title": item_data['name'].replace('_', ' ').title(),
            "description": item_data['name'],
            "datetime": item_data['discovered_at'],
            "created": item_data.get('created') or item_data['discovered_at'],
            "updated": item_data.get('updated') or item_data['discovered_at'],
            "providers": [
                {
                    "name": "StormwaterHeatmap.Org",
                    "roles": ["producer", "processor", "host"],
                    "url": self.base_url
                }
            ]
        }
        
        # Add rich metadata if available
        if layer_meta:
            properties.update({
                "title": layer_meta.get('layer', {}).get('name', item_data['name'].replace('_', ' ').title()),
                "description": layer_meta.get('description', f"{item_type.title()} dataset: {item_data['name']}"),
                "gsd": layer_meta.get('scale', None),
                "platform": layer_meta.get('sourceName', 'StormwaterHeatmap.Org'),
                "swmh:units": layer_meta.get('units'),
                "swmh:discrete": layer_meta.get('discrete'),
                "swmh:viz_type": layer_meta.get('vizType'),
                "swmh:default_reduction": layer_meta.get('default_reduction'),
                "swmh:safe_name": layer_meta.get('safe_name'),
                "swmh:docs_link": layer_meta.get('docs_link')
            })
            
            # Add source information if available
            if layer_meta.get('sourceUrl'):
                properties["swmh:source_url"] = layer_meta['sourceUrl']
            
            # Add visualization parameters
            if layer_meta.get('layer', {}).get('visParams'):
                properties["swmh:vis_params"] = layer_meta['layer']['visParams']
            
            # Add categorical information for discrete layers
            if layer_meta.get('discrete') and layer_meta.get('labels'):
                properties["swmh:labels"] = layer_meta['labels']
                properties["swmh:values"] = layer_meta.get('values', [])
            
            # Update provider with actual source if available
            if layer_meta.get('sourceName'):
                properties["providers"] = [
                    {
                        "name": layer_meta['sourceName'],
                        "roles": ["producer"],
                        "url": layer_meta.get('sourceUrl', self.base_url)
                    },
                    {
                        "name": "StormwaterHeatmap.Org",
                        "roles": ["processor", "host"],
                        "url": self.base_url
                    }
                ]
        
        # Base STAC item structure
        stac_item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": item_id,
            "properties": properties,
            "geometry": geometry,
            "bbox": bbox,
            "assets": {},
            "links": [
                {
                    "rel": "self",
                    "href": f"{self.base_url}/{self.prefix}{item_data['stac_dir']}/{item_id}.json" if item_data['stac_dir'] else f"{self.base_url}/{self.prefix}{item_id}.json",
                    "type": "application/json"
                },
                {
                    "rel": "parent",
                    "href": f"{self.base_url}/{self.prefix}{parent_path}/collection.json" if has_collection and parent_path else f"{self.base_url}/{self.prefix}catalog.json",
                    "type": "application/json"
                },
                {
                    "rel": "root",
                    "href": f"{self.base_url}/{self.prefix}catalog.json",
                    "type": "application/json"
                }
            ]
        }
        
        # Only add collection link if item has a collection
        if has_collection and parent_path:
            stac_item["links"].append({
                "rel": "collection",
                "href": f"{self.base_url}/{self.prefix}{parent_path}/collection.json",
                "type": "application/json"
            })
        
        # Add assets based on type
        if item_type == 'vector':
            stac_item["assets"]["data"] = {
                "href": item_data['url'],
                "type": "application/geo+json",
                "title": "GeoJSON data",
                "description": "Vector data in GeoJSON format",
                "roles": ["data"],
                "file:size": item_data.get('size_bytes'),
                "file:checksum": item_data.get('md5_hash')
            }
        elif item_type == 'raster':
            # Enhanced raster asset with metadata
            raster_asset = {
                "href": item_data['url'],
                "type": "image/tiff; application=geotiff",
                "title": properties.get('title', 'GeoTIFF data'),
                "description": properties.get('description', 'Raster data in GeoTIFF format'),
                "roles": ["data"],
                "file:size": item_data.get('size_bytes'),
                "file:checksum": item_data.get('md5_hash')
            }
            
            # Add raster band information if metadata available
            if layer_meta:
                band_info = {
                    "name": layer_meta.get('safe_name', item_data['name']),
                    "description": layer_meta.get('description', 'Raster band'),
                    "data_type": "float32"  # Default, could be enhanced with actual inspection
                }
                
                if layer_meta.get('units'):
                    band_info["unit"] = layer_meta['units']
                
                # Add value range information
                vis_params = layer_meta.get('layer', {}).get('visParams', {})
                if 'min' in vis_params and 'max' in vis_params:
                    band_info["statistics"] = {
                        "minimum": vis_params['min'],
                        "maximum": vis_params['max']
                    }
                
                # Add categorical information if available
                if layer_meta.get('discrete') and layer_meta.get('labels'):
                    band_info["classification:classes"] = []
                    labels = layer_meta.get('labels', [])
                    values = layer_meta.get('values', [])
                    for i, label in enumerate(labels):
                        class_info = {
                            "value": int(values[i]) if i < len(values) and values[i] else i,
                            "description": label
                        }
                        band_info["classification:classes"].append(class_info)
                
                raster_asset["raster:bands"] = [band_info]
                
                # Add additional metadata to the asset
                if layer_meta.get('sourceName'):
                    raster_asset["swmh:source_name"] = layer_meta['sourceName']
                if layer_meta.get('sourceUrl'):
                    raster_asset["swmh:source_url"] = layer_meta['sourceUrl']
                if layer_meta.get('scale'):
                    raster_asset["gsd"] = layer_meta['scale']
                if layer_meta.get('discrete') is not None:
                    raster_asset["swmh:discrete"] = layer_meta['discrete']
                if layer_meta.get('vizType'):
                    raster_asset["swmh:viz_type"] = layer_meta['vizType']
            
            stac_item["assets"]["data"] = raster_asset
            
            # Add COG asset if it's a Cloud Optimized GeoTIFF
            cog_asset = {
                "href": item_data['url'],
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "title": "Cloud Optimized GeoTIFF",
                "description": "Cloud Optimized GeoTIFF for web access",
                "roles": ["data", "overview"]
            }
            
            # Copy metadata to COG asset as well
            if layer_meta:
                if "raster:bands" in raster_asset:
                    cog_asset["raster:bands"] = raster_asset["raster:bands"]
                if "swmh:source_name" in raster_asset:
                    cog_asset["swmh:source_name"] = raster_asset["swmh:source_name"]
                if "swmh:source_url" in raster_asset:
                    cog_asset["swmh:source_url"] = raster_asset["swmh:source_url"]
                if "gsd" in raster_asset:
                    cog_asset["gsd"] = raster_asset["gsd"]
                if "swmh:discrete" in raster_asset:
                    cog_asset["swmh:discrete"] = raster_asset["swmh:discrete"]
                if "swmh:viz_type" in raster_asset:
                    cog_asset["swmh:viz_type"] = raster_asset["swmh:viz_type"]
                    
            stac_item["assets"]["cog"] = cog_asset
        
        return stac_item
    
    def generate_collection(self, dir_info: Dict, parent_path: str = "") -> Dict:
        """Generate a collection for a directory."""
        collection_id = dir_info['name']
        collection_path = dir_info['path']
        
        # Determine parent URL
        if parent_path:
            parent_url = f"{self.base_url}/{self.prefix}{parent_path}/collection.json"
        else:
            parent_url = f"{self.base_url}/{self.prefix}catalog.json"
        
        # Calculate bbox based on items in this collection
        print(f"Calculating bbox for collection {collection_id} with {len(dir_info['items'])} items...")
        collection_bbox = calculate_collection_bbox(dir_info['items'])
        
        # Base collection structure
        collection = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": collection_id,
            "title": collection_id.replace('_', ' ').title(),
            "description": f"Collection for {collection_id}",
            "keywords": [collection_id],
            "license": "MPL-2.0",
            "extent": {
                "spatial": {
                    "bbox": [collection_bbox]  # Use calculated bbox
                },
                "temporal": {
                    "interval": [[None, None]]
                }
            },
            "providers": [
                {
                    "name": "StormwaterHeatmap.Org",
                    "roles": ["producer", "processor", "host"],
                    "url": self.base_url
                }
            ],
            "links": [
                {
                    "rel": "self",
                    "href": f"{self.base_url}/{self.prefix}{collection_path}/collection.json",
                    "type": "application/json"
                },
                {
                    "rel": "parent",
                    "href": parent_url,
                    "type": "application/json"
                },
                {
                    "rel": "root",
                    "href": f"{self.base_url}/{self.prefix}catalog.json",
                    "type": "application/json"
                }
            ],
            "item_assets": {}
        }
        
        # Add child collections
        for child_name, child_info in dir_info['children'].items():
            collection["links"].append({
                "rel": "child",
                "href": f"{self.base_url}/{self.prefix}{child_info['path']}/collection.json",
                "type": "application/json",
                "title": child_name.replace('_', ' ').title()
            })
        
        # Add items in this collection
        for item in dir_info['items']:
            item_id = item['name']
            collection["links"].append({
                "rel": "item",
                "href": f"{self.base_url}/{self.prefix}{item['stac_dir']}/{item_id}.json",
                "type": "application/json",
                "title": item['name'].replace('_', ' ').title()
            })
            
            # Add to item_assets based on type
            if item['type'] == 'vector':
                collection["item_assets"]["data"] = {
                    "type": "application/geo+json",
                    "title": "GeoJSON data",
                    "roles": ["data"]
                }
            elif item['type'] == 'raster':
                collection["item_assets"]["data"] = {
                    "type": "image/tiff; application=geotiff",
                    "title": "GeoTIFF data",
                    "roles": ["data"]
                }
                collection["item_assets"]["cog"] = {
                    "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                    "title": "Cloud Optimized GeoTIFF",
                    "roles": ["data", "overview"]
                }
        
        return collection
    
    def generate_master_catalog(self) -> Dict:
        """Generate the master catalog containing all top-level collections and items."""
        catalog = {
            "type": "Catalog",
            "catalog_type": "Published_Absolute",
            "stac_version": "1.0.0",
            "id": "data-catalog",
            "title": "Stormwater Heatmap Data Catalog",
            "description": "STAC catalog for Stormwater Heatmap datasets including raster layers and pollutant concentration models",
            "created": datetime.now().isoformat(),
            "updated": datetime.now().isoformat(),
            "keywords": ["geospatial", "stormwater", "hydrology", "pollution", "environmental"],
            "providers": [
                {
                    "name": "StormwaterHeatmap.Org",
                    "roles": ["producer", "processor", "host"],
                    "url": self.base_url
                }
            ],
            "assets": {
                "layer_metadata": {
                    "href": f"{self.base_url}/{self.prefix}layer_metadata/layer_metadata.json",
                    "type": "application/json",
                    "title": "Layer Metadata",
                    "description": "Complete metadata for all data layers including visualization parameters, units, and source information",
                    "roles": ["metadata"]
                }
            },
            "links": [
                {
                    "rel": "self",
                    "href": f"{self.base_url}/{self.prefix}catalog.json",
                    "type": "application/json",
                    "title": "Data Catalog"
                }
            ],
            "conformsTo": [
                "https://api.stacspec.org/v1.0.0/core",
                "https://api.stacspec.org/v1.0.0/collections"
            ]
        }
        
        # Add top-level collections
        for collection_name, collection_info in self.directory_tree.items():
            catalog["links"].append({
                "rel": "child",
                "href": f"{self.base_url}/{self.prefix}{collection_info['path']}/collection.json",
                "type": "application/json",
                "title": collection_name.replace('_', ' ').title()
            })
        
        # Add items that don't have collections (less than 2 levels deep) directly to catalog
        for item in self.items:
            if len(item['path_parts']) < 2:
                catalog["links"].append({
                    "rel": "item",
                    "href": f"{self.base_url}/{self.prefix}{item['stac_dir']}/{item['name']}.json" if item['stac_dir'] else f"{self.base_url}/{self.prefix}{item['name']}.json",
                    "type": "application/json",
                    "title": item['name'].replace('_', ' ').title()
                })
        
        return catalog
    
    def generate_all_stac_items(self) -> List[Dict]:
        """Generate all STAC items for all data assets."""
        stac_items = []
        
        # Generate items for all discovered assets
        for item in self.items:
            stac_item = self.generate_stac_item(item)
            stac_items.append(stac_item)
            
        return stac_items
    
    def generate_all_collections(self) -> Dict[str, Dict]:
        """Generate all collections recursively."""
        collections = {}
        
        def process_directory(dir_info, parent_path=""):
            # Generate collections for all directories in the tree
            # (the tree building logic already ensures only appropriate directories are included)
            collection = self.generate_collection(dir_info, parent_path)
            collections[dir_info['path']] = collection
            
            # Process child directories
            for child_info in dir_info['children'].values():
                process_directory(child_info, dir_info['path'])
        
        # Process all top-level directories
        for collection_info in self.directory_tree.values():
            process_directory(collection_info)
        
        return collections


def clear_catalog_directory(catalog_dir: str = "catalog"):
    """Clear all JSON files from the catalog directory."""
    if not os.path.exists(catalog_dir):
        print(f"Catalog directory '{catalog_dir}' does not exist.")
        return
    
    json_files = []
    for root, dirs, files in os.walk(catalog_dir):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))
    
    if not json_files:
        print(f"No JSON files found in '{catalog_dir}' directory.")
        return
    
    print(f"Found {len(json_files)} JSON files to remove:")
    for file in json_files[:10]:  # Show first 10 files
        print(f"  - {file}")
    if len(json_files) > 10:
        print(f"  ... and {len(json_files) - 10} more files")
    
    # Ask for confirmation
    response = input("\nAre you sure you want to delete all these JSON files? (y/N): ")
    if response.lower() not in ['y', 'yes']:
        print("Operation cancelled.")
        return
    
    # Remove files
    removed_count = 0
    for file in json_files:
        try:
            os.remove(file)
            removed_count += 1
        except Exception as e:
            print(f"Error removing {file}: {e}")
    
    # Remove empty directories
    for root, dirs, files in os.walk(catalog_dir, topdown=False):
        if root != catalog_dir:  # Don't remove the main catalog directory
            try:
                if not os.listdir(root):  # Directory is empty
                    os.rmdir(root)
                    print(f"Removed empty directory: {root}")
            except Exception as e:
                print(f"Error removing directory {root}: {e}")
    
    print(f"\n✅ Successfully removed {removed_count} JSON files from catalog directory.")


def save_json(data: Dict, filepath: str):
    """Save data to JSON file with pretty formatting and STAC validation."""
    # Validate STAC structure if pystac is available
    if PYSTAC_AVAILABLE:
        try:
            validate_dict(data)
            print(f"✅ STAC validation passed for {filepath}")
        except Exception as e:
            print(f"❌ STAC validation failed for {filepath}: {e}")
            # Continue saving even if validation fails to allow inspection
    
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Saved: {filepath}")


def main():
    """Main function to crawl bucket and generate catalog with hierarchical structure."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Crawl a GCS bucket and generate a hierarchical STAC catalog.")
    parser.add_argument("--bucket", required=True, help="GCS bucket name (e.g., 'swhm_data')")
    parser.add_argument("--prefix", default="public/layers/", help="Path prefix within the bucket")
    parser.add_argument("--project", default=None, help="GCP project ID (optional)")
    parser.add_argument("--clear", action="store_true", help="Clear existing JSON files from catalog directory before generating")
    args = parser.parse_args()

    bucket_name = args.bucket
    prefix = args.prefix
    project_id = args.project
    
    # Clear existing catalog files if requested
    if args.clear:
        print("Clearing existing catalog files...")
        clear_catalog_directory()
        print()
    
    # Initialize crawler
    crawler = GCPBucketCrawler(bucket_name, prefix, project_id)
    
    # Crawl the bucket
    print("Starting bucket crawl...")
    crawl_data = crawler.crawl_bucket()
    
    print(f"Found {len(crawl_data['items'])} items in hierarchical structure")
    
    # Add bucket info to crawl data for generator
    crawl_data['bucket_name'] = bucket_name
    crawl_data['prefix'] = prefix
    
    # Generate collections and catalog
    generator = CatalogGenerator(crawl_data)
    
    # Generate all STAC items
    print("Generating STAC items...")
    stac_items = generator.generate_all_stac_items()
    
    # Generate all collections
    print("Generating collections...")
    collections = generator.generate_all_collections()
    
    # Generate master catalog
    master_catalog = generator.generate_master_catalog()
    
    # Save all files with hierarchical structure
    print("\nSaving catalog files...")
    
    # Save master catalog
    save_json(master_catalog, "catalog/catalog.json")
    
    # Save collections in their respective directories
    for collection_path, collection_data in collections.items():
        collection_file_path = f"catalog/{collection_path}/collection.json"
        save_json(collection_data, collection_file_path)
    
    # Save individual STAC items in their respective directories
    print("Saving individual STAC items...")
    
    for item in stac_items:
        # Find the corresponding item data to get the directory
        item_data = next((i for i in crawl_data['items'] if i['name'] == item['id']), None)
        if item_data:
            if item_data['stac_dir']:
                item_path = f"catalog/{item_data['stac_dir']}/{item['id']}.json"
            else:
                # Items without collections (less than 2 levels deep) go in catalog root
                item_path = f"catalog/{item['id']}.json"
            save_json(item, item_path)
    
    # Copy layer metadata file to catalog directory
    metadata_source = "../layer_metadata/layer_metadata.json"
    metadata_dest = "catalog/layer_metadata/layer_metadata.json"
    if os.path.exists(metadata_source):
        os.makedirs(os.path.dirname(metadata_dest), exist_ok=True)
        if metadata_source != metadata_dest:  # Only copy if different paths
            shutil.copy2(metadata_source, metadata_dest)
        print(f"Metadata file available at: {metadata_dest}")
    else:
        print(f"Warning: Layer metadata file not found at {metadata_source}")
    
    # Save summary with enhanced metadata
    summary = {
        "crawl_summary": {
            "bucket_name": bucket_name,
            "prefix": prefix,
            "crawl_time": datetime.now().isoformat(),
            "total_items": crawl_data['total_items'],
            "items_found": len(crawl_data['items']),
            "collections_generated": len(collections),
            "stac_items_generated": len(stac_items)
        },
        "discovered_items": crawl_data['items'],
        "directory_structure": crawl_data['directory_tree'],
        "stac_structure": {
            "catalog": "catalog/catalog.json",
            "collections": [f"catalog/{path}/collection.json" for path in collections.keys()],
            "items": [f"catalog/{item['stac_dir']}/{item['name']}.json" if item['stac_dir'] else f"catalog/{item['name']}.json" for item in crawl_data['items']]
        }
    }
    save_json(summary, "catalog/crawl_summary.json")
    
    print(f"\n✅ Catalog generation complete!")
    print(f"   - Master catalog: catalog/catalog.json")
    print(f"   - Collections: {len(collections)} collections in hierarchical structure")
    print(f"   - Items: {len(stac_items)} items in their respective directories")
    print(f"   - Crawl summary: catalog/crawl_summary.json")
    
    # Print directory structure summary
    print(f"\n📁 Generated directory structure:")
    print(f"   catalog/")
    print(f"   ├── catalog.json")
    print(f"   ├── crawl_summary.json")
    
    # Show directory tree structure
    def print_tree(tree, indent="   "):
        for name, info in tree.items():
            print(f"{indent}├── {name}/")
            print(f"{indent}│   ├── collection.json")
            if info['items']:
                for item in info['items']:
                    print(f"{indent}│   ├── {item['name']}.json")
            if info['children']:
                print_tree(info['children'], indent + "│   ")
    
    print_tree(crawl_data['directory_tree'])
    
    # Print next steps
    print(f"\n🚀 Next steps:")
    print(f"   1. Review generated files in catalog/ directory")
    print(f"   2. Run upload_stac_catalog.py to upload to GCS")
    

if __name__ == "__main__":
    main()


# python generate_stac_catalog.py --bucket swhm-stac-data --prefix "/" --project swhm-prod
# python generate_stac_catalog.py --bucket swhm-stac-data --prefix "/" --project swhm-prod --clear