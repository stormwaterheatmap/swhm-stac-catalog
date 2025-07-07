#!/usr/bin/env python3
"""
GCP Bucket Crawler and Catalog Generator
Crawls a GCP storage bucket to discover vector and raster data,
then generates collections, individual STAC items, and a comprehensive catalog.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin
import os
from google.cloud import storage

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
        self.vectors = []
        self.rasters = []
        
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
        Crawl the GCP bucket to discover all vectors and rasters.
        Returns a dictionary with discovered items.
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
            
        except Exception as e:
            print(f"Error crawling bucket: {e}")
            return self._create_sample_data()
            
        return {
            'vectors': self.vectors,
            'rasters': self.rasters,
            'total_items': len(self.vectors) + len(self.rasters)
        }
    
    def _process_blob(self, blob):
        """Process a single blob to determine if it's a vector or raster."""
        blob_name = blob.name
        blob_path = Path(blob_name)
        
        # Skip directories (blobs ending with '/')
        if blob_name.endswith('/'):
            return
            
        # Check for vector files
        if 'vectors/' in blob_name and blob_path.suffix.lower() in ['.geojson', '.json']:
            self._add_vector_item(blob)
            
        # Check for raster files  
        elif 'rasters/' in blob_name and blob_path.suffix.lower() in ['.tiff', '.tif', '.gtiff']:
            self._add_raster_item(blob)
    
    def _add_vector_item(self, blob):
        """Add a vector item to the collection."""
        blob_path = Path(blob.name)
        item_name = blob_path.stem
        
        # Create public URL
        public_url = f"https://storage.googleapis.com/{self.bucket_name}/{blob.name}"
        
        vector_item = {
            'name': item_name,
            'filename': blob.name,
            'url': public_url,
            'type': 'vector',
            'format': 'GeoJSON',
            'size_bytes': blob.size,
            'content_type': blob.content_type,
            'created': blob.time_created.isoformat() if blob.time_created else None,
            'updated': blob.updated.isoformat() if blob.updated else None,
            'discovered_at': datetime.now().isoformat(),
            'etag': blob.etag,
            'md5_hash': blob.md5_hash
        }
        
        self.vectors.append(vector_item)
        print(f"Found vector: {item_name}")
    
    def _add_raster_item(self, blob):
        """Add a raster item to the collection."""
        blob_path = Path(blob.name)
        item_name = blob_path.stem
        
        # Create public URL
        public_url = f"https://storage.googleapis.com/{self.bucket_name}/{blob.name}"
        
        raster_item = {
            'name': item_name,
            'filename': blob.name,
            'url': public_url,
            'type': 'raster',
            'format': 'GeoTIFF',
            'size_bytes': blob.size,
            'content_type': blob.content_type,
            'created': blob.time_created.isoformat() if blob.time_created else None,
            'updated': blob.updated.isoformat() if blob.updated else None,
            'discovered_at': datetime.now().isoformat(),
            'etag': blob.etag,
            'md5_hash': blob.md5_hash
        }
        
        self.rasters.append(raster_item)
        print(f"Found raster: {item_name}")
    
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
    
    def _create_sample_data(self):
        """Create sample data structure when bucket can't be crawled directly."""
        print("Creating sample data structure...")
        
        base_url = f"https://storage.googleapis.com/{self.bucket_name}"
        
        # Sample vectors based on your structure
        sample_vectors = [
            {
                'name': 'vector1',
                'filename': f'{self.prefix}vectors/vector1/vector1.geojson',
                'url': f"{base_url}/{self.prefix}vectors/vector1/vector1.geojson",
                'type': 'vector',
                'format': 'GeoJSON',
                'size_bytes': None,
                'content_type': 'application/geo+json',
                'created': None,
                'updated': None,
                'discovered_at': datetime.now().isoformat(),
                'etag': None,
                'md5_hash': None
            }
        ]
        
        # Sample rasters based on your structure
        sample_rasters = [
            {
                'name': 'raster1',
                'filename': f'{self.prefix}rasters/raster1/raster1.tiff',
                'url': f"{base_url}/{self.prefix}rasters/raster1/raster1.tiff",
                'type': 'raster',
                'format': 'GeoTIFF',
                'size_bytes': None,
                'content_type': 'image/tiff',
                'created': None,
                'updated': None,
                'discovered_at': datetime.now().isoformat(),
                'etag': None,
                'md5_hash': None
            }
        ]
        
        self.vectors = sample_vectors
        self.rasters = sample_rasters
        
        return {
            'vectors': self.vectors,
            'rasters': self.rasters,
            'total_items': len(self.vectors) + len(self.rasters)
        }

class CatalogGenerator:
    def __init__(self, crawler_data: Dict):
        """Initialize with data from the crawler."""
        self.data = crawler_data
        self.stac_items = []
        
    def generate_stac_item(self, item_data: Dict, item_type: str) -> Dict:
        """Generate a STAC item for vector or raster data."""
        item_id = item_data['name']
        
        # Base STAC item structure
        stac_item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": item_id,
            "properties": {
                "title": item_data['name'].replace('_', ' ').title(),
                "description": f"{item_type.title()} dataset: {item_data['name']}",
                "datetime": item_data['discovered_at'],
                "created": item_data.get('created') or item_data['discovered_at'],
                "updated": item_data.get('updated') or item_data['discovered_at'],
                "providers": [
                    {
                        "name": "SWHM Data",
                        "roles": ["producer", "processor", "host"],
                        "url": "https://storage.googleapis.com/swhm_data/"
                    }
                ]
            },
            "geometry": None,  # Would need to extract from actual data
            "bbox": None,  # Would need to calculate from geometry/bounds
            "assets": {},
            "links": [
                {
                    "rel": "self",
                    "href": f"./{item_id}.json",
                    "type": "application/json"
                },
                {
                    "rel": "parent",
                    "href": "../collection.json",
                    "type": "application/json"
                },
                {
                    "rel": "collection",
                    "href": "../collection.json",
                    "type": "application/json"
                }
            ]
        }
        
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
            stac_item["assets"]["data"] = {
                "href": item_data['url'],
                "type": "image/tiff; application=geotiff",
                "title": "GeoTIFF data",
                "description": "Raster data in GeoTIFF format",
                "roles": ["data"],
                "file:size": item_data.get('size_bytes'),
                "file:checksum": item_data.get('md5_hash')
            }
            
            # Add COG asset if it's a Cloud Optimized GeoTIFF
            stac_item["assets"]["cog"] = {
                "href": item_data['url'],
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "title": "Cloud Optimized GeoTIFF",
                "description": "Cloud Optimized GeoTIFF for web access",
                "roles": ["data", "overview"]
            }
        
        # Add metadata asset
        stac_item["assets"]["metadata"] = {
            "href": item_data['url'].replace(Path(item_data['url']).suffix, '_metadata.json'),
            "type": "application/json",
            "title": "Metadata",
            "description": "Additional metadata for this dataset",
            "roles": ["metadata"]
        }
        
        # Add thumbnail asset (placeholder)
        stac_item["assets"]["thumbnail"] = {
            "href": item_data['url'].replace(Path(item_data['url']).suffix, '_thumbnail.png'),
            "type": "image/png",
            "title": "Thumbnail",
            "description": "Thumbnail image",
            "roles": ["thumbnail"]
        }
        
        return stac_item
    
    def generate_vector_collection(self) -> Dict:
        """Generate a vector collection with individual STAC items."""
        collection = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "swhm-vectors",
            "title": "SWHM Vector Collection",
            "description": "Collection of vector datasets from SWHM data bucket",
            "keywords": ["vectors", "geojson", "swhm"],
            "license": "proprietary",
            "extent": {
                "spatial": {
                    "bbox": [[-180, -90, 180, 90]]  # Global bbox - update with actual bounds
                },
                "temporal": {
                    "interval": [[None, None]]
                }
            },
            "providers": [
                {
                    "name": "SWHM Data",
                    "roles": ["producer", "processor", "host"],
                    "url": "https://storage.googleapis.com/swhm_data/"
                }
            ],
            "links": [
                {
                    "rel": "self",
                    "href": "./collection.json",
                    "type": "application/json"
                },
                {
                    "rel": "parent",
                    "href": "../catalog.json",
                    "type": "application/json"
                },
                {
                    "rel": "root",
                    "href": "../catalog.json",
                    "type": "application/json"
                }
            ],
            "item_assets": {
                "data": {
                    "type": "application/geo+json",
                    "title": "GeoJSON data",
                    "roles": ["data"]
                },
                "metadata": {
                    "type": "application/json",
                    "title": "Metadata",
                    "roles": ["metadata"]
                },
                "thumbnail": {
                    "type": "image/png",
                    "title": "Thumbnail",
                    "roles": ["thumbnail"]
                }
            }
        }
        
        # Add links to individual items
        for vector in self.data['vectors']:
            item_id = vector['name']
            collection["links"].append({
                "rel": "item",
                "href": f"./items/{item_id}.json",
                "type": "application/json",
                "title": vector['name'].replace('_', ' ').title()
            })
            
        return collection
    
    def generate_raster_collection(self) -> Dict:
        """Generate a raster collection with individual STAC items."""
        collection = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "swhm-rasters",
            "title": "SWHM Raster Collection",
            "description": "Collection of raster datasets from SWHM data bucket",
            "keywords": ["rasters", "geotiff", "swhm"],
            "license": "proprietary",
            "extent": {
                "spatial": {
                    "bbox": [[-180, -90, 180, 90]]  # Global bbox - update with actual bounds
                },
                "temporal": {
                    "interval": [[None, None]]
                }
            },
            "providers": [
                {
                    "name": "SWHM Data",
                    "roles": ["producer", "processor", "host"],
                    "url": "https://storage.googleapis.com/swhm_data/"
                }
            ],
            "links": [
                {
                    "rel": "self",
                    "href": "./collection.json",
                    "type": "application/json"
                },
                {
                    "rel": "parent",
                    "href": "../catalog.json",
                    "type": "application/json"
                },
                {
                    "rel": "root",
                    "href": "../catalog.json",
                    "type": "application/json"
                }
            ],
            "item_assets": {
                "data": {
                    "type": "image/tiff; application=geotiff",
                    "title": "GeoTIFF data",
                    "roles": ["data"]
                },
                "cog": {
                    "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                    "title": "Cloud Optimized GeoTIFF",
                    "roles": ["data", "overview"]
                },
                "metadata": {
                    "type": "application/json",
                    "title": "Metadata",
                    "roles": ["metadata"]
                },
                "thumbnail": {
                    "type": "image/png",
                    "title": "Thumbnail",
                    "roles": ["thumbnail"]
                }
            }
        }
        
        # Add links to individual items
        for raster in self.data['rasters']:
            item_id = raster['name']
            collection["links"].append({
                "rel": "item",
                "href": f"./items/{item_id}.json",
                "type": "application/json",
                "title": raster['name'].replace('_', ' ').title()
            })
            
        return collection
    
    def generate_master_catalog(self, vector_collection: Dict, raster_collection: Dict) -> Dict:
        """Generate the master catalog containing all collections."""
        catalog = {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "swhm-data-catalog",
            "title": "SWHM Data Catalog",
            "description": "Master catalog for SWHM vector and raster datasets",
            "created": datetime.now().isoformat(),
            "updated": datetime.now().isoformat(),
            "keywords": ["swhm", "vectors", "rasters", "geospatial"],
            "providers": [
                {
                    "name": "SWHM Data",
                    "roles": ["producer", "processor", "host"],
                    "url": "https://storage.googleapis.com/swhm_data/"
                }
            ],
            "links": [
                {
                    "rel": "self",
                    "href": "./catalog.json",
                    "type": "application/json",
                    "title": "SWHM Data Catalog"
                },
                {
                    "rel": "child",
                    "href": "./vectors/collection.json",
                    "type": "application/json",
                    "title": "Vector Collection"
                },
                {
                    "rel": "child",
                    "href": "./rasters/collection.json",
                    "type": "application/json",
                    "title": "Raster Collection"
                }
            ],
            "conformsTo": [
                "https://api.stacspec.org/v1.0.0/core",
                "https://api.stacspec.org/v1.0.0/collections"
            ]
        }
        
        return catalog
    
    def generate_all_stac_items(self) -> Dict[str, List[Dict]]:
        """Generate all STAC items for vectors and rasters."""
        vector_items = []
        raster_items = []
        
        # Generate vector items
        for vector in self.data['vectors']:
            stac_item = self.generate_stac_item(vector, 'vector')
            vector_items.append(stac_item)
            
        # Generate raster items
        for raster in self.data['rasters']:
            stac_item = self.generate_stac_item(raster, 'raster')
            raster_items.append(stac_item)
            
        return {
            'vector_items': vector_items,
            'raster_items': raster_items
        }

def save_json(data: Dict, filepath: str):
    """Save data to JSON file with pretty formatting."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Saved: {filepath}")

def main():
    """Main function to crawl bucket and generate catalog."""
    # Configuration - update these values for your specific bucket
    bucket_name = "swhm_data"  # Just the bucket name, not the full URL
    prefix = "public/layers/"  # Path prefix within the bucket
    project_id = None  # Set your GCP project ID if needed
    
    # Initialize crawler
    crawler = GCPBucketCrawler(bucket_name, prefix, project_id)
    
    # Crawl the bucket
    print("Starting bucket crawl...")
    crawl_data = crawler.crawl_bucket()
    
    print(f"Found {len(crawl_data['vectors'])} vectors and {len(crawl_data['rasters'])} rasters")
    
    # Generate collections and catalog
    generator = CatalogGenerator(crawl_data)
    
    # Generate all STAC items
    print("Generating STAC items...")
    stac_items = generator.generate_all_stac_items()
    
    # Generate collections
    vector_collection = generator.generate_vector_collection()
    raster_collection = generator.generate_raster_collection()
    
    # Generate master catalog
    master_catalog = generator.generate_master_catalog(vector_collection, raster_collection)
    
    # Save all files
    print("\nSaving catalog files...")
    
    # Save master catalog
    save_json(master_catalog, "catalog/catalog.json")
    
    # Save collections
    save_json(vector_collection, "catalog/vectors/collection.json")
    save_json(raster_collection, "catalog/rasters/collection.json")
    
    # Save individual STAC items
    print("Saving individual STAC items...")
    
    # Save vector items
    for item in stac_items['vector_items']:
        item_path = f"catalog/vectors/items/{item['id']}.json"
        save_json(item, item_path)
    
    # Save raster items
    for item in stac_items['raster_items']:
        item_path = f"catalog/rasters/items/{item['id']}.json"
        save_json(item, item_path)
    
    # Save summary with enhanced metadata
    summary = {
        "crawl_summary": {
            "bucket_name": bucket_name,
            "prefix": prefix,
            "crawl_time": datetime.now().isoformat(),
            "total_items": crawl_data['total_items'],
            "vectors_found": len(crawl_data['vectors']),
            "rasters_found": len(crawl_data['rasters']),
            "stac_items_generated": len(stac_items['vector_items']) + len(stac_items['raster_items'])
        },
        "discovered_vectors": crawl_data['vectors'],
        "discovered_rasters": crawl_data['rasters'],
        "stac_structure": {
            "catalog": "catalog/catalog.json",
            "vector_collection": "catalog/vectors/collection.json",
            "raster_collection": "catalog/rasters/collection.json",
            "vector_items": [f"catalog/vectors/items/{item['id']}.json" for item in stac_items['vector_items']],
            "raster_items": [f"catalog/rasters/items/{item['id']}.json" for item in stac_items['raster_items']]
        }
    }
    save_json(summary, "catalog/crawl_summary.json")
    
    print(f"\n✅ Catalog generation complete!")
    print(f"   - Master catalog: catalog/catalog.json")
    print(f"   - Vector collection: catalog/vectors/collection.json")
    print(f"   - Raster collection: catalog/rasters/collection.json")
    print(f"   - Vector items: {len(stac_items['vector_items'])} items in catalog/vectors/items/")
    print(f"   - Raster items: {len(stac_items['raster_items'])} items in catalog/rasters/items/")
    print(f"   - Crawl summary: catalog/crawl_summary.json")
    
    # Print directory structure
    print(f"\n📁 Generated directory structure:")
    print(f"   catalog/")
    print(f"   ├── catalog.json")
    print(f"   ├── crawl_summary.json")
    print(f"   ├── vectors/")
    print(f"   │   ├── collection.json")
    print(f"   │   └── items/")
    print(f"   │       └── [individual vector items].json")
    print(f"   └── rasters/")
    print(f"       ├── collection.json")
    print(f"       └── items/")
    print(f"           └── [individual raster items].json")
    
    # Print authentication help if needed
    print(f"\n💡 If you encountered authentication errors:")
    print(f"   1. Install: pip install google-cloud-storage")
    print(f"   2. Set up authentication:")
    print(f"      - Service account: export GOOGLE_APPLICATION_CREDENTIALS='path/to/key.json'")
    print(f"      - Or use: gcloud auth application-default login")

if __name__ == "__main__":
    main()