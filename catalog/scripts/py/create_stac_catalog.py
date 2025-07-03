import os
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pystac
from google.cloud import storage
from rio_stac.stac import create_stac_item

# --- Configuration ---

# Google Cloud Project ID
GCS_PROJECT_ID = "swhm-prod"

# Google Cloud Storage bucket and prefix (folder) where the COG assets are located.
GCS_BUCKET_NAME = "swhm_data"
GCS_ASSET_PREFIX = "public/layers/raster/"

# The root URL where the final STAC catalog will be publicly accessible.
# This is used to create absolute HREFs in the generated STAC files.
# For GCS, this is typically: "https://storage.googleapis.com/{BUCKET_NAME}/{PATH_TO_CATALOG}"
CATALOG_ROOT_URL = f"https://storage.googleapis.com/{GCS_BUCKET_NAME}/{GCS_ASSET_PREFIX}"

# Details for the root STAC Catalog.
CATALOG_ID = "swhm-catalog"
CATALOG_TITLE = "Stormwater Heatmap Catalog"
CATALOG_DESCRIPTION = "A STAC catalog for Cloud-Optimized GeoTIFFs (COGs) stored in Google Cloud Storage."

# Details for the STAC Collection that will hold the raster items.
COLLECTION_ID = "raster"
COLLECTION_TITLE = "SWHM Raster Layers"
COLLECTION_DESCRIPTION = "A collection of raster data layers for the Stormwater Heatmap project."
COLLECTION_LICENSE = "CC-BY-4.0"
COLLECTION_KEYWORDS = ["COG", "stormwater", "heatmap", "geospatial"]

# --- End of Configuration ---


def list_cogs_in_bucket(
    storage_client: storage.Client, bucket_name: str, prefix: str
) -> list[storage.Blob]:
    """
    Lists all GeoTIFF files (.tif, .tiff) in a GCS bucket with a given prefix.

    Args:
        storage_client: An authenticated Google Cloud Storage client.
        bucket_name: The name of the GCS bucket.
        prefix: The prefix (folder path) to search within the bucket.

    Returns:
        A list of Blob objects representing the found COGs.
    """
    print(f"Fetching blobs from gs://{bucket_name}/{prefix}...")
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    cog_blobs = [
        blob
        for blob in blobs
        if blob.name.lower().endswith((".tif", ".tiff"))
    ]
    print(f"Found {len(cog_blobs)} COG files.")
    return cog_blobs


def create_stac_item_from_blob(blob: storage.Blob) -> pystac.Item:
    """
    Creates a STAC Item from a GCS Blob object representing a COG.

    Args:
        blob: The GCS Blob object.

    Returns:
        A PySTAC Item object.
    """
    item_id = Path(blob.name).stem
    public_url = blob.public_url
    acquisition_datetime = blob.time_created

    print(f"  Creating STAC Item for: {item_id}...")

    # These properties can be customized or extracted from file metadata
    item_properties = {
        'gsd': 10,  # Ground Sample Distance in meters (example)
        'platform': 'unknown',
        'constellation': 'unknown'
    }

    try:
        # rio-stac reads the file header to get projection and geometry info.
        # It can read directly from the GCS public URL.
        stac_item = create_stac_item(
            source=public_url,
            input_datetime=acquisition_datetime,
            id=item_id,
            asset_name="image",
            properties=item_properties,
            asset_href=public_url,
            asset_media_type=pystac.MediaType.COG,
            with_proj=True,
            with_raster=False,  # Set to True to add raster bands extension
        )
        return stac_item
    except Exception as e:
        print(f"    ERROR: Could not create STAC item for {blob.name}. Reason: {e}")
        return None


def upload_json_to_gcs(
    storage_client: storage.Client,
    bucket_name: str,
    destination_path: str,
    json_content: dict,
):
    """
    Uploads a dictionary as a JSON file to Google Cloud Storage.

    Args:
        storage_client: An authenticated Google Cloud Storage client.
        bucket_name: The name of the GCS bucket.
        destination_path: The full path (including filename) in the bucket.
        json_content: The dictionary to upload as JSON.
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_path)
    blob.upload_from_string(
        data=json.dumps(json_content, indent=2),
        content_type="application/json",
    )
    print(f"  Uploaded to: gs://{bucket_name}/{destination_path}")


def main():
    """
    Main function to generate and upload the STAC catalog.
    """
    # 1. Initialize Google Cloud Storage client
    print(f"Authenticating with Google Cloud (Project: {GCS_PROJECT_ID})...")
    try:
        storage_client = storage.Client(project=GCS_PROJECT_ID)
    except Exception as e:
        print(f"ERROR: Could not authenticate with GCS. Please check your credentials. {e}")
        return

    # 2. Create the root Catalog
    root_catalog = pystac.Catalog(
        id=CATALOG_ID,
        description=CATALOG_DESCRIPTION,
        title=CATALOG_TITLE,
    )

    # 3. Create the Collection for raster data
    # We define a broad extent first, which PySTAC can update later.
    initial_spatial_extent = pystac.SpatialExtent([[-180.0, -90.0, 180.0, 90.0]])
    initial_temporal_extent = pystac.TemporalExtent([[datetime.now(timezone.utc), None]])
    initial_extent = pystac.Extent(spatial=initial_spatial_extent, temporal=initial_temporal_extent)

    raster_collection = pystac.Collection(
        id=COLLECTION_ID,
        description=COLLECTION_DESCRIPTION,
        extent=initial_extent,
        license=COLLECTION_LICENSE,
        title=COLLECTION_TITLE,
        keywords=COLLECTION_KEYWORDS,
    )
    root_catalog.add_child(raster_collection)

    # 4. Find all COG assets and create STAC Items
    cog_blobs = list_cogs_in_bucket(storage_client, GCS_BUCKET_NAME, GCS_ASSET_PREFIX)
    if not cog_blobs:
        print("No COGs found. Exiting.")
        return

    print("\nProcessing COGs and creating STAC Items...")
    for blob in cog_blobs:
        item = create_stac_item_from_blob(blob)
        if item:
            raster_collection.add_item(item)
    print("Finished creating STAC Items.")

    # 5. Save catalog locally to a temporary directory to resolve HREFs
    # PySTAC needs to write files to disk to correctly structure the links.
    with tempfile.TemporaryDirectory() as tmp_dir:
        print(f"\nSaving catalog to temporary directory: {tmp_dir}")
        root_catalog.normalize_hrefs(CATALOG_ROOT_URL)
        root_catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED, dest_href=tmp_dir)
        print("Catalog saved locally. Now uploading to GCS...")

        # 6. Walk the temporary directory and upload all JSON files to GCS
        for root, _, files in os.walk(tmp_dir):
            for filename in files:
                if filename.lower().endswith(".json"):
                    local_path = Path(root) / filename
                    relative_path = local_path.relative_to(tmp_dir)
                    gcs_path = f"{GCS_ASSET_PREFIX}{relative_path}"

                    with open(local_path) as f:
                        json_data = json.load(f)

                    upload_json_to_gcs(
                        storage_client,
                        GCS_BUCKET_NAME,
                        gcs_path,
                        json_data,
                    )

    print("\n--- STAC Catalog Generation Complete ---")
    print(f"Root catalog should be available at: {CATALOG_ROOT_URL}/catalog.json")


if __name__ == "__main__":
    main()
