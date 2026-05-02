# Static Files Directory

This directory contains static files that are uploaded to S3 buckets via Terraform.

## Files

### `glue_deps/requirements.txt`

**Purpose:** Single source of truth for Glue job Python dependencies (e.g. defusedxml). The congress_bills fetcher job (Glue 5.0) installs from it via `--additional-python-modules s3://.../glue_deps/requirements.txt` and `--python-modules-installer-option -r`. Glue runs `pip -r` against the S3 file; a zip is not used because Glue does not extract it for pip. Add or remove packages here and run `terraform apply`.

**Terraform:** Uploaded as `glue_deps/requirements.txt` via `glue_scripts_s3` module `static_files`.

### `lists/congress-legislators.csv`

**Source:** `congress-legislators` GitHub repository  
**Repo URL:** `https://github.com/unitedstates/congress-legislators.git`

**Purpose:** Reference list of current U.S. legislators (House, Senate) with metadata including:
- Name, party, position, state/district
- Contact information and URLs (useful for future tile implementation)
- Social media handles
- Various ID mappings (Bioguide, FEC, etc.)

**Updating:**

To keep this file up to date, run:

```powershell
cd Cosine-Base-Infra
.\scripts\update-legislators-csv.ps1
```

The script will:
1. Download the CSV file directly from the congress-legislators gh-pages branch
2. Save it to `static-files/lists/congress-legislators.csv`

**After updating, apply Terraform:**
```powershell
cd terraform
terraform apply
```

**Terraform Upload:**
The file is automatically uploaded to S3 bucket `cosine-politician-trades-{env}` via the `politician_trades_s3` module's `static_files` parameter.

**Note:** The Lambda functions (`politician_trades_house_matcher` and `politician_trades_senate_matcher`) will read this file from S3 to match trades to politicians.
