# Data Folder

This folder contains the sample dataset used for the Kasparro Agentic FB Analyst assignment.

## Files

- `synthetic_fb_ads_undergarments.csv`  
  Synthetic Meta Ads performance data for an undergarments brand. Includes:
  - `campaign_name`, `adset_name`, `date`
  - `spend`, `impressions`, `clicks`, `purchases`, `revenue`
  - `creative_type`, `creative_message`
  - `audience_type`, `platform`, `country`

## Usage

The main pipeline expects this path (configurable via `config/config.yaml`):

```yaml
data:
  path: data/synthetic_fb_ads_undergarments.csv
  sample_mode: true
  sample_frac: 0.5
