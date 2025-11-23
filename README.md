# ER Harness Validation

A comprehensive validation framework designed to evaluate the accuracy and reliability of Entity Resolution (ER) outputs by comparing them against known ground truth datasets.

## 🎯 Overview

The ER Harness Validation framework provides automated validation of entity resolution results through:

- **MD5 Hash Comparison**: Fast 1:1 record matching using cryptographic hashes
- **Error Matrix Classification**: TP (True Positive), TN (True Negative), FP (False Positive), FN (False Negative)
- **Column-Level Analysis**: Detailed comparison of each individual columns
- **Token-Based Name Matching**: Smart handling of compound names (e.g., "Kirby-Jones" vs “Jones”)
- **Mismatch Pattern Analysis**: Identification and categorization of data discrepancies

## 💡 Objectives
- Evaluate the accuracy of ER outputs by comparing them with a truth dataset.
- Identify matches, mismatches, and extra/missing records.
- Generate a comprehensive validation report with mismatch diagnostics.

## 📁 Project Structure

```
er-harness-validation/
│
├── src/                          # Source code
│   ├── __init__.py
│   ├── core/                     # Core validation logic
│   │   ├── __init__.py
│   │   ├── md5_generator.py     # MD5 hash generation
│   │   ├── record_matcher.py    # Record matching logic
│   │   └── validation_analyzer.py # Mismatch analysis
│   │
│   └── utils/                    # Utility modules
│       ├── __init__.py
│       ├── data_loader.py       # Data loading utilities
│       └── data_transformer.py  # Data transformation helpers
│
├── config/                       # Configuration files
│   └── config.py                # Main configuration
│
├── tests/                        # Unit tests
│
├── docs/                         # Documentation
│
├── main.py                       # Main orchestration script
├── ER Harness.py                # Original Databricks notebook
├── setup.py                      # Package setup
├── requirements.txt              # Python dependencies
├── .gitignore                    # Git ignore rules
└── README.md                     # This file
```

## 🚀 Getting Started

### Configuration

Edit `config/config.py` to customize:

- **Database tables**: Set paths to your ER output and truth tables
- **Matching rules**: Adjust score thresholds for entity matching
- **Column mappings**: Define which columns to compare
- **Exception pairs**: Specify test cases with expected different results

Key configuration sections:

```python
# Database Configuration
DATABASE_CONFIG = {
    "er_output_table": "your.database.er_output_table",
    "truth_table": "your.database.truth_table",
}

# ER Matching Rules
ER_MATCHING_RULES = {
    "er_score_threshold": 0.8,
    "first_name_score": 0.12,
    # ... other thresholds
}
```

## 💻 Usage

### Basic Usage

```python
from main import ERHarnessValidator

# Initialize validator
validator = ERHarnessValidator()

# Run validation
results = validator.run_validation()

# Display results
validator.display_results()

# Save results
validator.save_results("/path/to/output", format="parquet")
```

## 📊 Output

The validation produces a comprehensive dataset with the following columns:

| Column | Description |
|--------|-------------|
| `source_primary_key` | Primary key from source data |
| `target_primary_key` | Primary key from target data |
| `target_entity_id` | Resolved entity identifier (WE_PID) |
| `validation_status` | Match or Mismatch |
| `error_matrix` | TP, TN, FP, or FN classification |
| `mismatch_type` | Detailed categorization of mismatches |
| `mismatches_code` | Binary code showing which columns mismatch |
| `mismatches_columns` | List of mismatched column names |
| `mismatch_deviation` | Percentage of columns that don't match |
| `batch_id` | Timestamp-based batch identifier |

### Mismatch Types

- **NA**: Perfect match
- **Only WE_PID mismatch**: Only entity ID differs
- **WE_PID and other column mismatch**: Entity ID and data fields differ
- **Other columns mismatch**: Data fields differ, entity ID matches
- **Extra Record in ER Output**: Record exists in ER but not in truth
- **Record Missing from ER Output**: Record exists in truth but not in ER

## 🏗️ Architecture

### Core Modules

1. **md5_generator.py**: Generates cryptographic hashes for fast comparison
2. **record_matcher.py**: Implements matching algorithms including token-based name matching
3. **validation_analyzer.py**: Analyzes mismatch patterns and categorizes results

### Utility Modules

1. **data_loader.py**: Handles data ingestion and preprocessing
2. **data_transformer.py**: Provides data transformation utilities

### Validation Workflow

```
1. Load Data → Load ER output and truth datasets
2. Generate Hashes → Create MD5 hashes for comparison
3. Match Records → Identify matches and mismatches
4. Find Extra Records → Detect records unique to each dataset
5. Analyze Patterns → Categorize and quantify mismatches
6. Generate Report → Create comprehensive validation output
```

---
