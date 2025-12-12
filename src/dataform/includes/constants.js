/**
 * constants.js - Shared constants for the ETL pipeline
 * 
 * This file defines common values used across all SQLX files,
 * such as dataset names and project IDs.
 */

const PROJECT_ID = "sistema-dengue-clima";

const DATASETS = {
    BRONZE: "01_brz_raw_sistema_dengue",
    SILVER: "02_slv_sistema_dengue",
    GOLD: "03_gold_sistema_dengue",
    ASSERTIONS: "dataform_assertions"
};

module.exports = {
    PROJECT_ID,
    DATASETS
};
