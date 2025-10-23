# Custard dbt Project

A clean dbt project for building effective data models step by step.

## Getting Started

1. Install dbt: `pip install dbt-duckdb`
2. Install dependencies: `dbt deps`
3. Run the project: `dbt run`

## Project Structure

- `models/` - dbt models (SQL files)
- `tests/` - dbt tests
- `macros/` - dbt macros
- `seeds/` - seed data files
- `snapshots/` - snapshot configurations

## Development Approach

This project follows a step-by-step approach to building effective models:

1. Start with simple, well-tested models
2. Build incrementally with clear dependencies
3. Focus on data quality and performance
4. Document and test each model thoroughly