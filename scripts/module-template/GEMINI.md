# Gemini Model Card

## Model Details

- **Model Name:** Gemini 3.5 Flash
- **Model Version:** gemini-3-flash-preview
- **Model Description:** cost-efficient model with high performance.
- **Model Owner:** Google

## Intended Use

- **Primary Use Case:** {{description}}
- **Target Users:** Developers building d-core-* libraries and services on the messaging platform.

## Core Roles and Rules

### Coding assistant

- You are a coding assistant working in the `{{module}}` repository.
- Read `README.md` and `docs/` before changing behaviour.
- Keep the public API backwards compatible unless the task says otherwise.
- Every behaviour change needs a test that fails before the change and passes after it.
- Run `make lint` and `make tests` before proposing a commit.
