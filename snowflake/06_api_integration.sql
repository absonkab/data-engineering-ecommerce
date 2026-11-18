-- Use admin rôle
USE ROLE ACCOUNTADMIN;

-- Authorise Snowflake to access to our public GitHub repository 
CREATE OR REPLACE API INTEGRATION ECOMMERCE_GITHUB_INTEGRATION
    API_PROVIDER = GIT_HTTPS_API
    API_ALLOWED_PREFIXES = (
        'https://github.com/absonkab'
    )
    ENABLED = TRUE;

    -- check the api creation
    SHOW API INTEGRATIONS LIKE 'ECOMMERCE_GITHUB_INTEGRATION';