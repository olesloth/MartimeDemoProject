from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    snowflake_account: str
    snowflake_user: str
    snowflake_password: str
    snowflake_warehouse: str = "MARITIME_WH"
    snowflake_database: str = "MARITIME_ANALYTICS"
    snowflake_role: str = "SYSADMIN"

    class Config:
        env_file = ".env"


settings = Settings()
