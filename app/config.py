from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "retail_dw"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
