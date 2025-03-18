from dynaconf import Dynaconf

settings = Dynaconf(
    settings_files=[
        "configs/default_settings.toml",
        "configs/settings.toml",
        "configs/.secrets.toml",
    ],
    environments=True,
    load_dotenv=True,
    envvar_prefix=False,
    env_switcher="ENV_FOR_DYNACONF",
    dotenv_path="./.env",
)
