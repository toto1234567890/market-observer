#!/usr/bin/env python
# coding:utf-8

config = None
logger = None


def get_config_logger(name, config=None):
    try: 
        # relative import
        from sys import path;path.extend("..")
        from _common.Helpers.helpers import init_logger

        if config is None:
            config=name
        config, logger = init_logger(name=name, config=config)

    except:
        import logging
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger("aiosqlite").setLevel(logging.WARNING)
        logger = logging.getLogger()

        from src.config.config import Config
        config = Config(config_path=config)
    return config, logger


