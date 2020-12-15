# Standard Library
import os

# Third Party Libraries
from aiohttp import web

from git_cdn.app import app

if __name__ == "__main__":
    if not (os.getenv("GITSERVER_UPSTREAM") and os.getenv("WORKING_DIRECTORY")):
        print("please define GITSERVER_UPSTREAM and WORKING_DIRECTORY")
    else:
        web.run_app(app, port=8000)
