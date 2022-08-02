# Allow core tortoise module to exists within package,
# while not having to import from "".tortoise.tortoise
from microservices.contrib.tortoise.consumer import *  # noqa: F401, F403
from microservices.contrib.tortoise.tortoise import *  # noqa: F401, F403
