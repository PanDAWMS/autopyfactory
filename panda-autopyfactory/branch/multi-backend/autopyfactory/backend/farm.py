from backend.base import base

from backend.condorg import condorgBackend
from backend.torque import torqueBackend

backendSelector = {
                   condorg.backend : condorgBackend,
                   torque.backend : torqueBackend,
                   }