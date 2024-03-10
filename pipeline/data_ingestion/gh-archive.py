from dataclasses import dataclass

@dataclass
class Repo:
    id: int
    name: str
    url: str

@dataclass
class Actor:
    id: int
    login: str
    gravatar_id: str
    url: str
    avatar_url: str

@dataclass
class Org:
    id: int
    login: str
    gravatar_id: str
    url: str
    avatar_url: str

@dataclass
class Events:
    type: str
    public: bool
    payload: str
    repo: Repo
    actor: Actor
    org: Org
    created_at: str
    id: str
    other: str