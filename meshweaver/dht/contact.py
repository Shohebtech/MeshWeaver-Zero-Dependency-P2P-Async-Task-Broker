from dataclasses import dataclass

@dataclass(frozen=True)
class Contact:
    node_id: int
    host: str
    port: int

    @property
    def addr(self):
        return (self.host, self.port)

    def to_dict(self) -> dict:
        
        return {"id": format(self.node_id, "040x"), "host": self.host, "port": self.port}

    @classmethod
    def from_dict(cls, d: dict) -> "Contact":
        return cls(int(d["id"], 16), d["host"], d["port"])