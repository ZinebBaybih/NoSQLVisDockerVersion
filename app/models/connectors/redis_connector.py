# app/models/connectors/redis_connector.py
import redis
import json
import csv
from typing import List, Dict, Any, Optional
from collections import Counter


class RedisConnector:
    """
    Simple Redis connector wrapper used by the GUI.
    All returned keys/values are Python strings (decode_responses=True).
    """

    def __init__(self, host: str = "localhost", port: int = 6379, password: Optional[str] = None, db: int = 0, sample_limit: int = 10000):
        self.host = host
        self.port = int(port) if port is not None else 6379
        self.password = password
        self.db = int(db)
        self.sample_limit = sample_limit
        self.client: Optional[redis.Redis] = None

    def connect(self):
        """Establish Redis connection with text decoding enabled."""
        self.client = redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            decode_responses=True,  # IMPORTANT: returns strings instead of bytes
            socket_timeout=5
        )
        # Validate connection
        self.client.ping()

    def disconnect(self):
        self.client = None

    def list_databases(self) -> List[Dict[str, Any]]:
        """
        Return a list of logical DBs (0..15) with their sizes.
        Creating a short-lived client per DB to read dbsize safely.
        """
        dbs = []
        # Many Redis servers use 0..15, but this is configurable; we keep 0..15 as common default.
        for i in range(16):
            try:
                tmp = redis.Redis(host=self.host, port=self.port, db=i, password=self.password, decode_responses=True, socket_timeout=2)
                size = tmp.dbsize()
                dbs.append({"name": f"DB {i}", "index": i, "count": size})
            except Exception:
                dbs.append({"name": f"DB {i}", "index": i, "count": None})
        return dbs

    def list_collections(self, db_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Redis has no collections; return keys as 'collections' for compatibility.
        If db_name provided like 'DB 1', attempt to read that DB's keys (sample).
        """
        # If db_name indicates a DB index, switch; otherwise use current client/db.
        target_db = self.db
        if db_name and isinstance(db_name, str) and db_name.lower().startswith("db"):
            try:
                target_db = int(db_name.split()[1])
            except Exception:
                pass

        tmp = redis.Redis(host=self.host, port=self.port, db=target_db, password=self.password, decode_responses=True, socket_timeout=5)

        keys = []
        try:
            for k in tmp.scan_iter(match='*', count=100):
                keys.append({"name": k, "count": 1})
                if len(keys) >= 10000:
                    break
        except Exception:
            pass

        return keys

    def list_documents(self, db_name: Optional[str], col_name: str) -> List[Dict[str, Any]]:
        """
        Return a simple representation for a key (col_name).
        Uses get_key_value to produce a display-friendly structure.
        """
        if self.client is None:
            self.connect()

        try:
            # If the key doesn't exist, return empty list
            if not self.client.exists(col_name):
                return []

            kv = self.get_key_value(col_name)
            # Normalize to a list of dictionaries for UI consumption
            if kv.get("type") == "hash" and isinstance(kv.get("value"), dict):
                # return each hash as a single dict
                return [{col_name: kv.get("value")}]
            else:
                return [{col_name: kv.get("value")}]
        except Exception:
            return []


    def get_metadata(self, top_prefixes=None) -> Dict[str, Any]:
        """
        Return basic metadata: total_keys, sampled_keys, type_counts.
        Sampling limited by self.sample_limit to keep UI responsive.
        Also returns top key prefixes as a dict {prefix: count}.
        """
        if self.client is None:
            self.connect()

        try:
            total = self.client.dbsize()
        except Exception:
            total = None

        type_counts = {}
        sampled = 0
        prefix_counter = Counter()

        try:
            for key in self.client.scan_iter(match='*', count=100):
                if sampled >= self.sample_limit:
                    break

                # Count key types
                ktype = self.client.type(key)
                if isinstance(ktype, bytes):
                    ktype = ktype.decode()
                type_counts[ktype] = type_counts.get(ktype, 0) + 1

                # Count prefixes
                if isinstance(key, bytes):
                    key_str = key.decode()
                else:
                    key_str = key
                prefix = key_str.split(":", 1)[0] + ":" if ":" in key_str else key_str
                prefix_counter[prefix] += 1

                sampled += 1
        except Exception:
            pass

        # Additional Metadata
        info = self.client.info()
        keys = self.client.keys("*")

        # Count expiring & persistent keys
        expiring = 0
        persistent = 0
        for k in keys:
            ttl = self.client.ttl(k)
            if ttl > 0:
                expiring += 1
            else:
                persistent += 1

        # Return all prefixes if top_prefixes=None, otherwise top N
        if top_prefixes is None:
            top_prefix_dict = dict(prefix_counter)
        else:
            top_prefix_dict = dict(prefix_counter.most_common(top_prefixes))


        metadata = {
            "total_keys": total,
            "sampled_keys": sampled,
            "type_counts": type_counts,
            "info": info,
            "expiring_keys": expiring,
            "persistent_keys": persistent,
            "memory_used": info.get("used_memory", 0),
            "uptime_seconds": info.get("uptime_in_seconds", 0),
            "top_prefixes": top_prefix_dict
        }

        return metadata



    def list_keys(self, pattern: str = "*", limit: int = 10000) -> List[Dict[str, Any]]:
        """
        Return list of keys (string), type, ttl, and size in bytes. Decode_responses=True ensures strings.
        """
        if self.client is None:
            self.connect()

        results = []
        found = 0
        try:
            for key in self.client.scan_iter(match=pattern, count=100):
                if found >= limit:
                    break

                ktype = self.client.type(key)
                if isinstance(ktype, bytes):
                    ktype = ktype.decode()

                try:
                    ttl = self.client.ttl(key)
                except Exception:
                    ttl = None

                try:
                    size = self.client.memory_usage(key)  # fetch key size in bytes
                except Exception:
                    size = None

                results.append({"key": key, "type": ktype, "ttl": ttl, "size": size})
                found += 1
        except Exception as e:
            raise e

        return results


    def get_key_value(self, key: str, list_limit: int = 50) -> Dict[str, Any]:
        """
        Return the value and the Redis type for a given key.
        Structure: {"type": "<type>", "value": ...} or {"type":..., "error": ...}
        """
        if self.client is None:
            self.connect()

        try:
            ktype = self.client.type(key)
            if isinstance(ktype, bytes):
                ktype = ktype.decode()

            if ktype == "string":
                val = self.client.get(key)
                return {"type": "string", "value": val}
            elif ktype == "list":
                val = self.client.lrange(key, 0, list_limit - 1)
                return {"type": "list", "value": val}
            elif ktype == "set":
                # return up to 100 sample members
                try:
                    size = self.client.scard(key) or 0
                    n = min(100, size)
                    members = list(self.client.srandmember(key, number=n)) if n > 0 else []
                    return {"type": "set", "value": members}
                except Exception:
                    return {"type": "set", "value": []}
            elif ktype == "zset":
                val = self.client.zrange(key, 0, list_limit - 1, withscores=True)
                return {"type": "zset", "value": [{"member": m, "score": s} for m, s in val]}
            elif ktype == "hash":
                val = self.client.hgetall(key)
                return {"type": "hash", "value": val}
            elif ktype == "stream":
                try:
                    entries = self.client.xrange(key, count=list_limit)
                    return {"type": "stream", "value": [{"id": e[0], "fields": e[1]} for e in entries]}
                except Exception:
                    return {"type": "stream", "value": []}
            else:
                # fallback (unknown type)
                val = self.client.get(key)
                return {"type": ktype, "value": val}
        except Exception as e:
            return {"type": "error", "error": str(e)}

    def export_keys_to_csv(self, keys: List[str], filepath: str) -> bool:
        """
        Export selected keys to CSV. Value serialized as JSON string in 'value' column.
        """
        if self.client is None:
            self.connect()

        rows = []
        for k in keys:
            try:
                ttl = self.client.ttl(k)
            except Exception:
                ttl = None
            kv = self.get_key_value(k)
            val_serializable = kv.get("value")
            try:
                val_json = json.dumps(val_serializable, ensure_ascii=False)
            except Exception:
                val_json = str(val_serializable)
            rows.append({
                "key": k,
                "type": kv.get("type"),
                "ttl": ttl,
                "value": val_json
            })

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["key", "type", "ttl", "value"])
            writer.writeheader()
            writer.writerows(rows)

        return True
