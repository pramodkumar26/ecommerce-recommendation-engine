import sys

import redis

KEY = "smoke:trending"


def main():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    if not r.ping():
        print("FAIL ping")
        return 1
    print("ping ok")

    r.delete(KEY)
    r.zadd(KEY, {"item-1": 5, "item-2": 9, "item-3": 2})
    top = r.zrevrange(KEY, 0, 0, withscores=True)
    print(f"top item: {top}")
    r.delete(KEY)

    mem = r.info("memory")
    print(f"used memory: {mem['used_memory_human']}, maxmemory: {mem['maxmemory_human']}")

    ok = top == [("item-2", 9.0)]
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
