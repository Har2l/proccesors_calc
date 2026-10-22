# distributed md5 cracker - client side
import argparse
import hashlib
import json
import multiprocessing
import os
import socket
import uuid

NUMBER_LENGTH = 10   # 10 digit number, pad with leading zeros


def make_machine_id():
    # hostname + mac so every core on this pc reports the same id
    host = socket.gethostname()
    node = uuid.getnode()
    return f"{host}-{node:012x}"


def worker(host, port, target, machine_id, core_index):
    # this runs in its own process (one per core)
    target = target.lower()
    worker_id = f"{machine_id}#pid{os.getpid()}"

    try:
        sock = socket.create_connection((host, port))
    except OSError as exc:
        print(f"[core {core_index}] cant reach server {host}:{port} -> {exc}",
              flush=True)
        return

    reader = sock.makefile("r", encoding="utf-8")
    writer = sock.makefile("w", encoding="utf-8")

    def send(obj):
        writer.write(json.dumps(obj) + "\n")
        writer.flush()

    def recv():
        line = reader.readline()
        if not line:
            return None
        return json.loads(line)

    try:
        while True:
            # ask the server for a block
            send({"type": "get_block",
                  "machine_id": machine_id,
                  "worker_id": worker_id})
            msg = recv()

            if msg is None or msg.get("type") == "stop":
                break   # someone found it or no work left

            start = msg["start"]
            size = msg["size"]
            end = start + size - 1
            print(f"[{machine_id} core {core_index}] got block "
                  f"{start:0{NUMBER_LENGTH}d}-{end:0{NUMBER_LENGTH}d} "
                  f"({size:,} options)", flush=True)

            # try every number in the block
            found = None
            for n in range(start, start + size):
                candidate = f"{n:0{NUMBER_LENGTH}d}"
                if hashlib.md5(candidate.encode()).hexdigest() == target:
                    found = candidate
                    break

            if found is not None:
                print(f"[{machine_id} core {core_index}] got a match: {found}, "
                      f"telling server", flush=True)
                send({"type": "found",
                      "machine_id": machine_id,
                      "candidate": found})
                ack = recv()
                if ack and ack.get("type") == "found_ack" and ack.get("correct"):
                    print(f"[{machine_id} core {core_index}] server confirmed it",
                          flush=True)
                break
    except (ConnectionError, OSError):
        pass
    finally:
        try:
            sock.close()
        except OSError:
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")   # local by default
    parser.add_argument("--port", type=int, default=9009)
    parser.add_argument("--target", default="ec9c0f7edcc18a98b1f31853b1813301")
    parser.add_argument("--processes", type=int, default=None)
    args = parser.parse_args()

    machine_id = make_machine_id()
    # use all cores by default, so a 4 core pc asks for 4x the work
    cores = args.processes or multiprocessing.cpu_count()

    print(f"[client] machine id: {machine_id}", flush=True)
    print(f"[client] using {cores} cores", flush=True)
    print(f"[client] connecting to {args.host}:{args.port}\n", flush=True)

    # one process per core (threads dont run in parallel in python because of the GIL)
    procs = []
    for i in range(cores):
        p = multiprocessing.Process(
            target=worker,
            args=(args.host, args.port, args.target, machine_id, i),
        )
        p.start()
        procs.append(p)

    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        print("\n[client] stopping", flush=True)
        for p in procs:
            p.terminate()

    print("[client] done", flush=True)


if __name__ == "__main__":
    multiprocessing.freeze_support()   # needed for multiprocessing on windows
    main()
